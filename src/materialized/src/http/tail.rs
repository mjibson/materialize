// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
use std::future::Future;

use anyhow::bail;
use futures::{FutureExt, SinkExt, TryFutureExt, TryStreamExt};
use headers::{Connection, HeaderMapExt, SecWebsocketAccept, SecWebsocketKey, Upgrade};
use hyper::{Body, Request, Response, StatusCode};
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::{tungstenite::protocol, WebSocketStream};

use coord::ExecuteResponse;
use sql::ast::{
    Expr, Ident, ObjectName, Statement, TailStatement, Value, WithOption, WithOptionValue,
};
use sql::plan::Params;

use crate::http::Server;

impl Server {
    pub fn handle_tail(
        &self,
        req: Request<Body>,
    ) -> impl Future<Output = anyhow::Result<Response<Body>>> {
        let mut coord_client = self.coord_client.clone();
        let res = async move {
            // Start with request processing. Errors here are the request's fault, so
            // 400s. We do this weird closure thing so that we can use ? inside it.
            let (key, stmt, with_desc) = match || -> anyhow::Result<_> {
                // Verify we have an upgradable websocket connection.
                let headers = req.headers();
                match headers.typed_get::<Connection>() {
                    Some(connection) if connection.contains("upgrade") => {}
                    _ => bail!("missing connection upgrade"),
                };
                match headers.get("upgrade") {
                    Some(upgrade) if upgrade == "websocket" => {}
                    _ => bail!("missing upgrade websocket"),
                }
                match headers.get("sec-websocket-version") {
                    Some(version) if version == "13" => {}
                    _ => bail!("unexpected websocket version"),
                }

                let key = headers.typed_get::<SecWebsocketKey>();

                // Generate the tail statement.
                let params = form_urlencoded::parse(req.uri().query().unwrap_or("").as_bytes());
                let mut options = vec![];
                let mut name: Option<String> = None;
                let mut as_of = None;
                let mut with_desc = false;
                for (key, value) in params {
                    let key: &str = &key;
                    match key.to_lowercase().as_str() {
                        "name" => {
                            name = Some(value.to_string());
                        }
                        "as_of" => {
                            as_of = Some(Expr::Value(Value::Boolean(value.parse()?)));
                        }
                        "with_desc" => {
                            with_desc = value.parse()?;
                        }
                        "snapshot" | "progress" => {
                            options.push(WithOption {
                                key: Ident::from(key),
                                value: Some(WithOptionValue::Value(Value::Boolean(value.parse()?))),
                            });
                        }
                        _ => bail!("unknown option: {}", key),
                    }
                }
                let name = match name {
                    Some(name) => ObjectName(name.split('.').map(Ident::from).collect::<Vec<_>>()),
                    None => bail!("no view specified"),
                };
                let stmt = Statement::Tail(TailStatement {
                    name,
                    options,
                    as_of,
                });
                Ok((key, stmt, with_desc))
            }() {
                Ok(ret) => ret,
                Err(err) => return mk_resp(StatusCode::BAD_REQUEST, err.to_string()),
            };
            let res = match coord_client.execute(stmt, Params::empty()).await {
                Ok(res) => res,
                Err(err) => return mk_resp(StatusCode::BAD_REQUEST, err.to_string()),
            };
            let desc = match res.desc {
                Some(desc) => desc,
                None => return mk_resp(StatusCode::INTERNAL_SERVER_ERROR, "expected description"),
            };
            let mut tail = match res.response {
                ExecuteResponse::Tailing { rx } => rx,
                _ => {
                    return mk_resp(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "unexpected ExecuteResponse",
                    )
                }
            };

            let ws_fut = req
                .into_body()
                //.on_upgrade()
                .and_then(move |upgraded| {
                    WebSocketStream::from_raw_socket(upgraded, protocol::Role::Server, None).map(Ok)
                })
                .and_then(move |mut ws| {
                    async move {
                        if with_desc {
                            let text = serde_json::to_string(&desc).unwrap();
                            let msg = protocol::Message::text(text);
                            if ws.send(msg).await.is_err() {
                                // If we can't send just abort without sending a Close.
                                return;
                            }
                        }
                        while let Some(batch) = tail.try_next().await.unwrap() {
                            for row in batch {
                                let values = pgrepr::values_from_row(row, desc.typ());
                                let values = values
                                    .into_iter()
                                    .map(|v| {
                                        v.map(|v| {
                                            let mut s: String = "".into();
                                            v.encode_text(&mut s);
                                            s
                                        })
                                    })
                                    .collect::<Vec<_>>();
                                let text = serde_json::to_string(&values).unwrap();
                                let msg = protocol::Message::text(text);
                                if ws.send(msg).await.is_err() {
                                    // If we can't send just abort without sending a Close.
                                    return;
                                }
                            }
                        }
                        let _ = ws
                            .send(protocol::Message::Close(Some(protocol::CloseFrame {
                                code: CloseCode::Normal,
                                reason: Cow::Borrowed("TAIL completed"),
                            })))
                            .await;
                    }
                    .map(Ok)
                });
            tokio::spawn(ws_fut);

            let mut res = Response::new(Body::empty());
            *res.status_mut() = StatusCode::SWITCHING_PROTOCOLS;
            res.headers_mut().typed_insert(Connection::upgrade());
            res.headers_mut().typed_insert(Upgrade::websocket());
            if let Some(key) = key {
                res.headers_mut()
                    .typed_insert(SecWebsocketAccept::from(key));
            }
            Ok(res)
        };
        res
    }
}

fn mk_resp<S: Into<String>>(code: StatusCode, msg: S) -> anyhow::Result<Response<Body>> {
    let s: String = msg.into();
    println!("RESP {}, {}", code, s);
    Ok(Response::builder().status(code).body(Body::from(s))?)
}
