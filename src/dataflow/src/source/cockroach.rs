use std::collections::HashMap;
use std::ops::Deref;

use futures::stream::BoxStream;
use futures::{pin_mut, TryStreamExt};
use genawaiter::sync::{Co, Gen};
use itertools::Itertools;
use serde::Deserialize;
use serde_json::Value;
use tokio_postgres::types::Type as PgType;
use tokio_postgres::NoTls;

use crate::source::{SimpleSource, StreamItem, Timestamper};
use dataflow_types::CockroachSourceConnector;
use expr::parse_datum;
use repr::{Row, RowPacker, ScalarType};

/// Information required to sync data from Cockroach
pub struct CockroachSimpleSource {
    connector: CockroachSourceConnector,
    packer: RowPacker,
}

#[derive(Debug)]
struct PgColumn {
    scalar_type: PgType,
    _nullable: bool,
}

impl CockroachSimpleSource {
    /// Constructs a new instance
    pub fn new(connector: CockroachSourceConnector) -> Self {
        Self {
            connector,
            packer: RowPacker::new(),
        }
    }

    /// Uses a normal postgres connection (i.e not in replication mode) to get the column types of
    /// the remote table as well as the relation id that is used to filter the replication stream
    async fn table_info(&self) -> Vec<PgColumn> {
        let conninfo = &self.connector.conn;

        let (client, connection) = tokio_postgres::connect(&conninfo, NoTls).await.unwrap();

        // TODO communicate errors back into the stream
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        // Get the column type info
        // TODO: escape table name
        // TODO: verify column ordering
        // TOOD: maybe just use pg_catalog
        let col_types = client
            .query(
                format!(
                    "SELECT data_type, is_nullable
                  FROM [SHOW COLUMNS FROM {}]
                ",
                    self.connector.table,
                )
                .as_str(),
                &[],
            )
            .await
            .unwrap()
            .into_iter()
            .map(|row| PgColumn {
                scalar_type: match row.get::<_, String>(0).to_lowercase().as_str() {
                    "int8" => PgType::INT8,
                    _ => unreachable!(),
                },
                _nullable: row.get::<_, bool>(1),
            })
            .collect_vec();

        col_types
    }

    /// Converts this instance into a Generator. The generator encodes an async state machine that
    /// initially produces the initial snapshot of the table and then atomically switches to the
    /// replication stream starting where the snapshot left of
    async fn into_generator(mut self, timestamper: Timestamper, co: Co<StreamItem>) {
        let col_types = self.table_info().await;

        let conninfo = &self.connector.conn;
        let (client, connection) = tokio_postgres::connect(&conninfo, NoTls).await.unwrap();

        // TODO communicate errors back into the stream
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        // TODO: correctly escape table names.
        // TODO: remove unwrap
        let params: Vec<String> = vec![];
        let rows = client
            .query_raw(
                format!(
                    "EXPERIMENTAL CHANGEFEED FOR {}
                    WITH
                      updated,
                      resolved,
                      format = json
                  ;",
                    self.connector.table,
                )
                .as_str(),
                params,
            )
            .await
            .unwrap();
        pin_mut!(rows);

        let mut time_lease = Some(timestamper.lease().await);
        let mut current_values: HashMap<Vec<u8>, Row> = HashMap::new();
        // TODO: remove unwraps
        // TODO: remove assert
        while let Some(row) = rows.try_next().await.unwrap() {
            let value: JsonData = serde_json::from_slice(row.get(2)).unwrap();
            println!("VALUE {:?}", value);
            if let Some(_resolved) = value.resolved {
                // TODO: indicate closed timestamp
                // For now, just get a new timestamp.
                time_lease = None;
                time_lease = Some(timestamper.lease().await);
            } else if let Some(after) = value.after {
                assert_eq!(self.connector.table, row.get::<_, String>(0));
                let time = time_lease.as_deref().cloned().unwrap();
                // TODO: this assumes cockroach will serialize json keys identically. probably
                // need to verify this or do it ourselves.
                let key: Vec<u8> = row.get(1);
                let (diff, row) = match after {
                    Value::Null => (-1, current_values.remove(&key).unwrap()),
                    Value::Object(map) => {
                        self.packer.clear();
                        for (val, ty) in map.values().zip(col_types.iter()) {
                            println!("\t{:?}, {:?}", ty, val);
                            let val = val.to_string();
                            let datum = match ty.scalar_type {
                                PgType::BOOL => parse_datum(&val, ScalarType::Bool).unwrap(),
                                PgType::INT4 => parse_datum(&val, ScalarType::Int32).unwrap(),
                                PgType::INT8 => parse_datum(&val, ScalarType::Int64).unwrap(),
                                PgType::FLOAT4 => parse_datum(&val, ScalarType::Float32).unwrap(),
                                PgType::FLOAT8 => parse_datum(&val, ScalarType::Float64).unwrap(),
                                PgType::DATE => parse_datum(&val, ScalarType::Date).unwrap(),
                                PgType::TIME => parse_datum(&val, ScalarType::Time).unwrap(),
                                PgType::TIMESTAMP => {
                                    parse_datum(&val, ScalarType::Timestamp).unwrap()
                                }
                                PgType::TEXT => parse_datum(&val, ScalarType::String).unwrap(),
                                PgType::UUID => parse_datum(&val, ScalarType::Uuid).unwrap(),
                                ref other => todo!("Unsupported data type {:?}", other),
                            };
                            self.packer.push(datum);
                        }
                        let row = self.packer.finish_and_reuse();
                        if let Some(previous) = current_values.insert(key, row.clone()) {
                            co.yield_(Ok((previous, time, -1))).await;
                        }
                        (1, row)
                    }
                    _ => unreachable!(),
                };
                co.yield_(Ok((row, time, diff))).await;
            } else {
                unreachable!()
            }
        }
    }
}

impl SimpleSource for CockroachSimpleSource {
    fn into_stream(self, timestamper: Timestamper) -> BoxStream<'static, StreamItem> {
        Box::pin(Gen::new(move |co| self.into_generator(timestamper, co)))
    }
}

#[derive(Debug, Deserialize)]
struct JsonData {
    after: Option<Value>,
    resolved: Option<String>,
}
