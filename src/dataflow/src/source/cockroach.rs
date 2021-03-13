use std::collections::HashMap;

use async_trait::async_trait;
use futures::{pin_mut, TryStreamExt};
use serde::Deserialize;
use serde_json::Value;
use tokio_postgres::types::Type as PgType;
use tokio_postgres::NoTls;

use crate::source::{SimpleSource, SourceError, Timestamper};
use dataflow_types::CockroachSourceConnector;
use repr::{Datum, Row, RowArena, RowPacker};

/// Information required to sync data from Cockroach
pub struct CockroachSimpleSource {
    connector: CockroachSourceConnector,
    packer: RowPacker,
}

#[derive(Debug)]
struct PgColumn {
    scalar_type: PgType,
    nullable: bool,
}

/// Information about the remote table
struct TableInfo {
    /// The datatype and nullability of each column, in order
    schema: Vec<PgColumn>,
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
    async fn table_info(&self) -> Result<TableInfo, anyhow::Error> {
        let conninfo = &self.connector.conn;

        let (client, connection) = tokio_postgres::connect(&conninfo, NoTls).await.unwrap();
        tokio::spawn(connection);

        // Get the column type info
        // TODO: escape table name
        // TODO: verify column ordering
        // TOOD: maybe just use pg_catalog
        let schema = client
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
            .await?
            .into_iter()
            .map(|row| {
                Ok(PgColumn {
                    scalar_type: match row.get::<_, String>(0).to_lowercase().as_str() {
                        "int8" => PgType::INT8,
                        _ => unreachable!(),
                    },
                    nullable: row.get::<_, bool>(1),
                })
            })
            .collect::<Result<Vec<_>, anyhow::Error>>()?;

        Ok(TableInfo { schema })
    }

    async fn produce_replication(
        &mut self,
        timestamper: &Timestamper,
    ) -> Result<(), anyhow::Error> {
        let conninfo = &self.connector.conn;
        let (client, connection) = tokio_postgres::connect(&conninfo, NoTls).await?;
        tokio::spawn(connection);

        // TODO: correctly escape table names.
        // TODO: remove unwrap
        let params: Vec<String> = vec![];
        let rows = client
            .query_raw(
                format!(
                    "EXPERIMENTAL CHANGEFEED FOR {}
                    WITH
                      resolved,
                      format = json
                  ;",
                    self.connector.table,
                )
                .as_str(),
                params,
            )
            .await?;
        // TODO: is this needed? it's in the tokio_postgres docs for query_raw.
        pin_mut!(rows);

        // TODO: should we care about possible duplicate rows?

        let mut tx = None;
        let mut current_values: HashMap<Vec<u8>, Row> = HashMap::new();
        // TODO: remove unwraps
        // TODO: remove assert
        while let Some(row) = rows.try_next().await? {
            let value: JsonData = serde_json::from_slice(row.get(2))?;
            if let Some(_resolved) = value.resolved {
                tx = None;
            } else if let Some(after) = value.after {
                if tx.is_none() {
                    tx = Some(timestamper.start_tx().await);
                }
                let tx = tx.as_mut().unwrap();

                assert_eq!(self.connector.table, row.get::<_, String>(0));
                // TODO: this assumes cockroach will serialize json keys identically. probably
                // need to verify this or do it ourselves.
                let key: Vec<u8> = row.get(1);
                match after {
                    Value::Null => {
                        let previous = current_values.remove(&key).expect("expected key to exist");
                        tx.delete(previous).await?;
                    }
                    Value::Object(map) => {
                        self.packer.clear();
                        let arena = RowArena::new();
                        for (val, cast_expr) in map.values().zip(self.connector.cast_exprs.iter()) {
                            let val = val.to_string();
                            let txt_datum: Datum = val.as_str().into();
                            let datum = cast_expr.eval(&[txt_datum], &arena)?;
                            self.packer.push(datum);
                        }
                        let row = self.packer.finish_and_reuse();
                        if let Some(previous) = current_values.insert(key, row.clone()) {
                            tx.delete(previous).await?;
                        }
                        tx.insert(row).await?;
                    }
                    _ => unreachable!(),
                };
            } else {
                if tx.is_none() {
                    tx = Some(timestamper.start_tx().await);
                }
                let tx = tx.as_mut().unwrap();

                // For now, assume this means "after" was explicitly passed as null.
                let key: Vec<u8> = row.get(1);
                let previous = current_values.remove(&key).expect("expected key to exist");
                tx.delete(previous).await?;
            }
        }

        // TODO: return an error or retry here. check the connection for an error. will
        // need to remove the unwrap in the while above.
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
struct JsonData {
    after: Option<Value>,
    resolved: Option<String>,
}

#[async_trait]
impl SimpleSource for CockroachSimpleSource {
    /// The top-level control of the state machine and retry logic
    async fn start(mut self, timestamper: &Timestamper) -> Result<(), SourceError> {
        // TODO: retry on recoverable errors
        self.produce_replication(timestamper)
            .await
            .map_err(|e| SourceError::FileIO(e.to_string()))?;

        Ok(())
    }
}
