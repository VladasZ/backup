use std::path::Path;

use anyhow::{Context, Result};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{from_str, to_string};

pub const RUNS: TableDefinition<'static, &str, &str> = TableDefinition::new("runs");
pub const DELIVERIES: TableDefinition<'static, (&str, &str), &str> =
    TableDefinition::new("deliveries");
pub const SCHEDULES: TableDefinition<'static, &str, i64> = TableDefinition::new("schedules");
pub const RETRIES: TableDefinition<'static, &str, &str> = TableDefinition::new("retries");

pub struct Store {
    database: Database,
}

impl Store {
    pub fn open(path: &Path) -> Result<Self> {
        let database = Database::create(path)
            .with_context(|| format!("open state database {}", path.display()))?;
        let transaction = database.begin_write()?;
        transaction.open_table(RUNS)?;
        transaction.open_table(DELIVERIES)?;
        transaction.open_table(SCHEDULES)?;
        transaction.open_table(RETRIES)?;
        transaction.commit()?;
        Ok(Self { database })
    }

    pub fn read<T>(
        &self,
        operation: impl FnOnce(&redb::ReadTransaction) -> Result<T>,
    ) -> Result<T> {
        let transaction = self.database.begin_read()?;
        operation(&transaction)
    }

    pub fn write<T>(
        &self,
        operation: impl FnOnce(&redb::WriteTransaction) -> Result<T>,
    ) -> Result<T> {
        let transaction = self.database.begin_write()?;
        let value = operation(&transaction)?;
        transaction.commit()?;
        Ok(value)
    }
}

pub fn encode<T: Serialize>(value: &T) -> Result<String> {
    to_string(value).context("encode state record")
}

pub fn decode<T: DeserializeOwned>(value: &str) -> Result<T> {
    from_str(value).context("decode state record")
}

pub fn runs(transaction: &redb::ReadTransaction) -> Result<Vec<(String, String)>> {
    let table = transaction.open_table(RUNS)?;
    let mut rows = Vec::new();
    for entry in table.iter()? {
        let (key, value) = entry?;
        rows.push((key.value().to_owned(), value.value().to_owned()));
    }
    Ok(rows)
}

pub fn deliveries(transaction: &redb::ReadTransaction) -> Result<Vec<((String, String), String)>> {
    let table = transaction.open_table(DELIVERIES)?;
    let mut rows = Vec::new();
    for entry in table.iter()? {
        let (key, value) = entry?;
        let (run_id, destination) = key.value();
        rows.push((
            (run_id.to_owned(), destination.to_owned()),
            value.value().to_owned(),
        ));
    }
    Ok(rows)
}
