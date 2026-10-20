use crate::ApplicationError;
use nalarvo_domain::AuditRecord;
use nalarvo_persistence::m5::insert_audit_record_tx;
use sqlx::{Sqlite, SqlitePool, Transaction};

#[derive(Clone)]
pub struct SqliteAuditSink {
    pool: SqlitePool,
}

impl SqliteAuditSink {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn record(&self, record: &AuditRecord) -> Result<(), ApplicationError> {
        let mut tx = self.pool.begin().await?;
        self.record_tx(&mut tx, record).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn record_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        record: &AuditRecord,
    ) -> Result<(), ApplicationError> {
        insert_audit_record_tx(tx, record).await?;
        Ok(())
    }
}
