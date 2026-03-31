use crate::framework::DatabaseProcessor;
use kanau::processor::Processor;
use ocrch_sdk::objects::OrderStatus as SdkOrderStatus;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct OrderRecord {
    pub order_id: Uuid,
    pub merchant_order_id: String,
    pub amount: rust_decimal::Decimal,
    pub created_at: time::PrimitiveDateTime,
    pub status: OrderStatus,
    pub webhook_success_at: Option<time::PrimitiveDateTime>,
    pub webhook_url: String,
    pub webhook_retry_count: i32,
    pub webhook_last_tried_at: Option<time::PrimitiveDateTime>,
}

/// Order status for database operations.
///
/// This is the sqlx::Type version. For API/DTO use, see `ocrch_sdk::objects::OrderStatus`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, sqlx::Type)]
#[sqlx(rename_all = "lowercase", type_name = "order_status")]
pub enum OrderStatus {
    Pending,
    Paid,
    Expired,
    Cancelled,
}

impl From<OrderStatus> for SdkOrderStatus {
    fn from(value: OrderStatus) -> Self {
        match value {
            OrderStatus::Pending => SdkOrderStatus::Pending,
            OrderStatus::Paid => SdkOrderStatus::Paid,
            OrderStatus::Expired => SdkOrderStatus::Expired,
            OrderStatus::Cancelled => SdkOrderStatus::Cancelled,
        }
    }
}

impl From<SdkOrderStatus> for OrderStatus {
    fn from(value: SdkOrderStatus) -> Self {
        match value {
            SdkOrderStatus::Pending => OrderStatus::Pending,
            SdkOrderStatus::Paid => OrderStatus::Paid,
            SdkOrderStatus::Expired => OrderStatus::Expired,
            SdkOrderStatus::Cancelled => OrderStatus::Cancelled,
        }
    }
}

/// Data returned when fetching orders for webhook retry.
#[derive(Debug, Clone)]
pub struct OrderForWebhookRetry {
    pub order_id: Uuid,
    pub merchant_order_id: String,
    pub amount: rust_decimal::Decimal,
    pub status: OrderStatus,
    pub webhook_url: String,
    pub webhook_retry_count: i32,
}

#[derive(Debug, Clone)]
/// Get an order by its ID.
pub struct GetOrderRecordById {
    pub order_id: Uuid,
}

impl Processor<GetOrderRecordById> for DatabaseProcessor {
    type Output = Option<OrderRecord>;
    type Error = sqlx::Error;
    #[tracing::instrument(skip_all, err, name = "SQL:GetOrderRecordById")]
    async fn process(&self, query: GetOrderRecordById) -> Result<Option<OrderRecord>, sqlx::Error> {
        let order = sqlx::query_as!(
            OrderRecord,
            r#"
            SELECT 
                order_id,
                merchant_order_id,
                amount,
                created_at,
                status as "status: OrderStatus",
                webhook_success_at,
                webhook_url,
                webhook_retry_count,
                webhook_last_tried_at
            FROM order_records
            WHERE order_id = $1
            "#,
            query.order_id,
        )
        .fetch_optional(&self.pool)
        .await?;
        Ok(order)
    }
}

#[derive(Debug, Clone)]
/// Mark a webhook as successfully delivered.
pub struct MarkOrderWebhookSuccess {
    pub order_id: Uuid,
}

impl Processor<MarkOrderWebhookSuccess> for DatabaseProcessor {
    type Output = ();
    type Error = sqlx::Error;
    #[tracing::instrument(skip_all, err, name = "SQL:MarkOrderWebhookSuccess")]
    async fn process(&self, cmd: MarkOrderWebhookSuccess) -> Result<(), sqlx::Error> {
        sqlx::query!(
            r#"
            UPDATE order_records
            SET webhook_success_at = NOW()
            WHERE order_id = $1
            "#,
            cmd.order_id,
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
/// Increment the retry count for a failed webhook delivery.
pub struct IncrementOrderWebhookRetryCount {
    pub order_id: Uuid,
}

impl Processor<IncrementOrderWebhookRetryCount> for DatabaseProcessor {
    type Output = ();
    type Error = sqlx::Error;
    #[tracing::instrument(skip_all, err, name = "SQL:IncrementOrderWebhookRetryCount")]
    async fn process(&self, cmd: IncrementOrderWebhookRetryCount) -> Result<(), sqlx::Error> {
        sqlx::query!(
            r#"
            UPDATE order_records
            SET 
                webhook_retry_count = webhook_retry_count + 1,
                webhook_last_tried_at = NOW()
            WHERE order_id = $1
            "#,
            cmd.order_id,
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
/// Get orders that need webhook retry based on exponential backoff.
///
/// Returns orders that:
/// - Have a status change (not pending)
/// - Haven't been successfully delivered
/// - Haven't exceeded max retries
/// - Are due for retry based on exponential backoff (2^retry_count seconds)
pub struct GetOrdersForWebhookRetry {
    pub max_retry_count: i32,
    pub limit: i64,
}

impl Processor<GetOrdersForWebhookRetry> for DatabaseProcessor {
    type Output = Vec<OrderForWebhookRetry>;
    type Error = sqlx::Error;
    #[tracing::instrument(skip_all, err, name = "SQL:GetOrdersForWebhookRetry")]
    async fn process(
        &self,
        query: GetOrdersForWebhookRetry,
    ) -> Result<Vec<OrderForWebhookRetry>, sqlx::Error> {
        let orders = sqlx::query_as!(
            OrderForWebhookRetry,
            r#"
            SELECT 
                order_id,
                merchant_order_id,
                amount,
                status as "status: OrderStatus",
                webhook_url,
                webhook_retry_count
            FROM order_records
            WHERE webhook_success_at IS NULL
              AND status != 'pending'
              AND webhook_retry_count < $1
              AND (
                webhook_last_tried_at IS NULL
                OR webhook_last_tried_at + (POWER(2, webhook_retry_count) || ' seconds')::interval < NOW()
              )
            LIMIT $2
            "#,
            query.max_retry_count,
            query.limit,
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(orders)
    }
}

#[derive(Debug, Clone)]
/// Update the status of an order.
pub struct UpdateOrderStatus {
    pub order_id: Uuid,
    pub status: OrderStatus,
}

impl Processor<UpdateOrderStatus> for DatabaseProcessor {
    type Output = ();
    type Error = sqlx::Error;
    #[tracing::instrument(skip_all, err, name = "SQL:UpdateOrderStatus")]
    async fn process(&self, cmd: UpdateOrderStatus) -> Result<(), sqlx::Error> {
        sqlx::query!(
            r#"
            UPDATE order_records
            SET status = $1
            WHERE order_id = $2
            "#,
            cmd.status as OrderStatus,
            cmd.order_id,
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

/// Create a new order record.
///
/// Generates a new UUID for `order_id` and inserts the row with `status = 'pending'`.
/// Returns the complete newly-created record.
#[derive(Debug, Clone)]
pub struct CreateOrderRecord {
    pub merchant_order_id: String,
    pub amount: rust_decimal::Decimal,
    pub webhook_url: String,
}

impl Processor<CreateOrderRecord> for DatabaseProcessor {
    type Output = OrderRecord;
    type Error = sqlx::Error;
    #[tracing::instrument(skip_all, err, name = "SQL:CreateOrderRecord")]
    async fn process(&self, cmd: CreateOrderRecord) -> Result<OrderRecord, sqlx::Error> {
        let order_id = Uuid::now_v7();
        let order = sqlx::query_as!(
            OrderRecord,
            r#"
            INSERT INTO order_records (order_id, merchant_order_id, amount, webhook_url)
            VALUES ($1, $2, $3, $4)
            RETURNING
                order_id,
                merchant_order_id,
                amount,
                created_at,
                status as "status: OrderStatus",
                webhook_success_at,
                webhook_url,
                webhook_retry_count,
                webhook_last_tried_at
            "#,
            order_id,
            cmd.merchant_order_id,
            cmd.amount,
            cmd.webhook_url,
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(order)
    }
}

/// List order records with pagination and optional filters.
#[derive(Debug, Clone)]
pub struct ListOrderRecords {
    pub limit: i64,
    pub offset: i64,
    pub status: Option<OrderStatus>,
    pub merchant_order_id: Option<String>,
}

impl Processor<ListOrderRecords> for DatabaseProcessor {
    type Output = Vec<OrderRecord>;
    type Error = sqlx::Error;
    #[tracing::instrument(skip_all, err, name = "SQL:ListOrderRecords")]
    async fn process(&self, query: ListOrderRecords) -> Result<Vec<OrderRecord>, sqlx::Error> {
        sqlx::query_as!(
            OrderRecord,
            r#"
            SELECT
                order_id,
                merchant_order_id,
                amount,
                created_at,
                status as "status: OrderStatus",
                webhook_success_at,
                webhook_url,
                webhook_retry_count,
                webhook_last_tried_at
            FROM order_records
            WHERE ($1::order_status IS NULL OR status = $1)
              AND ($2::text IS NULL OR merchant_order_id = $2)
            ORDER BY created_at DESC
            LIMIT $3
            OFFSET $4
            "#,
            query.status as Option<OrderStatus>,
            query.merchant_order_id as Option<String>,
            query.limit,
            query.offset,
        )
        .fetch_all(&self.pool)
        .await
    }
}

impl OrderRecord {
    /// Update the status of an order within a transaction.
    pub async fn update_status_tx(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        order_id: Uuid,
        status: OrderStatus,
    ) -> Result<(), sqlx::Error> {
        sqlx::query!(
            r#"
            UPDATE order_records
            SET status = $1
            WHERE order_id = $2
            "#,
            status as OrderStatus,
            order_id,
        )
        .execute(&mut **tx)
        .await?;
        Ok(())
    }

    /// Update the status of multiple orders in a single query within a transaction.
    ///
    /// Uses `ANY` to batch-update all matching rows in one SQL statement.
    pub async fn update_status_many_tx(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        order_ids: &[Uuid],
        status: OrderStatus,
    ) -> Result<u64, sqlx::Error> {
        if order_ids.is_empty() {
            return Ok(0);
        }

        let result = sqlx::query(
            r#"
            UPDATE order_records
            SET status = $1
            WHERE order_id = ANY($2)
            "#,
        )
        .bind(status)
        .bind(order_ids)
        .execute(&mut **tx)
        .await?;
        Ok(result.rows_affected())
    }
}
