use crate::entities::StablecoinName;
use crate::framework::DatabaseProcessor;
use kanau::processor::Processor;

#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct Trc20PendingDeposit {
    pub id: i64,
    pub order: uuid::Uuid,
    pub token_name: StablecoinName,
    pub user_address: Option<String>,
    pub wallet_address: String,
    pub value: rust_decimal::Decimal,
    pub started_at: time::PrimitiveDateTime,
    pub last_scanned_at: time::PrimitiveDateTime,
}

/// A pending deposit for matching operations.
#[derive(Debug, Clone)]
pub struct Trc20PendingDepositMatch {
    pub id: i64,
    pub order_id: uuid::Uuid,
    pub wallet_address: String,
    pub value: rust_decimal::Decimal,
    pub started_at_timestamp: i64,
}

#[derive(Debug, Clone)]
/// Get pending deposits for matching with transfers.
pub struct GetTrc20DepositsForMatching {
    pub token: StablecoinName,
}

impl Processor<GetTrc20DepositsForMatching> for DatabaseProcessor {
    type Output = Vec<Trc20PendingDepositMatch>;
    type Error = sqlx::Error;
    #[tracing::instrument(skip_all, err, name = "SQL:GetTrc20DepositsForMatching")]
    async fn process(
        &self,
        query: GetTrc20DepositsForMatching,
    ) -> Result<Vec<Trc20PendingDepositMatch>, sqlx::Error> {
        let deposits = sqlx::query_as!(
            Trc20PendingDepositMatch,
            r#"
            SELECT 
                d.id,
                d."order" as order_id,
                d.wallet_address,
                d.value,
                EXTRACT(EPOCH FROM d.started_at)::bigint as "started_at_timestamp!"
            FROM trc20_pending_deposits d
            JOIN order_records o ON d."order" = o.order_id
            WHERE d.token_name = $1
              AND o.status = 'pending'
            "#,
            query.token as StablecoinName,
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(deposits)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Trc20PendingDepositInsert {
    pub order: uuid::Uuid,
    pub token_name: StablecoinName,
    pub user_address: Option<String>,
    pub wallet_address: String,
    pub value: rust_decimal::Decimal,
}

impl Processor<Trc20PendingDepositInsert> for DatabaseProcessor {
    type Output = Trc20PendingDeposit;
    type Error = sqlx::Error;
    #[tracing::instrument(skip_all, err, name = "SQL:Trc20PendingDepositInsert")]
    async fn process(
        &self,
        insert: Trc20PendingDepositInsert,
    ) -> Result<Trc20PendingDeposit, sqlx::Error> {
        let deposit = sqlx::query_as!(
            Trc20PendingDeposit,
            r#"
            INSERT INTO trc20_pending_deposits ("order", token_name, user_address, wallet_address, value)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING
            id,
            "order",
            token_name as "token_name: StablecoinName",
            user_address,
            wallet_address,
            value,
            started_at,
            last_scanned_at
            "#,
            insert.order,
            insert.token_name as StablecoinName,
            insert.user_address as Option<String>,
            insert.wallet_address as String,
            insert.value,
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(deposit)
    }
}

/// List TRC-20 pending deposits with pagination and optional filters.
#[derive(Debug, Clone)]
pub struct ListTrc20PendingDeposits {
    pub limit: i64,
    pub offset: i64,
    pub order_id: Option<uuid::Uuid>,
    pub token: Option<StablecoinName>,
}

impl Processor<ListTrc20PendingDeposits> for DatabaseProcessor {
    type Output = Vec<Trc20PendingDeposit>;
    type Error = sqlx::Error;
    #[tracing::instrument(skip_all, err, name = "SQL:ListTrc20PendingDeposits")]
    async fn process(
        &self,
        query: ListTrc20PendingDeposits,
    ) -> Result<Vec<Trc20PendingDeposit>, sqlx::Error> {
        sqlx::query_as!(
            Trc20PendingDeposit,
            r#"
            SELECT
                id,
                "order",
                token_name as "token_name: StablecoinName",
                user_address,
                wallet_address,
                value,
                started_at,
                last_scanned_at
            FROM trc20_pending_deposits
            WHERE ($1::uuid IS NULL OR "order" = $1)
              AND ($2::stablecoin_name IS NULL OR token_name = $2)
            ORDER BY started_at DESC
            LIMIT $3
            OFFSET $4
            "#,
            query.order_id as Option<uuid::Uuid>,
            query.token as Option<StablecoinName>,
            query.limit,
            query.offset,
        )
        .fetch_all(&self.pool)
        .await
    }
}

impl Trc20PendingDeposit {
    /// Delete pending deposits for an order except for one (the matched one), within a transaction.
    pub async fn delete_for_order_except_tx(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        order_id: uuid::Uuid,
        except_id: i64,
    ) -> Result<(), sqlx::Error> {
        sqlx::query!(
            r#"
            DELETE FROM trc20_pending_deposits
            WHERE "order" = $1 AND id != $2
            "#,
            order_id,
            except_id,
        )
        .execute(&mut **tx)
        .await?;
        Ok(())
    }

    /// Delete all pending deposits for an order within a transaction.
    pub async fn delete_for_order_tx(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        order_id: uuid::Uuid,
    ) -> Result<(), sqlx::Error> {
        sqlx::query!(
            r#"
            DELETE FROM trc20_pending_deposits
            WHERE "order" = $1
            "#,
            order_id,
        )
        .execute(&mut **tx)
        .await?;
        Ok(())
    }

    /// Delete all pending deposits for multiple orders in a single query.
    ///
    /// Uses `ANY` to batch-delete in one SQL statement.
    pub async fn delete_for_orders_many_tx(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        order_ids: &[uuid::Uuid],
    ) -> Result<u64, sqlx::Error> {
        if order_ids.is_empty() {
            return Ok(0);
        }

        let result = sqlx::query(
            r#"
            DELETE FROM trc20_pending_deposits
            WHERE "order" = ANY($1)
            "#,
        )
        .bind(order_ids)
        .execute(&mut **tx)
        .await?;
        Ok(result.rows_affected())
    }
}
