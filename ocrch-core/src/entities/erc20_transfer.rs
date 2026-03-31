use crate::entities::erc20_pending_deposit::EtherScanChain;
use crate::entities::{StablecoinName, TransferStatus};
use crate::framework::DatabaseProcessor;
use kanau::processor::Processor;
use rust_decimal::Decimal;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct Erc20TokenTransfer {
    pub id: i64,
    pub token_name: StablecoinName,
    pub chain: EtherScanChain,
    pub from_address: String,
    pub to_address: String,
    pub txn_hash: String,
    pub value: rust_decimal::Decimal,
    pub block_number: i64,
    pub block_timestamp: i64,
    pub blockchain_confirmed: bool,
    pub created_at: time::PrimitiveDateTime,
    pub status: TransferStatus,
    pub fulfillment_id: Option<i64>,
}

/// Data for inserting a new ERC-20 transfer.
#[derive(Debug, Clone)]
pub struct Erc20TransferInsert {
    pub token_name: StablecoinName,
    pub chain: EtherScanChain,
    pub from_address: String,
    pub to_address: String,
    pub txn_hash: String,
    pub value: Decimal,
    pub block_number: i64,
    pub block_timestamp: i64,
}

/// An unmatched transfer for matching operations.
#[derive(Debug, Clone)]
pub struct Erc20UnmatchedTransfer {
    pub id: i64,
    pub to_address: String,
    pub value: Decimal,
    pub block_timestamp: i64,
}

/// Sync cursor from the erc20_sync_cursor materialized view.
/// Contains the block number to start syncing from.
#[derive(Debug, Clone)]
pub struct Erc20SyncCursor {
    pub chain: EtherScanChain,
    pub token_name: StablecoinName,
    /// The block number to start syncing from.
    /// This is either:
    /// - The earliest block of unconfirmed transfers within the last 1 day, or
    /// - The latest block number if all recent transfers are confirmed.
    pub cursor_block_number: i64,
    /// Whether there are unconfirmed transfers within the last 1 day.
    pub has_pending_confirmation: bool,
}

#[derive(Debug, Clone)]
/// Get the sync cursor from the materialized view for a chain-token pair.
///
/// The cursor implements the algorithm:
/// 1. If there are unconfirmed transfers within the last 1 day, return the earliest block number
/// 2. Otherwise, return the latest block number
/// 3. If no transfers exist, return None
pub struct GetErc20TokenTransSyncCursor {
    pub chain: EtherScanChain,
    pub token: StablecoinName,
}

impl Processor<GetErc20TokenTransSyncCursor> for DatabaseProcessor {
    type Output = Option<Erc20SyncCursor>;
    type Error = sqlx::Error;
    #[tracing::instrument(skip_all, err, name = "SQL:GetErc20TokenTransSyncCursor")]
    async fn process(
        &self,
        query: GetErc20TokenTransSyncCursor,
    ) -> Result<Option<Erc20SyncCursor>, sqlx::Error> {
        let cursor = sqlx::query_as!(
            Erc20SyncCursor,
            r#"
            SELECT
                chain as "chain!: EtherScanChain",
                token_name as "token_name!: StablecoinName",
                cursor_block_number as "cursor_block_number!",
                has_pending_confirmation as "has_pending_confirmation!"
            FROM erc20_sync_cursor
            WHERE chain = $1 AND token_name = $2
            "#,
            query.chain as EtherScanChain,
            query.token as StablecoinName,
        )
        .fetch_optional(&self.pool)
        .await?;
        Ok(cursor)
    }
}

#[derive(Debug, Clone)]
/// Insert multiple transfers in a single query.
///
/// Uses QueryBuilder for efficient bulk insert with ON CONFLICT DO NOTHING.
/// Returns the number of rows actually inserted (excluding duplicates).
pub struct InsertManyErc20TokenTransfers {
    pub transfers: Vec<Erc20TransferInsert>,
}

impl Processor<InsertManyErc20TokenTransfers> for DatabaseProcessor {
    type Output = u64;
    type Error = sqlx::Error;
    #[tracing::instrument(skip_all, err, name = "SQL:InsertManyErc20TokenTransfers")]
    async fn process(&self, insert: InsertManyErc20TokenTransfers) -> Result<u64, sqlx::Error> {
        if insert.transfers.is_empty() {
            return Ok(0);
        }

        let mut query_builder = sqlx::QueryBuilder::new(
            "INSERT INTO erc20_token_transfers \
            (token_name, chain, from_address, to_address, txn_hash, value, block_number, block_timestamp) ",
        );

        query_builder.push_values(insert.transfers, |mut b, transfer| {
            b.push_bind(transfer.token_name)
                .push_bind(transfer.chain)
                .push_bind(transfer.from_address)
                .push_bind(transfer.to_address)
                .push_bind(transfer.txn_hash)
                .push_bind(transfer.value)
                .push_bind(transfer.block_number)
                .push_bind(transfer.block_timestamp);
        });

        query_builder.push(" ON CONFLICT (txn_hash, chain) DO NOTHING");

        let result = query_builder.build().execute(&self.pool).await?;
        Ok(result.rows_affected())
    }
}

#[derive(Debug, Clone)]
/// Get unmatched transfers that are waiting for a deposit match.
pub struct GetErc20TokenTransfersUnmatched {
    pub chain: EtherScanChain,
    pub token: StablecoinName,
}

impl Processor<GetErc20TokenTransfersUnmatched> for DatabaseProcessor {
    type Output = Vec<Erc20UnmatchedTransfer>;
    type Error = sqlx::Error;
    #[tracing::instrument(skip_all, err, name = "SQL:GetErc20TokenTransfersUnmatched")]
    async fn process(
        &self,
        query: GetErc20TokenTransfersUnmatched,
    ) -> Result<Vec<Erc20UnmatchedTransfer>, sqlx::Error> {
        let GetErc20TokenTransfersUnmatched { chain, token } = query;
        let transfers = sqlx::query_as!(
            Erc20UnmatchedTransfer,
            r#"
            SELECT 
                id,
                to_address,
                value,
                block_timestamp
            FROM erc20_token_transfers
            WHERE chain = $1 
              AND token_name = $2
              AND status = 'waiting_for_match'
              AND blockchain_confirmed = true
            ORDER BY block_timestamp ASC
            "#,
            chain as EtherScanChain,
            token as StablecoinName,
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(transfers)
    }
}

#[derive(Debug, Clone)]
/// Get IDs of old unmatched transfers (older than 1 hour) for marking as unknown.
pub struct GetOldUnmatchedErc20TransferIds {
    pub chain: EtherScanChain,
    pub token: StablecoinName,
}

impl Processor<GetOldUnmatchedErc20TransferIds> for DatabaseProcessor {
    type Output = Vec<i64>;
    type Error = sqlx::Error;
    #[tracing::instrument(skip_all, err, name = "SQL:GetOldUnmatchedErc20TransferIds")]
    async fn process(
        &self,
        query: GetOldUnmatchedErc20TransferIds,
    ) -> Result<Vec<i64>, sqlx::Error> {
        let ids = sqlx::query_scalar!(
            r#"
            SELECT id
            FROM erc20_token_transfers
            WHERE chain = $1 
              AND token_name = $2
              AND status = 'waiting_for_match'
              AND blockchain_confirmed = true
              AND created_at < NOW() - INTERVAL '1 hour'
            "#,
            query.chain as EtherScanChain,
            query.token as StablecoinName,
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(ids)
    }
}

#[derive(Debug, Clone)]
/// Mark multiple transfers as having no matched deposit in a single query.
///
/// Returns the number of rows updated.
pub struct MarkErc20TransfersNoMatchedDeposit {
    pub transfer_ids: Vec<i64>,
}

impl Processor<MarkErc20TransfersNoMatchedDeposit> for DatabaseProcessor {
    type Output = u64;
    type Error = sqlx::Error;
    #[tracing::instrument(skip_all, err, name = "SQL:MarkErc20TransfersNoMatchedDeposit")]
    async fn process(&self, cmd: MarkErc20TransfersNoMatchedDeposit) -> Result<u64, sqlx::Error> {
        if cmd.transfer_ids.is_empty() {
            return Ok(0);
        }

        let result = sqlx::query!(
            r#"
            UPDATE erc20_token_transfers
            SET status = 'no_matched_deposit'
            WHERE id = ANY($1)
            "#,
            &cmd.transfer_ids,
        )
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }
}

#[derive(Debug, Clone)]
/// Handle matched ERC-20 transfers in a single transaction.
///
/// Executes 4 SQL statements atomically:
/// 1. Mark ERC-20 transfers as matched with their deposit (fulfillment) IDs
/// 2. Update order statuses to `Paid`
/// 3. Delete ERC-20 pending deposits for matched orders (keep the matched deposit)
/// 4. Delete TRC-20 pending deposits for matched orders (cross-chain cleanup)
pub struct HandleErc20MatchedTrans {
    pub transfer_ids: Vec<i64>,
    pub deposit_ids: Vec<i64>,
    pub order_ids: Vec<Uuid>,
}

impl Processor<HandleErc20MatchedTrans> for DatabaseProcessor {
    type Output = ();
    type Error = sqlx::Error;
    #[tracing::instrument(skip_all, err, name = "SQL-Transaction:HandleErc20MatchedTrans")]
    async fn process(&self, cmd: HandleErc20MatchedTrans) -> Result<(), sqlx::Error> {
        let mut tx = self.pool.begin().await?;

        // 1. Mark ERC-20 transfers as matched with their deposit (fulfillment) IDs
        sqlx::query!(
            r#"
            UPDATE erc20_token_transfers AS t
            SET status = 'matched', fulfillment_id = u.fulfillment_id
            FROM UNNEST($1::bigint[], $2::bigint[]) AS u(id, fulfillment_id)
            WHERE t.id = u.id
            "#,
            &cmd.transfer_ids,
            &cmd.deposit_ids,
        )
        .execute(&mut *tx)
        .await?;

        // 2. Update order statuses to Paid
        sqlx::query!(
            r#"
            UPDATE order_records
            SET status = 'paid'
            WHERE order_id = ANY($1)
            "#,
            &cmd.order_ids,
        )
        .execute(&mut *tx)
        .await?;

        // 3. Delete ERC-20 pending deposits for matched orders (keep the matched deposit)
        sqlx::query!(
            r#"
            DELETE FROM erc20_pending_deposits AS d
            USING UNNEST($1::uuid[], $2::bigint[]) AS u(order_id, except_id)
            WHERE d."order" = u.order_id AND d.id != u.except_id
            "#,
            &cmd.order_ids,
            &cmd.deposit_ids,
        )
        .execute(&mut *tx)
        .await?;

        // 4. Delete TRC-20 pending deposits for matched orders (cross-chain cleanup)
        sqlx::query!(
            r#"
            DELETE FROM trc20_pending_deposits
            WHERE "order" = ANY($1)
            "#,
            &cmd.order_ids,
        )
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(())
    }
}

/// List ERC-20 transfers by wallet address with pagination and optional filters.
#[derive(Debug, Clone)]
pub struct ListErc20TransfersByWallet {
    pub wallet_address: String,
    pub limit: i64,
    pub offset: i64,
    pub status: Option<TransferStatus>,
    pub chain: Option<EtherScanChain>,
    pub token: Option<StablecoinName>,
}

impl Processor<ListErc20TransfersByWallet> for DatabaseProcessor {
    type Output = Vec<Erc20TokenTransfer>;
    type Error = sqlx::Error;
    #[tracing::instrument(skip_all, err, name = "SQL:ListErc20TransfersByWallet")]
    async fn process(
        &self,
        query: ListErc20TransfersByWallet,
    ) -> Result<Vec<Erc20TokenTransfer>, sqlx::Error> {
        sqlx::query_as!(
            Erc20TokenTransfer,
            r#"
            SELECT
                id,
                token_name as "token_name: StablecoinName",
                chain as "chain: EtherScanChain",
                from_address,
                to_address,
                txn_hash,
                value,
                block_number,
                block_timestamp,
                blockchain_confirmed,
                created_at,
                status as "status: TransferStatus",
                fulfillment_id
            FROM erc20_token_transfers
            WHERE to_address = $1
              AND ($2::transfer_status IS NULL OR status = $2)
              AND ($3::etherscan_chain IS NULL OR chain = $3)
              AND ($4::stablecoin_name IS NULL OR token_name = $4)
            ORDER BY created_at DESC
            LIMIT $5
            OFFSET $6
            "#,
            query.wallet_address,
            query.status as Option<TransferStatus>,
            query.chain as Option<EtherScanChain>,
            query.token as Option<StablecoinName>,
            query.limit,
            query.offset,
        )
        .fetch_all(&self.pool)
        .await
    }
}

/// Get a single ERC-20 transfer by ID.
#[derive(Debug, Clone)]
pub struct GetErc20TransferById {
    pub id: i64,
}

impl Processor<GetErc20TransferById> for DatabaseProcessor {
    type Output = Option<Erc20TokenTransfer>;
    type Error = sqlx::Error;
    #[tracing::instrument(skip_all, err, name = "SQL:GetErc20TransferById")]
    async fn process(
        &self,
        query: GetErc20TransferById,
    ) -> Result<Option<Erc20TokenTransfer>, sqlx::Error> {
        sqlx::query_as!(
            Erc20TokenTransfer,
            r#"
            SELECT
                id,
                token_name as "token_name: StablecoinName",
                chain as "chain: EtherScanChain",
                from_address,
                to_address,
                txn_hash,
                value,
                block_number,
                block_timestamp,
                blockchain_confirmed,
                created_at,
                status as "status: TransferStatus",
                fulfillment_id
            FROM erc20_token_transfers
            WHERE id = $1
            "#,
            query.id,
        )
        .fetch_optional(&self.pool)
        .await
    }
}
