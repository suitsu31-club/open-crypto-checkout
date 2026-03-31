use crate::entities::StablecoinName;
use crate::framework::DatabaseProcessor;
use kanau::processor::Processor;

#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct Erc20PendingDeposit {
    pub id: i64,
    pub order: uuid::Uuid,
    pub token_name: StablecoinName,
    pub chain: EtherScanChain,
    pub user_address: Option<String>,
    pub wallet_address: String,
    pub value: rust_decimal::Decimal,
    pub started_at: time::PrimitiveDateTime,
    pub last_scanned_at: time::PrimitiveDateTime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, sqlx::Type)]
#[sqlx(rename_all = "lowercase", type_name = "etherscan_chain")]
/// https://docs.etherscan.io/supported-chains
pub enum EtherScanChain {
    Ethereum = 1,
    Polygon = 137,
    Base = 8453,
    ArbitrumOne = 42161,
    Linea = 59144,
    Optimism = 10,
    AvalancheC = 43114,
}

impl From<EtherScanChain> for ocrch_sdk::objects::blockchains::Blockchain {
    fn from(value: EtherScanChain) -> Self {
        match value {
            EtherScanChain::Ethereum => ocrch_sdk::objects::blockchains::Blockchain::Ethereum,
            EtherScanChain::Polygon => ocrch_sdk::objects::blockchains::Blockchain::Polygon,
            EtherScanChain::Base => ocrch_sdk::objects::blockchains::Blockchain::Base,
            EtherScanChain::ArbitrumOne => ocrch_sdk::objects::blockchains::Blockchain::ArbitrumOne,
            EtherScanChain::Linea => ocrch_sdk::objects::blockchains::Blockchain::Linea,
            EtherScanChain::Optimism => ocrch_sdk::objects::blockchains::Blockchain::Optimism,
            EtherScanChain::AvalancheC => ocrch_sdk::objects::blockchains::Blockchain::AvalancheC,
        }
    }
}

impl serde::Serialize for EtherScanChain {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&(*self as i64).to_string())
    }
}

impl<'de> serde::Deserialize<'de> for EtherScanChain {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let value: i64 = s.parse().map_err(serde::de::Error::custom)?;
        match value {
            1 => Ok(EtherScanChain::Ethereum),
            137 => Ok(EtherScanChain::Polygon),
            8453 => Ok(EtherScanChain::Base),
            42161 => Ok(EtherScanChain::ArbitrumOne),
            59144 => Ok(EtherScanChain::Linea),
            10 => Ok(EtherScanChain::Optimism),
            43114 => Ok(EtherScanChain::AvalancheC),
            _ => Err(serde::de::Error::unknown_variant(
                &s,
                &["1", "137", "8453", "42161", "59144", "10", "43114"],
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Erc20PendingDepositInsert {
    pub order: uuid::Uuid,
    pub token_name: StablecoinName,
    pub chain: EtherScanChain,
    pub user_address: Option<String>,
    pub wallet_address: String,
    pub value: rust_decimal::Decimal,
}

/// A pending deposit for matching operations.
#[derive(Debug, Clone)]
pub struct Erc20PendingDepositMatch {
    pub id: i64,
    pub order_id: uuid::Uuid,
    pub wallet_address: String,
    pub value: rust_decimal::Decimal,
    pub started_at_timestamp: i64,
}

impl Processor<Erc20PendingDepositInsert> for DatabaseProcessor {
    type Output = Erc20PendingDeposit;
    type Error = sqlx::Error;
    #[tracing::instrument(skip_all, err, name = "SQL:Erc20PendingDepositInsert")]
    async fn process(
        &self,
        insert: Erc20PendingDepositInsert,
    ) -> Result<Erc20PendingDeposit, sqlx::Error> {
        let deposit = sqlx::query_as!(
            Erc20PendingDeposit,
            r#"
            INSERT INTO erc20_pending_deposits ("order", token_name, chain, user_address, wallet_address, value)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING 
            id,
            "order",
            token_name as "token_name: StablecoinName",
            chain as "chain: EtherScanChain",
            user_address,
            wallet_address,
            value,
            started_at,
            last_scanned_at
            "#,
            insert.order,
            insert.token_name as StablecoinName,
            insert.chain as EtherScanChain,
            insert.user_address as Option<String>,
            insert.wallet_address as String,
            insert.value,
        )
            .fetch_one(&self.pool)
            .await?;
        Ok(deposit)
    }
}

#[derive(Debug, Clone)]
pub struct GetErc20DepositsForMatching {
    pub chain: EtherScanChain,
    pub token: StablecoinName,
}

impl Processor<GetErc20DepositsForMatching> for DatabaseProcessor {
    type Output = Vec<Erc20PendingDepositMatch>;
    type Error = sqlx::Error;
    #[tracing::instrument(skip_all, err, name = "SQL:GetErc20DepositsForMatching")]
    async fn process(
        &self,
        query: GetErc20DepositsForMatching,
    ) -> Result<Vec<Erc20PendingDepositMatch>, sqlx::Error> {
        let deposits = sqlx::query_as!(
            Erc20PendingDepositMatch,
            r#"
            SELECT 
                d.id,
                d."order" as order_id,
                d.wallet_address,
                d.value,
                EXTRACT(EPOCH FROM d.started_at)::bigint as "started_at_timestamp!"
            FROM erc20_pending_deposits d
            JOIN order_records o ON d."order" = o.order_id
            WHERE d.chain = $1 
              AND d.token_name = $2
              AND o.status = 'pending'
            "#,
            query.chain as EtherScanChain,
            query.token as StablecoinName,
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(deposits)
    }
}

/// List ERC-20 pending deposits with pagination and optional filters.
#[derive(Debug, Clone)]
pub struct ListErc20PendingDeposits {
    pub limit: i64,
    pub offset: i64,
    pub order_id: Option<uuid::Uuid>,
    pub chain: Option<EtherScanChain>,
    pub token: Option<StablecoinName>,
}

impl Processor<ListErc20PendingDeposits> for DatabaseProcessor {
    type Output = Vec<Erc20PendingDeposit>;
    type Error = sqlx::Error;
    #[tracing::instrument(skip_all, err, name = "SQL:ListErc20PendingDeposits")]
    async fn process(
        &self,
        query: ListErc20PendingDeposits,
    ) -> Result<Vec<Erc20PendingDeposit>, sqlx::Error> {
        sqlx::query_as!(
            Erc20PendingDeposit,
            r#"
            SELECT
                id,
                "order",
                token_name as "token_name: StablecoinName",
                chain as "chain: EtherScanChain",
                user_address,
                wallet_address,
                value,
                started_at,
                last_scanned_at
            FROM erc20_pending_deposits
            WHERE ($1::uuid IS NULL OR "order" = $1)
              AND ($2::etherscan_chain IS NULL OR chain = $2)
              AND ($3::stablecoin_name IS NULL OR token_name = $3)
            ORDER BY started_at DESC
            LIMIT $4
            OFFSET $5
            "#,
            query.order_id as Option<uuid::Uuid>,
            query.chain as Option<EtherScanChain>,
            query.token as Option<StablecoinName>,
            query.limit,
            query.offset,
        )
        .fetch_all(&self.pool)
        .await
    }
}

impl Erc20PendingDeposit {
    /// Delete all pending deposits for an order within a transaction.
    pub async fn delete_for_order_tx(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        order_id: uuid::Uuid,
    ) -> Result<(), sqlx::Error> {
        sqlx::query!(
            r#"
            DELETE FROM erc20_pending_deposits
            WHERE "order" = $1
            "#,
            order_id,
        )
        .execute(&mut **tx)
        .await?;
        Ok(())
    }

    /// Delete pending deposits for multiple orders, each keeping one (the matched one).
    ///
    /// Uses `UNNEST` to batch-delete in a single SQL statement.
    /// `order_ids[i]` and `except_ids[i]` are paired: for each order, the deposit
    /// with `except_ids[i]` is kept.
    pub async fn delete_for_orders_except_many_tx(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        order_ids: &[uuid::Uuid],
        except_ids: &[i64],
    ) -> Result<u64, sqlx::Error> {
        if order_ids.is_empty() {
            return Ok(0);
        }

        let result = sqlx::query(
            r#"
            DELETE FROM erc20_pending_deposits AS d
            USING UNNEST($1::uuid[], $2::bigint[]) AS u(order_id, except_id)
            WHERE d."order" = u.order_id AND d.id != u.except_id
            "#,
        )
        .bind(order_ids)
        .bind(except_ids)
        .execute(&mut **tx)
        .await?;
        Ok(result.rows_affected())
    }
}
