//! Open Crypto Checkout Server
//!
//! A headless cryptocurrency checkout counter for accepting stablecoin payments.

mod api;
mod config;
mod server;
mod shutdown;
mod state;

use clap::Parser;
use config::{ConfigLoader, get_database_url};
use ocrch_core::config::{ConfigStore, WalletConfig};
use ocrch_core::entities::StablecoinName;
use ocrch_core::entities::erc20_pending_deposit::EtherScanChain;
use ocrch_core::events::{
    BlockchainTarget, EventSenders, WebhookEvent, match_tick_channel,
    pending_deposit_changed_channel, pooling_tick_channel, webhook_event_channel,
};
use ocrch_core::framework::DatabaseProcessor;
use ocrch_core::processors::blockchain_sync::BlockchainSyncRunner;
use ocrch_core::processors::{
    Erc20BlockchainSync, OrderBookWatcher, PoolingKey, PoolingManager, PoolingManagerConfig,
    Trc20BlockchainSync, WebhookSender,
};
use ocrch_sdk::objects::blockchains::Blockchain;
use server::{build_router, run_server};
use shutdown::spawn_config_reload_handler;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use state::{AppState, OrderStatusUpdate};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{broadcast, watch};
use tokio::task::JoinHandle;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

/// Open Crypto Checkout - Headless cryptocurrency payment gateway
#[derive(Parser, Debug)]
#[command(name = "ocrch-server")]
#[command(version, about, long_about = None)]
struct Args {
    /// Path to the configuration file
    #[arg(short, long, default_value = "./ocrch-config.toml")]
    config: PathBuf,

    /// Override the listen address (e.g., 0.0.0.0:3000)
    #[arg(short, long)]
    listen: Option<SocketAddr>,

    /// Run database migrations on startup
    #[arg(long, default_value = "false")]
    migrate: bool,
}

/// The result of setting up the event pipeline.
struct EventPipeline {
    /// Event senders for API handlers to emit events.
    event_senders: EventSenders,
    /// Broadcast sender for order status changes (consumed by WebSocket handlers).
    order_status_tx: broadcast::Sender<OrderStatusUpdate>,
    /// Shutdown signal sender -- set to `true` to stop all processors.
    shutdown_tx: watch::Sender<bool>,
    /// Join handles for all spawned processor tasks.
    join_handles: Vec<JoinHandle<()>>,
    /// Config store for PoolingManager (needed for config reload).
    pooling_config_store: ConfigStore<PoolingManagerConfig>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    init_tracing();

    // Parse command line arguments
    let args = Args::parse();

    tracing::info!("Starting ocrch-server v{}", env!("CARGO_PKG_VERSION"));

    // Load configuration
    let config_loader = Arc::new(ConfigLoader::new(&args.config, args.listen));
    let loaded_config = config_loader.load().map_err(|e| {
        tracing::error!("Failed to load configuration: {}", e);
        e
    })?;

    let listen_addr = loaded_config.server.listen;
    tracing::info!("Configuration loaded from {:?}", args.config);

    // Convert to shared config with separate locks for each section
    let shared_config = loaded_config.into_shared();

    // Get database URL from environment
    let database_url = get_database_url().inspect_err(|e| {
        tracing::error!("DATABASE_URL environment variable not set: {e}");
    })?;

    // Create database connection pool
    tracing::info!("Connecting to database...");
    let db_pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&database_url)
        .await
        .map_err(|e| {
            tracing::error!("Failed to connect to database: {}", e);
            e
        })?;
    tracing::info!("Database connection established");

    // Run migrations if requested
    if args.migrate {
        tracing::info!("Running database migrations...");
        sqlx::migrate!("../migrations")
            .run(&db_pool)
            .await
            .map_err(|e| {
                tracing::error!("Failed to run migrations: {}", e);
                e
            })?;
        tracing::info!("Migrations completed successfully");
    }

    // Set up the event pipeline (channels + processors)
    let pipeline = setup_event_pipeline(&shared_config, &db_pool).await;

    // Create application state
    let state = AppState::new(
        db_pool.clone(),
        shared_config,
        pipeline.event_senders,
        pipeline.order_status_tx,
    );

    // Spawn config reload handler (listens for SIGHUP)
    let shutdown_notify =
        spawn_config_reload_handler(state.clone(), config_loader, pipeline.pooling_config_store);

    // Build the router
    let router = build_router(state);

    // Run the server
    tracing::info!("Starting HTTP server on {}", listen_addr);
    let result = run_server(router, listen_addr).await;

    // --- Graceful shutdown sequence ---

    // 1. Signal all processors to stop
    tracing::info!("Signaling processors to shut down...");
    let _ = pipeline.shutdown_tx.send(true);

    // 2. Signal the config reload handler to stop
    shutdown_notify.notify_one();

    // 3. Wait for all processor tasks to finish draining
    for handle in pipeline.join_handles {
        let _ = handle.await;
    }
    tracing::info!("All processors shut down");

    // 4. Close database connections gracefully
    tracing::info!("Closing database connections...");
    db_pool.close().await;
    tracing::info!("Server shutdown complete");

    result.map_err(Into::into)
}

/// Set up the event-driven processing pipeline.
///
/// Creates all event channels, constructs processor instances from the current
/// configuration, and spawns them as background tasks.
///
/// # Event flow
///
/// ```text
/// PendingDepositChanged -> PoolingManager
/// PoolingManager -> PoolingTick -> BlockchainSyncRunner (one per wallet+coin)
/// BlockchainSyncRunner -> MatchTick -> OrderBookWatcher
/// OrderBookWatcher -> WebhookEvent -> WebhookSender
/// ```
async fn setup_event_pipeline(
    config: &ocrch_core::config::SharedConfig,
    db_pool: &PgPool,
) -> EventPipeline {
    // -- Shutdown channel (shared by all processors) -----------------------
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // -- Global event channels ---------------------------------------------
    let (pdc_tx, pdc_rx) = pending_deposit_changed_channel();
    let (match_tx, match_rx) = match_tick_channel();
    let (webhook_tx, webhook_rx) = webhook_event_channel();

    // -- Build per-wallet-coin PoolingTick channels & sync runners ---------
    let wallets = config.wallets.read().await;
    let api_keys = config.api_keys.read().await;

    let mut tick_senders = Vec::new();
    let mut join_handles: Vec<JoinHandle<()>> = Vec::new();

    for wallet in wallets.iter() {
        for coin in &wallet.enabled_coins {
            let (tick_tx, tick_rx) = pooling_tick_channel();
            let blockchain_target = blockchain_to_target(wallet.blockchain);
            let token: StablecoinName = (*coin).into();
            let key = PoolingKey::new(blockchain_target, token);

            tick_senders.push((key, tick_tx));

            // Spawn the appropriate BlockchainSyncRunner
            let handle = spawn_sync_runner(
                wallet,
                token,
                &api_keys.etherscan_api_key,
                &api_keys.tronscan_api_key,
                db_pool.clone(),
                shutdown_rx.clone(),
                tick_rx,
                match_tx.clone(),
            );
            join_handles.push(handle);
        }
    }

    drop(wallets);
    drop(api_keys);

    tracing::info!(
        sync_runners = join_handles.len(),
        "Spawned BlockchainSync runners"
    );

    // -- PoolingManager ----------------------------------------------------
    let pooling_config = PoolingManagerConfig { tick_senders };
    let pooling_config_store = ConfigStore::new(pooling_config);
    let config_watcher = pooling_config_store.subscribe();

    let pm_shutdown_rx = shutdown_rx.clone();
    let pm_config_store = pooling_config_store.clone();
    let pm_handle = tokio::spawn(async move {
        PoolingManager::new()
            .run(pm_shutdown_rx, pdc_rx, pm_config_store, config_watcher)
            .await;
    });
    join_handles.push(pm_handle);

    // -- Order status broadcast channel (consumed by WebSocket handlers) ----
    let (order_status_tx, _order_status_rx) = broadcast::channel::<OrderStatusUpdate>(256);

    // -- OrderBookWatcher --------------------------------------------------
    //
    // The watcher writes to an intermediate channel (`obw_tx`). A fan-out
    // task reads from it, broadcasts `OrderStatusChanged` events to
    // WebSocket clients, and forwards every event to the real `webhook_tx`.
    let (obw_tx, mut obw_rx) = webhook_event_channel();

    let obw_shutdown_rx = shutdown_rx.clone();
    let obw_pool = db_pool.clone();
    let obw_handle = tokio::spawn(async move {
        let watcher = OrderBookWatcher {
            processor: DatabaseProcessor { pool: obw_pool },
        };
        watcher.run(obw_shutdown_rx, match_rx, obw_tx).await;
    });
    join_handles.push(obw_handle);

    // -- Fan-out interceptor -----------------------------------------------
    let fanout_broadcast_tx = order_status_tx.clone();
    let fanout_webhook_tx = webhook_tx.clone();
    let fanout_handle = tokio::spawn(async move {
        while let Some(event) = obw_rx.recv().await {
            // Broadcast order-status changes to WebSocket clients
            if let WebhookEvent::OrderStatusChanged { order_id, .. } = &event {
                let _ = fanout_broadcast_tx.send(OrderStatusUpdate {
                    order_id: *order_id,
                });
            }
            // Forward every event to the real WebhookSender
            if let Err(e) = fanout_webhook_tx.send(event).await {
                tracing::warn!(error = %e, "Fan-out: failed to forward event to WebhookSender");
            }
        }
        tracing::info!("Fan-out interceptor shut down");
    });
    join_handles.push(fanout_handle);

    // -- WebhookSender -----------------------------------------------------
    let ws_shutdown_rx = shutdown_rx.clone();
    let ws_pool = db_pool.clone();
    let ws_config = config.clone();
    let ws_handle = tokio::spawn(async move {
        let sender = WebhookSender::new(DatabaseProcessor { pool: ws_pool }, ws_config);
        sender.run(ws_shutdown_rx, webhook_rx).await;
    });
    join_handles.push(ws_handle);

    // -- Build EventSenders for use by API handlers ------------------------
    let event_senders = EventSenders {
        pending_deposit_changed: pdc_tx,
        match_tick: match_tx,
        webhook_event: webhook_tx,
    };

    EventPipeline {
        event_senders,
        order_status_tx,
        shutdown_tx,
        join_handles,
        pooling_config_store,
    }
}

/// Spawn a `BlockchainSyncRunner` for a specific wallet + coin pair.
#[allow(clippy::too_many_arguments)]
fn spawn_sync_runner(
    wallet: &WalletConfig,
    token: StablecoinName,
    etherscan_api_key: &str,
    tronscan_api_key: &str,
    pool: PgPool,
    shutdown_rx: watch::Receiver<bool>,
    tick_rx: ocrch_core::events::PoolingTickReceiver,
    match_tx: ocrch_core::events::MatchTickSender,
) -> JoinHandle<()> {
    match wallet.blockchain {
        Blockchain::Tron => {
            let sdk_coin: ocrch_sdk::objects::Stablecoin = token.into();
            let contract_address = sdk_coin
                .get_data()
                .get_contract_address(Blockchain::Tron)
                .expect("TRC-20 contract address must exist for enabled coin")
                .to_string();

            let sync = Trc20BlockchainSync::new(
                token,
                wallet.address.clone(),
                contract_address,
                wallet.starting_tx.clone(),
                tronscan_api_key.to_string(),
            );
            let runner = BlockchainSyncRunner::new(sync, pool);
            tokio::spawn(async move {
                runner.run(shutdown_rx, tick_rx, match_tx).await;
            })
        }
        other => {
            // All non-Tron blockchains are ERC-20 compatible
            let chain = blockchain_to_etherscan_chain(other);
            let sync = Erc20BlockchainSync::new(
                chain,
                token,
                wallet.address.clone(),
                etherscan_api_key.to_string(),
                wallet.starting_tx.clone(),
            );
            let runner = BlockchainSyncRunner::new(sync, pool);
            tokio::spawn(async move {
                runner.run(shutdown_rx, tick_rx, match_tx).await;
            })
        }
    }
}

/// Map an SDK `Blockchain` variant to a `BlockchainTarget` for the event system.
fn blockchain_to_target(blockchain: Blockchain) -> BlockchainTarget {
    match blockchain {
        Blockchain::Tron => BlockchainTarget::Trc20,
        other => BlockchainTarget::Erc20(blockchain_to_etherscan_chain(other)),
    }
}

/// Map an SDK `Blockchain` variant to an `EtherScanChain`.
///
/// # Panics
///
/// Panics if called with `Blockchain::Tron` (Tron is TRC-20, not ERC-20).
fn blockchain_to_etherscan_chain(blockchain: Blockchain) -> EtherScanChain {
    match blockchain {
        Blockchain::Ethereum => EtherScanChain::Ethereum,
        Blockchain::Polygon => EtherScanChain::Polygon,
        Blockchain::Base => EtherScanChain::Base,
        Blockchain::ArbitrumOne => EtherScanChain::ArbitrumOne,
        Blockchain::Linea => EtherScanChain::Linea,
        Blockchain::Optimism => EtherScanChain::Optimism,
        Blockchain::AvalancheC => EtherScanChain::AvalancheC,
        Blockchain::Tron => panic!("Tron is not an EtherScan chain"),
    }
}

/// Initialize the tracing subscriber with environment-based filtering.
fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,sqlx=warn,tower_http=debug"));

    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer())
        .init();
}
