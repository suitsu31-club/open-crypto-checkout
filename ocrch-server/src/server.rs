//! Axum server setup and router configuration.

use crate::api;
use crate::shutdown::shutdown_signal;
use crate::state::AppState;
use axum::{Json, Router, response::IntoResponse, routing::get};
use serde::Serialize;
use std::net::SocketAddr;
use tokio::net::TcpListener;

/// Build the main application router.
pub fn build_router(state: AppState) -> Router {
    Router::new()
        // Health check endpoint
        .route("/health", get(health_check))
        // Service API (application backend)
        .nest("/api/v1/service", api::service::router())
        // User API (checkout frontend)
        .nest("/api/v1/user", api::user::router())
        // Admin API (admin dashboard)
        .nest("/api/v1/admin", api::admin::router())
        // Add state to all routes
        .with_state(state)
}

/// Health check response.
#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    version: &'static str,
}

/// Simple health check - returns OK if the server is running.
async fn health_check() -> impl IntoResponse {
    Json(HealthResponse {
        status: "healthy",
        version: env!("CARGO_PKG_VERSION"),
    })
}

/// Run the server with graceful shutdown support.
pub async fn run_server(router: Router, addr: SocketAddr) -> Result<(), std::io::Error> {
    let listener = TcpListener::bind(addr).await?;
    tracing::info!("Server listening on {}", addr);

    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await
}
