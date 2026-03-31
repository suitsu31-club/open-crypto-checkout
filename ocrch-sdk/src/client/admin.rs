//! Admin API client (admin dashboard → Ocrch server).
//!
//! All requests carry the plaintext admin secret in the
//! `Ocrch-Admin-Authorization` header.

use reqwest::Client;
use url::Url;
use uuid::Uuid;

use super::ClientError;
use crate::objects::admin::{
    AdminOrderResponse, AdminPendingDepositResponse, AdminTransferResponse, AdminWalletResponse,
    ListDepositsQuery, ListOrdersQuery, ListTransfersQuery,
};
use crate::signature::ADMIN_AUTH_HEADER;

/// Typed HTTP client for the Ocrch **Admin API**.
///
/// Authentication uses a plaintext secret sent in the
/// `Ocrch-Admin-Authorization` header, verified server-side against an
/// argon2-hashed value.
#[derive(Debug, Clone)]
pub struct AdminClient {
    http: Client,
    base_url: Url,
    admin_secret: String,
}

impl AdminClient {
    /// Create a new `AdminClient`.
    ///
    /// * `base_url` – root URL of the Ocrch server.
    /// * `admin_secret` – the plaintext admin secret.
    pub fn new(base_url: Url, admin_secret: impl Into<String>) -> Self {
        Self {
            http: Client::new(),
            base_url,
            admin_secret: admin_secret.into(),
        }
    }

    /// Replace the default `reqwest::Client` with a custom one.
    pub fn with_http_client(mut self, client: Client) -> Self {
        self.http = client;
        self
    }

    /// `GET /api/v1/admin/orders` – list orders with optional filters.
    pub async fn list_orders(
        &self,
        query: &ListOrdersQuery,
    ) -> Result<Vec<AdminOrderResponse>, ClientError> {
        let url = self.base_url.join("/api/v1/admin/orders")?;

        let resp = self
            .http
            .get(url)
            .header(ADMIN_AUTH_HEADER, &self.admin_secret)
            .query(query)
            .send()
            .await?;

        parse_response(resp).await
    }

    /// `GET /api/v1/admin/deposits` – list pending deposits with optional
    /// filters.
    pub async fn list_deposits(
        &self,
        query: &ListDepositsQuery,
    ) -> Result<Vec<AdminPendingDepositResponse>, ClientError> {
        let url = self.base_url.join("/api/v1/admin/deposits")?;

        let resp = self
            .http
            .get(url)
            .header(ADMIN_AUTH_HEADER, &self.admin_secret)
            .query(query)
            .send()
            .await?;

        parse_response(resp).await
    }

    /// `GET /api/v1/admin/wallets/{address}/transfers` – list transfers for
    /// a specific wallet.
    pub async fn list_transfers(
        &self,
        wallet_address: &str,
        query: &ListTransfersQuery,
    ) -> Result<Vec<AdminTransferResponse>, ClientError> {
        let url = self.base_url.join(&format!(
            "/api/v1/admin/wallets/{}/transfers",
            urlencoding::encode(wallet_address)
        ))?;

        let resp = self
            .http
            .get(url)
            .header(ADMIN_AUTH_HEADER, &self.admin_secret)
            .query(query)
            .send()
            .await?;

        parse_response(resp).await
    }

    /// `GET /api/v1/admin/wallets` – list configured wallets and their
    /// enabled coins.
    pub async fn list_wallets(&self) -> Result<Vec<AdminWalletResponse>, ClientError> {
        let url = self.base_url.join("/api/v1/admin/wallets")?;

        let resp = self
            .http
            .get(url)
            .header(ADMIN_AUTH_HEADER, &self.admin_secret)
            .send()
            .await?;

        parse_response(resp).await
    }

    /// `POST /api/v1/admin/orders/{order_id}/mark-paid` – force-mark an
    /// order as paid.
    pub async fn mark_order_paid(&self, order_id: Uuid) -> Result<AdminOrderResponse, ClientError> {
        let url = self
            .base_url
            .join(&format!("/api/v1/admin/orders/{order_id}/mark-paid"))?;

        let resp = self
            .http
            .post(url)
            .header(ADMIN_AUTH_HEADER, &self.admin_secret)
            .send()
            .await?;

        parse_response(resp).await
    }

    /// `POST /api/v1/admin/orders/{order_id}/resend-webhook` – resend the
    /// order status webhook.
    pub async fn resend_order_webhook(&self, order_id: Uuid) -> Result<(), ClientError> {
        let url = self
            .base_url
            .join(&format!("/api/v1/admin/orders/{order_id}/resend-webhook"))?;

        let resp = self
            .http
            .post(url)
            .header(ADMIN_AUTH_HEADER, &self.admin_secret)
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(ClientError::Api { status, body });
        }

        Ok(())
    }

    /// `POST /api/v1/admin/transfers/{transfer_id}/resend-webhook` – resend
    /// the unknown-transfer webhook.
    pub async fn resend_transfer_webhook(&self, transfer_id: i64) -> Result<(), ClientError> {
        let url = self.base_url.join(&format!(
            "/api/v1/admin/transfers/{transfer_id}/resend-webhook"
        ))?;

        let resp = self
            .http
            .post(url)
            .header(ADMIN_AUTH_HEADER, &self.admin_secret)
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(ClientError::Api { status, body });
        }

        Ok(())
    }
}

async fn parse_response<T: serde::de::DeserializeOwned>(
    resp: reqwest::Response,
) -> Result<T, ClientError> {
    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(ClientError::Api { status, body });
    }
    let bytes = resp.bytes().await?;
    serde_json::from_slice(&bytes).map_err(ClientError::Json)
}
