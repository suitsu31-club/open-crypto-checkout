//! User API client (checkout frontend → Ocrch server).
//!
//! All requests use URL-signed HMAC-SHA256 authentication.  The merchant
//! backend signs the checkout frontend URL and passes it to the frontend;
//! the frontend then includes the signature and the signed URL as headers
//! on every request.

use reqwest::Client;
use url::Url;
use uuid::Uuid;

use super::ClientError;
use crate::objects::create_payment::OrderResponse;
use crate::objects::user::{ChainCoinPair, PaymentDetail, SelectPaymentMethod};
use crate::signature::{SIGNATURE_HEADER, SIGNED_URL_HEADER, sign_url};

/// Typed HTTP client for the Ocrch **User API**.
///
/// The user API is called by the checkout frontend.  Each request carries
/// two extra headers:
///
/// - `Ocrch-Signed-Url` – the frontend URL that was signed by the merchant
///   backend.
/// - `Ocrch-Signature` – `{timestamp}.{base64}` HMAC of `"{url}.{timestamp}"`.
///
/// The client re-signs `frontend_url` on every call so the timestamp stays
/// fresh.
#[derive(Debug, Clone)]
pub struct UserClient {
    http: Client,
    base_url: Url,
    secret: Vec<u8>,
    frontend_url: String,
}

impl UserClient {
    /// Create a new `UserClient`.
    ///
    /// * `base_url` – root URL of the Ocrch server.
    /// * `merchant_secret` – the shared HMAC secret (same one the merchant
    ///   backend uses to sign URLs).
    /// * `frontend_url` – the checkout page URL to sign in every request.
    pub fn new(
        base_url: Url,
        merchant_secret: impl Into<Vec<u8>>,
        frontend_url: impl Into<String>,
    ) -> Self {
        Self {
            http: Client::new(),
            base_url,
            secret: merchant_secret.into(),
            frontend_url: frontend_url.into(),
        }
    }

    /// Replace the default `reqwest::Client` with a custom one.
    pub fn with_http_client(mut self, client: Client) -> Self {
        self.http = client;
        self
    }

    fn sign_headers(&self) -> (String, &str) {
        let sig = sign_url(&self.frontend_url, &self.secret);
        (sig, &self.frontend_url)
    }

    /// `GET /api/v1/user/chains` – list available blockchain + stablecoin
    /// payment options.
    pub async fn list_chains(&self) -> Result<Vec<ChainCoinPair>, ClientError> {
        let (sig, signed_url) = self.sign_headers();

        let url = self.base_url.join("/api/v1/user/chains")?;

        let resp = self
            .http
            .get(url)
            .header(SIGNATURE_HEADER, sig)
            .header(SIGNED_URL_HEADER, signed_url)
            .send()
            .await?;

        parse_response(resp).await
    }

    /// `POST /api/v1/user/orders/{order_id}/payment` – select a payment
    /// method and create a pending deposit.
    pub async fn select_payment_method(
        &self,
        order_id: Uuid,
        method: SelectPaymentMethod,
    ) -> Result<PaymentDetail, ClientError> {
        let (sig, signed_url) = self.sign_headers();

        let url = self
            .base_url
            .join(&format!("/api/v1/user/orders/{order_id}/payment"))?;

        let resp = self
            .http
            .post(url)
            .header(SIGNATURE_HEADER, sig)
            .header(SIGNED_URL_HEADER, signed_url)
            .json(&method)
            .send()
            .await?;

        parse_response(resp).await
    }

    /// `POST /api/v1/user/orders/{order_id}/cancel` – cancel a pending order.
    pub async fn cancel_order(&self, order_id: Uuid) -> Result<OrderResponse, ClientError> {
        let (sig, signed_url) = self.sign_headers();

        let url = self
            .base_url
            .join(&format!("/api/v1/user/orders/{order_id}/cancel"))?;

        let resp = self
            .http
            .post(url)
            .header(SIGNATURE_HEADER, sig)
            .header(SIGNED_URL_HEADER, signed_url)
            .send()
            .await?;

        parse_response(resp).await
    }

    /// `GET /api/v1/user/orders/{order_id}/status` – poll the current order
    /// status.
    pub async fn get_order_status(&self, order_id: Uuid) -> Result<OrderResponse, ClientError> {
        let (sig, signed_url) = self.sign_headers();

        let url = self
            .base_url
            .join(&format!("/api/v1/user/orders/{order_id}/status"))?;

        let resp = self
            .http
            .get(url)
            .header(SIGNATURE_HEADER, sig)
            .header(SIGNED_URL_HEADER, signed_url)
            .send()
            .await?;

        parse_response(resp).await
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
