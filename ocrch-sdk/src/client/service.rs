//! Service API client (merchant backend → Ocrch server).
//!
//! All requests use body-signed HMAC-SHA256 authentication via
//! [`SignedObject`].

use reqwest::Client;
use url::Url;
use uuid::Uuid;

use super::ClientError;
use crate::objects::create_payment::{GetOrderRequest, OrderResponse, PaymentCreatingEssential};
use crate::signature::{SIGNATURE_HEADER, SignedObject};

/// Typed HTTP client for the Ocrch **Service API**.
///
/// The service API is called by the merchant backend to create orders and
/// query their status.  Every request body is signed with
/// `HMAC-SHA256("{timestamp}.{json}", merchant_secret)`.
#[derive(Debug, Clone)]
pub struct ServiceClient {
    http: Client,
    base_url: Url,
    secret: Vec<u8>,
}

impl ServiceClient {
    /// Create a new `ServiceClient`.
    ///
    /// * `base_url` – root URL of the Ocrch server (e.g. `https://pay.example.com`).
    /// * `merchant_secret` – the shared HMAC secret for body signing.
    pub fn new(base_url: Url, merchant_secret: impl Into<Vec<u8>>) -> Self {
        Self {
            http: Client::new(),
            base_url,
            secret: merchant_secret.into(),
        }
    }

    /// Replace the default `reqwest::Client` with a custom one (e.g. to
    /// configure timeouts or a proxy).
    pub fn with_http_client(mut self, client: Client) -> Self {
        self.http = client;
        self
    }

    /// `POST /api/v1/service/orders` – create a new pending order.
    pub async fn create_order(
        &self,
        payload: PaymentCreatingEssential,
    ) -> Result<OrderResponse, ClientError> {
        let signed = SignedObject::new(payload, &self.secret).map_err(ClientError::Json)?;

        let url = self.base_url.join("/api/v1/service/orders")?;

        let resp = self
            .http
            .post(url)
            .header(SIGNATURE_HEADER, signed.to_header())
            .body(signed.json)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .send()
            .await?;

        parse_response(resp).await
    }

    /// `POST /api/v1/service/orders/status` – get the status of an existing
    /// order.
    pub async fn get_order_status(&self, order_id: Uuid) -> Result<OrderResponse, ClientError> {
        let body = GetOrderRequest { order_id };
        let signed = SignedObject::new(body, &self.secret).map_err(ClientError::Json)?;

        let url = self.base_url.join("/api/v1/service/orders/status")?;

        let resp = self
            .http
            .post(url)
            .header(SIGNATURE_HEADER, signed.to_header())
            .body(signed.json)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
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
