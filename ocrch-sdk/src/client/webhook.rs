//! Webhook signature verification helper.
//!
//! Convenience wrapper around [`SignedObject`] for verifying incoming
//! webhook payloads sent by the Ocrch server.

use crate::signature::{Signature, SignatureError, SignedObject};

/// Verify and deserialize an incoming Ocrch webhook.
///
/// * `signature_header` – value of the `Ocrch-Signature` request header.
/// * `body` – raw JSON request body string.
/// * `secret` – the merchant HMAC secret shared with the Ocrch server.
///
/// Returns the deserialized, authenticated payload on success.
///
/// # Example
///
/// ```ignore
/// use ocrch_sdk::client::verify_webhook;
/// use ocrch_sdk::objects::webhook::OrderStatusChangedPayload;
///
/// let payload: OrderStatusChangedPayload =
///     verify_webhook(signature_header, &body, merchant_secret)?;
/// ```
pub fn verify_webhook<T: Signature>(
    signature_header: &str,
    body: &str,
    secret: &[u8],
) -> Result<T, SignatureError> {
    SignedObject::<T>::from_header_and_body(signature_header, body.to_owned())?.verify(secret)
}
