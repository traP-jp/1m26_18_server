use serde::Serialize;
use utoipa::ToSchema;

/// Response containing the information required for clients to establish a
/// WebTransport connection.
///
/// The server uses a self-signed certificate regenerated on every process
/// start; browsers must pass its SHA-256 digest as `serverCertificateHashes`
/// when connecting.
#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CertificateHashResponse {
    /// SHA-256 digest of the self-signed certificate, lowercase hex (64 chars).
    ///
    /// Decode into 32 raw bytes and pass them as
    /// `{ algorithm: "sha-256", value: <ArrayBuffer> }` in the browser's
    /// `serverCertificateHashes`.
    pub certificate_hash: String,
    /// UDP port the WebTransport server listens on.
    pub port: u16,
}
