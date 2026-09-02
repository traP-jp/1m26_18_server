use axum::{Json, extract::State};

use crate::domain::webtransport::CertificateHashResponse;
use crate::rest::AppState;

/// WebTransport接続に必要な証明書ハッシュとポートを取得します
#[utoipa::path(
    get,
    path = "/webtransport/certificate-hash",
    responses(
        (status = 200, body = CertificateHashResponse, description = "自己署名証明書のSHA-256ハッシュ(小文字hex)とWebTransportのUDPポートを返します。ハッシュは生32バイトにデコードして`serverCertificateHashes`に渡してください。"),
    ),
    tag = "WebTransport",
)]
pub async fn get_certificate_hash(State(state): State<AppState>) -> Json<CertificateHashResponse> {
    Json(CertificateHashResponse {
        certificate_hash: state.webtransport_cert_hash,
        port: state.webtransport_port,
    })
}
