use super::*;

impl FallbackProxy {
    pub(crate) async fn forward_recording(
        &self,
        mut request: Request<Incoming>,
        peer_addr: SocketAddr,
    ) -> GatewayResponse {
        let fallback = request.extensions_mut().remove::<RecordingFallback>();
        let Some(RecordingFallback(fallback)) = fallback else {
            return self.forward(request, peer_addr).await;
        };
        // Keep the upgrade handle until an origin accepts the handshake.
        // A failed connection must leave it available for the original path.
        let upgrade = is_upgrade(request.headers());
        let mut downstream_upgrade = upgrade.then(|| hyper::upgrade::on(&mut request));
        let (parts, body) = request.into_parts();
        let collected = match body.collect().await {
            Ok(body) => body,
            Err(error) => {
                debug!(%error, "failed to retain recording request body");
                return error_response(StatusCode::BAD_REQUEST, "failed to read request body");
            }
        };
        let trailers = collected.trailers().cloned();
        let bytes = collected.to_bytes();
        let request = Request::from_parts(parts, replay_body(bytes.clone(), trailers.clone()));
        // Clone the original request head before proxy preparation changes URI
        // or forwarded headers. Both destinations receive the same payload.
        let mut retry = Request::new(replay_body(bytes, trailers));
        *retry.method_mut() = request.method().clone();
        *retry.uri_mut() = request.uri().clone();
        *retry.version_mut() = request.version();
        *retry.headers_mut() = request.headers().clone();
        retry.headers_mut().remove(crate::ADMISSION_SCOPE_HEADER);
        retry.headers_mut().remove(crate::ADMISSION_TOKEN_HEADER);
        match self
            .try_forward_body(request, peer_addr, &mut downstream_upgrade)
            .await
        {
            Ok(response) if !upgrade && response.status() == StatusCode::NOT_FOUND => {
                drop(response)
            }
            Ok(response) => return response,
            // Connect errors occur before the HTTP request is dispatched.
            Err(ConnectFailure) => {}
        }
        // This branch is executed at most once. Errors and other status codes
        // are returned directly, including another 404 from the original path.
        fallback
            .try_forward_body(retry, peer_addr, &mut downstream_upgrade)
            .await
            .unwrap_or_else(|_| upstream_unavailable())
    }
}

fn replay_body(bytes: Bytes, trailers: Option<HeaderMap>) -> GatewayBody {
    ReplayBody {
        bytes: Some(bytes),
        trailers,
    }
    .boxed()
}

struct ReplayBody {
    bytes: Option<Bytes>,
    trailers: Option<HeaderMap>,
}

impl hyper::body::Body for ReplayBody {
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<hyper::body::Frame<Bytes>, BoxError>>> {
        if let Some(bytes) = self.bytes.take() {
            if !bytes.is_empty() {
                return std::task::Poll::Ready(Some(Ok(hyper::body::Frame::data(bytes))));
            }
        }
        std::task::Poll::Ready(
            self.trailers
                .take()
                .map(|trailers| Ok(hyper::body::Frame::trailers(trailers))),
        )
    }

    fn is_end_stream(&self) -> bool {
        self.bytes.is_none() && self.trailers.is_none()
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        let mut hint = hyper::body::SizeHint::new();
        if self.trailers.is_none() {
            hint.set_exact(self.bytes.as_ref().map_or(0, |bytes| bytes.len() as u64));
        }
        hint
    }
}
