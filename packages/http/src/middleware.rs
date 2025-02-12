use crate::{
	service::{middleware, BoxLayer, Service},
	Body, Request, Response,
};
use async_compression::tokio::bufread::{ZstdDecoder, ZstdEncoder};
use http::{header, HeaderMap, HeaderValue};
use http_body::Body as _;
use tower::{Service as _, ServiceExt as _};

#[derive(Clone, Copy, Debug)]
pub struct CompressionPredicate {
	minimum_size_bytes: u16,
}

impl CompressionPredicate {
	pub fn should_compress(&self, headers: &HeaderMap<HeaderValue>, body: &Body) -> bool {
		// Try to get the size hint from the body, and if not available, try to read CONTENT_LENGTH.
		let content_size = body.size_hint().exact().or_else(|| {
			headers
				.get(header::CONTENT_LENGTH)
				.and_then(|h| h.to_str().ok())
				.and_then(|val| val.parse().ok())
		});

		match content_size {
			Some(size) => size >= u64::from(self.minimum_size_bytes),
			_ => true,
		}
	}
}

impl Default for CompressionPredicate {
	fn default() -> Self {
		Self {
			minimum_size_bytes: 32,
		}
	}
}

/// Compress requests with zstd.
#[must_use]
pub fn request_compression_layer<S, E>(predicate: CompressionPredicate) -> BoxLayer<S, E>
where
	S: Service<E>,
	S::Future: Send + 'static,
	E: Send + 'static,
{
	middleware::<S, E, _, _>(move |mut service, request| async move {
		// Don't recompress requests that are already compressed.
		let should_compress = !request.headers().contains_key(header::CONTENT_ENCODING)
			&& !request.headers().contains_key(header::CONTENT_RANGE)
			&& predicate.should_compress(request.headers(), request.body());

		let (mut parts, mut body) = request.into_parts();
		if should_compress {
			// The CONTENT_LENGTH has changed.
			parts.headers.remove(header::CONTENT_LENGTH);
			// The CONTENT_ENCODING has changed.
			parts.headers.insert(
				header::CONTENT_ENCODING,
				HeaderValue::from_str("zstd").unwrap(),
			);

			body = Body::with_reader(ZstdEncoder::new(body.into_reader()));
		}

		let request = Request::from_parts(parts, body);
		let response = service.ready().await?.call(request).await?;
		Ok(response)
	})
}

/// Decompress zstd-compressed requests.
#[must_use]
pub fn request_decompression_layer<S, E>() -> BoxLayer<S, E>
where
	S: Service<E>,
	S::Future: Send + 'static,
	E: Send + 'static,
{
	middleware::<S, E, _, _>(move |mut service, request| async move {
		let (mut parts, mut body) = request.into_parts();
		if let header::Entry::Occupied(entry) = parts.headers.entry(header::CONTENT_ENCODING) {
			let encoding = entry.get().as_bytes();
			if encoding == b"zstd" {
				body = Body::with_reader(ZstdDecoder::new(body.into_reader()));
				// The content encoding is no longer applicable.
				entry.remove();
				// The content length changed.
				parts.headers.remove(header::CONTENT_LENGTH);
			} else {
				tracing::warn!(?encoding, "unexpected content-encoding");
			}
		}
		let request = Request::from_parts(parts, body);
		let response = service.ready().await?.call(request).await?;
		Ok(response)
	})
}

/// Compress responses with zstd.
#[must_use]
pub fn response_compression_layer<S, E>(predicate: CompressionPredicate) -> BoxLayer<S, E>
where
	S: Service<E>,
	S::Future: Send + 'static,
	E: Send + 'static,
{
	middleware::<S, E, _, _>(move |mut service, request| async move {
		let response = service.ready().await?.call(request).await?;

		// Don't recompress responses that are already compressed or event streams.
		let should_compress = !response.headers().contains_key(header::CONTENT_ENCODING)
			&& !response.headers().contains_key(header::CONTENT_RANGE)
			&& !response
				.headers()
				.get(header::CONTENT_TYPE)
				.is_some_and(|ct| ct.to_str().unwrap_or("").eq("text/event-stream"))
			&& predicate.should_compress(response.headers(), response.body());

		if !should_compress {
			return Ok(response);
		}

		let (mut parts, body) = response.into_parts();

		// This response does not accept ranges.
		parts.headers.remove(header::ACCEPT_RANGES);
		// The CONTENT_LENGTH has changed.
		parts.headers.remove(header::CONTENT_LENGTH);
		// The CONTENT_ENCODING has changed.
		parts.headers.insert(
			header::CONTENT_ENCODING,
			HeaderValue::from_str("zstd").unwrap(),
		);

		let body = Body::with_reader(ZstdEncoder::new(body.into_reader()));

		let response = Response::from_parts(parts, body);
		Ok(response)
	})
}

/// Decompress zstd-compressed responses.
#[must_use]
pub fn response_decompression_layer<S, E>() -> BoxLayer<S, E>
where
	S: Service<E>,
	S::Future: Send + 'static,
	E: Send + 'static,
{
	middleware::<S, E, _, _>(move |mut service, request| async move {
		let response = service.ready().await?.call(request).await?;

		let (mut parts, mut body) = response.into_parts();
		if let header::Entry::Occupied(entry) = parts.headers.entry(header::CONTENT_ENCODING) {
			let encoding = entry.get().as_bytes();
			if encoding == b"zstd" {
				body = Body::with_reader(ZstdDecoder::new(body.into_reader()));
				// The content encoding is no longer applicable.
				entry.remove();
				// The content length changed.
				parts.headers.remove(header::CONTENT_LENGTH);
			} else {
				tracing::warn!(?encoding, "unexpected content-encoding");
			}
		}
		let response = Response::from_parts(parts, body);
		Ok(response)
	})
}

#[derive(Clone, Debug)]
pub struct TraceLayerArg {
	request: bool,
	response: bool,
}

impl Default for TraceLayerArg {
	fn default() -> Self {
		Self {
			request: true,
			response: true,
		}
	}
}

/// Add tracing.
#[must_use]
pub fn trace_layer<S, E>(arg: &TraceLayerArg) -> BoxLayer<S, E>
where
	S: Service<E>,
	S::Future: Send + 'static,
	E: Send + 'static,
{
	let arg = arg.clone();
	middleware::<S, E, _, _>(move |mut service, request| async move {
		if arg.request {
			tracing::trace!(headers = ?request.headers(), method = ?request.method(), path = ?request.uri().path(), "request");
		}
		let response = service.ready().await?.call(request).await?;
		if arg.response {
			tracing::trace!(headers = ?response.headers(), status = ?response.status(), "response");
		}
		Ok(response)
	})
}
