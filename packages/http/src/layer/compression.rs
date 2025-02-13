use crate::{Body, Request, Response};
use async_compression::tokio::bufread::{ZstdDecoder, ZstdEncoder};
use http::{header, HeaderMap, HeaderValue};
use http_body::Body as _;
use tower::ServiceExt as _;

#[derive(Clone, Default)]
pub struct RequestCompressionLayer {
	pub predicate: Predicate,
}

impl<S> tower::layer::Layer<S> for RequestCompressionLayer
where
	S: tower::Service<Request, Response = Response> + Clone + Send + Sync + 'static,
	S::Future: Send + 'static,
	S::Error: Send + 'static,
{
	type Service = tower::util::BoxCloneSyncService<Request, Response, S::Error>;

	fn layer(&self, service: S) -> Self::Service {
		let layer = self.clone();
		tower::util::BoxCloneSyncService::new(tower::service_fn(move |request: Request| {
			let layer = layer.clone();
			let mut service = service.clone();
			async move {
				let should_compress = !request.headers().contains_key(header::CONTENT_ENCODING)
					&& !request.headers().contains_key(header::CONTENT_RANGE)
					&& layer
						.predicate
						.should_compress(request.headers(), request.body());
				let (mut parts, mut body) = request.into_parts();
				if should_compress {
					parts.headers.remove(header::CONTENT_LENGTH);
					parts.headers.insert(
						header::CONTENT_ENCODING,
						HeaderValue::from_str("zstd").unwrap(),
					);
					body = Body::with_reader(ZstdEncoder::new(body.into_reader()));
				}
				let request = Request::from_parts(parts, body);
				let response = service.ready().await?.call(request).await?;
				Ok(response)
			}
		}))
	}
}

pub struct RequestDecompressionLayer;

impl<S> tower::layer::Layer<S> for RequestDecompressionLayer
where
	S: tower::Service<Request, Response = Response> + Clone + Send + Sync + 'static,
	S::Future: Send + 'static,
	S::Error: Send + 'static,
{
	type Service = tower::util::BoxCloneSyncService<Request, Response, S::Error>;

	fn layer(&self, service: S) -> Self::Service {
		tower::util::BoxCloneSyncService::new(tower::service_fn(move |request: Request| {
			let mut service = service.clone();
			async move {
				let (mut parts, mut body) = request.into_parts();
				if let header::Entry::Occupied(entry) =
					parts.headers.entry(header::CONTENT_ENCODING)
				{
					let encoding = entry.get().as_bytes();
					if encoding == b"zstd" {
						body = Body::with_reader(ZstdDecoder::new(body.into_reader()));
						entry.remove();
						parts.headers.remove(header::CONTENT_LENGTH);
					} else {
						tracing::warn!(?encoding, "unexpected content-encoding");
					}
				}
				let request = Request::from_parts(parts, body);
				let response = service.ready().await?.call(request).await?;
				Ok(response)
			}
		}))
	}
}

#[derive(Clone, Default)]
pub struct ResponseCompressionLayer {
	pub predicate: Predicate,
}

impl<S> tower::layer::Layer<S> for ResponseCompressionLayer
where
	S: tower::Service<Request, Response = Response> + Clone + Send + Sync + 'static,
	S::Future: Send + 'static,
	S::Error: Send + 'static,
{
	type Service = tower::util::BoxCloneSyncService<Request, Response, S::Error>;

	fn layer(&self, service: S) -> Self::Service {
		let layer = self.clone();
		tower::util::BoxCloneSyncService::new(tower::service_fn(move |request: Request| {
			let layer = layer.clone();
			let mut service = service.clone();
			async move {
				let response = service.ready().await?.call(request).await?;
				let should_compress = !response.headers().contains_key(header::CONTENT_ENCODING)
					&& !response.headers().contains_key(header::CONTENT_RANGE)
					&& !response
						.headers()
						.get(header::CONTENT_TYPE)
						.is_some_and(|ct| ct.to_str().unwrap_or("").eq("text/event-stream"))
					&& layer
						.predicate
						.should_compress(response.headers(), response.body());
				if !should_compress {
					return Ok(response);
				}
				let (mut parts, body) = response.into_parts();
				parts.headers.remove(header::ACCEPT_RANGES);
				parts.headers.remove(header::CONTENT_LENGTH);
				parts.headers.insert(
					header::CONTENT_ENCODING,
					HeaderValue::from_str("zstd").unwrap(),
				);
				let body = Body::with_reader(ZstdEncoder::new(body.into_reader()));
				let response = Response::from_parts(parts, body);
				Ok(response)
			}
		}))
	}
}

pub struct ResponseDecompressionLayer;

impl<S> tower::layer::Layer<S> for ResponseDecompressionLayer
where
	S: tower::Service<Request, Response = Response> + Clone + Send + Sync + 'static,
	S::Future: Send + 'static,
	S::Error: Send + 'static,
{
	type Service = tower::util::BoxCloneSyncService<Request, Response, S::Error>;

	fn layer(&self, service: S) -> Self::Service {
		tower::util::BoxCloneSyncService::new(tower::service_fn(move |request: Request| {
			let mut service = service.clone();
			async move {
				let response = service.ready().await?.call(request).await?;
				let (mut parts, mut body) = response.into_parts();
				if let header::Entry::Occupied(entry) =
					parts.headers.entry(header::CONTENT_ENCODING)
				{
					let encoding = entry.get().as_bytes();
					if encoding == b"zstd" {
						body = Body::with_reader(ZstdDecoder::new(body.into_reader()));
						entry.remove();
						parts.headers.remove(header::CONTENT_LENGTH);
					} else {
						tracing::warn!(?encoding, "unexpected content-encoding");
					}
				}
				let response = Response::from_parts(parts, body);
				Ok(response)
			}
		}))
	}
}

#[derive(Clone, Copy, Debug)]
pub struct Predicate {
	pub minimum_length: u16,
}

impl Predicate {
	pub fn should_compress(&self, headers: &HeaderMap<HeaderValue>, body: &Body) -> bool {
		let content_size = body.size_hint().exact().or_else(|| {
			headers
				.get(header::CONTENT_LENGTH)
				.and_then(|value| value.to_str().ok())
				.and_then(|value| value.parse().ok())
		});
		match content_size {
			Some(size) => size >= u64::from(self.minimum_length),
			_ => true,
		}
	}
}

impl Default for Predicate {
	fn default() -> Self {
		Self { minimum_length: 32 }
	}
}
