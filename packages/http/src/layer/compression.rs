use crate::{
	header::{accept_encoding::AcceptEncoding, content_encoding::ContentEncoding},
	Body, Request, Response,
};
use async_compression::tokio::bufread::{ZstdDecoder, ZstdEncoder};
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
				let has_content_encoding = request
					.headers()
					.contains_key(http::header::CONTENT_ENCODING);
				let is_text_event_stream = request
					.headers()
					.get(http::header::CONTENT_TYPE)
					.is_some_and(|content_type| {
						matches!(content_type.to_str(), Ok("text/event-stream"))
					});
				let predicate = layer.predicate.run(request.headers(), request.body());
				let compress = !has_content_encoding && !is_text_event_stream && predicate;
				let (mut parts, mut body) = request.into_parts();
				if compress {
					parts.headers.remove(http::header::CONTENT_LENGTH);
					parts.headers.insert(
						http::header::CONTENT_ENCODING,
						http::HeaderValue::from_str("zstd").unwrap(),
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
				let encoding = parts
					.headers
					.get(http::header::CONTENT_ENCODING)
					.and_then(|value| value.to_str().ok())
					.and_then(|value| value.parse::<ContentEncoding>().ok());
				if encoding.is_some() {
					parts.headers.remove(http::header::CONTENT_ENCODING);
					parts.headers.remove(http::header::CONTENT_LENGTH);
				}
				match encoding {
					Some(ContentEncoding::Zstd) => {
						body = Body::with_reader(ZstdDecoder::new(body.into_reader()));
					},
					None => (),
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
				let accept_encoding: Option<AcceptEncoding> = request
					.headers()
					.get(http::header::ACCEPT_ENCODING)
					.and_then(|value| value.to_str().ok())
					.and_then(|value| value.parse().ok());
				let response = service.ready().await?.call(request).await?;
				let (mut parts, mut body) = response.into_parts();
				let has_content_encoding =
					parts.headers.contains_key(http::header::CONTENT_ENCODING);
				let is_text_event_stream = parts
					.headers
					.get(http::header::CONTENT_TYPE)
					.is_some_and(|content_type| {
						matches!(content_type.to_str(), Ok("text/event-stream"))
					});
				let predicate = layer.predicate.run(&parts.headers, &body);
				let compress = !has_content_encoding && !is_text_event_stream && predicate;
				let zstd = accept_encoding.is_some_and(|accept_encoding| {
					accept_encoding
						.preferences
						.iter()
						.any(|preference| preference.encoding == ContentEncoding::Zstd)
				});
				if compress && zstd {
					parts.headers.remove(http::header::CONTENT_LENGTH);
					parts.headers.insert(
						http::header::CONTENT_ENCODING,
						http::HeaderValue::from_str("zstd").unwrap(),
					);
					body = Body::with_reader(ZstdEncoder::new(body.into_reader()));
				}
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
		tower::util::BoxCloneSyncService::new(tower::service_fn(move |mut request: Request| {
			let mut service = service.clone();
			async move {
				request.headers_mut().insert(
					http::header::ACCEPT_ENCODING,
					http::HeaderValue::from_str(&ContentEncoding::Zstd.to_string()).unwrap(),
				);
				let response = service.ready().await?.call(request).await?;
				let (mut parts, mut body) = response.into_parts();
				let encoding = parts
					.headers
					.get(http::header::CONTENT_ENCODING)
					.and_then(|value| value.to_str().ok())
					.and_then(|value| value.parse::<ContentEncoding>().ok());
				if encoding.is_some() {
					parts.headers.remove(http::header::CONTENT_ENCODING);
					parts.headers.remove(http::header::CONTENT_LENGTH);
				}
				match encoding {
					Some(ContentEncoding::Zstd) => {
						body = Body::with_reader(ZstdDecoder::new(body.into_reader()));
					},
					None => (),
				}
				let response = Response::from_parts(parts, body);
				Ok(response)
			}
		}))
	}
}

#[derive(Clone, Copy, Debug)]
pub struct Predicate {
	pub minimum_length: u64,
}

impl Predicate {
	pub fn run(&self, headers: &http::HeaderMap<http::HeaderValue>, body: &Body) -> bool {
		let content_size = body.size_hint().exact().or_else(|| {
			headers
				.get(http::header::CONTENT_LENGTH)
				.and_then(|value| value.to_str().ok())
				.and_then(|value| value.parse().ok())
		});
		match content_size {
			Some(size) => size >= self.minimum_length,
			_ => true,
		}
	}
}

impl Default for Predicate {
	fn default() -> Self {
		Self { minimum_length: 32 }
	}
}
