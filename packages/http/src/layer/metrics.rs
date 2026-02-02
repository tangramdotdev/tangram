use {
	crate::{Request, Response},
	opentelemetry as otel,
	std::time::Instant,
	tower::ServiceExt as _,
};

#[derive(Clone)]
pub struct MetricsLayer {
	request_count: otel::metrics::Counter<u64>,
	request_duration: otel::metrics::Histogram<f64>,
}

impl MetricsLayer {
	#[must_use]
	pub fn new() -> Self {
		let meter = otel::global::meter("tangram_http");
		let request_count = meter
			.u64_counter("http.request.count")
			.with_description("Total number of HTTP requests")
			.build();

		let request_duration = meter
			.f64_histogram("http.request.duration")
			.with_description("HTTP request duration in seconds")
			.with_unit("s")
			.build();

		Self {
			request_count,
			request_duration,
		}
	}
}

impl Default for MetricsLayer {
	fn default() -> Self {
		Self::new()
	}
}

impl<S> tower::layer::Layer<S> for MetricsLayer
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
				let method = request.method().to_string();
				let path = request.uri().path().to_owned();
				let start = Instant::now();

				let response = service.ready().await?.call(request).await?;

				let duration = start.elapsed().as_secs_f64();
				let status = response.status().as_u16().to_string();

				let attributes = [
					otel::KeyValue::new("http.request.method", method),
					otel::KeyValue::new("http.route", path),
					otel::KeyValue::new("http.response.status_code", status),
				];

				layer.request_count.add(1, &attributes);
				layer.request_duration.record(duration, &attributes);

				Ok(response)
			}
		}))
	}
}
