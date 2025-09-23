use {
	crate::{Request, Response},
	tower::ServiceExt as _,
};

#[derive(Clone)]
pub struct TracingLayer {
	request: bool,
	response: bool,
}

impl TracingLayer {
	#[must_use]
	pub fn new() -> Self {
		Self {
			request: true,
			response: true,
		}
	}
}

impl Default for TracingLayer {
	fn default() -> Self {
		Self::new()
	}
}

impl<S> tower::layer::Layer<S> for TracingLayer
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
				if layer.request {
					tracing::trace!(headers = ?request.headers(), method = ?request.method(), path = ?request.uri().path(), "request");
				}
				let response = service.ready().await?.call(request).await?;
				if layer.response {
					tracing::trace!(headers = ?response.headers(), status = ?response.status(), "response");
				}
				Ok(response)
			}
		}))
	}
}
