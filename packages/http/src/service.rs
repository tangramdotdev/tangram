use crate::{Request, Response};
use futures::Future;

pub trait Service<E>:
	tower::Service<Request, Response = Response, Error = E> + Clone + Send + Sync + 'static
where
	Self::Future: Send + 'static,
	E: Send + 'static,
{
}

impl<S, E> Service<E> for S
where
	S: tower::Service<Request, Response = Response, Error = E> + Clone + Send + Sync + 'static,
	S::Future: Send + 'static,
	E: Send + 'static,
{
}

pub type BoxLayer<S, E> = tower::util::BoxCloneSyncServiceLayer<S, Request, Response, E>;

pub fn middleware<S, E, F, Fut>(f: F) -> BoxLayer<S, E>
where
	S: Service<E>,
	<S as tower::Service<Request>>::Future: Send + 'static,
	F: FnMut(tower::util::BoxCloneSyncService<Request, Response, E>, Request) -> Fut
		+ Clone
		+ Send
		+ Sync
		+ 'static,
	Fut: Future<Output = Result<Response, E>> + Send + 'static,
	E: Send + 'static,
{
	tower::util::BoxCloneSyncServiceLayer::new(tower::layer::layer_fn(move |service| {
		let f = f.clone();
		let service = tower::util::BoxCloneSyncService::new(service);
		tower::service_fn(move |request| {
			let mut f = f.clone();
			let service = service.clone();
			async move { f(service, request).await }
		})
	}))
}
