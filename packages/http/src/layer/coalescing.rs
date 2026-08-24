use {
	crate::body::{Boxed as BoxBody, Coalesce, coalesce::DEFAULT_COALESCING_TARGET_SIZE},
	pin_project::pin_project,
	std::{
		future::Future,
		pin::Pin,
		task::{Context, Poll},
	},
};

#[derive(Clone, Copy)]
pub struct RequestCoalescingLayer {
	target_size: usize,
}

#[derive(Clone)]
pub struct RequestCoalescing<S> {
	service: S,
	target_size: usize,
}

#[derive(Clone, Copy)]
pub struct ResponseCoalescingLayer {
	target_size: usize,
}

#[derive(Clone)]
pub struct ResponseCoalescing<S> {
	service: S,
	target_size: usize,
}

#[pin_project]
pub struct ResponseFuture<F> {
	#[pin]
	future: F,
	target_size: usize,
}

impl ResponseCoalescingLayer {
	#[must_use]
	pub fn new(target_size: usize) -> Self {
		assert!(target_size > 0, "the target size must be greater than zero");
		Self { target_size }
	}
}

impl RequestCoalescingLayer {
	#[must_use]
	pub fn new(target_size: usize) -> Self {
		assert!(target_size > 0, "the target size must be greater than zero");
		Self { target_size }
	}
}

impl<S> tower::layer::Layer<S> for RequestCoalescingLayer {
	type Service = RequestCoalescing<S>;

	fn layer(&self, service: S) -> Self::Service {
		RequestCoalescing {
			service,
			target_size: self.target_size,
		}
	}
}

impl<S> tower::layer::Layer<S> for ResponseCoalescingLayer {
	type Service = ResponseCoalescing<S>;

	fn layer(&self, service: S) -> Self::Service {
		ResponseCoalescing {
			service,
			target_size: self.target_size,
		}
	}
}

impl Default for ResponseCoalescingLayer {
	fn default() -> Self {
		Self::new(DEFAULT_COALESCING_TARGET_SIZE)
	}
}

impl Default for RequestCoalescingLayer {
	fn default() -> Self {
		Self::new(DEFAULT_COALESCING_TARGET_SIZE)
	}
}

impl<S> tower::Service<http::Request<BoxBody>> for RequestCoalescing<S>
where
	S: tower::Service<http::Request<BoxBody>>,
{
	type Error = S::Error;
	type Future = S::Future;
	type Response = S::Response;

	fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
		self.service.poll_ready(cx)
	}

	fn call(&mut self, request: http::Request<BoxBody>) -> Self::Future {
		let request =
			request.map(|body| BoxBody::new(Coalesce::with_target_size(body, self.target_size)));
		self.service.call(request)
	}
}

impl<S, B> tower::Service<http::Request<B>> for ResponseCoalescing<S>
where
	S: tower::Service<http::Request<B>, Response = http::Response<BoxBody>>,
{
	type Error = S::Error;
	type Future = ResponseFuture<S::Future>;
	type Response = http::Response<BoxBody>;

	fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
		self.service.poll_ready(cx)
	}

	fn call(&mut self, request: http::Request<B>) -> Self::Future {
		ResponseFuture {
			future: self.service.call(request),
			target_size: self.target_size,
		}
	}
}

impl<F, E> Future for ResponseFuture<F>
where
	F: Future<Output = Result<http::Response<BoxBody>, E>>,
{
	type Output = Result<http::Response<BoxBody>, E>;

	fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
		let this = self.project();
		match this.future.poll(cx) {
			Poll::Pending => Poll::Pending,
			Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
			Poll::Ready(Ok(response)) => {
				let response = response
					.map(|body| BoxBody::new(Coalesce::with_target_size(body, *this.target_size)));
				Poll::Ready(Ok(response))
			},
		}
	}
}
