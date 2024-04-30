// use futures::{Future, FutureExt};

// pub struct TaskSet {
// 	task_tracker: tokio_util::task::TaskTracker,
// }

// pub struct Task<T> {
// 	join_handle: tokio::task::JoinHandle<T>,
// 	cancellation_token: tokio_util::sync::CancellationToken,
// }

// pub struct Token;

// impl<T> Task<T> {
// 	pub fn new<F, Fut>(f: F) -> Self where F: FnOnce(Cancel) -> Fut, Fut: {
// 		todo!()
// 	}
// }

// impl<T> Task<T> {
// 	pub fn abort()

// 	pub fn cancel(&self) {
// 		self.cancellation_token.cancel();
// 	}
// }

// impl<T> Future for Task<T> {
// 	type Output = Result<T, tokio::task::JoinError>;

// 	fn poll(
// 		mut self: std::pin::Pin<&mut Self>,
// 		cx: &mut std::task::Context<'_>,
// 	) -> std::task::Poll<Self::Output> {
// 		self.as_mut().join_handle.poll_unpin(cx)
// 	}
// }
