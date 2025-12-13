use {
	crate::Error,
	bytes::Bytes,
	pin_project::pin_project,
	std::{
		pin::Pin,
		sync::{Arc, Mutex},
		time::Duration,
	},
};

#[derive(Clone)]
pub struct Idle {
	receiver: tokio::sync::watch::Receiver<bool>,
	sender: tokio::sync::watch::Sender<bool>,
	state: Arc<Mutex<State>>,
	timeout: Duration,
}

pub struct State {
	count: usize,
	task: Option<tokio::task::JoinHandle<()>>,
}

pub struct Token {
	idle: Idle,
}

#[pin_project]
pub struct Body<T: hyper::body::Body<Data = Bytes, Error = Error>> {
	#[pin]
	body: T,
	token: Token,
}

impl Idle {
	#[must_use]
	pub fn new(timeout: Duration) -> Self {
		let (sender, receiver) = tokio::sync::watch::channel(false);
		let task = tokio::spawn({
			let sender = sender.clone();
			async move {
				tokio::time::sleep(timeout).await;
				sender.send_replace(true);
			}
		});
		let state = Arc::new(Mutex::new(State {
			count: 0,
			task: Some(task),
		}));
		Self {
			receiver,
			sender,
			state,
			timeout,
		}
	}

	pub async fn wait(&self) {
		self.receiver
			.clone()
			.wait_for(|value| *value)
			.await
			.unwrap();
	}

	#[must_use]
	pub fn token(&self) -> Token {
		let mut state = self.state.lock().unwrap();
		if let Some(task) = state.task.take() {
			task.abort();
		}
		state.count += 1;
		Token { idle: self.clone() }
	}
}

impl Drop for Token {
	fn drop(&mut self) {
		let mut state = self.idle.state.lock().unwrap();
		state.count -= 1;
		if state.count == 0 {
			let task = tokio::spawn({
				let timeout = self.idle.timeout;
				let sender = self.idle.sender.clone();
				async move {
					tokio::time::sleep(timeout).await;
					sender.send_replace(true);
				}
			});
			state.task.replace(task);
		}
	}
}

impl<T> Body<T>
where
	T: hyper::body::Body<Data = Bytes, Error = Error>,
{
	#[must_use]
	pub fn new(token: Token, body: T) -> Self
	where
		T: hyper::body::Body,
	{
		Self { body, token }
	}
}

impl<T> hyper::body::Body for Body<T>
where
	T: hyper::body::Body<Data = Bytes, Error = Error>,
{
	type Data = Bytes;

	type Error = Error;

	fn poll_frame(
		self: Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
		let this = self.project();
		this.body.poll_frame(cx)
	}
}
