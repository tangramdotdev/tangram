use crate::{Config, Server};
use futures::{Future, FutureExt as _};
use std::{panic::AssertUnwindSafe, sync::Arc};
use tangram_temp::Temp;

pub async fn test<F, Fut>(f: F)
where
	F: FnOnce(Arc<tokio::sync::Mutex<Context>>) -> Fut,
	Fut: Future<Output = ()> + Send,
{
	// Create the context.
	let context = Arc::new(tokio::sync::Mutex::new(Context::new()));

	// Run the test and catch a panic if one occurs.
	let result = AssertUnwindSafe(f(context.clone())).catch_unwind().await;

	// Handle the result.
	let context = context.lock().await;
	for server in &context.servers {
		server.stop();
		server.wait().await;
	}
	match result {
		Ok(()) => (),
		Err(error) => {
			std::panic::resume_unwind(error);
		},
	}
}

#[derive(Default)]
pub struct Context {
	temps: Vec<Temp>,
	servers: Vec<Server>,
}

impl Context {
	#[must_use]
	pub fn new() -> Self {
		Self::default()
	}

	pub async fn start_server(&mut self) -> Server {
		let temp = Temp::new();
		let config = Config::with_path(temp.path().to_owned());
		self.temps.push(temp);
		let server = Server::start(config).await.unwrap();
		self.servers.push(server.clone());
		server
	}

	pub async fn start_server_with_temp_and_config(
		&mut self,
		temp: Temp,
		config: Config,
	) -> Server {
		self.temps.push(temp);
		let server = Server::start(config).await.unwrap();
		self.servers.push(server.clone());
		server
	}
}
