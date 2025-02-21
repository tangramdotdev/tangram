use crate::{Config, Server};
use futures::FutureExt as _;
use std::panic::AssertUnwindSafe;
use tangram_temp::Temp;

pub async fn test<F>(f: F)
where
	F: AsyncFnOnce(&mut Context) -> () + Send,
{
	// Create the context.
	let mut context = Context::new();

	// Run the test and catch a panic if one occurs.
	let result = AssertUnwindSafe(f(&mut context)).catch_unwind().await;

	// Handle the result.
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
