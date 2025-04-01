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
		let path = temp.path().to_owned();
		let advanced = crate::config::Advanced {
			file_descriptor_semaphore_size: 1,
			..Default::default()
		};
		let authentication = None;
		let cleaner = None;
		let database = crate::config::Database::Sqlite(crate::config::SqliteDatabase {
			connections: 1,
			path: path.join("database"),
		});
		let index = crate::config::Index::Sqlite(crate::config::SqliteIndex {
			connections: 1,
			path: path.join("index"),
		});
		let indexer = Some(crate::config::Indexer::default());
		let messenger = crate::config::Messenger::default();
		let remotes = Some(Vec::new());
		let runner = Some(crate::config::Runner::default());
		let store = crate::config::Store::Lmdb(crate::config::LmdbStore {
			path: path.join("store"),
		});
		let http = Some(crate::config::Http::default());
		let version = None;
		let vfs = None;
		let watchdog = Some(crate::config::Watchdog::default());
		let config = Config {
			advanced,
			authentication,
			cleaner,
			database,
			http,
			index,
			indexer,
			messenger,
			path,
			remotes,
			runner,
			store,
			version,
			vfs,
			watchdog,
		};
		self.start_server_with_temp_and_config(temp, config).await
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
