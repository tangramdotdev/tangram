use {
	super::{
		Compiler,
		document::{Document, Key},
	},
	std::{collections::BTreeMap, sync::Mutex},
	tangram_client::prelude::*,
};

mod db;
mod library;
mod system;

pub mod load;
pub mod resolve;

type Documents = BTreeMap<Key, Document>;

type RequestSender = tokio::sync::mpsc::UnboundedSender<(Request, ResponseSender)>;
type ResponseSender = tokio::sync::oneshot::Sender<tg::Result<Response>>;

#[derive(Default)]
pub struct Service {
	request_sender: Mutex<Option<RequestSender>>,
	thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

enum Request {
	Check(Vec<tg::module::Data>),
	Query(crate::Request, tg::position::Encoding),
}

enum Response {
	Check(crate::check::Response),
	Query(crate::Response),
}

impl Service {
	#[must_use]
	pub fn new() -> Self {
		Self {
			request_sender: Mutex::new(None),
			thread: Mutex::new(None),
		}
	}

	async fn request(&self, compiler: &Compiler, request: Request) -> tg::Result<Response> {
		// Start the worker lazily, just like the TypeScript service.
		{
			let mut thread = self.thread.lock().unwrap();
			if thread.is_none() {
				let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
				self.request_sender.lock().unwrap().replace(sender);
				let compiler = compiler.clone();
				thread.replace(std::thread::spawn(move || run(&compiler, receiver)));
			}
		}

		// Serialize updates and analysis on the worker's persistent database.
		let (sender, receiver) = tokio::sync::oneshot::channel();
		self.request_sender
			.lock()
			.unwrap()
			.as_ref()
			.unwrap()
			.send((request, sender))
			.map_err(|error| tg::error!(!error, "failed to send the python request"))?;
		let response = receiver
			.await
			.map_err(|error| tg::error!(!error, "failed to receive the python response"))??;
		Ok(response)
	}

	#[must_use]
	pub fn is_started(&self) -> bool {
		self.thread.lock().unwrap().is_some()
	}

	pub fn stop(&self) {
		self.request_sender.lock().unwrap().take();
	}

	pub async fn join(&self) {
		let thread = self.thread.lock().unwrap().take();
		if let Some(thread) = thread {
			tokio::task::spawn_blocking(move || thread.join().unwrap())
				.await
				.unwrap();
		}
	}
}

impl Compiler {
	pub(super) async fn check_python(
		&self,
		modules: Vec<tg::module::Data>,
	) -> tg::Result<crate::check::Response> {
		let Response::Check(response) = self.python.request(self, Request::Check(modules)).await?
		else {
			return Err(tg::error!("unexpected python response"));
		};
		Ok(response)
	}

	pub(super) async fn request_python(
		&self,
		request: crate::Request,
	) -> tg::Result<crate::Response> {
		let encoding = *self.position_encoding.read().unwrap();
		let Response::Query(response) = self
			.python
			.request(self, Request::Query(request, encoding))
			.await?
		else {
			return Err(tg::error!("unexpected python response"));
		};
		Ok(response)
	}

	fn py_documents(&self) -> Documents {
		self.documents
			.iter()
			.filter(|entry| entry.open)
			.map(|entry| (entry.key().clone(), entry.value().clone()))
			.collect()
	}
}

fn run(
	compiler: &Compiler,
	mut receiver: tokio::sync::mpsc::UnboundedReceiver<(Request, ResponseSender)>,
) {
	let mut database = None;
	while let Some((request, sender)) = receiver.blocking_recv() {
		let response = (|| {
			if database.is_none() {
				database = Some(db::Database::new(compiler.clone(), Documents::new())?);
			}
			let database = database.as_mut().unwrap();
			database.update(compiler.py_documents())?;
			match request {
				Request::Check(modules) => database.check(modules).map(Response::Check),
				Request::Query(request, encoding) => {
					database.request(request, encoding).map(Response::Query)
				},
			}
		})();
		sender.send(response).ok();
	}
}
