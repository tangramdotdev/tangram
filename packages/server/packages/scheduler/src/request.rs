use {
	crate::Component,
	std::sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
	tangram_client::prelude::*,
	tangram_server_core::{Server, Session, service},
	tangram_server_scheduler_core::{RequestArg, ResponseOutput},
};

pub(crate) type Request = service::Request<RequestArg>;

pub(crate) type Response = service::Response<ResponseOutput>;

pub(crate) type Sender = service::Sender<Kind>;

/// The scheduler kind of server.
pub(crate) struct Kind;

/// The handler that sends the accepted requests to the scheduling task through a bounded channel.
pub(crate) struct Handler {
	pub available: Arc<AtomicBool>,
	pub sender: tokio::sync::mpsc::Sender<Request>,
}

impl Component {
	pub(crate) async fn send_scheduler_request(
		&self,
		session: &Session,
		arg: RequestArg,
	) -> tg::Result<(tg::scheduler::Id, tg::Result<ResponseOutput>)> {
		// Send the request to one scheduler until it answers, and move to another scheduler only if it is unavailable.
		let server = &session.server;
		let config = &server.config.scheduler.request;
		let retry = config.retry.clone().into();
		service::retry(
			&self.schedulers,
			server,
			&retry,
			config.response_ttl,
			service::start(),
			|scheduler, id| {
				let arg = arg.clone();
				async move { Self::send_scheduler_request_inner(server, &scheduler, &id, arg).await }
			},
		)
		.await
	}

	pub(crate) async fn send_scheduler_request_to(
		&self,
		session: &Session,
		scheduler: &tg::scheduler::Id,
		arg: RequestArg,
	) -> tg::Result<tg::Result<ResponseOutput>> {
		// Send the request to the scheduler until it answers and accepts the request.
		let server = &session.server;
		let config = &server.config.scheduler.request;
		let retry = config.retry.clone().into();
		service::retry_to(&retry, config.response_ttl, |id| {
			let arg = arg.clone();
			async move { Self::send_scheduler_request_inner(server, scheduler, &id, arg).await }
		})
		.await
	}

	async fn send_scheduler_request_inner(
		server: &Server,
		scheduler: &tg::scheduler::Id,
		id: &str,
		arg: RequestArg,
	) -> tg::Result<Option<tg::Result<ResponseOutput>>> {
		let config = &server.config.scheduler.request;
		let options = tangram_server_core::control::Options {
			retry: config.retry.clone().into(),
			timeout: config.timeout,
		};
		service::send::<Kind>(server, scheduler, id, arg, options).await
	}
}

impl service::Kind for Kind {
	type Arg = RequestArg;

	type Id = tg::scheduler::Id;

	type Output = ResponseOutput;

	const NAME: &'static str = "scheduler";

	async fn available(server: &Server) -> tg::Result<Vec<Self::Id>> {
		let schedulers = server
			.index
			.get_schedulers()
			.await
			.map_err(|error| tg::error!(!error, "failed to get the schedulers"))?;
		let schedulers = schedulers
			.into_iter()
			.map(|scheduler| scheduler.id)
			.collect();

		Ok(schedulers)
	}
}

impl service::Handler<Kind> for Handler {
	type Accepted = ();

	fn accept(&mut self, _request: &Request) -> Option<Self::Accepted> {
		self.available.load(Ordering::SeqCst).then_some(())
	}

	async fn handle(
		&mut self,
		request: Request,
		(): Self::Accepted,
		_sender: Sender,
	) -> tg::Result<()> {
		self.sender
			.send(request)
			.await
			.map_err(|_| tg::error!("the scheduling task stopped"))?;

		Ok(())
	}
}
