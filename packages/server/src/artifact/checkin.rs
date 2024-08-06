use crate::Server;
use futures::{
	future::{self, BoxFuture},
	stream, Stream, StreamExt as _,
};
use tangram_client as tg;
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};

impl Server {
	pub async fn check_in_artifact(
		&self,
		_arg: tg::artifact::checkin::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::artifact::checkin::Event>>> {
		Ok(stream::once(future::ready(todo!())))
	}

	pub(crate) fn try_store_artifact_future(
		&self,
		_id: &tg::artifact::Id,
	) -> BoxFuture<'static, tg::Result<bool>> {
		let server = self.clone();
		let id = id.clone();
		Box::pin(async move { server.try_store_artifact_inner(&id).await })
	}

	pub(crate) async fn try_store_artifact_inner(&self, id: &tg::artifact::Id) -> tg::Result<bool> {
		// Check if the artifact exists in the checkouts directory.
		let permit = self.file_descriptor_semaphore.acquire().await.unwrap();
		let path = self.checkouts_path().join(id.to_string());
		let exists = tokio::fs::try_exists(&path)
			.await
			.map_err(|source| tg::error!(!source, "failed to check if the file exists"))?;
		if !exists {
			return Ok(false);
		}
		drop(permit);

		// Create the state.
		let count = ProgressState {
			current: AtomicU64::new(0),
			total: None,
		};
		let weight = ProgressState {
			current: AtomicU64::new(0),
			total: None,
		};
		let graph = Mutex::new(graph::Graph::default());
		let visited = Mutex::new(BTreeMap::new());
		let state = Arc::new(State {
			count,
			weight,
			graph,
			visited,
		});

		// Check in the artifact.
		let arg = tg::artifact::checkin::Arg {
			dependencies: true,
			deterministic: true,
			destructive: false,
			locked: true,
			path: path.try_into()?,
		};
		self.check_in_artifact_inner(arg.clone(), &state).await?;
		let artifact_id = state
			.visited
			.lock()
			.unwrap()
			.get(&arg.path)
			.ok_or_else(|| tg::error!("invalid graph"))?
			.artifact_id
			.clone()
			.ok_or_else(|| tg::error!("invalid graph"))?;

		if &artifact_id != id {
			return Err(tg::error!("corrupted internal checkout"));
		}

		// Get a database connection.
		let mut connection = self
			.database
			.connection(db::Priority::Low)
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Begin a transaction.
		let transaction = connection
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

		// Collect the output.
		let output = state
			.visited
			.lock()
			.unwrap()
			.values()
			.cloned()
			.collect::<Vec<_>>();

		// Insert into the database.
		for output in output {
			// Validate output.
			let id = output
				.artifact_id
				.ok_or_else(|| tg::error!("invalid graph"))?;
			let data = output.data.ok_or_else(|| tg::error!("invalid graph"))?;
			let bytes = data.serialize()?;
			let count = output.count;
			let weight = output.weight;

			// Insert.
			let p = transaction.p();
			let statement = formatdoc!(
				"
					insert into objects (id, bytes, complete, count, weight, touched_at)
					values ({p}1, {p}2, {p}3, {p}4, {p}5, {p}6)
					on conflict (id) do update set touched_at = {p}6;
				"
			);
			let now = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();
			let params = db::params![id, bytes, 1, count, weight, now];
			transaction
				.execute(statement, params)
				.await
				.map_err(|source| {
					tg::error!(!source, "failed to put the artifact into the database")
				})?;
		}

		// Commit the transaction.
		transaction
			.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

		Ok(true)
	}
}

impl Server {
	pub(crate) async fn handle_check_in_artifact_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let arg = request.json().await?;
		let stream = handle.check_in_artifact(arg).await?;
		let sse = stream.map(|result| match result {
			Ok(tg::artifact::checkin::Event::Progress(progress)) => {
				let data = serde_json::to_string(&progress).unwrap();
				let event = tangram_http::sse::Event {
					data,
					..Default::default()
				};
				Ok::<_, tg::Error>(event)
			},
			Ok(tg::artifact::checkin::Event::End(artifact)) => {
				let event = "end".to_owned();
				let data = serde_json::to_string(&artifact).unwrap();
				let event = tangram_http::sse::Event {
					event: Some(event),
					data,
					..Default::default()
				};
				Ok::<_, tg::Error>(event)
			},
			Err(error) => {
				let data = serde_json::to_string(&error).unwrap();
				let event = "error".to_owned();
				let event = tangram_http::sse::Event {
					data,
					event: Some(event),
					..Default::default()
				};
				Ok::<_, tg::Error>(event)
			},
		});
		let body = Outgoing::sse(sse);
		let response = http::Response::builder().ok().body(body).unwrap();
		Ok(response)
	}
}
