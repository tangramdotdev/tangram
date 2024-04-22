use crate::{
	runtime::Trait as _,
	util::http::{full, Incoming, Outgoing},
	BuildState, Http, Permit, Server,
};
use either::Either;
use futures::{future, FutureExt as _, TryFutureExt as _};
use http_body_util::BodyExt as _;
use std::{pin::pin, sync::Arc};
use tangram_client as tg;

impl Server {
	pub async fn get_or_create_build(
		&self,
		arg: tg::build::GetOrCreateArg,
	) -> tg::Result<tg::build::GetOrCreateOutput> {
		// Get a local build if one exists that satisfies the retry constraint.
		let build = 'a: {
			// Find a build.
			let list_arg = tg::build::ListArg {
				limit: Some(1),
				order: Some(tg::build::Order::CreatedAtDesc),
				status: None,
				target: Some(arg.target.clone()),
			};
			let Some(output) = self.list_builds(list_arg).await?.items.first().cloned() else {
				break 'a None;
			};
			let build = tg::Build::with_id(output.id);

			// Verify the build satisfies the retry constraint.
			let outcome_arg = tg::build::outcome::GetArg {
				timeout: Some(std::time::Duration::ZERO),
			};
			let outcome = build.get_outcome(self, outcome_arg).await?;
			if let Some(outcome) = outcome {
				if outcome.retry() <= arg.retry {
					break 'a None;
				}
			}

			// Touch the build.
			self.touch_build(build.id()).await?;

			Some(build)
		};

		// Get a remote build if one exists that satisfies the retry constraint.
		let build = 'a: {
			if let Some(build) = build {
				break 'a Some(build);
			}

			// Get the remote.
			let Some(remote) = self.remotes.first() else {
				break 'a None;
			};

			// Find a build.
			let list_arg = tg::build::ListArg {
				limit: Some(1),
				order: Some(tg::build::Order::CreatedAtDesc),
				status: None,
				target: Some(arg.target.clone()),
			};
			let Some(output) = remote.list_builds(list_arg).await?.items.first().cloned() else {
				break 'a None;
			};
			let build = tg::Build::with_id(output.id);

			// Verify the build satisfies the retry constraint.
			let outcome_arg = tg::build::outcome::GetArg {
				timeout: Some(std::time::Duration::ZERO),
			};
			let outcome = build.get_outcome(self, outcome_arg).await?;
			if let Some(outcome) = outcome {
				if outcome.retry() <= arg.retry {
					break 'a None;
				}
			}

			// Touch the build.
			remote.touch_build(build.id()).await?;

			Some(build)
		};

		// If a local or remote build was found that satisfies the retry constraint, then add it as a child of the parent and return it.
		if let Some(build) = build {
			// Add the build as a child of the parent.
			if let Some(parent) = arg.parent.as_ref() {
				self.add_build_child(parent, build.id()).await?;
			}

			let output = tg::build::GetOrCreateOutput {
				id: build.id().clone(),
			};

			return Ok(output);
		}

		// If the build has a parent and it is not local, or if the build has no parent and the remote flag is set, then attempt to get or create a remote build.
		'a: {
			// Determine if the build should be remote.
			let remote = if arg.remote {
				true
			} else if let Some(parent) = arg.parent.as_ref() {
				!self.get_build_exists_local(parent).await?
			} else {
				false
			};
			if !remote {
				break 'a;
			}

			// Get the remote.
			let Some(remote) = self.remotes.first() else {
				break 'a;
			};

			// Push the target.
			let object = tg::object::Handle::with_id(arg.target.clone().into());
			let Ok(()) = object.push(self, remote, None).await else {
				break 'a;
			};

			// Get or create the build on the remote.
			let Ok(output) = remote.get_or_create_build(arg.clone()).await else {
				break 'a;
			};

			return Ok(output);
		}

		// Otherwise, create a new build.
		let id = tg::build::Id::new();

		// Get the host.
		let target = tg::Target::with_id(arg.target.clone());
		let host = target.host(self).await?;

		// Create the build state.
		let permit = Arc::new(tokio::sync::Mutex::new(None));
		let (stop, _) = tokio::sync::watch::channel(false);
		let state = Arc::new(BuildState {
			permit,
			stop,
			task: std::sync::Mutex::new(None),
		});

		// Add the build state.
		self.build_state.write().unwrap().insert(id.clone(), state);

		// Put the build.
		let put_arg = tg::build::PutArg {
			id: id.clone(),
			children: Vec::new(),
			count: None,
			host: host.clone(),
			log: None,
			outcome: None,
			retry: arg.retry,
			status: tg::build::Status::Created,
			target: arg.target.clone(),
			weight: None,
			created_at: time::OffsetDateTime::now_utc(),
			dequeued_at: None,
			started_at: None,
			finished_at: None,
		};
		self.put_build(&id, &put_arg).await?;

		// Create the build.
		let build = tg::Build::with_id(id.clone());

		// Add the build to the parent.
		if let Some(parent) = arg.parent.as_ref() {
			self.add_build_child(parent, build.id()).await?;
		}

		// Send the message.
		self.messenger.publish_to_build_created().await?;

		// Spawn a task to dequeue and start the build when the parent's permit is available.
		let server = self.clone();
		let parent = arg.parent.clone();
		let build = build.clone();
		let task = async move {
			// Get the parent build's state if it is available.
			let Some(state) =
				parent.and_then(|parent| server.build_state.read().unwrap().get(&parent).cloned())
			else {
				return Ok(());
			};

			// Acquire the parent's permit.
			let permit = state
				.permit
				.clone()
				.lock_owned()
				.map(|guard| Permit(Either::Right(guard)))
				.await;

			// Start the build.
			server.start_build(build, permit).await?;

			Ok::<_, tg::Error>(())
		}
		.inspect_err(|error| tracing::error!(?error, "failed to start the build"));
		tokio::spawn(task);

		let output = tg::build::GetOrCreateOutput { id };

		Ok(output)
	}

	pub(crate) async fn start_build(&self, build: tg::Build, permit: Permit) -> tg::Result<()> {
		// Set the build's status to started.
		self.set_build_status(build.id(), tg::build::Status::Started)
			.await?;

		// Lock the build state.
		let state = self
			.build_state
			.write()
			.unwrap()
			.get_mut(build.id())
			.unwrap()
			.clone();

		// Set the permit.
		state.permit.lock().await.replace(permit);

		// Spawn the task.
		let server = self.clone();
		let build = build.clone();
		let mut stop = self
			.build_state
			.read()
			.unwrap()
			.get(build.id())
			.unwrap()
			.stop
			.subscribe();
		let task = async move {
			// Build.
			let inner = server.start_build_inner(build.clone());
			let stop = stop.wait_for(|s| *s);
			let outcome = match future::select(pin!(inner), pin!(stop)).await {
				future::Either::Left((Err(error), _)) => return Err(error),
				future::Either::Left((Ok(outcome), _)) => outcome,
				future::Either::Right(_) => tg::build::Outcome::Canceled,
			};

			// Set the build's outcome.
			build
				.set_outcome(&server, outcome)
				.await
				.map_err(|source| tg::error!(!source, "failed to set the build outcome"))?;

			// Remove the build state.
			server.build_state.write().unwrap().remove(build.id());

			Ok::<_, tg::Error>(())
		}
		.inspect_err(|error| {
			tracing::error!(?error, "failed to run the build");
		})
		.map(|_| ());
		let task = tokio::spawn(task);
		state.task.lock().unwrap().replace(task);

		// Unlock the build state.
		drop(state);

		Ok(())
	}

	async fn start_build_inner(&self, build: tg::Build) -> tg::Result<tg::build::Outcome> {
		// Get the runtime.
		let target = build.target(self).await?;
		let host = target.host(self).await?;
		let runtime = self
			.runtimes
			.read()
			.unwrap()
			.get(&*host)
			.ok_or_else(
				|| tg::error!(?id = build.id(), ?host = &*host, "no runtime to build the target"),
			)?
			.clone();

		// Build.
		let result = runtime.run(&build).await;

		// Log an error if one occurred.
		if let Err(error) = &result {
			let options = &self.options.advanced.error_trace_options;
			let trace = error.trace(options);
			let log = trace.to_string();
			build.add_log(self, log.into()).await?;
		}

		// Create the outcome.
		let outcome = match result {
			Ok(value) => tg::build::Outcome::Succeeded(value),
			Err(error) => tg::build::Outcome::Failed(error),
		};

		Ok(outcome)
	}
}

impl<H> Http<H>
where
	H: tg::Handle,
{
	pub async fn handle_get_or_create_build_request(
		&self,
		request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>> {
		// Read the body.
		let bytes = request
			.into_body()
			.collect()
			.await
			.map_err(|source| tg::error!(!source, "failed to read the body"))?
			.to_bytes();
		let arg = serde_json::from_slice(&bytes)
			.map_err(|source| tg::error!(!source, "failed to deserialize the body"))?;

		// Get or create the build.
		let output = self.handle.get_or_create_build(arg).await?;

		// Create the body.
		let body = serde_json::to_vec(&output)
			.map_err(|source| tg::error!(!source, "failed to serialize the body"))?;
		let body = full(body);

		// Create the response.
		let response = http::Response::builder().body(body).unwrap();

		Ok(response)
	}
}
