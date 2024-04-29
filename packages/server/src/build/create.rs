use crate::{runtime::Trait as _, BuildPermit, Server};
use bytes::Bytes;
use either::Either;
use futures::{FutureExt as _, TryFutureExt as _};
use http_body_util::BodyExt as _;
use std::sync::Arc;
use tangram_client as tg;
use tangram_http::{Incoming, Outgoing};
use tangram_messenger::Messenger as _;

impl Server {
	pub async fn create_build(
		&self,
		arg: tg::build::create::Arg,
	) -> tg::Result<tg::build::create::Output> {
		// Get a local build if one exists that satisfies the retry constraint.
		let build = 'a: {
			// Find a build.
			let list_arg = tg::build::list::Arg {
				limit: Some(1),
				order: Some(tg::build::list::Order::CreatedAtDesc),
				status: None,
				target: Some(arg.target.clone()),
			};
			let Some(output) = self.list_builds(list_arg).await?.items.first().cloned() else {
				break 'a None;
			};
			let build = tg::Build::with_id(output.id);

			// Verify the build satisfies the retry constraint.
			let outcome_arg = tg::build::outcome::Arg {
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
			let list_arg = tg::build::list::Arg {
				limit: Some(1),
				order: Some(tg::build::list::Order::CreatedAtDesc),
				status: None,
				target: Some(arg.target.clone()),
			};
			let Some(output) = remote.list_builds(list_arg).await?.items.first().cloned() else {
				break 'a None;
			};
			let build = tg::Build::with_id(output.id);

			// Verify the build satisfies the retry constraint.
			let outcome_arg = tg::build::outcome::Arg {
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

			let output = tg::build::create::Output {
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
			let Ok(output) = remote.create_build(arg.clone()).await else {
				break 'a;
			};

			return Ok(output);
		}

		// Otherwise, create a new build.
		let id = tg::build::Id::new();

		// Get the host.
		let target = tg::Target::with_id(arg.target.clone());
		let host = target.host(self).await?;

		// Put the build.
		let put_arg = tg::build::put::Arg {
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
		self.put_build(&id, put_arg).await?;

		// Create the build.
		let build = tg::Build::with_id(id.clone());

		// Add the build to the parent.
		if let Some(parent) = arg.parent.as_ref() {
			self.add_build_child(parent, build.id()).await?;
		}

		// Send the message.
		self.messenger
			.publish("builds.created".to_owned(), Bytes::new())
			.await
			.map_err(|source| tg::error!(!source, "failed to publish"))?;

		// Spawn a task to dequeue and start the build when the parent's permit is available.
		let server = self.clone();
		let parent = arg.parent.clone();
		let build = build.clone();
		let task = async move {
			// Acquire the parent's permit.
			let Some(permit) = parent.as_ref().and_then(|parent| {
				server
					.build_permits
					.get(parent)
					.map(|permit| permit.clone())
			}) else {
				return;
			};
			let permit = permit
				.lock_owned()
				.map(|guard| BuildPermit(Either::Right(guard)))
				.await;

			// Start the build.
			server
				.try_start_build_internal(build, permit)
				.await
				.inspect_err(|error| tracing::error!(?error, "failed to start the build"))
				.ok();
		};
		tokio::spawn(task);

		let output = tg::build::create::Output { id };

		Ok(output)
	}

	pub(crate) async fn try_start_build_internal(
		&self,
		build: tg::Build,
		permit: BuildPermit,
	) -> tg::Result<bool> {
		// Attempt to start the build.
		if !self
			.try_start_build(build.id())
			.await?
			.ok_or_else(|| tg::error!("failed to find the build"))?
		{
			return Ok(false);
		};

		// Set the permit for the build.
		let permit = Arc::new(tokio::sync::Mutex::new(Some(permit)));
		self.build_permits.insert(build.id().clone(), permit);

		// Spawn the task.
		let server = self.clone();
		let build = build.clone();
		let task = async move {
			// Run the build.
			let result = server.start_build_internal_inner(build.clone()).await;
			let outcome = match result {
				Ok(outcome) => outcome,
				Err(error) => tg::build::Outcome::Failed(error),
			};

			// Set the build's outcome.
			let outcome = outcome.data(&server).await?;
			let arg = tg::build::finish::Arg { outcome };
			build
				.finish(&server, arg)
				.await
				.map_err(|source| tg::error!(!source, "failed to set the build outcome"))?;

			// Drop the build's permit.
			server.build_permits.remove(build.id());

			Ok::<_, tg::Error>(())
		}
		.inspect_err(|error| {
			tracing::error!(?error, "failed to run the build");
		})
		.map(|_| ());
		tokio::spawn(task);

		Ok(true)
	}

	async fn start_build_internal_inner(&self, build: tg::Build) -> tg::Result<tg::build::Outcome> {
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

impl Server {
	pub(crate) async fn handle_create_build_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let bytes = request
			.into_body()
			.collect()
			.await
			.map_err(|source| tg::error!(!source, "failed to read the body"))?
			.to_bytes();
		let arg = serde_json::from_slice(&bytes)
			.map_err(|source| tg::error!(!source, "failed to deserialize the body"))?;

		// Get or create the build.
		let output = handle.create_build(arg).await?;

		// Create the response.
		let response = http::Response::builder()
			.body(Outgoing::json(output))
			.unwrap();

		Ok(response)
	}
}
