use crate::{
	util::http::{full, Incoming, Outgoing},
	BuildState, Http, Server,
};
use http_body_util::BodyExt as _;
use std::sync::Arc;
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
			let Some(build) = self
				.list_builds(list_arg)
				.await?
				.items
				.first()
				.cloned()
				.map(|state| tg::Build::with_id(state.id))
			else {
				break 'a None;
			};

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
			let Some(build) = remote
				.list_builds(list_arg)
				.await?
				.items
				.first()
				.cloned()
				.map(|state| tg::Build::with_id(state.id))
			else {
				break 'a None;
			};

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
		let build_id = tg::build::Id::new();

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
		self.build_state
			.write()
			.unwrap()
			.insert(build_id.clone(), state);

		// Insert the build.
		let put_arg = tg::build::PutArg {
			id: build_id.clone(),
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
			queued_at: None,
			started_at: None,
			finished_at: None,
		};
		self.insert_build(&build_id, &put_arg).await?;

		// Add the build to the parent.
		if let Some(parent) = arg.parent.as_ref() {
			self.add_build_child(parent, &build_id).await?;
		}

		// Send the message.
		self.messenger.publish_to_build_created().await?;

		let output = tg::build::GetOrCreateOutput { id: build_id };

		Ok(output)
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
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["builds"] = path_components.as_slice() else {
			let path = request.uri().path();
			return Err(tg::error!(%path, "unexpected path"));
		};

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
