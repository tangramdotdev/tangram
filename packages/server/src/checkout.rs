use {
	crate::Session,
	futures::{StreamExt as _, stream, stream::BoxStream},
	std::{
		fs::File,
		os::fd::AsRawFd as _,
		path::{Path, PathBuf},
		sync::Arc,
	},
	tangram_client::prelude::*,
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
};

mod external;
pub(super) mod internal;

pub(super) enum Lock {
	File {
		mutex: tokio::sync::Mutex<()>,
		path: PathBuf,
	},
	Mutex(tokio::sync::Mutex<()>),
}

pub(super) enum Guard<'a> {
	File {
		_file: File,
		_guard: tokio::sync::MutexGuard<'a, ()>,
	},
	Mutex {
		_guard: tokio::sync::MutexGuard<'a, ()>,
	},
}

struct InternalOutput {
	artifact_paths: Vec<PathBuf>,
	extension: Option<String>,
	id_paths: Vec<PathBuf>,
	named_checkouts: Vec<NamedCheckout>,
	paths: Vec<PathBuf>,
}

#[derive(Clone)]
struct Node {
	artifact: Option<tg::Referent<tg::artifact::Id>>,
	artifacts: Vec<tg::Referent<tg::artifact::Id>>,
	named: Option<NamedNode>,
	named_checkouts: Vec<NamedCheckout>,
}

#[derive(Clone)]
struct NamedCheckout {
	nodes: Vec<NamedNode>,
	target: Option<tg::Id>,
}

pub(super) struct NamedTree {
	pub ancestors: Vec<NamedNode>,
	pub nodes: Vec<NamedNode>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct NamedNode {
	pub id: tg::Id,
	pub parent: Option<tg::Id>,
	pub permissions: Vec<tg::authorization::Permission>,
	pub specifier: tg::Specifier,
	pub target: Option<tg::Either<tg::object::Id, tg::process::Id>>,
}

impl Lock {
	#[must_use]
	pub fn new(path: &Path, single_process: bool) -> Self {
		if single_process {
			Self::Mutex(tokio::sync::Mutex::new(()))
		} else {
			Self::File {
				mutex: tokio::sync::Mutex::new(()),
				path: path.to_owned(),
			}
		}
	}

	pub async fn acquire(&self) -> tg::Result<Guard<'_>> {
		match self {
			Self::File { mutex, path } => {
				let guard = mutex.lock().await;
				let path = path.clone();
				let file = tokio::task::spawn_blocking(move || {
					let file = std::fs::OpenOptions::new()
						.create(true)
						.read(true)
						.truncate(false)
						.write(true)
						.open(path)?;
					// SAFETY: The file descriptor is valid and remains open for the duration of the lock.
					let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
					if result != 0 {
						return Err(std::io::Error::last_os_error());
					}

					Ok::<_, std::io::Error>(file)
				})
				.await
				.map_err(|error| tg::error!(!error, "the checkout lock task panicked"))?
				.map_err(|error| tg::error!(!error, "failed to acquire the checkout lock file"))?;

				Ok(Guard::File {
					_file: file,
					_guard: guard,
				})
			},
			Self::Mutex(mutex) => {
				let guard = mutex.lock().await;

				Ok(Guard::Mutex { _guard: guard })
			},
		}
	}
}

impl Session {
	pub(crate) async fn checkout(
		&self,
		arg: tg::checkout::Arg,
	) -> tg::Result<BoxStream<'static, tg::Result<tg::progress::Event<tg::checkout::Output>>>> {
		if !self.server.checkouts_enabled() {
			return Err(tg::error!("checkouts are disabled"));
		}
		if arg.path.is_some() {
			if arg.nodes.len() != 1 {
				return Err(tg::error!("an external checkout requires exactly one node"));
			}
			let node = self
				.resolve_checkout_node(arg.nodes[0].clone(), false)
				.await?;
			let artifact = node
				.artifact
				.ok_or_else(|| tg::error!("an external checkout requires an artifact"))?;
			let stream = self.checkout_external(arg, artifact).await?.boxed();

			return Ok(stream);
		}
		if !arg.dependencies {
			return Err(tg::error!(
				"the dependencies option cannot be disabled for an internal checkout"
			));
		}
		if arg.force {
			return Err(tg::error!(
				"the force option cannot be set for an internal checkout"
			));
		}
		if matches!(
			arg.lock,
			Some(tg::checkout::Lock::Attr | tg::checkout::Lock::File)
		) {
			return Err(tg::error!(
				"the lock option cannot be set for an internal checkout"
			));
		}

		let vfs_enabled = self.server.vfs.lock().unwrap().is_some();
		let mut nodes = Vec::with_capacity(arg.nodes.len());
		for node in arg.nodes {
			nodes.push(self.resolve_checkout_node(node, !vfs_enabled).await?);
		}
		let extension = arg.extension;
		if extension.is_some() && nodes.iter().any(|node| node.artifact.is_none()) {
			return Err(tg::error!(
				"the extension option requires every node to resolve to an artifact"
			));
		}
		let artifacts = nodes
			.iter()
			.flat_map(|node| node.artifacts.clone())
			.collect::<Vec<_>>();
		let artifact_paths = artifacts
			.iter()
			.map(|artifact| self.checkout_internal_path(&artifact.node, extension.as_deref()))
			.collect::<Vec<_>>();
		let paths = nodes
			.iter()
			.map(|node| {
				if let Some(named) = &node.named {
					self.named_checkout_path(named, extension.as_deref())
				} else {
					let artifact = node.artifact.as_ref().unwrap();
					self.checkout_internal_path(&artifact.node, extension.as_deref())
				}
			})
			.collect::<Vec<_>>();
		if vfs_enabled {
			let paths = paths
				.into_iter()
				.map(|path| self.guest_path_for_host_path(&path))
				.collect::<tg::Result<Vec<_>>>()?;
			let output = tg::checkout::Output { paths };
			let event = tg::progress::Event::Output(output);
			let stream = stream::once(async move { Ok(event) }).boxed();

			return Ok(stream);
		}
		let id_paths = artifacts
			.iter()
			.map(|artifact| self.checkout_internal_path(&artifact.node, None))
			.collect::<Vec<_>>();
		let named_checkouts = nodes
			.into_iter()
			.flat_map(|node| node.named_checkouts)
			.collect();
		let internal_output = Arc::new(InternalOutput {
			artifact_paths,
			extension,
			id_paths,
			named_checkouts,
			paths,
		});

		let stream = self
			.checkout_internal(artifacts)
			.await?
			.then({
				let internal_output = internal_output.clone();
				let session = self.clone();
				move |event| {
					let internal_output = internal_output.clone();
					let session = session.clone();
					async move {
						let event = event?;
						let event = match event {
							tg::progress::Event::Output(()) => {
								if internal_output.extension.is_some() {
									for (id_path, artifact_path) in std::iter::zip(
										&internal_output.id_paths,
										&internal_output.artifact_paths,
									) {
										std::fs::hard_link(id_path, artifact_path).ok();
									}
								}
								session
									.materialize_named_checkouts(
										&internal_output.named_checkouts,
										internal_output.extension.as_deref(),
									)
									.await?;
								let paths = internal_output
									.paths
									.iter()
									.map(|path| session.guest_path_for_host_path(path))
									.collect::<tg::Result<Vec<_>>>()?;
								let output = tg::checkout::Output { paths };
								tg::progress::Event::Output(output)
							},
							event => event.map_output(|()| unreachable!()),
						};

						Ok(event)
					}
				}
			})
			.boxed();

		Ok(stream)
	}

	async fn resolve_checkout_node(
		&self,
		node: tg::Referent<tg::Id>,
		include_hierarchy: bool,
	) -> tg::Result<Node> {
		let artifact = tg::artifact::Id::try_from(node.node.clone()).ok();
		if let Some(id) = artifact {
			let artifact = node.map(|_| id);
			return Ok(Node {
				artifact: Some(artifact.clone()),
				artifacts: vec![artifact],
				named: None,
				named_checkouts: Vec::new(),
			});
		}
		if !matches!(
			node.node.kind(),
			tg::id::Kind::Group
				| tg::id::Kind::Organization
				| tg::id::Kind::Tag
				| tg::id::Kind::User
		) {
			if node.node.kind().is_object() {
				return Err(tg::error!("expected an artifact"));
			}
			return Err(tg::error!(kind = %node.node.kind(), "expected an object ID"));
		}
		if matches!(&node.options.location, Some(tg::Location::Remote(_))) {
			return Err(tg::error!(
				id = %node.node,
				"a named node checkout must be local"
			));
		}

		let tree = self
			.checkout_named_nodes_local(&node.node, include_hierarchy)
			.await?;
		let authorization_args = tree
			.nodes
			.iter()
			.map(|named| {
				let permission = Self::named_checkout_permission(&named.id)?;
				let resource = tg::Referent::with_node_and_tokens(
					tg::Selector::Id(named.id.clone()),
					node.options.tokens.clone(),
				);
				let permissions = tg::authorization::permission::Set::from_permission(permission);

				Ok::<_, tg::Error>((resource, permissions))
			})
			.collect::<tg::Result<Vec<_>>>()?;
		let authorization = self.authorize_batch(authorization_args).await?;
		let mut allowed = tree
			.ancestors
			.iter()
			.map(|ancestor| ancestor.id.clone())
			.collect::<std::collections::BTreeSet<_>>();
		let mut named_checkouts = Vec::new();
		if !tree.ancestors.is_empty() {
			named_checkouts.push(NamedCheckout {
				nodes: tree.ancestors,
				target: None,
			});
		}
		let mut artifact = None;
		let mut artifacts = Vec::new();
		let mut named = None;
		for (named_node, authorization) in std::iter::zip(tree.nodes, authorization) {
			let permission = Self::named_checkout_permission(&named_node.id)?;
			let parent_allowed = named_node.id == node.node
				|| named_node
					.parent
					.as_ref()
					.is_none_or(|parent| allowed.contains(parent));
			let authorized = parent_allowed
				&& authorization.is_some_and(|permissions| permissions.contains(permission));
			if !authorized {
				if named_node.id == node.node {
					return Err(tg::error!(id = %named_node.id, "unauthorized"));
				}
				continue;
			}
			allowed.insert(named_node.id.clone());
			let target = named_node.target.as_ref().map(|target| match target {
				tg::Either::Left(target) => tg::Id::from(target.clone()),
				tg::Either::Right(target) => tg::Id::from(target.clone()),
			});
			if named_node.id == node.node
				&& let Some(expected_artifact) = &node.options.artifact
			{
				let target = target.as_ref().ok_or_else(
					|| tg::error!(id = %named_node.id, "the tag does not have a target"),
				)?;
				let expected_object = tg::object::Id::from(expected_artifact.clone());
				if tg::Id::from(expected_object) != *target
					&& node.options.id.clone().map(tg::Id::from) != Some(target.clone())
				{
					return Err(tg::error!(
						id = %named_node.id,
						"the artifact does not belong to the tag target"
					));
				}
			}
			let target_artifact = if let Some(target) = &target
				&& let Ok(target_artifact) = tg::artifact::Id::try_from(target.clone())
			{
				let token = self.create_tag_target_token_with_permissions(
					target,
					named_node.permissions.clone(),
				)?;
				let options = tg::referent::Options {
					location: node.options.location.clone(),
					tokens: tg::authorization::Tokens::with_local(token),
					..Default::default()
				};
				Some(tg::Referent::new(target_artifact, options))
			} else {
				None
			};
			if named_node.id == node.node {
				artifact.clone_from(&target_artifact);
				named = Some(named_node.clone());
			}
			if let Some(target_artifact) = target_artifact {
				artifacts.push(target_artifact);
			}
			named_checkouts.push(NamedCheckout {
				nodes: vec![named_node],
				target,
			});
		}
		let named =
			named.ok_or_else(|| tg::error!(id = %node.node, "the named node was not found"))?;

		Ok(Node {
			artifact,
			artifacts,
			named: Some(named),
			named_checkouts,
		})
	}

	fn named_checkout_permission(id: &tg::Id) -> tg::Result<tg::authorization::Permission> {
		let permission = match id.kind() {
			tg::id::Kind::Group => tg::authorization::Permission::Group(
				tg::authorization::permission::group::Permission::Read,
			),
			tg::id::Kind::Organization => tg::authorization::Permission::Organization(
				tg::authorization::permission::organization::Permission::Read,
			),
			tg::id::Kind::Tag => tg::authorization::Permission::Tag(
				tg::authorization::permission::tag::Permission::Read,
			),
			tg::id::Kind::User => tg::authorization::Permission::User(
				tg::authorization::permission::user::Permission::Read,
			),
			_ => return Err(tg::error!(%id, "the node is not named")),
		};

		Ok(permission)
	}

	fn checkout_internal_path(
		&self,
		artifact: &tg::artifact::Id,
		extension: Option<&str>,
	) -> PathBuf {
		let name = extension.map_or_else(
			|| artifact.to_string(),
			|extension| format!("{artifact}{extension}"),
		);

		self.server.store_path().join(name)
	}

	pub(crate) async fn checkout_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;

		// Get the arg.
		let arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;

		// Get the stream.
		let stream = self
			.checkout(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to start the checkout"))?;

		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR) | (mime::TEXT, mime::EVENT_STREAM)) => {
				let content_type = mime::TEXT_EVENT_STREAM;
				let stream = stream.map(|result| match result {
					Ok(event) => event.try_into(),
					Err(error) => error.try_into(),
				});
				(Some(content_type), BoxBody::with_sse_stream(stream))
			},

			Some((type_, subtype)) => {
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
			},
		};

		// Create the response.
		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		let response = response.body(body).unwrap();

		Ok(response)
	}
}
