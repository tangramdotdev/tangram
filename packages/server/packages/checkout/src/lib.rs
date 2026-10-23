use {
	futures::{FutureExt as _, StreamExt as _, future::BoxFuture, stream, stream::BoxStream},
	std::{
		collections::BTreeMap,
		path::PathBuf,
		sync::{Arc, Mutex},
	},
	tangram_client::prelude::*,
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tangram_server_core::{
		Session,
		checkout::{Guard, NamedNode},
	},
};

mod external;

pub(crate) mod internal;

type Tasks = tangram_futures::task::Map<
	tg::artifact::Id,
	tg::Result<()>,
	tg::progress::Handle<()>,
	tg::id::BuildHasher,
>;

type GraphTasks = tangram_futures::task::Map<
	tg::graph::Id,
	tg::Result<()>,
	tg::progress::Handle<()>,
	tg::id::BuildHasher,
>;

/// The component.
#[derive(Clone)]
pub struct Component(Arc<State>);

pub struct State {
	graph_tasks: GraphTasks,
	tasks: Tasks,
	xattr_value_size: Mutex<Option<usize>>,
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

pub(crate) struct NamedTree {
	pub ancestors: Vec<NamedNode>,
	pub nodes: Vec<NamedNode>,
}

impl Component {
	#[must_use]
	pub fn new() -> Self {
		// Create the checkout graph tasks.
		let graph_tasks = tangram_futures::task::Map::default();

		// Create the checkout tasks.
		let tasks = tangram_futures::task::Map::default();

		let xattr_value_size = Mutex::new(None);
		let state = State {
			graph_tasks,
			tasks,
			xattr_value_size,
		};

		Self(Arc::new(state))
	}

	pub(crate) async fn checkout(
		&self,
		session: &Session,
		arg: tg::checkout::Arg,
	) -> tg::Result<BoxStream<'static, tg::Result<tg::progress::Event<tg::checkout::Output>>>> {
		if !session.server.checkouts_enabled() {
			return Err(tg::error!("checkouts are disabled"));
		}
		if arg.path.is_some() {
			if arg.nodes.len() != 1 {
				return Err(tg::error!("an external checkout requires exactly one node"));
			}
			let node = self
				.resolve_checkout_node(session, arg.nodes[0].clone(), false)
				.await?;
			let artifact = node
				.artifact
				.ok_or_else(|| tg::error!("an external checkout requires an artifact"))?;
			let stream = self
				.checkout_external(session, arg, artifact)
				.await?
				.boxed();

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

		let vfs_enabled = session.server.components.vfs.mounted();
		let mut nodes = Vec::with_capacity(arg.nodes.len());
		for node in arg.nodes {
			nodes.push(
				self.resolve_checkout_node(session, node, !vfs_enabled)
					.await?,
			);
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
			.map(|artifact| {
				Self::checkout_internal_path(session, &artifact.node, extension.as_deref())
			})
			.collect::<Vec<_>>();
		let paths = nodes
			.iter()
			.map(|node| {
				if let Some(named) = &node.named {
					Self::named_checkout_path(session, named, extension.as_deref())
				} else {
					let artifact = node.artifact.as_ref().unwrap();
					Self::checkout_internal_path(session, &artifact.node, extension.as_deref())
				}
			})
			.collect::<Vec<_>>();
		if vfs_enabled {
			let paths = paths
				.into_iter()
				.map(|path| session.guest_path_for_host_path(&path))
				.collect::<tg::Result<Vec<_>>>()?;
			let output = tg::checkout::Output { paths };
			let event = tg::progress::Event::Output(output);
			let stream = stream::once(async move { Ok(event) }).boxed();

			return Ok(stream);
		}
		let id_paths = artifacts
			.iter()
			.map(|artifact| Self::checkout_internal_path(session, &artifact.node, None))
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
			.checkout_internal(session, artifacts)
			.await?
			.then({
				let internal_output = internal_output.clone();
				let component = self.clone();
				let session = session.clone();
				move |event| {
					let internal_output = internal_output.clone();
					let component = component.clone();
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
								component
									.materialize_named_checkouts(
										&session,
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
		session: &Session,
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
			.checkout_named_nodes_local(session, &node.node, include_hierarchy)
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
		let authorization = session.authorize_batch(authorization_args, None).await?;
		tangram_server_core::authorization::check_exhaustion(&authorization)?;
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
			let authorized = parent_allowed && authorization.permissions.contains(permission);
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
				let id = tg::tag::Id::try_from(named_node.id.clone())?;
				let token = session
					.server
					.components
					.get
					.create_tag_target_token(session, &id, target, &node.options.tokens)
					.await?;
				let options = tg::referent::Options {
					location: node.options.location.clone(),
					tokens: tg::authorization::Tokens::with_authorization(token),
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
		session: &Session,
		artifact: &tg::artifact::Id,
		extension: Option<&str>,
	) -> PathBuf {
		let name = extension.map_or_else(
			|| artifact.to_string(),
			|extension| format!("{artifact}{extension}"),
		);

		session.server.store_path().join(name)
	}

	async fn checkout_pull_source(&self, session: &Session) -> tg::Result<Option<tg::Location>> {
		let remote = session
			.try_get_remote("default", tg::remote::get::Arg::default())
			.await?;
		let source = remote.map(|_| {
			tg::Location::Remote(tg::location::Remote {
				name: "default".to_owned(),
				region: None,
			})
		});
		Ok(source)
	}

	pub(crate) async fn checkout_request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the accept header"))?;

		// Get the arg.
		let arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;

		// Get the stream.
		let stream = self
			.checkout(session, arg)
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
				return Err(tg::error!(argument, %type_, %subtype, "invalid accept type"));
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

	pub(crate) async fn request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let method = request.method().clone();
		let path = request.uri().path().to_owned();
		let path_components = path.split('/').skip(1).collect::<Vec<_>>();
		match (method, path_components.as_slice()) {
			(http::Method::POST, ["checkout"]) => self.checkout_request(session, request).await,
			(_, _) => Ok(tangram_server_core::http::not_found()),
		}
	}

	pub(crate) fn internal_xattr_capabilities(
		&self,
		server: &tangram_server_core::Server,
	) -> tg::Result<tg::file::xattrs::Options> {
		let mut cached = self.xattr_value_size.lock().unwrap();
		let max_value_size = if let Some(max_value_size) = *cached {
			max_value_size
		} else {
			let path = server.checkout_path();
			let max_value_size =
				tangram_server_core::checkout::xattrs::probe(&path)?.max_value_size;
			cached.replace(max_value_size);
			max_value_size
		};

		let options = tg::file::xattrs::Options { max_value_size };

		Ok(options)
	}
}

impl Default for Component {
	fn default() -> Self {
		Self::new()
	}
}

impl std::ops::Deref for Component {
	type Target = State;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl tangram_server_core::component::Lifecycle for Component {
	fn stop<'a>(&'a self, _server: &'a tangram_server_core::Server) -> BoxFuture<'a, ()> {
		async move {
			// Abort the checkout graph tasks.
			self.graph_tasks.abort_all();
			let results = self.graph_tasks.wait().await;
			for result in results {
				if let Err(error) = result
					&& !error.is_cancelled()
				{
					tracing::error!(?error, "a checkout graph task failed");
				}
			}
			tracing::trace!("checkout graph tasks");

			// Abort the checkout tasks.
			self.tasks.abort_all();
			let results = self.tasks.wait().await;
			for result in results {
				if let Err(error) = result
					&& !error.is_cancelled()
				{
					tracing::error!(?error, "a checkout task panicked");
				}
			}
			tracing::trace!("checkout tasks");
		}
		.boxed()
	}

	fn abort(&self) {
		self.graph_tasks.abort_all();
		self.tasks.abort_all();
	}
}

impl tangram_server_core::checkout::Component for Component {
	fn internal_xattr_capabilities(
		&self,
		server: &tangram_server_core::Server,
	) -> tg::Result<tg::file::xattrs::Options> {
		self.internal_xattr_capabilities(server)
	}

	fn checkout_internal<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		artifacts: Vec<tg::Referent<tg::artifact::Id>>,
	) -> BoxFuture<'a, tg::Result<BoxStream<'static, tg::Result<tg::progress::Event<()>>>>> {
		self.checkout_internal(session, artifacts).boxed()
	}

	fn checkout_await_indexing<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.checkout_await_indexing(session).boxed()
	}

	fn remove_all_named_checkout_entries_with_lock<'a>(
		&'a self,
		server: &'a tangram_server_core::Server,
		guard: &'a tangram_server_core::checkout::Guard<'_>,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.remove_all_named_checkout_entries_with_lock(server, guard)
			.boxed()
	}

	fn remove_named_checkout_entry_with_lock<'a>(
		&'a self,
		server: &'a tangram_server_core::Server,
		guard: &'a tangram_server_core::checkout::Guard<'_>,
		id: &'a tg::Id,
		specifier: &'a tg::Specifier,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.remove_named_checkout_entry_with_lock(server, guard, id, specifier)
			.boxed()
	}

	fn try_get_named_checkout_nodes<'a>(
		&'a self,
		server: &'a tangram_server_core::Server,
		ids: &'a [tg::Id],
	) -> BoxFuture<'a, tg::Result<BTreeMap<tg::Id, tangram_server_core::checkout::NamedNode>>> {
		self.try_get_named_checkout_nodes(server, ids).boxed()
	}

	fn checkout<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		arg: tg::checkout::Arg,
	) -> BoxFuture<
		'a,
		tg::Result<BoxStream<'static, tg::Result<tg::progress::Event<tg::checkout::Output>>>>,
	> {
		self.checkout(session, arg).boxed()
	}

	fn request<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>> {
		self.request(session, request).boxed()
	}
}
