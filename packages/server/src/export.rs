use crate::Server;
use dashmap::DashMap;
use futures::{
	future,
	stream::{self, FuturesUnordered},
	FutureExt as _, Stream, StreamExt as _, TryStreamExt as _,
};
use itertools::Itertools as _;
use num::ToPrimitive as _;
use smallvec::SmallVec;
use std::{
	panic::AssertUnwindSafe,
	pin::{pin, Pin},
	sync::{Arc, RwLock, Weak},
	time::Duration,
};
use tangram_client::{self as tg, handle::Ext as _};
use tangram_either::Either;
use tangram_futures::stream::Ext as _;
use tangram_http::{request::Ext, Body};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::task::AbortOnDropHandle;

#[derive(Clone)]
struct Graph {
	nodes: Arc<DashMap<Either<tg::process::Id, tg::object::Id>, Arc<Node>, fnv::FnvBuildHasher>>,
}

struct Node {
	output: RwLock<Either<tg::process::put::Output, tg::object::put::Output>>,
	parents: RwLock<SmallVec<[Weak<Self>; 1]>>,
	children: RwLock<Vec<Arc<Self>>>,
}

pub(crate) struct InnerObjectOutput {
	pub(crate) count: u64,
	pub(crate) weight: u64,
}

pub(crate) struct InnerProcessOutput {
	pub(crate) process_count: u64,
	pub(crate) object_count: u64,
	pub(crate) object_weight: u64,
}

struct ProcessStats {
	process_count: Option<u64>,
	object_count: Option<u64>,
	object_weight: Option<u64>,
}

impl Server {
	pub async fn export(
		&self,
		arg: tg::export::Arg,
		stream: Pin<Box<dyn Stream<Item = tg::Result<tg::import::Event>> + Send + 'static>>,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::export::Event>> + Send + 'static> {
		Ok(stream::empty())
	}

	pub(crate) async fn handle_export_request<H>(
		handle: &H,
		request: http::Request<Body>,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		// Parse the arg.
		let arg = request
			.query_params::<tg::export::QueryArg>()
			.transpose()?
			.ok_or_else(|| tg::error!("query parameters required"))?
			.into();

		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()?;

		// Create the incoming stream.
		let stream = request
			.sse()
			.map_err(|source| tg::error!(!source, "failed to read an event"))
			.and_then(|event| {
				future::ready(
					if event.event.as_deref().is_some_and(|event| event == "error") {
						match event.try_into() {
							Ok(error) | Err(error) => Err(error),
						}
					} else {
						event.try_into()
					},
				)
			})
			.boxed();

		// Create the outgoing stream.
		let stream = handle.export(arg, stream).await?;

		// Create the response body.
		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			Some((mime::APPLICATION, mime::OCTET_STREAM)) => {
				let content_type = mime::APPLICATION_OCTET_STREAM;
				let stream = stream.then(|result| async {
					let frame = match result {
						Ok(item) => {
							let bytes = item.to_bytes().await;
							hyper::body::Frame::data(bytes)
						},
						Err(error) => {
							let mut trailers = http::HeaderMap::new();
							trailers.insert("x-tg-event", http::HeaderValue::from_static("error"));
							let json = serde_json::to_string(&error).unwrap();
							trailers
								.insert("x-tg-data", http::HeaderValue::from_str(&json).unwrap());
							hyper::body::Frame::trailers(trailers)
						},
					};
					Ok::<_, tg::Error>(frame)
				});
				(Some(content_type), Body::with_stream(stream))
			},
			_ => {
				return Err(tg::error!(?accept, "invalid accept header"));
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

impl Graph {
	fn new() -> Self {
		Self {
			nodes: Arc::new(DashMap::default()),
		}
	}

	fn insert(
		&self,
		parent: Option<&Either<&tg::process::Id, &tg::object::Id>>,
		item: &Either<&tg::process::Id, &tg::object::Id>,
	) -> bool {
		let item = item.cloned();

		// Check if the node already exists.
		if self.nodes.contains_key(&item) {
			return false;
		}

		// Create the node.
		let new_node = match item {
			Either::Left(_) => Node::process(),
			Either::Right(_) => Node::object(),
		};
		let new_node = Arc::new(new_node);

		// If there's a parent, create the relationship.
		if let Some(parent) = parent {
			let parent = parent.cloned();
			// Add the new node as a child of the parent.
			if let Some(parent_node) = self.nodes.get(&parent) {
				parent_node
					.children
					.write()
					.unwrap()
					.push(Arc::clone(&new_node));
				// Add the parent as a weak reference to the new node.
				new_node
					.parents
					.write()
					.unwrap()
					.push(Arc::downgrade(&parent_node));
			} else {
				tracing::debug!("parent not found");
			}
		}

		// Insert the new node.
		self.nodes.insert(item, new_node);

		true
	}

	fn is_complete(&self, item: &Either<&tg::process::Id, &tg::object::Id>) -> bool {
		let item = item.cloned();
		let Some(node) = self.nodes.get(&item) else {
			return false;
		};

		// If this node is complete, return true.
		if node.is_complete() {
			return true;
		}

		// If this node is not complete, check ancestors recursively.

		// Track a list of nodes to a specific ancestor.
		let mut completion_path = Vec::new();
		let found_complete_ancestor = Self::check_ancestor_completion(&node, &mut completion_path);

		// if we found a path to complete, mark intermediate nodes as complete.
		if found_complete_ancestor {
			for node in completion_path {
				let mut output = node.output.write().unwrap();
				match *output {
					Either::Left(ref mut process_output) => process_output.complete = true, // FIXME respect arg.
					Either::Right(ref mut object_output) => object_output.complete = true,
				}
			}
			true
		} else {
			false
		}
	}

	fn check_ancestor_completion(node: &Node, completion_path: &mut Vec<Arc<Node>>) -> bool {
		let parents = node.parents.read().unwrap();

		for parent_weak in parents.iter() {
			if let Some(parent) = parent_weak.upgrade() {
				// If this parent is complete, mark this path complete.
				if parent.is_complete() {
					return true;
				}

				// Cycle detection - if we already stored this node, keep looking.
				if completion_path
					.iter()
					.any(|node| Arc::ptr_eq(node, &parent))
				{
					continue;
				}

				// Add this path to the completion path and and check ancestors.
				completion_path.push(Arc::clone(&parent));
				if Self::check_ancestor_completion(&parent, completion_path) {
					// If we found one we're done.
					return true;
				}
				// If not, remove ourselves from the path and try again.
				completion_path.pop();
			} else {
				tracing::debug!("failed to upgrade weak pointer");
			}
		}

		false
	}

	fn get_output(
		&self,
		item: &Either<&tg::process::Id, &tg::object::Id>,
	) -> Option<Either<tg::process::put::Output, tg::object::put::Output>> {
		let item = item.cloned();
		let node = self.nodes.get(&item)?;
		let output = node.output.read().unwrap().clone();
		Some(output)
	}

	fn update_output(
		&self,
		output: &Either<&tg::import::ProcessOutput, &tg::import::ObjectOutput>,
	) -> bool {
		let (item, new_output) = match output {
			Either::Left(process) => (
				Either::Left(process.id.clone()),
				Either::Left(process.output.clone()),
			),
			Either::Right(object) => (
				Either::Right(object.id.clone()),
				Either::Right(object.output.clone()),
			),
		};
		if let Some(node) = self.nodes.get(&item) {
			let mut output = node.output.write().unwrap();
			match (&*output, &new_output) {
				(Either::Left(_), Either::Left(_)) | (Either::Right(_), Either::Right(_)) => {
					*output = new_output.clone();
					true
				},
				_ => {
					tracing::error!("attempted to update output with mismatched type");
					false
				},
			}
		} else {
			tracing::debug!("attempted to update output for non-existent node");
			false
		}
	}
}

impl Node {
	fn object() -> Self {
		Self {
			parents: RwLock::new(SmallVec::new()),
			children: RwLock::new(Vec::new()),
			output: RwLock::new(Either::Right(tg::object::put::Output { complete: false })),
		}
	}

	fn process() -> Self {
		Self {
			parents: RwLock::new(SmallVec::new()),
			children: RwLock::new(Vec::new()),
			output: RwLock::new(Either::Left(tg::process::put::Output {
				complete: false,
				commands_complete: false,
				logs_complete: false,
				outputs_complete: false,
			})),
		}
	}

	fn is_complete(&self) -> bool {
		let output = self.output.read().unwrap();
		match *output {
			Either::Left(ref process_output) => {
				// TODO logs/command/outputs?
				process_output.complete
			},
			Either::Right(ref object_output) => object_output.complete,
		}
	}
}
