use std::{collections::VecDeque, sync::Arc};

use dashmap::DashMap;
use futures::{stream, Stream};
use tangram_client::{self as tg, handle::Ext};

#[derive(Debug)]
pub struct Graph<H> {
	handle: H,
	inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
	root: tg::object::Id,
	nodes: DashMap<tg::object::Id, Node>,
}

#[derive(Debug, Clone)]
pub struct Node {
	pub complete: bool,
	pub metadata: tg::object::Metadata,
	pub children: Vec<tg::object::Id>,
}

impl<H> Clone for Graph<H>
where
	H: tg::Handle,
{
	fn clone(&self) -> Self {
		Self {
			handle: self.handle.clone(),
			inner: self.inner.clone(),
		}
	}
}

impl<H> Graph<H>
where
	H: tg::Handle,
{
	pub async fn new(handle: &H, root: tg::object::Id) -> tg::Result<Self> {
		let graph = Self {
			handle: handle.clone(),
			inner: Arc::new(Inner {
				root: root.clone(),
				nodes: DashMap::new(),
			}),
		};
		graph.insert(root).await?;
		Ok(graph)
	}

	/// Return a stream that visits nodes in breadth-first order.
	pub async fn visitor(
		&self,
	) -> impl Stream<Item = tg::Result<(tg::object::Id, Node)>> + 'static + Send {
		struct State<H: tg::Handle> {
			graph: Graph<H>,
			queue: VecDeque<tg::object::Id>,
		}
		let state = State {
			graph: self.clone(),
			queue: vec![self.inner.root.clone()].into(),
		};
		stream::try_unfold(state, |mut state| async move {
			// Pop the next ID of the queue.
			let Some(next) = state.queue.pop_front() else {
				return Ok(None);
			};

			// Get or create a node for this object.
			let node = state.graph.get_or_insert(&next).await?;

			// Only enqueue children for incomplete nodes.
			if !node.complete {
				state.queue.extend(node.children.iter().cloned());
			}

			// Return the node and next state.
			Ok::<_, tg::Error>(Some(((next, node), state)))
		})
	}

	pub async fn mark_complete(&self, object: &tg::object::Id) -> tg::Result<()> {
		self.inner.nodes.get_mut(object).unwrap().complete = true;
		Ok(())
	}

	async fn insert(&self, object: tg::object::Id) -> tg::Result<()> {
		// Skip loading leafs, which cannot have children.
		if matches!(object.kind(), tg::object::Kind::Leaf) {
			let metadata = self.handle.get_object_metadata(&object).await?;
			let node = Node {
				complete: false,
				metadata,
				children: Vec::new(),
			};
			self.inner.nodes.insert(object, node);
			return Ok(());
		}

		// Load objects that have children.
		let output = self.handle.get_object(&object).await?;
		let data = tg::object::Data::deserialize(object.kind(), &output.bytes)?;
		let children = data.children().into_iter().collect::<Vec<_>>();

		// Create a node.
		let node = Node {
			complete: false,
			children: children.clone(),
			metadata: output.metadata,
		};
		self.inner.nodes.insert(object, node);

		Ok(())
	}

	async fn get_or_insert(&self, object: &tg::object::Id) -> tg::Result<Node> {
		if !self.inner.nodes.contains_key(object) {
			self.insert(object.clone()).await?;
		}
		Ok(self.inner.nodes.get(object).unwrap().clone())
	}
}
