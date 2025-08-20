use {
	super::state::{Directory, File, Graph, Node, Object, State, Symlink, Variant},
	crate::Server,
	std::{collections::BTreeMap, path::PathBuf},
	tangram_client as tg,
	tangram_either::Either,
};

#[derive(Clone)]
struct Checkpoint<'a> {
	candidates: Option<im::Vector<Candidate>>,
	graph: Graph,
	queue: im::Vector<Item>,
	solutions: im::HashMap<tg::Tag, usize, fnv::FnvBuildHasher>,
	state: &'a State,
	visited: im::HashSet<Item>,
}

#[derive(Clone, Debug)]
pub struct Candidate {
	object: tg::object::Id,
	tag: tg::Tag,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct Item {
	node: usize,
	reference: tg::Reference,
}

impl Server {
	pub(super) async fn checkin_solve(&self, state: &mut super::State) -> tg::Result<()> {
		// Create the checkpoints.
		let mut checkpoints = Vec::new();

		// Create the first checkpoint.
		let graph = state.graph.clone();
		let queue = graph.unresolved(0);
		let mut checkpoint = Checkpoint {
			candidates: None,
			graph,
			queue,
			solutions: im::HashMap::default(),
			state: &state,
			visited: im::HashSet::new(),
		};

		// // Solve.
		// while let Some(unresolved) = current.queue.pop_front() {
		// 	self.walk_edge(&mut checkpoints, &mut current, &state.progress, unresolved)
		// 		.await;
		// 	current.candidates.take();
		// }

		// // Validate.
		// if current.errored {
		// 	return Err(tg::error!("failed to solve dependencies"));
		// }

		// // Update state.
		// state.graph = current.graph;

		Ok(())
	}
}
