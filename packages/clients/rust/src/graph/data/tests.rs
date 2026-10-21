use {super::*, std::collections::BTreeMap};

#[test]
fn a_leaf_serializes_like_a_graph_with_only_nodes() {
	let leaf = Leaf { nodes: files(2) };
	let bytes = Graph::Leaf(leaf.clone()).serialize().unwrap();
	let mut expected = vec![0];
	tangram_serialize::to_writer(&mut expected, &leaf).unwrap();
	assert_eq!(bytes.as_ref(), expected.as_slice());
	assert_eq!(Graph::deserialize(bytes).unwrap(), Graph::Leaf(leaf));
}

#[test]
fn a_branch_round_trips() {
	let branch = Branch {
		children: vec![Child {
			count: 2,
			graph: tg::graph::Id::new(b"graph"),
		}],
	};
	let graph = Graph::Branch(branch);
	let bytes = graph.serialize().unwrap();
	assert_eq!(bytes[0], 1);
	assert_eq!(Graph::deserialize(bytes).unwrap(), graph);
	let json = graph.serialize_json().unwrap();
	assert_eq!(Graph::deserialize(json).unwrap(), graph);
	assert_eq!(graph.count(), 2);
}

#[test]
fn split_keeps_nodes_that_fit_in_one_leaf() {
	let leaves = Leaf::split(files(tg::object::MAX_CHILDREN));
	assert_eq!(leaves.len(), 1);
	assert_eq!(leaves[0].nodes.len(), tg::object::MAX_CHILDREN);
}

#[test]
fn split_starts_a_new_leaf_when_the_children_do_not_fit() {
	let leaves = Leaf::split(files(tg::object::MAX_CHILDREN + 1));
	let counts = leaves
		.iter()
		.map(|leaf| leaf.nodes.len())
		.collect::<Vec<_>>();
	assert_eq!(counts, vec![tg::object::MAX_CHILDREN, 1]);
}

#[test]
fn split_starts_a_new_leaf_when_the_size_does_not_fit() {
	// Create symlink nodes with long paths and no children.
	let path = "x".repeat(100_000);
	let nodes = (0..25)
		.map(|_| {
			Node::Symlink(Symlink {
				artifact: None,
				path: Some(path.clone().into()),
			})
		})
		.collect::<Vec<_>>();
	let leaves = Leaf::split(nodes);
	assert!(leaves.len() > 1);
	for leaf in leaves {
		let bytes = Graph::Leaf(leaf).serialize().unwrap();
		assert!(bytes.len() <= tg::object::MAX_SIZE);
	}
}

#[test]
fn split_counts_each_child_once() {
	let contents = tg::blob::Id::new(b"contents");
	let nodes = (0..2 * tg::object::MAX_CHILDREN)
		.map(|_| {
			Node::File(File {
				contents: Some(contents.clone()),
				dependencies: BTreeMap::new(),
				executable: false,
				module: None,
			})
		})
		.collect();
	let leaves = Leaf::split(nodes);
	assert_eq!(leaves.len(), 1);
}

fn files(count: usize) -> Vec<Node> {
	(0..count)
		.map(|index| {
			Node::File(File {
				contents: Some(tg::blob::Id::new(index.to_string().as_bytes())),
				dependencies: BTreeMap::new(),
				executable: false,
				module: None,
			})
		})
		.collect()
}
