use {super::*, std::collections::BTreeMap};

#[test]
fn with_entries_creates_a_leaf_when_the_entries_fit() {
	let directory = Directory::with_entries_and_limits(entries(2), 2, 2);
	assert_eq!(shape(&directory), "0,1");
}

#[test]
fn with_entries_splits_entries_into_leaves() {
	let directory = Directory::with_entries_and_limits(entries(5), 2, 4);
	assert_eq!(shape(&directory), "(2 1 0,1) (2 3 2,3) (1 4 4)");
}

#[test]
fn with_entries_nests_branches_until_the_children_fit() {
	let directory = Directory::with_entries_and_limits(entries(9), 2, 2);
	assert_eq!(
		shape(&directory),
		"(8 7 (4 3 (2 1 0,1) (2 3 2,3)) (4 7 (2 5 4,5) (2 7 6,7))) (1 8 (1 8 (1 8 8)))",
	);
}

fn entries(count: usize) -> BTreeMap<String, Edge<tg::Artifact>> {
	(0..count)
		.map(|index| (index.to_string(), Edge::Index(index)))
		.collect()
}

// Describe a leaf by its names and a branch by each child's count, last name, and node.
fn shape(directory: &Directory) -> String {
	match directory {
		Directory::Leaf(leaf) => leaf.entries.keys().cloned().collect::<Vec<_>>().join(","),
		Directory::Branch(branch) => branch
			.children
			.iter()
			.map(|child| {
				let Edge::Object(directory) = &child.directory else {
					panic!("expected an object edge");
				};
				let Some(tg::object::Object::Directory(object)) = directory.state().object() else {
					panic!("expected a directory object");
				};
				let tg::directory::Object::Node(node) = object.as_ref() else {
					panic!("expected a directory node");
				};
				format!("({} {} {})", child.count, child.last, shape(node))
			})
			.collect::<Vec<_>>()
			.join(" "),
	}
}
