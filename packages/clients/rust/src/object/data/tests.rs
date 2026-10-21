use {super::*, std::collections::BTreeMap};

#[test]
fn check_limits_accepts_an_object_at_the_limits() {
	let data = directory(tg::object::MAX_CHILDREN);
	assert!(data.check_limits(tg::object::MAX_SIZE).is_ok());
}

#[test]
fn check_limits_rejects_an_object_that_is_too_large() {
	let data = directory(0);
	let error = data.check_limits(tg::object::MAX_SIZE + 1).unwrap_err();
	assert_eq!(error.to_string(), "the object is too large");
}

#[test]
fn check_limits_rejects_an_object_with_too_many_children() {
	let data = directory(tg::object::MAX_CHILDREN + 1);
	let error = data.check_limits(0).unwrap_err();
	assert_eq!(error.to_string(), "the object has too many children");
}

#[test]
fn check_limits_counts_unique_children() {
	let file = tg::file::Id::new(b"file");
	let entries = (0..=tg::object::MAX_CHILDREN)
		.map(|index| {
			let edge = tg::graph::data::Edge::Object(file.clone().into());
			(index.to_string(), edge)
		})
		.collect();
	let data = leaf(entries);
	assert!(data.check_limits(0).is_ok());
}

fn directory(children: usize) -> Object {
	let entries = (0..children)
		.map(|index| {
			let file = tg::file::Id::new(index.to_string().as_bytes());
			let edge = tg::graph::data::Edge::Object(file.into());
			(index.to_string(), edge)
		})
		.collect();
	leaf(entries)
}

fn leaf(entries: BTreeMap<String, tg::graph::data::Edge<tg::artifact::Id>>) -> Object {
	let leaf = tg::graph::data::DirectoryLeaf { entries };
	let node = tg::graph::data::Directory::Leaf(leaf);
	Object::Directory(tg::directory::Data::Node(node))
}
