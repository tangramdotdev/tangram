use super::*;

#[test]
fn build_keeps_children_that_fit_in_one_branch() {
	let blob = tg::Blob::new(children(tg::blob::MAX_BRANCH_CHILDREN));
	assert_eq!(shape(&blob), vec![1; tg::blob::MAX_BRANCH_CHILDREN]);
}

#[test]
fn build_nests_each_full_chunk_of_children_in_a_branch() {
	let blob = tg::Blob::new(children(2 * tg::blob::MAX_BRANCH_CHILDREN + 1));
	let length = u64::try_from(tg::blob::MAX_BRANCH_CHILDREN).unwrap();
	assert_eq!(shape(&blob), vec![length, length, 1]);
}

#[test]
fn with_bytes_creates_a_leaf_for_bytes_smaller_than_the_minimum_leaf_size() {
	let bytes = vec![1; tg::blob::MIN_LEAF_SIZE];
	let blob = tg::Blob::with_bytes(bytes.clone());
	let Some(tg::object::Object::Blob(object)) = blob.state().object() else {
		panic!("expected a blob object");
	};
	let tg::blob::Object::Leaf(leaf) = object.as_ref() else {
		panic!("expected a blob leaf");
	};
	assert_eq!(leaf.bytes.as_ref(), bytes.as_slice());
}

#[test]
fn with_bytes_splits_bytes_into_leaves() {
	let bytes = (0..4 * tg::blob::MAX_LEAF_SIZE)
		.map(|index| u8::try_from((index.wrapping_mul(2_654_435_761) >> 24) & 0xff).unwrap())
		.collect::<Vec<_>>();
	let blob = tg::Blob::with_bytes(bytes.clone());
	let lengths = shape(&blob);
	let expected = tg::blob::chunks(&bytes)
		.into_iter()
		.map(|range| u64::try_from(range.len()).unwrap())
		.collect::<Vec<_>>();
	assert!(lengths.len() > 1);
	assert_eq!(lengths, expected);
}

fn children(count: usize) -> Vec<tg::blob::Child> {
	(0..count)
		.map(|index| {
			let blob = tg::Blob::leaf(index.to_string().into_bytes());
			tg::blob::Child { blob, length: 1 }
		})
		.collect()
}

// Get the lengths of a branch's children.
fn shape(blob: &tg::Blob) -> Vec<u64> {
	let Some(tg::object::Object::Blob(object)) = blob.state().object() else {
		panic!("expected a blob object");
	};
	let tg::blob::Object::Branch(branch) = object.as_ref() else {
		panic!("expected a blob branch");
	};
	branch.children.iter().map(|child| child.length).collect()
}
