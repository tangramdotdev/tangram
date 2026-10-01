use crate::prelude::*;

#[test]
fn empty_storage_does_not_prove_a_node() {
	let object = tg::object::storage::Set::empty();
	assert!(object.is_empty());
	assert!(!object.contains(tg::object::storage::Set::NODE));
	let process = tg::process::storage::Set::empty();
	assert!(process.is_empty());
	assert!(!process.contains(tg::process::storage::Set::NODE));
}

#[test]
fn storage_union_preserves_independent_requirements() {
	use tg::process::storage::Set;
	let mut storage = Set::NODE;
	storage.insert(Set::SUBTREE);
	storage.insert(Set::NODE_COMMAND_OBJECTS);
	assert!(storage.contains(Set::NODE | Set::SUBTREE | Set::NODE_COMMAND_OBJECTS));
	assert!(!storage.contains(Set::NODE_OUTPUT_OBJECTS));
	storage.remove(Set::SUBTREE);
	assert!(!storage.contains(Set::SUBTREE));
	assert!(storage.contains(Set::NODE | Set::NODE_COMMAND_OBJECTS));
}

#[test]
fn storage_sets_preserve_empty_node_and_subtree_on_the_wire() {
	for storage in [
		tg::storage::Set::Object(tg::object::storage::Set::empty()),
		tg::storage::Set::Object(tg::object::storage::Set::NODE),
		tg::storage::Set::Object(
			tg::object::storage::Set::NODE | tg::object::storage::Set::SUBTREE,
		),
		tg::storage::Set::Process(tg::process::storage::Set::empty()),
		tg::storage::Set::Process(tg::process::storage::Set::NODE),
		tg::storage::Set::Process(tg::process::storage::Set::all()),
	] {
		let json = serde_json::to_value(storage).unwrap();
		let decoded: tg::storage::Set = serde_json::from_value(json).unwrap();
		assert_eq!(decoded, storage);
		let bytes = tangram_serialize::to_vec(&storage).unwrap();
		let decoded: tg::storage::Set = tangram_serialize::from_slice(&bytes).unwrap();
		assert_eq!(decoded, storage);
		assert!(storage.contains(storage.empty_like()));
	}
	let json = serde_json::to_value(tg::object::storage::Set::NODE).unwrap();
	assert_eq!(json, serde_json::json!(["node"]));
}
