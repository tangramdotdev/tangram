use tangram_client::prelude::*;

#[must_use]
pub fn create() -> (tg::token::Id, String) {
	let id = tg::token::Id::new();
	let bytes = rand::random::<[u8; 32]>();
	let token = tg::id::ENCODING.encode(&bytes);

	(id, token)
}

#[must_use]
pub fn hash(token: &str) -> String {
	blake3::Hash::from_bytes(hash_bytes(token))
		.to_hex()
		.to_string()
}

#[must_use]
pub fn hash_bytes(token: &str) -> [u8; 32] {
	*blake3::hash(token.as_bytes()).as_bytes()
}

#[must_use]
pub fn matches(actual: &str, expected: &str) -> bool {
	let actual = blake3::hash(actual.as_bytes());
	let expected = blake3::hash(expected.as_bytes());
	aws_lc_rs::constant_time::verify_slices_are_equal(actual.as_bytes(), expected.as_bytes())
		.is_ok()
}
