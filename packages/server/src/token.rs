use tangram_client::prelude::*;

pub(crate) fn create() -> (tg::token::Id, String) {
	let id = tg::token::Id::new();
	let bytes = rand::random::<[u8; 32]>();
	let token = tg::id::ENCODING.encode(&bytes);

	(id, token)
}

pub(crate) fn hash(token: &str) -> String {
	blake3::hash(token.as_bytes()).to_hex().to_string()
}

pub(crate) fn matches(actual: &str, expected: &str) -> bool {
	let actual = blake3::hash(actual.as_bytes());
	let expected = blake3::hash(expected.as_bytes());
	aws_lc_rs::constant_time::verify_slices_are_equal(actual.as_bytes(), expected.as_bytes())
		.is_ok()
}
