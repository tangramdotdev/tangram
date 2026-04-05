use crate::prelude::*;

pub const PREFIX: &str = "TANGRAM_ENV_";

pub fn env() -> tg::Result<tg::value::Map> {
	let mut env = tg::value::Map::new();
	let mut prefixed = Vec::new();
	for (key, value) in std::env::vars() {
		if let Some(key) = key.strip_prefix(PREFIX) {
			prefixed.push((key.to_owned(), value));
		} else {
			env.insert(key, tg::Value::String(value));
		}
	}
	for (key, value) in prefixed {
		let value = value.parse::<tg::Value>().map_err(
			|source| tg::error!(!source, key = %key, "failed to parse the prefixed env var"),
		)?;
		if !env.contains_key(&key) {
			return Err(tg::error!(key = %key, "expected the env var for the prefixed env var"));
		}
		env.insert(key, value);
	}
	Ok(env)
}
