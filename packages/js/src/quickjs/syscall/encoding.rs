use {
	super::Result,
	crate::quickjs::{serde::Serde, types::Uint8Array},
	tangram_client::prelude::*,
};

pub fn base64_decode(value: String) -> Result<Uint8Array> {
	let result = (|| {
		let bytes = data_encoding::BASE64
			.decode(value.as_bytes())
			.map_err(|source| tg::error!(!source, "failed to decode base64"))?;
		Ok(Uint8Array::from(bytes))
	})();
	Result(result)
}

pub fn base64_encode(value: Uint8Array) -> String {
	data_encoding::BASE64.encode(&value.0)
}

pub fn hex_decode(value: String) -> Result<Uint8Array> {
	let result = (|| {
		let bytes = data_encoding::HEXLOWER
			.decode(value.as_bytes())
			.map_err(|source| tg::error!(!source, "failed to decode hex"))?;
		Ok(Uint8Array::from(bytes))
	})();
	Result(result)
}

pub fn hex_encode(value: Uint8Array) -> String {
	data_encoding::HEXLOWER.encode(&value.0)
}

pub fn json_decode(value: String) -> Result<Serde<serde_json::Value>> {
	let result = (|| {
		let value: serde_json::Value = serde_json::from_str(&value)
			.map_err(|source| tg::error!(!source, "failed to decode json"))?;
		Ok(value)
	})();
	Result(result.map(Serde))
}

pub fn json_encode(value: Serde<serde_json::Value>) -> Result<String> {
	let Serde(value) = value;
	let result = (|| {
		let string = serde_json::to_string(&value)
			.map_err(|source| tg::error!(!source, "failed to encode json"))?;
		Ok(string)
	})();
	Result(result)
}

pub fn toml_decode(value: String) -> Result<Serde<toml::Value>> {
	let result = (|| {
		let value: toml::Value = toml::from_str(&value)
			.map_err(|source| tg::error!(!source, "failed to decode toml"))?;
		Ok(value)
	})();
	Result(result.map(Serde))
}

pub fn toml_encode(value: Serde<toml::Value>) -> Result<String> {
	let Serde(value) = value;
	let result = (|| {
		let string = toml::to_string(&value)
			.map_err(|source| tg::error!(!source, "failed to encode toml"))?;
		Ok(string)
	})();
	Result(result)
}

pub fn utf8_decode(value: Uint8Array) -> Result<String> {
	let result = (|| {
		let string = String::from_utf8(value.0.into())
			.map_err(|source| tg::error!(!source, "failed to decode utf8"))?;
		Ok(string)
	})();
	Result(result)
}

pub fn utf8_encode(value: String) -> Uint8Array {
	Uint8Array::from(value.into_bytes())
}

pub fn yaml_decode(value: String) -> Result<Serde<serde_yaml::Value>> {
	let result = (|| {
		let value: serde_yaml::Value = serde_yaml::from_str(&value)
			.map_err(|source| tg::error!(!source, "failed to decode yaml"))?;
		Ok(value)
	})();
	Result(result.map(Serde))
}

pub fn yaml_encode(value: Serde<serde_yaml::Value>) -> Result<String> {
	let Serde(value) = value;
	let result = (|| {
		let string = serde_yaml::to_string(&value)
			.map_err(|source| tg::error!(!source, "failed to encode yaml"))?;
		Ok(string)
	})();
	Result(result)
}
