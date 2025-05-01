use crate::compiler::Compiler;
use bytes::Bytes;
use tangram_client as tg;
use tangram_v8::Serde;

pub fn base64_decode(
	_scope: &mut v8::HandleScope,
	_compiler: &Compiler,
	args: (String,),
) -> tg::Result<Bytes> {
	let (value,) = args;
	let bytes = data_encoding::BASE64
		.decode(value.as_bytes())
		.map_err(|source| tg::error!(!source, "failed to decode the bytes"))?;
	Ok(bytes.into())
}

pub fn base64_encode(
	_scope: &mut v8::HandleScope,
	_compiler: &Compiler,
	args: (Bytes,),
) -> tg::Result<String> {
	let (value,) = args;
	let encoded = data_encoding::BASE64.encode(&value);
	Ok(encoded)
}

pub fn hex_decode(
	_scope: &mut v8::HandleScope,
	_compiler: &Compiler,
	args: (String,),
) -> tg::Result<Bytes> {
	let (string,) = args;
	let bytes = data_encoding::HEXLOWER
		.decode(string.as_bytes())
		.map_err(|source| tg::error!(!source, "failed to decode the string as hex"))?;
	Ok(bytes.into())
}

pub fn hex_encode(
	_scope: &mut v8::HandleScope,
	_compiler: &Compiler,
	args: (Bytes,),
) -> tg::Result<String> {
	let (bytes,) = args;
	let hex = data_encoding::HEXLOWER.encode(&bytes);
	Ok(hex)
}

pub fn json_decode(
	_scope: &mut v8::HandleScope,
	_compiler: &Compiler,
	args: (String,),
) -> tg::Result<Serde<serde_json::Value>> {
	let (json,) = args;
	let value = serde_json::from_str(&json)
		.map_err(|source| tg::error!(!source, "failed to decode the string as json"))?;
	let value = Serde::new(value);
	Ok(value)
}

pub fn json_encode(
	_scope: &mut v8::HandleScope,
	_compiler: &Compiler,
	args: (Serde<serde_json::Value>,),
) -> tg::Result<String> {
	let (value,) = args;
	let json = serde_json::to_string(&value.into_inner())
		.map_err(|source| tg::error!(!source, "failed to encode the value"))?;
	Ok(json)
}

pub fn toml_decode(
	_scope: &mut v8::HandleScope,
	_compiler: &Compiler,
	args: (String,),
) -> tg::Result<Serde<toml::Value>> {
	let (toml,) = args;
	let value = toml::from_str(&toml)
		.map_err(|source| tg::error!(!source, "failed to decode the string as toml"))?;
	let value = Serde::new(value);
	Ok(value)
}

pub fn toml_encode(
	_scope: &mut v8::HandleScope,
	_compiler: &Compiler,
	args: (Serde<toml::Value>,),
) -> tg::Result<String> {
	let (value,) = args;
	let toml = toml::to_string(&value.into_inner())
		.map_err(|source| tg::error!(!source, "failed to encode the value"))?;
	Ok(toml)
}

pub fn utf8_decode(
	_scope: &mut v8::HandleScope,
	_compiler: &Compiler,
	args: (Bytes,),
) -> tg::Result<String> {
	let (bytes,) = args;
	let string = String::from_utf8(bytes.into())
		.map_err(|source| tg::error!(!source, "failed to decode the bytes as UTF-8"))?;
	Ok(string)
}

pub fn utf8_encode(
	_scope: &mut v8::HandleScope,
	_compiler: &Compiler,
	args: (String,),
) -> tg::Result<Bytes> {
	let (string,) = args;
	let bytes = string.into_bytes().into();
	Ok(bytes)
}

pub fn yaml_decode(
	_scope: &mut v8::HandleScope,
	_compiler: &Compiler,
	args: (String,),
) -> tg::Result<Serde<serde_yaml::Value>> {
	let (yaml,) = args;
	let value = serde_yaml::from_str(&yaml)
		.map_err(|source| tg::error!(!source, "failed to decode the string as yaml"))?;
	let value = Serde::new(value);
	Ok(value)
}

pub fn yaml_encode(
	_scope: &mut v8::HandleScope,
	_compiler: &Compiler,
	args: (Serde<serde_yaml::Value>,),
) -> tg::Result<String> {
	let (value,) = args;
	let yaml = serde_yaml::to_string(&value.into_inner())
		.map_err(|source| tg::error!(!source, "failed to encode the value"))?;
	Ok(yaml)
}
