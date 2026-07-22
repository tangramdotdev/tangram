use {napi_derive::napi, tangram_client as tg};

#[napi]
pub fn object_id(input: String) -> napi::Result<String> {
	let input = input.into_boxed_str();
	object_id_inner(&input).map_err(|error| to_napi_error(&error))
}

#[napi]
pub fn parse_value(input: String) -> napi::Result<String> {
	let input = input.into_boxed_str();
	parse_value_inner(&input).map_err(|error| to_napi_error(&error))
}

#[napi]
pub fn stringify_value(input: String) -> napi::Result<String> {
	let input = input.into_boxed_str();
	stringify_value_inner(&input).map_err(|error| to_napi_error(&error))
}

fn to_napi_error(error: &tg::Error) -> napi::Error {
	napi::Error::from_reason(error.to_string())
}

fn object_id_inner(input: &str) -> tg::Result<String> {
	let data = serde_json::from_str::<tg::object::Data>(input)
		.map_err(|error| tg::error!(!error, "failed to deserialize the object data"))?;
	let bytes = data
		.serialize()
		.map_err(|error| tg::error!(!error, "failed to serialize the object data"))?;
	let id = tg::object::Id::new(data.kind(), &bytes);

	Ok(id.to_string())
}

fn parse_value_inner(input: &str) -> tg::Result<String> {
	let value = input
		.parse::<tg::Value>()
		.map_err(|error| tg::error!(!error, "failed to parse the value"))?;
	let output = serde_json::to_string(&value.to_data())
		.map_err(|error| tg::error!(!error, "failed to serialize the value data"))?;

	Ok(output)
}

fn stringify_value_inner(input: &str) -> tg::Result<String> {
	let data = serde_json::from_str::<tg::value::Data>(input)
		.map_err(|error| tg::error!(!error, "failed to deserialize the value data"))?;
	let value = tg::Value::try_from_data(data)
		.map_err(|error| tg::error!(!error, "failed to convert the value"))?;

	Ok(value.to_string())
}
