use {std::slice, tangram_client as tg};

const HEADER_LENGTH: usize = 5;

#[unsafe(no_mangle)]
pub extern "C" fn allocate(length: usize) -> *mut u8 {
	let bytes = vec![0; length].into_boxed_slice();
	Box::into_raw(bytes).cast::<u8>()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn deallocate(pointer: *mut u8, length: usize) {
	if pointer.is_null() {
		return;
	}
	let bytes = std::ptr::slice_from_raw_parts_mut(pointer, length);
	// SAFETY: The pointer and length were returned by `allocate` or `response` and have not been
	// deallocated yet.
	drop(unsafe { Box::from_raw(bytes) });
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn object_id(pointer: *const u8, length: usize) -> *mut u8 {
	// SAFETY: The caller allocated the input buffer with `allocate` and initialized `length` bytes.
	let input = unsafe { slice::from_raw_parts(pointer, length) };
	response(object_id_inner(input))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn parse_value(pointer: *const u8, length: usize) -> *mut u8 {
	// SAFETY: The caller allocated the input buffer with `allocate` and initialized `length` bytes.
	let input = unsafe { slice::from_raw_parts(pointer, length) };
	response(parse_value_inner(input))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn stringify_value(pointer: *const u8, length: usize) -> *mut u8 {
	// SAFETY: The caller allocated the input buffer with `allocate` and initialized `length` bytes.
	let input = unsafe { slice::from_raw_parts(pointer, length) };
	response(stringify_value_inner(input))
}

fn object_id_inner(input: &[u8]) -> tg::Result<Vec<u8>> {
	let data = serde_json::from_slice::<tg::object::Data>(input)
		.map_err(|error| tg::error!(!error, "failed to deserialize the object data"))?;
	let bytes = data
		.serialize()
		.map_err(|error| tg::error!(!error, "failed to serialize the object data"))?;
	let id = tg::object::Id::new(data.kind(), &bytes);
	Ok(id.to_string().into_bytes())
}

fn parse_value_inner(input: &[u8]) -> tg::Result<Vec<u8>> {
	let input = std::str::from_utf8(input)
		.map_err(|error| tg::error!(!error, "failed to decode the value"))?;
	let value = input
		.parse::<tg::Value>()
		.map_err(|error| tg::error!(!error, "failed to parse the value"))?;
	serde_json::to_vec(&value.to_data())
		.map_err(|error| tg::error!(!error, "failed to serialize the value data"))
}

fn response(result: tg::Result<Vec<u8>>) -> *mut u8 {
	let (status, payload) = match result {
		Ok(payload) => (0, payload),
		Err(error) => (1, error.to_string().into_bytes()),
	};
	let length = u32::try_from(payload.len()).expect("the response is too large");
	let mut output = Vec::with_capacity(HEADER_LENGTH + payload.len());
	output.extend_from_slice(&length.to_le_bytes());
	output.push(status);
	output.extend_from_slice(&payload);
	let output = output.into_boxed_slice();
	Box::into_raw(output).cast::<u8>()
}

fn stringify_value_inner(input: &[u8]) -> tg::Result<Vec<u8>> {
	let data = serde_json::from_slice::<tg::value::Data>(input)
		.map_err(|error| tg::error!(!error, "failed to deserialize the value data"))?;
	let value = tg::Value::try_from_data(data)
		.map_err(|error| tg::error!(!error, "failed to convert the value"))?;
	Ok(value.to_string().into_bytes())
}
