use {
	std::io::{Read, Write},
	tangram_client::prelude::*,
};

/// Encode a request payload of the form
/// `[u32 LE id_len][id bytes][u32 LE argc][per arg: u32 LE arg_len, arg bytes]`.
#[must_use]
pub fn encode_spawn_payload(sandbox_id: &str, argv: &[String]) -> Vec<u8> {
	let mut out =
		Vec::with_capacity(sandbox_id.len() + 8 + argv.iter().map(|s| s.len() + 4).sum::<usize>());
	let id_len = u32::try_from(sandbox_id.len()).expect("sandbox id length fits u32");
	out.extend_from_slice(&id_len.to_le_bytes());
	out.extend_from_slice(sandbox_id.as_bytes());
	let argc = u32::try_from(argv.len()).expect("argv length fits u32");
	out.extend_from_slice(&argc.to_le_bytes());
	for arg in argv {
		let len = u32::try_from(arg.len()).expect("arg length fits u32");
		out.extend_from_slice(&len.to_le_bytes());
		out.extend_from_slice(arg.as_bytes());
	}
	out
}

/// Decode a payload encoded with `encode_spawn_payload`.
pub fn decode_spawn_payload(payload: &[u8]) -> tg::Result<(String, Vec<String>)> {
	let mut cursor = payload;
	let id_len = read_u32(&mut cursor, "id length")? as usize;
	let id_bytes = take(&mut cursor, id_len, "sandbox id")?;
	let sandbox_id = std::str::from_utf8(id_bytes)
		.map_err(|source| tg::error!(!source, "sandbox id is not valid utf-8"))?
		.to_owned();
	let argc = read_u32(&mut cursor, "argc")? as usize;
	let mut argv = Vec::with_capacity(argc);
	for _ in 0..argc {
		let arg_len = read_u32(&mut cursor, "argv entry length")? as usize;
		let arg_bytes = take(&mut cursor, arg_len, "argv entry")?;
		let arg = std::str::from_utf8(arg_bytes)
			.map_err(|source| tg::error!(!source, "argv entry is not valid utf-8"))?
			.to_owned();
		argv.push(arg);
	}
	if !cursor.is_empty() {
		return Err(tg::error!("trailing bytes in spawn payload"));
	}
	Ok((sandbox_id, argv))
}

fn read_u32(cursor: &mut &[u8], context: &str) -> tg::Result<u32> {
	if cursor.len() < 4 {
		return Err(tg::error!("short {context}"));
	}
	let (head, rest) = cursor.split_at(4);
	*cursor = rest;
	Ok(u32::from_le_bytes(head.try_into().unwrap()))
}

fn take<'a>(cursor: &mut &'a [u8], n: usize, context: &str) -> tg::Result<&'a [u8]> {
	if cursor.len() < n {
		return Err(tg::error!("short {context}"));
	}
	let (head, rest) = cursor.split_at(n);
	*cursor = rest;
	Ok(head)
}

/// Request opcodes.
pub const OP_SPAWN_NETNS: u8 = 0x01;
pub const OP_SPAWN_PASTA: u8 = 0x02;
pub const OP_SPAWN_WRAPPER: u8 = 0x03;
pub const OP_WAIT_WRAPPER: u8 = 0x04;
pub const OP_DESTROY: u8 = 0x05;
pub const OP_SHUTDOWN: u8 = 0x06;

/// Response status codes.
pub const STATUS_OK: u8 = 0x00;
pub const STATUS_ERR: u8 = 0x01;

/// A request frame on the helper control socket. Layout:
/// `[u8: opcode][u32 LE: payload length][bytes: payload]`.
pub struct Request {
	pub opcode: u8,
	pub payload: Vec<u8>,
}

/// A response frame. Layout:
/// `[u8: status][u32 LE: payload length][bytes: payload]`.
pub struct Response {
	pub status: u8,
	pub payload: Vec<u8>,
}

impl Response {
	#[must_use]
	pub fn ok(payload: impl Into<Vec<u8>>) -> Self {
		Self {
			status: STATUS_OK,
			payload: payload.into(),
		}
	}

	#[must_use]
	pub fn err(message: impl AsRef<str>) -> Self {
		Self {
			status: STATUS_ERR,
			payload: message.as_ref().as_bytes().to_vec(),
		}
	}
}

pub fn write_request<W: Write>(mut writer: W, request: &Request) -> tg::Result<()> {
	writer
		.write_all(&[request.opcode])
		.map_err(|error| tg::error!(!error, "failed to write request opcode"))?;
	let len = u32::try_from(request.payload.len())
		.map_err(|source| tg::error!(!source, "request payload too large"))?;
	writer
		.write_all(&len.to_le_bytes())
		.map_err(|error| tg::error!(!error, "failed to write request length"))?;
	writer
		.write_all(&request.payload)
		.map_err(|error| tg::error!(!error, "failed to write request payload"))?;
	writer
		.flush()
		.map_err(|error| tg::error!(!error, "failed to flush the request"))?;
	Ok(())
}

pub fn read_request<R: Read>(mut reader: R) -> tg::Result<Option<Request>> {
	let mut opcode = [0u8; 1];
	match reader.read_exact(&mut opcode) {
		Ok(()) => (),
		Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
		Err(source) => return Err(tg::error!(!source, "failed to read request opcode")),
	}
	let mut len_bytes = [0u8; 4];
	reader
		.read_exact(&mut len_bytes)
		.map_err(|error| tg::error!(!error, "failed to read request length"))?;
	let len = u32::from_le_bytes(len_bytes) as usize;
	let mut payload = vec![0u8; len];
	reader
		.read_exact(&mut payload)
		.map_err(|error| tg::error!(!error, "failed to read request payload"))?;
	Ok(Some(Request {
		opcode: opcode[0],
		payload,
	}))
}

pub fn write_response<W: Write>(mut writer: W, response: &Response) -> tg::Result<()> {
	writer
		.write_all(&[response.status])
		.map_err(|error| tg::error!(!error, "failed to write response status"))?;
	let len = u32::try_from(response.payload.len())
		.map_err(|source| tg::error!(!source, "response payload too large"))?;
	writer
		.write_all(&len.to_le_bytes())
		.map_err(|error| tg::error!(!error, "failed to write response length"))?;
	writer
		.write_all(&response.payload)
		.map_err(|error| tg::error!(!error, "failed to write response payload"))?;
	writer
		.flush()
		.map_err(|error| tg::error!(!error, "failed to flush the response"))?;
	Ok(())
}

pub fn read_response<R: Read>(mut reader: R) -> tg::Result<Response> {
	let mut status = [0u8; 1];
	reader
		.read_exact(&mut status)
		.map_err(|error| tg::error!(!error, "failed to read response status"))?;
	let mut len_bytes = [0u8; 4];
	reader
		.read_exact(&mut len_bytes)
		.map_err(|error| tg::error!(!error, "failed to read response length"))?;
	let len = u32::from_le_bytes(len_bytes) as usize;
	let mut payload = vec![0u8; len];
	reader
		.read_exact(&mut payload)
		.map_err(|error| tg::error!(!error, "failed to read response payload"))?;
	Ok(Response {
		status: status[0],
		payload,
	})
}
