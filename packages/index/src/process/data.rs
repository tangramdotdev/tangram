use tangram_client::prelude::*;

pub fn serialize(data: &tg::process::Data) -> tg::Result<Vec<u8>> {
	tangram_serialize::to_vec(data)
		.map_err(|error| tg::error!(!error, "failed to serialize the process data"))
}

pub fn deserialize<T>(fragments: &[T]) -> tg::Result<tg::process::Data>
where
	T: AsRef<[u8]>,
{
	let bytes = fragments
		.iter()
		.flat_map(|fragment| fragment.as_ref().iter().copied())
		.collect::<Vec<_>>();
	let data = tangram_serialize::from_slice(&bytes)
		.map_err(|error| tg::error!(!error, "failed to deserialize the process data"))?;

	Ok(data)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn fragments_roundtrip() {
		// Create process data whose serialization spans several fragments.
		let data = tg::process::Data {
			cacheable: false,
			checksum: tg::process::Checksum::default(),
			children: None,
			command: tg::Referent::with_node(tg::Either::Right(tg::command::Id::new(b"command"))),
			created_at: 1,
			creator: tg::Principal::Root,
			debug: None,
			error: None,
			exit: Some(0),
			finished_at: Some(3),
			host: "host".repeat(1_000),
			log: None,
			output: Some(tg::Referent::with_node(tg::value::Data::String(
				"output".repeat(10_000),
			))),
			owner: tg::Principal::Root,
			retry: false,
			sandbox: Some(tg::sandbox::Id::new()),
			started_at: Some(2),
			status: tg::process::Status::Finished,
			stderr: tg::process::Stdio::default(),
			stdin: tg::process::Stdio::default(),
			stdout: tg::process::Stdio::default(),
			tty: None,
		};
		let bytes = serialize(&data).unwrap();

		// Split the data into fragments of several sizes and verify that it is reassembled.
		for size in [1, 7, 10_000, bytes.len(), bytes.len() + 1] {
			let fragments = bytes.chunks(size).collect::<Vec<_>>();
			assert_eq!(fragments.len(), bytes.len().div_ceil(size));
			assert!(fragments.iter().all(|fragment| fragment.len() <= size));
			let output = deserialize(&fragments).unwrap();
			assert_eq!(serialize(&output).unwrap(), bytes);
		}
	}

	#[test]
	fn missing_fragments_fail() {
		// Verify that an empty fragment list does not deserialize.
		let fragments: [&[u8]; 0] = [];
		assert!(deserialize(&fragments).is_err());
	}
}
