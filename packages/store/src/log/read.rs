use {
	std::{borrow::Cow, collections::BTreeSet},
	tangram_client::prelude::*,
};

#[derive(Clone, Debug)]
pub struct Arg {
	pub length: u64,
	pub position: u64,
	pub process: tg::process::Id,
	pub streams: BTreeSet<tg::process::stdio::Stream>,
}

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct Entry<'a> {
	#[tangram_serialize(id = 0)]
	pub bytes: Cow<'a, [u8]>,

	#[tangram_serialize(id = 1)]
	pub position: u64,

	#[tangram_serialize(id = 2)]
	pub stream: tg::process::stdio::Stream,

	#[tangram_serialize(id = 3)]
	pub stream_position: u64,

	#[tangram_serialize(id = 4)]
	pub timestamp: i64,
}

impl Entry<'_> {
	#[must_use]
	pub fn into_static(self) -> Entry<'static> {
		Entry {
			bytes: Cow::Owned(self.bytes.into_owned()),
			position: self.position,
			stream: self.stream,
			stream_position: self.stream_position,
			timestamp: self.timestamp,
		}
	}
}
