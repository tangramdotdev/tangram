use {foundationdb_tuple as fdbt, num_traits::ToPrimitive as _, tangram_client::prelude::*};

#[derive(Debug)]
pub enum Key<'a> {
	Log(crate::lmdb::log::Key<'a>),
	Object(crate::lmdb::object::Key<'a>),
	ObjectArchiveOutbox(crate::object::archive::outbox::Entry),
	ObjectIndexOutbox(crate::lmdb::outbox::Key),
}

#[derive(Clone, Copy, Debug, PartialEq, num_derive::FromPrimitive, num_derive::ToPrimitive)]
#[repr(u8)]
pub enum Kind {
	LogEntry = 2,
	LogStreamPosition = 3,
	Object = 0,
	ObjectArchiveOutbox = 4,
	ObjectIndexOutboxFragment = 1,
}

impl fdbt::TuplePack for Key<'_> {
	fn pack<W: std::io::Write>(
		&self,
		writer: &mut W,
		tuple_depth: fdbt::TupleDepth,
	) -> std::io::Result<fdbt::VersionstampOffset> {
		match self {
			Self::Log(crate::lmdb::log::Key::Entry { position, process }) => (
				Kind::LogEntry.to_i32().unwrap(),
				process.to_bytes().as_ref(),
				position,
			)
				.pack(writer, tuple_depth),
			Self::Log(crate::lmdb::log::Key::StreamPosition {
				position,
				process,
				stream: tg::process::stdio::Stream::Stderr,
			}) => (
				Kind::LogStreamPosition.to_i32().unwrap(),
				process.to_bytes().as_ref(),
				2,
				position,
			)
				.pack(writer, tuple_depth),
			Self::Log(crate::lmdb::log::Key::StreamPosition {
				position,
				process,
				stream: tg::process::stdio::Stream::Stdin,
			}) => {
				let _ = (position, process);
				Err(std::io::Error::new(
					std::io::ErrorKind::InvalidInput,
					"invalid stdio stream",
				))
			},
			Self::Log(crate::lmdb::log::Key::StreamPosition {
				position,
				process,
				stream: tg::process::stdio::Stream::Stdout,
			}) => (
				Kind::LogStreamPosition.to_i32().unwrap(),
				process.to_bytes().as_ref(),
				1,
				position,
			)
				.pack(writer, tuple_depth),
			Self::Object(crate::lmdb::object::Key::Object(id)) => {
				(Kind::Object.to_i32().unwrap(), id.to_bytes().as_ref()).pack(writer, tuple_depth)
			},
			Self::ObjectArchiveOutbox(entry) => (
				Kind::ObjectArchiveOutbox.to_i32().unwrap(),
				entry.partition,
				entry.stored_at,
				entry.id.to_bytes().as_ref(),
			)
				.pack(writer, tuple_depth),
			Self::ObjectIndexOutbox(crate::lmdb::outbox::Key::Fragment {
				batch,
				index,
				partition,
			}) => (
				Kind::ObjectIndexOutboxFragment.to_i32().unwrap(),
				partition,
				batch.as_slice(),
				index,
			)
				.pack(writer, tuple_depth),
		}
	}
}
