use {foundationdb_tuple as fdbt, num_traits::ToPrimitive as _, tangram_client::prelude::*};

#[derive(Debug)]
pub enum Key<'a> {
	Indexer(&'a tg::indexer::Id),
	Log(crate::lmdb::log::Key<'a>),
	Object(crate::lmdb::object::Key<'a>),
	ObjectArchiveQueue {
		indexer: &'a tg::indexer::Id,
		sequence: u64,
	},
	ObjectCache(crate::object::cache::Entry),
	ObjectIndexQueue {
		indexer: &'a tg::indexer::Id,
		sequence: u64,
	},
}

#[derive(Clone, Copy, Debug, PartialEq, num_derive::FromPrimitive, num_derive::ToPrimitive)]
#[repr(u8)]
pub enum Kind {
	Indexer = 6,
	LogEntry = 2,
	LogStreamPosition = 3,
	Object = 0,
	ObjectArchiveQueue = 7,
	ObjectCache = 5,
	ObjectIndexQueue = 8,
}

impl fdbt::TuplePack for Key<'_> {
	fn pack<W: std::io::Write>(
		&self,
		writer: &mut W,
		tuple_depth: fdbt::TupleDepth,
	) -> std::io::Result<fdbt::VersionstampOffset> {
		match self {
			Self::Indexer(id) => {
				(Kind::Indexer.to_i32().unwrap(), id.to_bytes().as_ref()).pack(writer, tuple_depth)
			},
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
			Self::ObjectArchiveQueue { indexer, sequence } => (
				Kind::ObjectArchiveQueue.to_i32().unwrap(),
				indexer.to_bytes().as_ref(),
				sequence,
			)
				.pack(writer, tuple_depth),
			Self::ObjectCache(entry) => (
				Kind::ObjectCache.to_i32().unwrap(),
				entry.partition,
				entry.cache.as_slice(),
			)
				.pack(writer, tuple_depth),
			Self::ObjectIndexQueue { indexer, sequence } => (
				Kind::ObjectIndexQueue.to_i32().unwrap(),
				indexer.to_bytes().as_ref(),
				sequence,
			)
				.pack(writer, tuple_depth),
		}
	}
}
