use tangram_client::prelude::*;

#[derive(Clone, Debug)]
pub struct Arg {
	pub id: tg::indexer::Id,
	pub value: Value,
}

#[derive(Clone, Debug)]
pub enum Value {
	ArchiveReadSequence(u64),
	ArchiveWriteSequence(u64),
	Available(bool),
	IndexReadSequence(u64),
	IndexWriteSequence(u64),
}
