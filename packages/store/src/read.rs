use {
	crate::{log, object},
	tangram_client::prelude::*,
};

pub(crate) const CHANNEL_CAPACITY: usize = 256;

pub(crate) type Receiver = tokio::sync::mpsc::Receiver<(Request, ResponseSender)>;
pub(crate) type ResponseSender = tokio::sync::oneshot::Sender<tg::Result<Response>>;
pub(crate) type Sender = tokio::sync::mpsc::Sender<(Request, ResponseSender)>;

pub(crate) enum Request {
	GetIndexers,
	GetObjectCacheEntries(object::cache::get::Arg),
	#[cfg(test)]
	GetTransactionId,
	TryGetLogLength(log::length::Arg),
	TryGetIndexer(crate::indexer::get::Arg),
	TryGetObject(object::get::Arg),
	TryGetObjectArchiveQueueEntry(object::archive::queue::get::Arg),
	TryGetObjectBatch(object::get::batch::Arg),
	TryGetObjectIndexQueueFragment(object::index::queue::get::Arg),
	TryReadLog(log::read::Arg),
}

pub(crate) enum Response {
	GetIndexers(Vec<crate::indexer::Indexer>),
	GetObjectCacheEntries(Vec<object::cache::Entry>),
	#[cfg(test)]
	GetTransactionId(u64),
	TryGetLogLength(Option<u64>),
	TryGetIndexer(Option<crate::indexer::Indexer>),
	TryGetObject(object::get::Output),
	TryGetObjectArchiveQueueEntry(Option<object::archive::queue::Entry>),
	TryGetObjectBatch(Vec<object::get::Output>),
	TryGetObjectIndexQueueFragment(Option<object::index::queue::Fragment>),
	TryReadLog(Vec<log::read::Entry<'static>>),
}
