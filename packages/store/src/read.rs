use {
	crate::{archive, index, log, object},
	tangram_client::prelude::*,
};

pub(crate) const CHANNEL_CAPACITY: usize = 256;

pub(crate) type Receiver = tokio::sync::mpsc::Receiver<(Request, ResponseSender)>;
pub(crate) type ResponseSender = tokio::sync::oneshot::Sender<tg::Result<Response>>;
pub(crate) type Sender = tokio::sync::mpsc::Sender<(Request, ResponseSender)>;

pub(crate) enum Request {
	GetArchiveQueueEntries(archive::queue::get::batch::Arg),
	GetIndexQueueFragments(index::queue::get::batch::Arg),
	GetObjectCacheEntries(object::cache::get::Arg),
	#[cfg(test)]
	GetTransactionId,
	TryGetArchiveQueueEntry(archive::queue::get::Arg),
	TryGetIndexQueueFragment(index::queue::get::Arg),
	TryGetLogEnd(tg::process::Id),
	TryGetLogLength(log::length::Arg),
	TryGetObject(object::get::Arg),
	TryGetObjectBatch(object::get::batch::Arg),
	TryReadLog(log::read::Arg),
}

pub(crate) enum Response {
	GetArchiveQueueEntries(Vec<archive::queue::Entry>),
	GetIndexQueueFragments(Vec<index::queue::Fragment>),
	GetObjectCacheEntries(Vec<object::cache::Entry>),
	#[cfg(test)]
	GetTransactionId(u64),
	TryGetArchiveQueueEntry(Option<archive::queue::Entry>),
	TryGetIndexQueueFragment(Option<index::queue::Fragment>),
	TryGetLogEnd(Option<tg::process::log::End>),
	TryGetLogLength(Option<u64>),
	TryGetObject(object::get::Output),
	TryGetObjectBatch(Vec<object::get::Output>),
	TryReadLog(Vec<log::read::Entry<'static>>),
}
