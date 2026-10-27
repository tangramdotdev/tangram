use {
	tangram_cache::{archive, index, log, object},
	tangram_client::prelude::*,
};

pub(crate) type Receiver = tokio::sync::mpsc::Receiver<(Request, ResponseSender)>;
pub(crate) type ResponseSender = tokio::sync::oneshot::Sender<tg::Result<Response>>;
pub(crate) type Sender = tokio::sync::mpsc::Sender<(Request, ResponseSender)>;

pub(crate) enum Request {
	GetArchiveQueueItems(archive::queue::get::batch::Arg),
	GetIndexQueueItems(index::queue::get::batch::Arg),
	GetLogCacheEntries(log::cache::get::Arg),
	GetObjectCacheEntries(object::cache::get::Arg),
	#[cfg(test)]
	GetTransactionId,
	TryGetArchiveQueueItem(archive::queue::get::Arg),
	TryGetIndexQueueItem(index::queue::get::Arg),
	TryGetLogEnd(tg::process::Id),
	TryGetLogLength(log::length::Arg),
	TryGetObject(object::get::Arg),
	TryGetObjectBatch(object::get::batch::Arg),
	TryReadLog(log::read::Arg),
}

pub(crate) enum Response {
	GetLogCacheEntries(Vec<log::cache::Entry>),
	GetArchiveQueueItems(Vec<archive::queue::Item>),
	GetIndexQueueItems(Vec<index::queue::Item>),
	GetObjectCacheEntries(Vec<object::cache::Entry>),
	#[cfg(test)]
	GetTransactionId(u64),
	TryGetArchiveQueueItem(Option<archive::queue::Item>),
	TryGetIndexQueueItem(Option<index::queue::Item>),
	TryGetLogEnd(Option<tg::process::log::End>),
	TryGetLogLength(Option<u64>),
	TryGetObject(object::get::Output),
	TryGetObjectBatch(Vec<object::get::Output>),
	TryReadLog(Vec<log::read::Entry<'static>>),
}
