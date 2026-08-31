use {
	crate::{log, object},
	tangram_client::prelude::*,
};

pub(crate) const CHANNEL_CAPACITY: usize = 256;

pub(crate) type Receiver = tokio::sync::mpsc::Receiver<(Request, ResponseSender)>;
pub(crate) type ResponseSender = tokio::sync::oneshot::Sender<tg::Result<Response>>;
pub(crate) type Sender = tokio::sync::mpsc::Sender<(Request, ResponseSender)>;

pub(crate) enum Request {
	DequeueObjectArchiveOutboxEntries(object::archive::outbox::dequeue::Arg),
	DequeueObjectIndexOutboxFragments(object::index::outbox::fragment::dequeue::Arg),
	GetObjectCacheEntries(object::cache::get::Arg),
	#[cfg(test)]
	GetTransactionId,
	TryGetLogLength(log::length::Arg),
	TryGetObject(object::get::Arg),
	TryGetObjectBatch(object::get::batch::Arg),
	TryGetObjectIndexOutboxBatchAtOrBefore(object::index::outbox::batch::get::Arg),
	TryReadLog(log::read::Arg),
}

pub(crate) enum Response {
	DequeueObjectArchiveOutboxEntries(Vec<object::archive::outbox::Entry>),
	DequeueObjectIndexOutboxFragments(Vec<object::index::outbox::fragment::Fragment>),
	GetObjectCacheEntries(Vec<object::cache::Entry>),
	#[cfg(test)]
	GetTransactionId(u64),
	TryGetLogLength(Option<u64>),
	TryGetObject(object::get::Output),
	TryGetObjectBatch(Vec<object::get::Output>),
	TryGetObjectIndexOutboxBatchAtOrBefore(Option<object::index::outbox::batch::Id>),
	TryReadLog(Vec<log::read::Entry<'static>>),
}
