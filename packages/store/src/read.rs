use {
	crate::{log, object, outbox},
	tangram_client::prelude::*,
};

pub(crate) const CHANNEL_CAPACITY: usize = 256;

pub(crate) type Receiver = tokio::sync::mpsc::Receiver<(Request, ResponseSender)>;
pub(crate) type ResponseSender = tokio::sync::oneshot::Sender<tg::Result<Response>>;
pub(crate) type Sender = tokio::sync::mpsc::Sender<(Request, ResponseSender)>;

pub(crate) enum Request {
	DequeueOutboxFragments(outbox::fragment::dequeue::Arg),
	#[cfg(test)]
	GetTransactionId,
	TryGetLogLength(log::length::Arg),
	TryGetObject(object::get::Arg),
	TryGetObjectBatch(object::get::batch::Arg),
	TryGetOutboxBatchAtOrBefore(outbox::batch::get::Arg),
	TryReadLog(log::read::Arg),
}

pub(crate) enum Response {
	DequeueOutboxFragments(Vec<outbox::fragment::Fragment>),
	#[cfg(test)]
	GetTransactionId(u64),
	TryGetLogLength(Option<u64>),
	TryGetObject(object::get::Output),
	TryGetObjectBatch(Vec<object::get::Output>),
	TryGetOutboxBatchAtOrBefore(Option<outbox::batch::Id>),
	TryReadLog(Vec<log::read::Entry<'static>>),
}
