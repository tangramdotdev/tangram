use tangram_client::prelude::*;

pub(crate) const CHANNEL_CAPACITY: usize = 256;

pub(crate) type Receiver = tokio::sync::mpsc::Receiver<(Request, ResponseSender)>;
pub(crate) type ResponseSender = tokio::sync::oneshot::Sender<tg::Result<Response>>;
pub(crate) type Sender = tokio::sync::mpsc::Sender<(Request, ResponseSender)>;

pub(crate) enum Request {
	DequeueOutboxFragments(crate::outbox::DequeueArg),
	#[cfg(test)]
	GetTransactionId,
	TryGet(crate::TryGetArg),
	TryGetBatch(crate::TryGetBatchArg),
	TryGetOutboxBatchAtOrBefore(crate::outbox::TryGetBatchArg),
}

pub(crate) enum Response {
	DequeueOutboxFragments(Vec<crate::outbox::Fragment>),
	#[cfg(test)]
	GetTransactionId(u64),
	TryGet(crate::TryGetOutput),
	TryGetBatch(Vec<crate::TryGetOutput>),
	TryGetOutboxBatchAtOrBefore(Option<crate::outbox::BatchId>),
}
