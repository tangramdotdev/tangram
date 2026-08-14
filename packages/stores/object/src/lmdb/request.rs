pub(super) enum Request {
	Delete(super::delete::Request),
	DeleteBatch(Vec<super::delete::Request>),
	DeleteOutboxFragments(crate::outbox::DeleteArg),
	EnqueueOutboxBatch(crate::outbox::Batch),
	Put(super::put::Request),
	PutBatch(Vec<super::put::Request>),
}
