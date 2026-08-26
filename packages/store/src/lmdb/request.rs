pub(super) enum Request {
	DeleteLog(crate::log::delete::Arg),
	DeleteObject(super::delete::Request),
	DeleteObjectBatch(Vec<super::delete::Request>),
	DeleteOutboxFragments(crate::outbox::fragment::delete::Arg),
	EnqueueOutboxBatch(crate::outbox::batch::enqueue::Arg),
	PutLogBatch(Vec<crate::log::put::Arg>),
	PutObject(super::put::Request),
	PutObjectBatch(Vec<super::put::Request>),
}
