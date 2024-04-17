use crate as tg;
use bytes::Bytes;
use http_body_util::BodyExt as _;

pub type Incoming = hyper::body::Incoming;

pub type Outgoing = http_body_util::combinators::UnsyncBoxBody<Bytes, tg::Error>;

#[must_use]
pub fn empty() -> Outgoing {
	http_body_util::Empty::new()
		.map_err(|_| unreachable!())
		.boxed_unsync()
}

#[must_use]
pub fn full(chunk: impl Into<Bytes>) -> Outgoing {
	http_body_util::Full::new(chunk.into())
		.map_err(|_| unreachable!())
		.boxed_unsync()
}
