use self::and_then_frame::AndThenFrame;

pub mod and_then_frame;

pub trait Ext: http_body::Body {
	fn and_then_frame<F, B>(self, f: F) -> AndThenFrame<Self, F>
	where
		Self: Sized,
		F: FnMut(http_body::Frame<Self::Data>) -> Result<http_body::Frame<B>, Self::Error>,
		B: bytes::Buf,
	{
		AndThenFrame::new(self, f)
	}
}
