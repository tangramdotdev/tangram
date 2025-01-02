use std::pin::Pin;
use tokio::io::AsyncRead;

pub enum Either<L, R> {
	Left(L),
	Right(R),
}

impl<L, R> Either<L, R> {
	#[must_use]
	pub fn as_pin_mut(self: Pin<&mut Self>) -> Either<Pin<&mut L>, Pin<&mut R>> {
		unsafe {
			match self.get_unchecked_mut() {
				Self::Left(left) => Either::Left(Pin::new_unchecked(left)),
				Self::Right(right) => Either::Right(Pin::new_unchecked(right)),
			}
		}
	}
}

impl<L, R> AsyncRead for Either<L, R>
where
	L: AsyncRead,
	R: AsyncRead,
{
	fn poll_read(
		self: Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
		buf: &mut tokio::io::ReadBuf<'_>,
	) -> std::task::Poll<std::io::Result<()>> {
		match self.as_pin_mut() {
			Either::Left(s) => s.poll_read(cx, buf),
			Either::Right(s) => s.poll_read(cx, buf),
		}
	}
}
