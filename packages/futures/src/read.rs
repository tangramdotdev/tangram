use self::attach::Attach;
use futures::Future;
use std::pin::Pin;
use tokio::io::{AsyncRead, AsyncReadExt as _};

pub mod attach;
pub mod shared_position_reader;

pub type Boxed<'a> = Pin<Box<dyn AsyncRead + Send + 'a>>;

pub trait Ext: AsyncRead {
	fn attach<T>(self, value: T) -> Attach<Self, T>
	where
		Self: Sized,
	{
		Attach::new(self, value)
	}

	fn boxed<'a>(self) -> Boxed<'a>
	where
		Self: Sized + Send + 'a,
	{
		Box::pin(self)
	}

	fn read_uvarint(&mut self) -> impl Future<Output = std::io::Result<u64>> + Send
	where
		Self: Unpin + Send,
	{
		async move {
			self.try_read_uvarint()
				.await?
				.ok_or_else(|| std::io::Error::other("expected a uvarint"))
		}
	}

	fn try_read_uvarint(&mut self) -> impl Future<Output = std::io::Result<Option<u64>>> + Send
	where
		Self: Unpin + Send,
	{
		async move {
			let mut result: u64 = 0;
			let mut shift = 0;
			loop {
				let mut byte: u8 = 0;
				let read = match self.read_exact(std::slice::from_mut(&mut byte)).await {
					Ok(read) => read,
					Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => 0,
					Err(error) => {
						return Err(error);
					},
				};
				if read == 0 && shift == 0 {
					return Ok(None);
				}
				result |= u64::from(byte & 0x7F) << shift;
				if byte & 0x80 == 0 {
					break;
				}
				shift += 7;
			}
			Ok(Some(result))
		}
	}
}

impl<T> Ext for T where T: AsyncRead {}
