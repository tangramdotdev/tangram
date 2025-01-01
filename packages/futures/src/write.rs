use futures::Future;
use std::pin::Pin;
use tokio::io::{AsyncWrite, AsyncWriteExt as _};

pub type Boxed<'a> = Pin<Box<dyn AsyncWrite + Send + 'a>>;

pub trait Ext: AsyncWrite {
	fn boxed<'a>(self) -> Boxed<'a>
	where
		Self: Sized + Send + 'a,
	{
		Box::pin(self)
	}

	fn write_uvarint(
		&mut self,
		mut value: u64,
	) -> impl Future<Output = std::io::Result<()>> + Send + '_
	where
		Self: Unpin + Send,
	{
		async move {
			loop {
				let mut byte = (value & 0x7F) as u8;
				value >>= 7;
				if value != 0 {
					byte |= 0x80;
				}
				self.write_all(&[byte]).await?;
				if value == 0 {
					break;
				}
			}
			Ok(())
		}
	}
}

impl<T> Ext for T where T: AsyncWrite {}
