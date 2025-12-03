use {
	std::pin::Pin,
	tokio::io::{AsyncBufRead, AsyncRead, AsyncWrite},
};

pub use self::retry::retry;

pub mod attach;
pub mod either;
pub mod future;
pub mod read;
pub mod retry;
pub mod stream;
pub mod task;
pub mod write;

pub type BoxAsyncBufRead<'a> = Pin<Box<dyn AsyncBufRead + Send + Unpin + 'a>>;

pub type BoxAsyncRead<'a> = Pin<Box<dyn AsyncRead + Send + 'a>>;

pub type BoxAsyncWrite<'a> = Pin<Box<dyn AsyncWrite + Send + Unpin + 'a>>;
