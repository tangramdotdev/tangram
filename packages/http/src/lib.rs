pub mod body;
pub mod error;
pub mod flow;
pub mod header;
pub mod http2;
pub mod idle;
pub mod layer;
pub mod request;
pub mod response;
pub mod sse;

pub type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

pub type Result<T, E = Error> = std::result::Result<T, E>;
