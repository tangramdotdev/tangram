pub use self::{body::Body, request::Request, response::Response};

pub mod body;
pub mod idle;
pub mod request;
pub mod response;
pub mod service;
pub mod sse;

pub type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

pub type Result<T, E = Error> = std::result::Result<T, E>;
