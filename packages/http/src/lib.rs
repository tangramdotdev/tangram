pub use self::{incoming::Incoming, outgoing::Outgoing};

pub mod idle;
pub mod incoming;
pub mod outgoing;
pub mod sse;

pub type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
