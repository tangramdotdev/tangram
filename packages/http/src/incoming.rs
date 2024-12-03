// use std::sync::Arc;

pub mod request;
pub mod response;

// pub struct Incoming {
// 	arc: Option<Arc<dyn std::any::Any + Send + Sync>>,
// 	inner: hyper::body::Incoming,
// }

pub type Incoming = hyper::body::Incoming;
