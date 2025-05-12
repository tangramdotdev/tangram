use std::sync::Arc;

#[derive(Debug)]
pub struct State<I, O> {
	pub id: Option<I>,
	pub object: Option<Arc<O>>,
	pub stored: bool,
}

impl<I, O> State<I, O> {
	#[must_use]
	pub fn new(id: Option<I>, object: Option<impl Into<Arc<O>>>) -> Self {
		assert!(id.is_some() || object.is_some());
		let object = object.map(Into::into);
		let stored = id.is_some();
		Self { id, object, stored }
	}

	#[must_use]
	pub fn with_id(id: I) -> Self {
		Self {
			id: Some(id),
			object: None,
			stored: true,
		}
	}

	#[must_use]
	pub fn with_object(object: impl Into<Arc<O>>) -> Self {
		Self {
			id: None,
			object: Some(object.into()),
			stored: false,
		}
	}
}
