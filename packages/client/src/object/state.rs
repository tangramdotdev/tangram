use std::sync::Arc;

#[derive(Debug)]
pub struct State<I, O> {
	pub id: Option<I>,
	pub object: Option<Arc<O>>,
}

impl<I, O> State<I, O> {
	#[must_use]
	pub fn new(id: Option<I>, object: Option<impl Into<Arc<O>>>) -> Self {
		assert!(id.is_some() || object.is_some());
		let object = object.map(Into::into);
		Self { id, object }
	}

	#[must_use]
	pub fn with_id(id: I) -> Self {
		Self {
			id: Some(id),
			object: None,
		}
	}

	#[must_use]
	pub fn with_object(object: impl Into<Arc<O>>) -> Self {
		Self {
			id: None,
			object: Some(object.into()),
		}
	}

	#[must_use]
	pub fn id(&self) -> Option<&I> {
		self.id.as_ref()
	}

	#[must_use]
	pub fn object(&self) -> Option<&Arc<O>> {
		self.object.as_ref()
	}
}
