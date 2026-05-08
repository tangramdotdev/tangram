use {crate::prelude::*, bytes::Bytes};

#[derive(Clone, Debug, Default)]
pub struct Builder {
	kind: Kind,
}

#[derive(Clone, Debug)]
enum Kind {
	Leaf(Bytes),
	Branch(Vec<tg::blob::Child>),
}

impl Builder {
	#[must_use]
	pub fn new() -> Self {
		Self::default()
	}

	#[must_use]
	pub fn bytes(mut self, bytes: impl Into<Bytes>) -> Self {
		self.kind = Kind::Leaf(bytes.into());
		self
	}

	#[must_use]
	pub fn child(mut self, child: tg::blob::Child) -> Self {
		match &mut self.kind {
			Kind::Leaf(_) => {
				self.kind = Kind::Branch(vec![child]);
			},
			Kind::Branch(children) => {
				children.push(child);
			},
		}
		self
	}

	#[must_use]
	pub fn children(mut self, children: impl IntoIterator<Item = tg::blob::Child>) -> Self {
		self.kind = Kind::Branch(children.into_iter().collect());
		self
	}

	#[must_use]
	pub fn build(self) -> tg::Blob {
		match self.kind {
			Kind::Leaf(bytes) => {
				tg::Blob::with_object(tg::blob::Object::Leaf(tg::blob::object::Leaf { bytes }))
			},
			Kind::Branch(children) => match children.len() {
				0 => tg::Blob::with_object(tg::blob::Object::Leaf(tg::blob::object::Leaf {
					bytes: Bytes::new(),
				})),
				1 => children.into_iter().next().unwrap().blob,
				_ => tg::Blob::with_object(tg::blob::Object::Branch(tg::blob::object::Branch {
					children,
				})),
			},
		}
	}
}

impl Default for Kind {
	fn default() -> Self {
		Self::Leaf(Bytes::new())
	}
}
