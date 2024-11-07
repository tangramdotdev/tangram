use super::Data;
use crate as tg;

#[derive(Clone, Debug)]
pub struct Branch {
	pub children: Vec<Child>,
}

#[derive(Clone, Debug)]
pub struct Child {
	pub blob: tg::Blob,
	pub size: u64,
}

impl Branch {
	#[must_use]
	pub fn children(&self) -> Vec<tg::Object> {
		self.children
			.iter()
			.map(|child| child.blob.clone().into())
			.collect()
	}
}

impl TryFrom<Data> for Branch {
	type Error = tg::Error;

	fn try_from(data: Data) -> std::result::Result<Self, Self::Error> {
		let children = data
			.children
			.into_iter()
			.map(|child| tg::branch::Child {
				blob: tg::Blob::with_id(child.blob),
				size: child.size,
			})
			.collect();
		Ok(Self { children })
	}
}
