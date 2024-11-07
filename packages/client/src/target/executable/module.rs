use crate as tg;
use std::collections::BTreeSet;

#[derive(Clone, Debug)]
pub struct Module {
	pub kind: tg::module::Kind,
	pub referent: tg::Referent<tg::Object>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Data {
	pub kind: tg::module::Kind,
	pub referent: tg::Referent<tg::object::Id>,
}

impl Module {
	#[must_use]
	pub fn objects(&self) -> Vec<tg::object::Handle> {
		vec![self.referent.item.clone()]
	}

	pub async fn data<H>(&self, handle: &H) -> tg::Result<Data>
	where
		H: tg::Handle,
	{
		let kind = self.kind;
		let item = self.referent.item.id(handle).await?;
		let subpath = self.referent.subpath.clone();
		let tag = self.referent.tag.clone();
		let referent = tg::Referent { item, subpath, tag };
		let data = Data { kind, referent };
		Ok(data)
	}
}

impl Data {
	#[must_use]
	pub fn children(&self) -> BTreeSet<tg::object::Id> {
		[self.referent.item.clone()].into()
	}
}

impl TryFrom<Data> for Module {
	type Error = tg::Error;

	fn try_from(data: Data) -> std::result::Result<Self, Self::Error> {
		let kind = data.kind;
		let item = tg::Object::with_id(data.referent.item);
		let subpath = data.referent.subpath;
		let tag = data.referent.tag;
		let referent = tg::Referent { item, subpath, tag };
		let module = Self { kind, referent };
		Ok(module)
	}
}

impl std::fmt::Display for Module {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.kind)?;
		if let Some(tag) = &self.referent.tag {
			write!(f, ":{tag}")?;
		} else {
			write!(f, ":{}", self.referent.item)?;
		}
		if let Some(subpath) = &self.referent.subpath {
			write!(f, ":{}", subpath.display())?;
		}
		Ok(())
	}
}
