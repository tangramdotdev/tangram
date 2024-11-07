pub use self::{
	builder::Builder, data::Directory as Data, handle::Directory as Handle, id::Id,
	object::Directory as Object,
};

pub mod builder;
pub mod data;
pub mod handle;
pub mod id;
pub mod object;

#[macro_export]
macro_rules! directory {
	{ $($name:expr => $artifact:expr),* $(,)? } => {{
		let mut entries = ::std::collections::BTreeMap::new();
		$(
			entries.insert($name.into(), $artifact.into());
		)*
		$crate::Directory::with_entries(entries)
	}};
}
