pub use {
	permission::Permission,
	subject::Subject,
	token::{Algorithm, Body, Metadata, PrivateKey, PublicKey, Token, Tokens},
};

pub mod permission;
pub mod subject;
pub mod token;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, derive_more::Display)]
#[display(rename_all = "snake_case")]
pub enum ResourceKind {
	Group,
	Object,
	Organization,
	Process,
	Sandbox,
	Tag,
	User,
}

impl ResourceKind {
	#[must_use]
	pub fn from_id_kind(kind: crate::id::Kind) -> Option<Self> {
		if kind.is_object() {
			return Some(Self::Object);
		}
		match kind {
			crate::id::Kind::Group => Some(Self::Group),
			crate::id::Kind::Organization => Some(Self::Organization),
			crate::id::Kind::Process => Some(Self::Process),
			crate::id::Kind::Sandbox => Some(Self::Sandbox),
			crate::id::Kind::Tag => Some(Self::Tag),
			crate::id::Kind::User => Some(Self::User),
			_ => None,
		}
	}
}
