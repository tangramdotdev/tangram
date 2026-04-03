pub use self::{
	data::Artifact as Data, handle::Artifact as Handle, id::Id, kind::Kind,
	object::Artifact as Object,
};

pub mod data;
pub mod handle;
pub mod id;
pub mod kind;
pub mod object;
