pub use self::{
	data::Symlink as Data,
	handle::{State, Symlink as Handle},
	id::Id,
	object::Symlink as Object,
};

pub mod data;
pub mod handle;
pub mod id;
pub mod object;
