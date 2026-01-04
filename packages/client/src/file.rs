pub use self::{
	builder::Builder,
	data::File as Data,
	handle::File as Handle,
	id::Id,
	object::{Dependency, File as Object},
};

pub mod builder;
pub mod data;
pub mod handle;
pub mod id;
pub mod object;

/// The extended attribute name of a lockattr.
pub const LOCKATTR_XATTR_NAME: &str = "user.tangram.lock";

/// The extended attribute name for dependencies.
pub const DEPENDENCIES_XATTR_NAME: &str = "user.tangram.dependencies";

/// The extended attribute name for module kind.
pub const MODULE_XATTR_NAME: &str = "user.tangram.module";
