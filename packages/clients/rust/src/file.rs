mod dependencies;

pub use self::{
	builder::Builder,
	data::File as Data,
	dependencies::{
		DependenciesXattr, dependencies_xattrs, deserialize_dependencies_xattr,
		is_dependencies_xattr_name,
	},
	handle::File as Handle,
	id::Id,
	object::{Dependency, File as Object},
};

pub mod builder;
pub mod checkout;
pub mod data;
pub mod handle;
pub mod id;
pub mod object;

/// The extended attribute name for the file's dependencies.
pub const DEPENDENCIES_XATTR_NAME: &str = "user.tangram.dependencies";

/// The maximum extended attribute value size used by virtual filesystems.
pub const DEPENDENCIES_XATTR_VALUE_SIZE: usize = 64 * 1024;

/// The extended attribute name for the file's lock.
pub const LOCK_XATTR_NAME: &str = "user.tangram.lock";

/// The extended attribute name for the file's module kind.
pub const MODULE_XATTR_NAME: &str = "user.tangram.module";

/// The extended attribute name for the file's authorization token.
pub const TOKEN_XATTR_NAME: &str = "user.tangram.token";
