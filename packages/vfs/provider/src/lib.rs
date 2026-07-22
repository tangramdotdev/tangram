#![allow(clippy::not_unsafe_ptr_arg_deref, clippy::cast_ptr_alignment)]

use {
	crate::provider::{Config, Provider},
	num::ToPrimitive as _,
	std::{
		ffi::{CStr, c_char, c_void},
		os::fd::AsRawFd,
		panic::{AssertUnwindSafe, catch_unwind},
		path::PathBuf,
		time::Duration,
	},
	tangram_client as tg,
	tangram_vfs::{Attrs, AttrsInner, EntryKind, Request, Response, Result, Timestamp},
};

mod provider;

pub type TgBatchCallback =
	Option<unsafe extern "system" fn(user_data: *mut c_void, batch: TgResponseBatch)>;
pub type TgResponse = *mut c_void;
pub type TgResponseBatch = *mut c_void;
pub type TgVfsProvider = *mut c_void;

#[derive(Clone, Copy)]
pub enum Status {
	InvalidArgument,
	NullPointer,
	Ok,
	OsError(i32),
	Panic,
}

/// The kind of a request. Selects which fields of a [`TgRequest`] are valid.
#[repr(i32)]
#[derive(Clone, Copy)]
pub enum TgRequestKind {
	Close = 0,
	Forget = 1,
	GetAttr = 2,
	GetXattr = 3,
	ListXattrs = 4,
	Lookup = 5,
	LookupParent = 6,
	Open = 7,
	OpenDir = 8,
	Read = 9,
	ReadDir = 10,
	ReadDirPlus = 11,
	ReadLink = 12,
	Remember = 13,
}

/// The kind of a response. Selects which accessor is valid for a [`TgResponse`].
#[repr(i32)]
#[derive(Clone, Copy)]
pub enum TgResponseKind {
	GetAttr = 0,
	GetXattr = 1,
	ListXattrs = 2,
	Lookup = 3,
	LookupParent = 4,
	Open = 5,
	OpenDir = 6,
	Read = 7,
	ReadDir = 8,
	ReadDirPlus = 9,
	ReadLink = 10,
	Unit = 11,
}

/// The kind of a directory entry.
#[repr(i32)]
#[derive(Clone, Copy)]
pub enum TgEntryKind {
	Directory = 1,
	File = 0,
	Symlink = 2,
}

/// The kind of a node's attributes.
#[repr(i32)]
#[derive(Clone, Copy)]
pub enum TgAttrsKind {
	Directory = 1,
	File = 0,
	Symlink = 2,
}

/// A timestamp with second and nanosecond components.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TgTimestamp {
	pub nanos: u32,
	pub secs: u64,
}

/// The attributes of a node.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TgAttrs {
	pub atime: TgTimestamp,
	pub ctime: TgTimestamp,
	pub executable: bool,
	pub gid: u32,
	pub kind: TgAttrsKind,
	pub mtime: TgTimestamp,
	pub size: u64,
	pub uid: u32,
}

/// A request. `kind` selects which of the remaining fields are meaningful.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TgRequest {
	pub handle: u64,
	pub id: u64,
	pub kind: TgRequestKind,
	pub length: u64,
	/// A pointer to the name bytes. Valid for `GetXattr` and `Lookup`.
	pub name: *const u8,
	pub name_len: usize,
	pub nlookup: u64,
	pub offset: u64,
	pub position: u64,
}

/// A directory entry, as returned by a `ReadDir` response.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TgDirEntry {
	pub id: u64,
	pub kind: TgEntryKind,
	/// A pointer to the name bytes, borrowed from the owning response.
	pub name: *const u8,
	pub name_len: usize,
}

/// A directory entry with attributes, as returned by a `ReadDirPlus` response.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TgDirEntryPlus {
	pub attrs: TgAttrs,
	pub id: u64,
	/// A pointer to the name bytes, borrowed from the owning response.
	pub name: *const u8,
	pub name_len: usize,
}

/// The configuration for a provider. A zero field selects the default.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TgConfig {
	/// The server's data directory, a null-terminated UTF-8 string owned by the caller. The fast path reads the object store and the cache directory within it directly instead of sending a request to the server. A null pointer disables the fast path.
	pub data_directory: *const c_char,
	/// The interval in seconds at which expired cache-only nodes are swept.
	pub node_eviction_interval_secs: u64,
	/// The duration in seconds a cache-only node created by directory enumeration is retained after its most recent access before it becomes eligible for eviction.
	pub node_ttl_secs: u64,
	/// The map size with which to open the object store. It must be at least the server's, so the server sends its own.
	pub object_store_map_size: u64,
	/// The object store's path within the data directory, a null-terminated UTF-8 string owned by the caller. A null pointer or an empty string selects the default.
	pub object_store_path: *const c_char,
	/// The prefix for the object store's POSIX lock semaphores, a null-terminated UTF-8 string owned by the caller. It must match the prefix the server opens the object store with so that the sandboxed provider and the server share the same lock. A null pointer or an empty string selects the default hash-derived names.
	pub object_store_posix_sem_prefix: *const c_char,
	/// The principal the mount serves, a null-terminated UTF-8 string owned by the caller in the display form of a principal. A null pointer or an empty string leaves the mount unenforced.
	pub principal: *const c_char,
	/// The grant tokens the mount holds, a null-terminated UTF-8 string owned by the caller containing a JSON array of grant tokens. A null pointer or an empty string provides no tokens.
	pub tokens: *const c_char,
}

impl From<Status> for i32 {
	fn from(status: Status) -> Self {
		match status {
			Status::InvalidArgument => 3,
			Status::NullPointer => 2,
			Status::Ok => 0,
			Status::OsError(errno) => -errno,
			Status::Panic => 1,
		}
	}
}

impl From<i32> for Status {
	fn from(value: i32) -> Self {
		match value {
			value if value < 0 => Status::OsError(-value),
			0 => Status::Ok,
			1 => Status::Panic,
			2 => Status::NullPointer,
			_ => Status::InvalidArgument,
		}
	}
}

/// Runs a body, converting a panic into [`Status::Panic`] rather than unwinding across the boundary, and returns the encoded status.
fn guard(body: impl FnOnce() -> Status) -> i32 {
	match catch_unwind(AssertUnwindSafe(body)) {
		Err(_error) => {
			// TODO: Print a backtrace.
			Status::Panic.into()
		},
		Ok(status) => status.into(),
	}
}

/// Recovers a reference to the provider from an opaque handle.
unsafe fn provider<'a>(provider: TgVfsProvider) -> &'a Provider {
	// SAFETY: The caller guarantees that the handle points to a live provider created by `tg_provider_new`.
	unsafe { &*provider.cast::<Provider>() }
}

/// Reads the bytes of a name argument.
unsafe fn name_arg(name: *const u8, name_len: usize) -> String {
	if name.is_null() {
		return String::new();
	}
	// SAFETY: The caller guarantees that the pointer addresses `name_len` readable bytes.
	let bytes = unsafe { std::slice::from_raw_parts(name, name_len) };
	String::from_utf8_lossy(bytes).into_owned()
}

/// Converts a C configuration into a configuration, selecting the default for each field that is zero.
fn config_from_c(config: &TgConfig) -> Config {
	// Load the defaults.
	let default = Config::default();

	// Convert the configuration.
	Config {
		data_directory: if config.data_directory.is_null() {
			None
		} else {
			// SAFETY: The caller guarantees that the non-null configuration field points to a terminated C string.
			unsafe { CStr::from_ptr(config.data_directory) }
				.to_str()
				.ok()
				.map(PathBuf::from)
		},
		node_eviction_interval: if config.node_eviction_interval_secs == 0 {
			default.node_eviction_interval
		} else {
			Duration::from_secs(config.node_eviction_interval_secs)
		},
		node_ttl: if config.node_ttl_secs == 0 {
			default.node_ttl
		} else {
			Duration::from_secs(config.node_ttl_secs)
		},
		object_store_map_size: if config.object_store_map_size == 0 {
			default.object_store_map_size
		} else {
			config
				.object_store_map_size
				.to_usize()
				.unwrap_or(usize::MAX)
		},
		object_store_path: if config.object_store_path.is_null() {
			default.object_store_path.clone()
		} else {
			// SAFETY: The caller guarantees that the non-null configuration field points to a terminated C string.
			unsafe { CStr::from_ptr(config.object_store_path) }
				.to_str()
				.ok()
				.filter(|path| !path.is_empty())
				.map_or_else(|| default.object_store_path.clone(), PathBuf::from)
		},
		object_store_posix_sem_prefix: if config.object_store_posix_sem_prefix.is_null() {
			None
		} else {
			// SAFETY: The caller guarantees that the non-null configuration field points to a terminated C string.
			unsafe { CStr::from_ptr(config.object_store_posix_sem_prefix) }
				.to_str()
				.ok()
				.filter(|prefix| !prefix.is_empty())
				.map(ToOwned::to_owned)
		},
		principal: if config.principal.is_null() {
			None
		} else {
			unsafe { CStr::from_ptr(config.principal) }
				.to_str()
				.ok()
				.filter(|principal| !principal.is_empty())
				.and_then(|principal| principal.parse::<tg::Principal>().ok())
		},
		tokens: if config.tokens.is_null() {
			Vec::new()
		} else {
			unsafe { CStr::from_ptr(config.tokens) }
				.to_str()
				.ok()
				.filter(|tokens| !tokens.is_empty())
				.and_then(|tokens| serde_json::from_str::<Vec<tg::grant::Token>>(tokens).ok())
				.unwrap_or_default()
		},
	}
}

/// Converts an array of C requests into a vector of requests.
unsafe fn requests_from_c(requests: *const TgRequest, requests_len: usize) -> Vec<Request> {
	// SAFETY: The caller guarantees that the pointer addresses `requests_len` initialized requests.
	let requests = unsafe { std::slice::from_raw_parts(requests, requests_len) };
	requests
		.iter()
		.map(|request| {
			// SAFETY: Each request and any selected name field satisfy the C request contract.
			unsafe { request_from_c(request) }
		})
		.collect()
}

/// Converts a single C request into a request.
unsafe fn request_from_c(request: &TgRequest) -> Request {
	match request.kind {
		TgRequestKind::Close => Request::Close {
			handle: request.handle,
		},
		TgRequestKind::Forget => Request::Forget {
			id: request.id,
			nlookup: request.nlookup,
		},
		TgRequestKind::GetAttr => Request::GetAttr { id: request.id },
		TgRequestKind::GetXattr => Request::GetXattr {
			id: request.id,
			// SAFETY: The request contract guarantees that the selected name field addresses `name_len` bytes.
			name: unsafe { name_arg(request.name, request.name_len) },
		},
		TgRequestKind::ListXattrs => Request::ListXattrs { id: request.id },
		TgRequestKind::Lookup => Request::Lookup {
			id: request.id,
			// SAFETY: The request contract guarantees that the selected name field addresses `name_len` bytes.
			name: unsafe { name_arg(request.name, request.name_len) },
		},
		TgRequestKind::LookupParent => Request::LookupParent { id: request.id },
		TgRequestKind::Open => Request::Open { id: request.id },
		TgRequestKind::OpenDir => Request::OpenDir { id: request.id },
		TgRequestKind::Read => Request::Read {
			handle: request.handle,
			length: request.length,
			position: request.position,
		},
		TgRequestKind::ReadDir => Request::ReadDir {
			handle: request.handle,
			length: request.length,
			offset: request.offset,
		},
		TgRequestKind::ReadDirPlus => Request::ReadDirPlus {
			handle: request.handle,
			length: request.length,
			offset: request.offset,
		},
		TgRequestKind::ReadLink => Request::ReadLink { id: request.id },
		TgRequestKind::Remember => Request::Remember { id: request.id },
	}
}

/// Converts a timestamp.
fn timestamp_to_c(timestamp: Timestamp) -> TgTimestamp {
	TgTimestamp {
		nanos: timestamp.nanos,
		secs: timestamp.secs,
	}
}

/// Converts attributes.
fn attrs_to_c(attrs: Attrs) -> TgAttrs {
	let (kind, executable, size) = match attrs.inner {
		AttrsInner::Directory => (TgAttrsKind::Directory, false, 0),
		AttrsInner::File { executable, size } => (TgAttrsKind::File, executable, size),
		AttrsInner::Symlink { size } => (TgAttrsKind::Symlink, false, size),
	};
	let atime = timestamp_to_c(attrs.atime);
	let ctime = timestamp_to_c(attrs.ctime);
	let gid = attrs.gid;
	let mtime = timestamp_to_c(attrs.mtime);
	let uid = attrs.uid;
	TgAttrs {
		atime,
		ctime,
		executable,
		gid,
		kind,
		mtime,
		size,
		uid,
	}
}

/// Converts a directory entry kind.
fn entry_kind_to_c(kind: EntryKind) -> TgEntryKind {
	match kind {
		EntryKind::Directory => TgEntryKind::Directory,
		EntryKind::File => TgEntryKind::File,
		EntryKind::Symlink => TgEntryKind::Symlink,
	}
}

/// Maps a filesystem result to a status.
fn os_error(error: &std::io::Error) -> Status {
	Status::OsError(error.raw_os_error().unwrap_or(libc::EIO))
}

/// Takes the single result of a synchronous operation and writes it to `out_response` as an owned response.
unsafe fn write_response(results: Vec<Result<Response>>, out_response: *mut TgResponse) -> Status {
	match results.into_iter().next() {
		None => Status::InvalidArgument,
		Some(Err(error)) => os_error(&error),
		Some(Ok(response)) => {
			// SAFETY: The caller guarantees that `out_response` points to writable storage.
			unsafe { *out_response = Box::into_raw(Box::new(response)).cast::<c_void>() };
			Status::Ok
		},
	}
}

/// Runs a single request synchronously and writes its response.
fn handle_sync(
	provider_handle: TgVfsProvider,
	request: Request,
	out_response: *mut TgResponse,
) -> i32 {
	guard(move || {
		if provider_handle.is_null() || out_response.is_null() {
			return Status::NullPointer;
		}
		// SAFETY: The null check and the provider handle contract guarantee a live provider.
		let provider = unsafe { provider(provider_handle) };
		let results = provider.handle_batch_sync(vec![request]);
		// SAFETY: The null check guarantees that the output pointer is writable under the C API contract.
		unsafe { write_response(results, out_response) }
	})
}

/// Runs a single request synchronously and discards its response.
fn handle_sync_unit(provider_handle: TgVfsProvider, request: Request) -> i32 {
	guard(move || {
		if provider_handle.is_null() {
			return Status::NullPointer;
		}
		// SAFETY: The null check and the provider handle contract guarantee a live provider.
		let provider = unsafe { provider(provider_handle) };
		let _ = provider.handle_batch_sync(vec![request]);
		Status::Ok
	})
}

/// Submits a batch of requests asynchronously, delivering the responses to the callback.
fn submit(
	provider_handle: TgVfsProvider,
	requests: Vec<Request>,
	callback: TgBatchCallback,
	user_data: *mut c_void,
) -> i32 {
	guard(move || {
		if provider_handle.is_null() {
			return Status::NullPointer;
		}
		// SAFETY: The null check and the provider handle contract guarantee a live provider.
		let provider = unsafe { provider(provider_handle) };
		// Carry the caller's opaque context as an address so the completion closure is `Send`.
		let user_data = user_data as usize;
		provider.submit_batch(requests, move |results| {
			if let Some(callback) = callback {
				let batch = Box::into_raw(Box::new(results)).cast::<c_void>();
				// SAFETY: The caller supplied the callback and guarantees that its context remains valid until invocation.
				unsafe { callback(user_data as *mut c_void, batch) };
			}
		});
		Status::Ok
	})
}

// Manage provider lifecycle.

/// Creates a new provider that connects a client to the server at `uri`, a null-terminated UTF-8 string owned by the caller; `config` optionally points to the provider configuration, and a null pointer selects the defaults.
#[unsafe(no_mangle)]
extern "system" fn tg_provider_new(
	uri: *const c_char,
	config: *const TgConfig,
	out_provider: *mut TgVfsProvider,
) -> i32 {
	guard(move || {
		if uri.is_null() || out_provider.is_null() {
			return Status::NullPointer;
		}
		// SAFETY: The caller guarantees that the non-null URI points to a terminated C string.
		let Ok(uri) = (unsafe { CStr::from_ptr(uri) }).to_str() else {
			return Status::InvalidArgument;
		};
		let config = if config.is_null() {
			Config::default()
		} else {
			// SAFETY: The caller guarantees that the non-null pointer addresses an initialized configuration.
			config_from_c(unsafe { &*config })
		};
		match Provider::new(uri, &config) {
			Err(error) => os_error(&error),
			Ok(provider) => {
				// SAFETY: The null check guarantees that the output pointer is writable under the C API contract.
				unsafe { *out_provider = Box::into_raw(Box::new(provider)).cast::<c_void>() };
				Status::Ok
			},
		}
	})
}

/// Drops a provider instance.
#[unsafe(no_mangle)]
extern "system" fn tg_provider_drop(provider: TgVfsProvider) -> i32 {
	guard(move || {
		if !provider.is_null() {
			// SAFETY: The provider handle contract transfers ownership of a live allocation exactly once.
			drop(unsafe { Box::from_raw(provider.cast::<Provider>()) });
		}
		Status::Ok
	})
}

// Handle batches.

/// Handles a batch of requests synchronously. On success, writes an owned batch of responses to `out_responses`.
#[unsafe(no_mangle)]
extern "system" fn tg_provider_handle_batch_sync(
	provider_handle: TgVfsProvider,
	requests: *const TgRequest,
	requests_len: usize,
	out_responses: *mut TgResponseBatch,
) -> i32 {
	guard(move || {
		if provider_handle.is_null() || requests.is_null() || out_responses.is_null() {
			return Status::NullPointer;
		}
		// SAFETY: The null check and the provider handle contract guarantee a live provider.
		let provider = unsafe { provider(provider_handle) };
		// SAFETY: The caller guarantees that the pointer addresses `requests_len` initialized requests.
		let requests = unsafe { requests_from_c(requests, requests_len) };
		let results = provider.handle_batch_sync(requests);
		// SAFETY: The null check guarantees that the output pointer is writable under the C API contract.
		unsafe { *out_responses = Box::into_raw(Box::new(results)).cast::<c_void>() };
		Status::Ok
	})
}

/// Submits a batch of requests asynchronously. The responses are delivered to `callback`.
#[unsafe(no_mangle)]
extern "system" fn tg_provider_submit_batch_async(
	provider_handle: TgVfsProvider,
	requests: *const TgRequest,
	requests_len: usize,
	callback: TgBatchCallback,
	user_data: *mut c_void,
) -> i32 {
	if requests.is_null() {
		return Status::NullPointer.into();
	}
	// SAFETY: The caller guarantees that the pointer addresses `requests_len` initialized requests.
	let requests = unsafe { requests_from_c(requests, requests_len) };
	submit(provider_handle, requests, callback, user_data)
}

// Handle synchronous operations.

/// Closes an open file handle.
#[unsafe(no_mangle)]
extern "system" fn tg_provider_close_sync(provider: TgVfsProvider, handle: u64) -> i32 {
	handle_sync_unit(provider, Request::Close { handle })
}

/// Records one kernel lookup reference for a node.
#[unsafe(no_mangle)]
extern "system" fn tg_provider_remember_sync(provider: TgVfsProvider, id: u64) -> i32 {
	handle_sync_unit(provider, Request::Remember { id })
}

/// Drops `nlookup` kernel lookup references for a node.
#[unsafe(no_mangle)]
extern "system" fn tg_provider_forget_sync(provider: TgVfsProvider, id: u64, nlookup: u64) -> i32 {
	handle_sync_unit(provider, Request::Forget { id, nlookup })
}

/// Gets the attributes for a node.
#[unsafe(no_mangle)]
extern "system" fn tg_provider_getattr_sync(
	provider: TgVfsProvider,
	id: u64,
	out_response: *mut TgResponse,
) -> i32 {
	handle_sync(provider, Request::GetAttr { id }, out_response)
}

/// Gets the value of an extended attribute for a node.
#[unsafe(no_mangle)]
extern "system" fn tg_provider_getxattr_sync(
	provider: TgVfsProvider,
	id: u64,
	name: *const u8,
	name_len: usize,
	out_response: *mut TgResponse,
) -> i32 {
	// SAFETY: The caller guarantees that the name pointer addresses `name_len` readable bytes or is null for an empty name.
	let name = unsafe { name_arg(name, name_len) };
	handle_sync(provider, Request::GetXattr { id, name }, out_response)
}

/// Lists the extended attributes for a node.
#[unsafe(no_mangle)]
extern "system" fn tg_provider_listxattrs_sync(
	provider: TgVfsProvider,
	id: u64,
	out_response: *mut TgResponse,
) -> i32 {
	handle_sync(provider, Request::ListXattrs { id }, out_response)
}

/// Looks up a node by name within a directory.
#[unsafe(no_mangle)]
extern "system" fn tg_provider_lookup_sync(
	provider: TgVfsProvider,
	id: u64,
	name: *const u8,
	name_len: usize,
	out_response: *mut TgResponse,
) -> i32 {
	// SAFETY: The caller guarantees that the name pointer addresses `name_len` readable bytes or is null for an empty name.
	let name = unsafe { name_arg(name, name_len) };
	handle_sync(provider, Request::Lookup { id, name }, out_response)
}

/// Looks up a node's parent.
#[unsafe(no_mangle)]
extern "system" fn tg_provider_lookup_parent_sync(
	provider: TgVfsProvider,
	id: u64,
	out_response: *mut TgResponse,
) -> i32 {
	handle_sync(provider, Request::LookupParent { id }, out_response)
}

/// Opens a file.
#[unsafe(no_mangle)]
extern "system" fn tg_provider_open_sync(
	provider: TgVfsProvider,
	id: u64,
	out_response: *mut TgResponse,
) -> i32 {
	handle_sync(provider, Request::Open { id }, out_response)
}

/// Opens a directory.
#[unsafe(no_mangle)]
extern "system" fn tg_provider_opendir_sync(
	provider: TgVfsProvider,
	id: u64,
	out_response: *mut TgResponse,
) -> i32 {
	handle_sync(provider, Request::OpenDir { id }, out_response)
}

/// Reads a range of bytes from an open file handle.
#[unsafe(no_mangle)]
extern "system" fn tg_provider_read_sync(
	provider: TgVfsProvider,
	handle: u64,
	position: u64,
	length: u64,
	out_response: *mut TgResponse,
) -> i32 {
	handle_sync(
		provider,
		Request::Read {
			handle,
			length,
			position,
		},
		out_response,
	)
}

/// Reads the entries of an open directory handle.
#[unsafe(no_mangle)]
extern "system" fn tg_provider_readdir_sync(
	provider: TgVfsProvider,
	handle: u64,
	offset: u64,
	length: u64,
	out_response: *mut TgResponse,
) -> i32 {
	handle_sync(
		provider,
		Request::ReadDir {
			handle,
			length,
			offset,
		},
		out_response,
	)
}

/// Reads the entries of an open directory handle with their attributes.
#[unsafe(no_mangle)]
extern "system" fn tg_provider_readdirplus_sync(
	provider: TgVfsProvider,
	handle: u64,
	offset: u64,
	length: u64,
	out_response: *mut TgResponse,
) -> i32 {
	handle_sync(
		provider,
		Request::ReadDirPlus {
			handle,
			length,
			offset,
		},
		out_response,
	)
}

/// Reads the target of a symbolic link.
#[unsafe(no_mangle)]
extern "system" fn tg_provider_readlink_sync(
	provider: TgVfsProvider,
	id: u64,
	out_response: *mut TgResponse,
) -> i32 {
	handle_sync(provider, Request::ReadLink { id }, out_response)
}

// Asynchronous operations submit work and return immediately; the responses are delivered to `callback` with `user_data`.

/// Closes an open file handle.
#[unsafe(no_mangle)]
extern "system" fn tg_provider_close_async(
	provider: TgVfsProvider,
	handle: u64,
	callback: TgBatchCallback,
	user_data: *mut c_void,
) -> i32 {
	submit(
		provider,
		vec![Request::Close { handle }],
		callback,
		user_data,
	)
}

/// Gets the attributes for a node.
#[unsafe(no_mangle)]
extern "system" fn tg_provider_getattr_async(
	provider: TgVfsProvider,
	id: u64,
	callback: TgBatchCallback,
	user_data: *mut c_void,
) -> i32 {
	submit(provider, vec![Request::GetAttr { id }], callback, user_data)
}

/// Gets the value of an extended attribute for a node.
#[unsafe(no_mangle)]
extern "system" fn tg_provider_getxattr_async(
	provider: TgVfsProvider,
	id: u64,
	name: *const u8,
	name_len: usize,
	callback: TgBatchCallback,
	user_data: *mut c_void,
) -> i32 {
	// SAFETY: The caller guarantees that the name pointer addresses `name_len` readable bytes or is null for an empty name.
	let name = unsafe { name_arg(name, name_len) };
	submit(
		provider,
		vec![Request::GetXattr { id, name }],
		callback,
		user_data,
	)
}

/// Lists the extended attributes for a node.
#[unsafe(no_mangle)]
extern "system" fn tg_provider_listxattrs_async(
	provider: TgVfsProvider,
	id: u64,
	callback: TgBatchCallback,
	user_data: *mut c_void,
) -> i32 {
	submit(
		provider,
		vec![Request::ListXattrs { id }],
		callback,
		user_data,
	)
}

/// Looks up a node by name within a directory.
#[unsafe(no_mangle)]
extern "system" fn tg_provider_lookup_async(
	provider: TgVfsProvider,
	id: u64,
	name: *const u8,
	name_len: usize,
	callback: TgBatchCallback,
	user_data: *mut c_void,
) -> i32 {
	// SAFETY: The caller guarantees that the name pointer addresses `name_len` readable bytes or is null for an empty name.
	let name = unsafe { name_arg(name, name_len) };
	submit(
		provider,
		vec![Request::Lookup { id, name }],
		callback,
		user_data,
	)
}

/// Looks up a node's parent.
#[unsafe(no_mangle)]
extern "system" fn tg_provider_lookup_parent_async(
	provider: TgVfsProvider,
	id: u64,
	callback: TgBatchCallback,
	user_data: *mut c_void,
) -> i32 {
	submit(
		provider,
		vec![Request::LookupParent { id }],
		callback,
		user_data,
	)
}

/// Opens a file.
#[unsafe(no_mangle)]
extern "system" fn tg_provider_open_async(
	provider: TgVfsProvider,
	id: u64,
	callback: TgBatchCallback,
	user_data: *mut c_void,
) -> i32 {
	submit(provider, vec![Request::Open { id }], callback, user_data)
}

/// Opens a directory.
#[unsafe(no_mangle)]
extern "system" fn tg_provider_opendir_async(
	provider: TgVfsProvider,
	id: u64,
	callback: TgBatchCallback,
	user_data: *mut c_void,
) -> i32 {
	submit(provider, vec![Request::OpenDir { id }], callback, user_data)
}

/// Reads a range of bytes from an open file handle.
#[unsafe(no_mangle)]
extern "system" fn tg_provider_read_async(
	provider: TgVfsProvider,
	handle: u64,
	position: u64,
	length: u64,
	callback: TgBatchCallback,
	user_data: *mut c_void,
) -> i32 {
	submit(
		provider,
		vec![Request::Read {
			handle,
			length,
			position,
		}],
		callback,
		user_data,
	)
}

/// Reads the entries of an open directory handle.
#[unsafe(no_mangle)]
extern "system" fn tg_provider_readdir_async(
	provider: TgVfsProvider,
	handle: u64,
	offset: u64,
	length: u64,
	callback: TgBatchCallback,
	user_data: *mut c_void,
) -> i32 {
	submit(
		provider,
		vec![Request::ReadDir {
			handle,
			length,
			offset,
		}],
		callback,
		user_data,
	)
}

/// Reads the entries of an open directory handle with their attributes.
#[unsafe(no_mangle)]
extern "system" fn tg_provider_readdirplus_async(
	provider: TgVfsProvider,
	handle: u64,
	offset: u64,
	length: u64,
	callback: TgBatchCallback,
	user_data: *mut c_void,
) -> i32 {
	submit(
		provider,
		vec![Request::ReadDirPlus {
			handle,
			length,
			offset,
		}],
		callback,
		user_data,
	)
}

/// Reads the target of a symbolic link.
#[unsafe(no_mangle)]
extern "system" fn tg_provider_readlink_async(
	provider: TgVfsProvider,
	id: u64,
	callback: TgBatchCallback,
	user_data: *mut c_void,
) -> i32 {
	submit(
		provider,
		vec![Request::ReadLink { id }],
		callback,
		user_data,
	)
}

// The response kind determines the valid accessor; borrowed byte and entry pointers remain valid until the response or its owning batch is freed.

/// Gets the kind of a response.
#[unsafe(no_mangle)]
extern "system" fn tg_response_kind(response: TgResponse, out_kind: *mut TgResponseKind) -> i32 {
	guard(move || {
		if response.is_null() || out_kind.is_null() {
			return Status::NullPointer;
		}
		// SAFETY: The response handle contract guarantees a live response of the expected allocation type.
		let response = unsafe { &*response.cast::<Response>() };
		let kind = match response {
			Response::GetAttr { .. } => TgResponseKind::GetAttr,
			Response::GetXattr { .. } => TgResponseKind::GetXattr,
			Response::ListXattrs { .. } => TgResponseKind::ListXattrs,
			Response::Lookup { .. } => TgResponseKind::Lookup,
			Response::LookupParent { .. } => TgResponseKind::LookupParent,
			Response::Open { .. } => TgResponseKind::Open,
			Response::OpenDir { .. } => TgResponseKind::OpenDir,
			Response::Read { .. } => TgResponseKind::Read,
			Response::ReadDir { .. } => TgResponseKind::ReadDir,
			Response::ReadDirPlus { .. } => TgResponseKind::ReadDirPlus,
			Response::ReadLink { .. } => TgResponseKind::ReadLink,
			Response::Unit => TgResponseKind::Unit,
		};
		// SAFETY: The null check guarantees that the output pointer is writable under the C API contract.
		unsafe { *out_kind = kind };
		Status::Ok
	})
}

/// Gets the attributes from a `GetAttr` response.
#[unsafe(no_mangle)]
extern "system" fn tg_response_attrs(response: TgResponse, out_attrs: *mut TgAttrs) -> i32 {
	guard(move || {
		if response.is_null() || out_attrs.is_null() {
			return Status::NullPointer;
		}
		// SAFETY: The response handle contract guarantees a live response of the expected allocation type.
		let response = unsafe { &*response.cast::<Response>() };
		match response {
			Response::GetAttr { attrs } => {
				// SAFETY: The null check guarantees that the output pointer is writable under the C API contract.
				unsafe { *out_attrs = attrs_to_c(*attrs) };
				Status::Ok
			},
			_ => Status::InvalidArgument,
		}
	})
}

/// Gets the result of a `Lookup` response. `out_present` is false when the name was not found.
#[unsafe(no_mangle)]
extern "system" fn tg_response_lookup(
	response: TgResponse,
	out_present: *mut bool,
	out_id: *mut u64,
) -> i32 {
	guard(move || {
		if response.is_null() || out_present.is_null() || out_id.is_null() {
			return Status::NullPointer;
		}
		// SAFETY: The response handle contract guarantees a live response of the expected allocation type.
		let response = unsafe { &*response.cast::<Response>() };
		match response {
			Response::Lookup { id, .. } => {
				// SAFETY: The null checks guarantee that the output pointers are writable under the C API contract.
				unsafe {
					*out_present = id.is_some();
					*out_id = id.unwrap_or(0);
				}
				Status::Ok
			},
			_ => Status::InvalidArgument,
		}
	})
}

/// Gets the id from a `LookupParent` response.
#[unsafe(no_mangle)]
extern "system" fn tg_response_lookup_parent(response: TgResponse, out_id: *mut u64) -> i32 {
	guard(move || {
		if response.is_null() || out_id.is_null() {
			return Status::NullPointer;
		}
		// SAFETY: The response handle contract guarantees a live response of the expected allocation type.
		let response = unsafe { &*response.cast::<Response>() };
		match response {
			Response::LookupParent { id } => {
				// SAFETY: The null check guarantees that the output pointer is writable under the C API contract.
				unsafe { *out_id = *id };
				Status::Ok
			},
			_ => Status::InvalidArgument,
		}
	})
}

/// Gets the handle and optional borrowed backing file descriptor from an `Open` response, writing `-1` when there is no descriptor.
#[unsafe(no_mangle)]
extern "system" fn tg_response_open(
	response: TgResponse,
	out_handle: *mut u64,
	out_backing_fd: *mut i32,
) -> i32 {
	guard(move || {
		if response.is_null() || out_handle.is_null() {
			return Status::NullPointer;
		}
		// SAFETY: The response handle contract guarantees a live response of the expected allocation type.
		let response = unsafe { &*response.cast::<Response>() };
		match response {
			Response::Open { backing_fd, handle } => {
				// SAFETY: The required output passed the null check, and the optional output is checked before writing.
				unsafe {
					*out_handle = *handle;
					// Callers that do not use passthrough leave `out_backing_fd` null.
					if !out_backing_fd.is_null() {
						*out_backing_fd = backing_fd.as_ref().map_or(-1, AsRawFd::as_raw_fd);
					}
				}
				Status::Ok
			},
			_ => Status::InvalidArgument,
		}
	})
}

/// Gets the handle from an `OpenDir` response.
#[unsafe(no_mangle)]
extern "system" fn tg_response_opendir(response: TgResponse, out_handle: *mut u64) -> i32 {
	guard(move || {
		if response.is_null() || out_handle.is_null() {
			return Status::NullPointer;
		}
		// SAFETY: The response handle contract guarantees a live response of the expected allocation type.
		let response = unsafe { &*response.cast::<Response>() };
		match response {
			Response::OpenDir { handle } => {
				// SAFETY: The null check guarantees that the output pointer is writable under the C API contract.
				unsafe { *out_handle = *handle };
				Status::Ok
			},
			_ => Status::InvalidArgument,
		}
	})
}

/// Gets the borrowed bytes from a `Read` or `ReadLink` response.
#[unsafe(no_mangle)]
extern "system" fn tg_response_bytes(
	response: TgResponse,
	out_ptr: *mut *const u8,
	out_len: *mut usize,
) -> i32 {
	guard(move || {
		if response.is_null() || out_ptr.is_null() || out_len.is_null() {
			return Status::NullPointer;
		}
		// SAFETY: The response handle contract guarantees a live response of the expected allocation type.
		let response = unsafe { &*response.cast::<Response>() };
		let bytes = match response {
			Response::Read { bytes } => bytes,
			Response::ReadLink { target } => target,
			_ => return Status::InvalidArgument,
		};
		// SAFETY: The null checks guarantee that the output pointers are writable under the C API contract.
		unsafe {
			*out_ptr = bytes.as_ptr();
			*out_len = bytes.len();
		}
		Status::Ok
	})
}

/// Gets the optional borrowed value from a `GetXattr` response. `out_present` is false when the attribute does not exist.
#[unsafe(no_mangle)]
extern "system" fn tg_response_xattr(
	response: TgResponse,
	out_present: *mut bool,
	out_ptr: *mut *const u8,
	out_len: *mut usize,
) -> i32 {
	guard(move || {
		if response.is_null() || out_present.is_null() || out_ptr.is_null() || out_len.is_null() {
			return Status::NullPointer;
		}
		// SAFETY: The response handle contract guarantees a live response of the expected allocation type.
		let response = unsafe { &*response.cast::<Response>() };
		match response {
			Response::GetXattr { value } => {
				// SAFETY: The null checks guarantee that the output pointers are writable under the C API contract.
				unsafe {
					if let Some(value) = value {
						*out_present = true;
						*out_ptr = value.as_ptr();
						*out_len = value.len();
					} else {
						*out_present = false;
						*out_ptr = std::ptr::null();
						*out_len = 0;
					}
				}
				Status::Ok
			},
			_ => Status::InvalidArgument,
		}
	})
}

/// Gets the number of names in a `ListXattrs` response.
#[unsafe(no_mangle)]
extern "system" fn tg_response_xattr_names_len(response: TgResponse, out_len: *mut usize) -> i32 {
	guard(move || {
		if response.is_null() || out_len.is_null() {
			return Status::NullPointer;
		}
		// SAFETY: The response handle contract guarantees a live response of the expected allocation type.
		let response = unsafe { &*response.cast::<Response>() };
		match response {
			Response::ListXattrs { names } => {
				// SAFETY: The null check guarantees that the output pointer is writable under the C API contract.
				unsafe { *out_len = names.len() };
				Status::Ok
			},
			_ => Status::InvalidArgument,
		}
	})
}

/// Gets the borrowed name at `index` from a `ListXattrs` response.
#[unsafe(no_mangle)]
extern "system" fn tg_response_xattr_names_get(
	response: TgResponse,
	index: usize,
	out_ptr: *mut *const u8,
	out_len: *mut usize,
) -> i32 {
	guard(move || {
		if response.is_null() || out_ptr.is_null() || out_len.is_null() {
			return Status::NullPointer;
		}
		// SAFETY: The response handle contract guarantees a live response of the expected allocation type.
		let response = unsafe { &*response.cast::<Response>() };
		match response {
			Response::ListXattrs { names } => {
				let Some(name) = names.get(index) else {
					return Status::InvalidArgument;
				};
				// SAFETY: The null checks guarantee that the output pointers are writable under the C API contract.
				unsafe {
					*out_ptr = name.as_ptr();
					*out_len = name.len();
				}
				Status::Ok
			},
			_ => Status::InvalidArgument,
		}
	})
}

/// Gets the number of entries in a `ReadDir` response.
#[unsafe(no_mangle)]
extern "system" fn tg_response_readdir_len(response: TgResponse, out_len: *mut usize) -> i32 {
	guard(move || {
		if response.is_null() || out_len.is_null() {
			return Status::NullPointer;
		}
		// SAFETY: The response handle contract guarantees a live response of the expected allocation type.
		let response = unsafe { &*response.cast::<Response>() };
		match response {
			Response::ReadDir { entries } => {
				// SAFETY: The null check guarantees that the output pointer is writable under the C API contract.
				unsafe { *out_len = entries.len() };
				Status::Ok
			},
			_ => Status::InvalidArgument,
		}
	})
}

/// Gets the entry at `index` from a `ReadDir` response.
#[unsafe(no_mangle)]
extern "system" fn tg_response_readdir_get(
	response: TgResponse,
	index: usize,
	out_entry: *mut TgDirEntry,
) -> i32 {
	guard(move || {
		if response.is_null() || out_entry.is_null() {
			return Status::NullPointer;
		}
		// SAFETY: The response handle contract guarantees a live response of the expected allocation type.
		let response = unsafe { &*response.cast::<Response>() };
		match response {
			Response::ReadDir { entries } => {
				let Some((name, id, kind)) = entries.get(index) else {
					return Status::InvalidArgument;
				};
				// SAFETY: The null check guarantees that the output pointer is writable under the C API contract.
				unsafe {
					*out_entry = TgDirEntry {
						id: *id,
						kind: entry_kind_to_c(*kind),
						name: name.as_ptr(),
						name_len: name.len(),
					};
				}
				Status::Ok
			},
			_ => Status::InvalidArgument,
		}
	})
}

/// Gets the number of entries in a `ReadDirPlus` response.
#[unsafe(no_mangle)]
extern "system" fn tg_response_readdirplus_len(response: TgResponse, out_len: *mut usize) -> i32 {
	guard(move || {
		if response.is_null() || out_len.is_null() {
			return Status::NullPointer;
		}
		// SAFETY: The response handle contract guarantees a live response of the expected allocation type.
		let response = unsafe { &*response.cast::<Response>() };
		match response {
			Response::ReadDirPlus { entries } => {
				// SAFETY: The null check guarantees that the output pointer is writable under the C API contract.
				unsafe { *out_len = entries.len() };
				Status::Ok
			},
			_ => Status::InvalidArgument,
		}
	})
}

/// Gets the entry at `index` from a `ReadDirPlus` response.
#[unsafe(no_mangle)]
extern "system" fn tg_response_readdirplus_get(
	response: TgResponse,
	index: usize,
	out_entry: *mut TgDirEntryPlus,
) -> i32 {
	guard(move || {
		if response.is_null() || out_entry.is_null() {
			return Status::NullPointer;
		}
		// SAFETY: The response handle contract guarantees a live response of the expected allocation type.
		let response = unsafe { &*response.cast::<Response>() };
		match response {
			Response::ReadDirPlus { entries } => {
				let Some((name, id, attrs)) = entries.get(index) else {
					return Status::InvalidArgument;
				};
				// SAFETY: The null check guarantees that the output pointer is writable under the C API contract.
				unsafe {
					*out_entry = TgDirEntryPlus {
						attrs: attrs_to_c(*attrs),
						id: *id,
						name: name.as_ptr(),
						name_len: name.len(),
					};
				}
				Status::Ok
			},
			_ => Status::InvalidArgument,
		}
	})
}

// Access batches.

/// Gets the number of responses in a batch.
#[unsafe(no_mangle)]
extern "system" fn tg_response_batch_len(batch: TgResponseBatch, out_len: *mut usize) -> i32 {
	guard(move || {
		if batch.is_null() || out_len.is_null() {
			return Status::NullPointer;
		}
		// SAFETY: The batch handle contract guarantees a live batch of the expected allocation type.
		let batch = unsafe { &*batch.cast::<Vec<Result<Response>>>() };
		// SAFETY: The null check guarantees that the output pointer is writable under the C API contract.
		unsafe { *out_len = batch.len() };
		Status::Ok
	})
}

/// Gets the response at `index` from a batch, writing a borrowed response on success or null and `-errno` on a per-response error.
#[unsafe(no_mangle)]
extern "system" fn tg_response_batch_get(
	batch: TgResponseBatch,
	index: usize,
	out_response: *mut TgResponse,
) -> i32 {
	guard(move || {
		if batch.is_null() || out_response.is_null() {
			return Status::NullPointer;
		}
		// SAFETY: The batch handle contract guarantees a live batch of the expected allocation type.
		let batch = unsafe { &*batch.cast::<Vec<Result<Response>>>() };
		let Some(result) = batch.get(index) else {
			return Status::InvalidArgument;
		};
		match result {
			Err(error) => {
				// SAFETY: The null check guarantees that the output pointer is writable under the C API contract.
				unsafe { *out_response = std::ptr::null_mut() };
				os_error(error)
			},
			Ok(response) => {
				let response = std::ptr::from_ref(response).cast::<c_void>().cast_mut();
				// SAFETY: The null check guarantees that the output pointer is writable under the C API contract.
				unsafe { *out_response = response };
				Status::Ok
			},
		}
	})
}

// Free responses and batches.

/// Frees a response returned by a `*_sync` method, which must not be borrowed from a batch.
#[unsafe(no_mangle)]
extern "system" fn tg_response_free(response: TgResponse) -> i32 {
	guard(move || {
		if !response.is_null() {
			// SAFETY: The response handle contract transfers ownership of a live allocation exactly once.
			drop(unsafe { Box::from_raw(response.cast::<Response>()) });
		}
		Status::Ok
	})
}

/// Frees a batch and every response it contains.
#[unsafe(no_mangle)]
extern "system" fn tg_response_batch_free(batch: TgResponseBatch) -> i32 {
	guard(move || {
		if !batch.is_null() {
			// SAFETY: The batch handle contract transfers ownership of a live allocation exactly once.
			drop(unsafe { Box::from_raw(batch.cast::<Vec<Result<Response>>>()) });
		}
		Status::Ok
	})
}
