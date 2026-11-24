use {
	crate::{Server, Session},
	dashmap::DashMap,
	futures::{future::BoxFuture, stream::BoxStream},
	std::{collections::BTreeMap, hash::BuildHasher as _, path::Path, time::Duration},
	tangram_client::prelude::*,
	tangram_http::body::Boxed as BoxBody,
};

#[cfg(test)]
mod tests;

/// The number of mutexes that serialize the puts and removals of checkouts.
const PUTS_MUTEX_COUNT: usize = 64;

/// The checkout component.
pub trait Component: crate::component::Lifecycle {
	fn checkout<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::checkout::Arg,
	) -> BoxFuture<
		'a,
		tg::Result<BoxStream<'static, tg::Result<tg::progress::Event<tg::checkout::Output>>>>,
	>;

	fn request<'a>(
		&'a self,
		session: &'a Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>>;

	fn checkout_await_indexing<'a>(&'a self, session: &'a Session)
	-> BoxFuture<'a, tg::Result<()>>;

	fn internal_xattr_capabilities(&self, server: &Server)
	-> tg::Result<tg::file::xattrs::Options>;

	fn lock(&self) -> &Lock;

	fn puts(&self) -> &Puts;

	fn named_checkout_maintenance_enabled(&self, server: &Server) -> bool;

	fn remove_named_checkout_entry_with_lock<'a>(
		&'a self,
		server: &'a Server,
		guard: &'a Guard<'_>,
		id: &'a tg::Id,
		specifier: &'a tg::Specifier,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn try_get_named_checkout_nodes<'a>(
		&'a self,
		server: &'a Server,
		ids: &'a [tg::Id],
	) -> BoxFuture<'a, tg::Result<BTreeMap<tg::Id, NamedNode>>>;

	fn checkout_internal<'a>(
		&'a self,
		session: &'a Session,
		artifacts: Vec<tg::Referent<tg::artifact::Id>>,
	) -> BoxFuture<'a, tg::Result<BoxStream<'static, tg::Result<tg::progress::Event<()>>>>>;
}

#[derive(Default)]
pub struct Lock(tokio::sync::Mutex<()>);

pub struct Guard<'a> {
	_guard: tokio::sync::MutexGuard<'a, ()>,
}

/// The latest puts and touches of artifact checkouts on this server.
pub struct Puts {
	entries: DashMap<tg::Id, PutsEntry, fnv::FnvBuildHasher>,
	mutexes: [tokio::sync::Mutex<()>; PUTS_MUTEX_COUNT],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PutsEntry {
	pub put: Option<[u8; 16]>,
	pub touched_at: i64,
}

/// A guard that holds the mutex for a checkout's ID.
pub struct PutsGuard<'a> {
	_guard: tokio::sync::MutexGuard<'a, ()>,
	id: &'a tg::Id,
	puts: &'a Puts,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamedNode {
	pub id: tg::Id,
	pub parent: Option<tg::Id>,
	pub specifier: tg::Specifier,
	pub target: Option<tg::Either<tg::object::Id, tg::process::Id>>,
}

impl Lock {
	#[must_use]
	pub fn new() -> Self {
		Self(tokio::sync::Mutex::new(()))
	}

	pub async fn acquire(&self) -> Guard<'_> {
		let guard = self.0.lock().await;
		Guard { _guard: guard }
	}
}

impl Puts {
	#[must_use]
	pub fn new() -> Self {
		let entries = DashMap::default();
		let mutexes = std::array::from_fn(|_| tokio::sync::Mutex::new(()));
		Self { entries, mutexes }
	}

	/// Publish a temp at a checkout path with a fresh put token, keeping a path that already exists, and return the token and whether the temp was renamed.
	pub async fn publish(
		&self,
		id: &tg::Id,
		touched_at: i64,
		src: &Path,
		dst: &Path,
	) -> tg::Result<([u8; 16], bool)> {
		let guard = self.lock(id).await;
		let put = guard.put(touched_at);
		let src = src.to_owned();
		let dst = dst.to_owned();
		let renamed = tokio::task::spawn_blocking(move || Self::rename(&src, &dst))
			.await
			.map_err(|error| tg::error!(!error, "the checkout rename task panicked"))??;
		drop(guard);
		Ok((put, renamed))
	}

	/// Publish a temp at a checkout path with a fresh put token from a blocking thread, keeping a path that already exists, and return the token and whether the temp was renamed.
	pub fn publish_sync(
		&self,
		id: &tg::Id,
		touched_at: i64,
		src: &Path,
		dst: &Path,
	) -> tg::Result<([u8; 16], bool)> {
		let guard = self.lock_sync(id);
		let put = guard.put(touched_at);
		let renamed = Self::rename(src, dst)?;
		drop(guard);
		Ok((put, renamed))
	}

	/// Acquire the mutex for a checkout's ID.
	pub async fn lock<'a>(&'a self, id: &'a tg::Id) -> PutsGuard<'a> {
		let guard = self.mutex(id).lock().await;
		PutsGuard {
			_guard: guard,
			id,
			puts: self,
		}
	}

	/// Acquire the mutex for a checkout's ID from a blocking thread.
	pub fn lock_sync<'a>(&'a self, id: &'a tg::Id) -> PutsGuard<'a> {
		let guard = self.mutex(id).blocking_lock();
		PutsGuard {
			_guard: guard,
			id,
			puts: self,
		}
	}

	/// Determine whether a checkout was put or touched within the time to touch.
	#[must_use]
	pub fn touched(&self, id: &tg::Id, now: i64, time_to_touch: Duration) -> bool {
		let time_to_touch = i64::try_from(time_to_touch.as_secs()).unwrap_or(i64::MAX);
		self.entries
			.get(id)
			.is_some_and(|entry| now.saturating_sub(entry.touched_at) < time_to_touch)
	}

	/// Drop the entries that were not put or touched within the time to touch.
	pub fn retain(&self, now: i64, time_to_touch: Duration) {
		let time_to_touch = i64::try_from(time_to_touch.as_secs()).unwrap_or(i64::MAX);
		self.entries
			.retain(|_, entry| now.saturating_sub(entry.touched_at) < time_to_touch);
	}

	fn mutex(&self, id: &tg::Id) -> &tokio::sync::Mutex<()> {
		let hash = fnv::FnvBuildHasher::default().hash_one(id);
		let index = usize::try_from(hash % PUTS_MUTEX_COUNT as u64).unwrap();
		&self.mutexes[index]
	}

	fn rename(src: &Path, dst: &Path) -> tg::Result<bool> {
		match tangram_util::fs::rename_noreplace_sync(src, dst) {
			Ok(()) => Ok(true),
			Err(error)
				if matches!(
					error.kind(),
					std::io::ErrorKind::AlreadyExists
						| std::io::ErrorKind::IsADirectory
						| std::io::ErrorKind::PermissionDenied
				) =>
			{
				Ok(false)
			},
			Err(error) => {
				let src = src.display();
				let dst = dst.display();
				let error = tg::error!(!error, %src, %dst, "failed to rename to the checkout path");
				Err(error)
			},
		}
	}
}

impl PutsGuard<'_> {
	/// Get the entry for the checkout.
	#[must_use]
	pub fn get(&self) -> Option<PutsEntry> {
		self.puts.entries.get(self.id).map(|entry| *entry)
	}

	/// Record a fresh put token for the checkout and return it.
	#[must_use]
	pub fn put(&self, touched_at: i64) -> [u8; 16] {
		let put = uuid::Uuid::now_v7().into_bytes();
		let entry = PutsEntry {
			put: Some(put),
			touched_at,
		};
		self.puts.entries.insert(self.id.clone(), entry);
		put
	}

	/// Remove the entry for the checkout.
	pub fn remove(&self) {
		self.puts.entries.remove(self.id);
	}

	/// Record a touch of the checkout, keeping its put token.
	pub fn touch(&self, touched_at: i64) {
		self.puts
			.entries
			.entry(self.id.clone())
			.and_modify(|entry| entry.touched_at = entry.touched_at.max(touched_at))
			.or_insert(PutsEntry {
				put: None,
				touched_at,
			});
	}
}

impl Default for Puts {
	fn default() -> Self {
		Self::new()
	}
}
