use {
	futures::{FutureExt as _, future::BoxFuture, pin_mut},
	std::{
		collections::BinaryHeap,
		ops::{Deref, DerefMut},
		sync::{Arc, Mutex},
		time::{Duration, Instant},
	},
	tangram_futures::task::Task,
};

pub struct Pool<T: Send + Sync + 'static, E: Send + 'static> {
	create: Arc<dyn Fn() -> BoxFuture<'static, Result<T, E>> + Send + Sync>,
	expiration_sender: tokio::sync::mpsc::UnboundedSender<Instant>,
	expiration_task: Option<Arc<Task<()>>>,
	state: Arc<Mutex<State<T, E>>>,
}

struct State<T: Send + Sync + 'static, E: Send + 'static> {
	entries: Vec<Entry<T>>,
	epoch: usize,
	next_id: usize,
	options: Options,
	order: usize,
	pending: usize,
	requests: BinaryHeap<Request<T, E>>,
}

struct Entry<T> {
	expires_at: Instant,
	expired: bool,
	id: usize,
	shared: usize,
	value: Option<Arc<T>>,
}

struct Pending<T: Send + Sync + 'static, E: Send + 'static> {
	active: bool,
	epoch: usize,
	state: Arc<Mutex<State<T, E>>>,
}

struct Request<T: Send + Sync + 'static, E: Send + 'static> {
	kind: Kind,
	priority: Priority,
	order: usize,
	sender: tokio::sync::oneshot::Sender<Response<T, E>>,
}

#[derive(Clone, Copy)]
enum Access {
	Exclusive(usize),
	Shared,
}

struct Lease<T: Send + Sync + 'static, E: Send + 'static> {
	access: Access,
	pool: Pool<T, E>,
	value: Option<Arc<T>>,
}

enum Response<T: Send + Sync + 'static, E: Send + 'static> {
	Create(Pending<T, E>),
	Lease(Lease<T, E>),
}

#[derive(Clone, Copy, Debug)]
pub struct Options {
	pub min: usize,
	pub max: usize,
	pub shared: usize,
	pub ttl: Option<Duration>,
}

#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub enum Priority {
	Low,
	#[default]
	Medium,
	High,
}

#[derive(Clone, Copy)]
enum Kind {
	Exclusive,
	Shared,
}

pub struct ExclusiveGuard<T: Send + Sync + 'static, E: Send + 'static> {
	id: usize,
	pool: Pool<T, E>,
	value: Option<Arc<T>>,
}

pub struct SharedGuard<T: Send + Sync + 'static, E: Send + 'static> {
	pool: Pool<T, E>,
	value: Option<Arc<T>>,
}

impl Default for Options {
	fn default() -> Self {
		Self {
			min: 0,
			max: usize::MAX,
			shared: 1,
			ttl: None,
		}
	}
}

impl<T, E> Pool<T, E>
where
	T: Send + Sync + 'static,
	E: Send + 'static,
{
	#[must_use]
	pub fn new<F, Fut>(options: Options, create: F) -> Self
	where
		F: Fn() -> Fut + Send + Sync + 'static,
		Fut: Future<Output = Result<T, E>> + Send + 'static,
	{
		let max = options.max.max(1);
		let options = Options {
			min: options.min.min(max),
			max,
			shared: options.shared.max(1),
			ttl: options.ttl,
		};
		let state = State {
			entries: Vec::new(),
			epoch: 0,
			next_id: 0,
			options,
			order: 0,
			pending: 0,
			requests: BinaryHeap::new(),
		};
		let create = Arc::new(move || create().boxed());
		let state = Arc::new(Mutex::new(state));
		let (expiration_sender, receiver) = tokio::sync::mpsc::unbounded_channel();
		let expiration_task = options.ttl.map(|_| {
			let expiration_task = Task::spawn({
				let state = state.clone();
				|_| expiration_task(state, receiver)
			});
			Arc::new(expiration_task)
		});
		Self {
			create,
			expiration_sender,
			expiration_task,
			state,
		}
	}

	pub fn add(&self, value: T) {
		let mut state = self.state.lock().unwrap();
		state.prune_expired();
		if state.entries.len() + state.pending >= state.options.max {
			return;
		}
		let expires_at = state.add(value);
		drop(state);
		self.dispatch();
		self.schedule_expiration(expires_at);
	}

	pub fn clear(&self) {
		{
			let mut state = self.state.lock().unwrap();
			state.clear();
		}
		self.dispatch();
		wake(&self.state);
	}

	pub async fn get_exclusive(&self, priority: Priority) -> Result<ExclusiveGuard<T, E>, E> {
		let lease = self.get(priority, Kind::Exclusive).await?;
		let (value, id) = lease.into_exclusive();
		let guard = ExclusiveGuard {
			id,
			pool: self.clone(),
			value: Some(value),
		};
		Ok(guard)
	}

	pub async fn get_shared(&self, priority: Priority) -> Result<SharedGuard<T, E>, E> {
		let lease = self.get(priority, Kind::Shared).await?;
		let value = lease.into_shared();
		let guard = SharedGuard {
			pool: self.clone(),
			value: Some(value),
		};
		Ok(guard)
	}

	#[must_use]
	pub fn available(&self) -> usize {
		let mut state = self.state.lock().unwrap();
		state.prune_expired();
		state
			.entries
			.iter()
			.filter(|entry| !entry.expired && entry.value.is_some() && entry.shared == 0)
			.count()
	}

	async fn get(&self, priority: Priority, kind: Kind) -> Result<Lease<T, E>, E> {
		let pool = self.clone();
		loop {
			let mut pending = None;
			let mut wake_ = false;
			let receiver = {
				let mut state = pool.state.lock().unwrap();
				state.prune_expired();
				if state.requests.is_empty()
					&& let Some((value, access)) = state.lease(kind)
				{
					return Ok(Lease {
						access,
						pool: pool.clone(),
						value: Some(value),
					});
				}
				if state.requests.is_empty()
					&& state.entries.len() + state.pending < state.options.max
				{
					state.pending += 1;
					pending = Some(Pending {
						active: true,
						epoch: state.epoch,
						state: pool.state.clone(),
					});
					None
				} else {
					let (sender, receiver) = tokio::sync::oneshot::channel();
					let order = state.order;
					let request = Request {
						kind,
						priority,
						order,
						sender,
					};
					state.order += 1;
					state.requests.push(request);
					wake_ = state.entries.len() + state.pending < state.options.max;
					Some(receiver)
				}
			};
			if wake_ {
				wake(&pool.state);
			}
			if let Some(receiver) = receiver {
				match receiver.await {
					Ok(Response::Create(pending_)) => {
						pending = Some(pending_);
					},
					Ok(Response::Lease(lease)) => return Ok(lease),
					Err(_) => continue,
				}
			}
			let mut pending = pending.unwrap();
			let result = (pool.create)().await;
			let mut state = pool.state.lock().unwrap();
			let current = pending.epoch == state.epoch;
			if current {
				state.pending -= 1;
			}
			pending.active = false;
			match result {
				Ok(value) if current => {
					let (value, access) = state.add_and_lease(value, kind);
					drop(state);
					pool.dispatch();
					return Ok(Lease {
						access,
						pool,
						value: Some(value),
					});
				},
				Err(error) if current => {
					drop(state);
					wake(&pool.state);
					return Err(error);
				},
				Ok(_) | Err(_) => {
					drop(state);
					wake(&pool.state);
				},
			}
		}
	}

	fn dispatch(&self) {
		loop {
			let mut state = self.state.lock().unwrap();
			let Some(request) = state.requests.pop() else {
				return;
			};
			if let Some((value, access)) = state.lease(request.kind) {
				let lease = Lease {
					access,
					pool: self.clone(),
					value: Some(value),
				};
				match request.sender.send(Response::Lease(lease)) {
					Ok(()) => (),
					Err(Response::Lease(mut lease)) => {
						let value = lease.value.take().unwrap();
						state.release(value, lease.access);
					},
					Err(Response::Create(_)) => unreachable!(),
				}
			} else {
				state.requests.push(request);
				return;
			}
		}
	}

	fn release(&self, value: Arc<T>, access: Access) {
		let mut state = self.state.lock().unwrap();
		let (expires_at, wake_) = state.release(value, access);
		drop(state);
		self.dispatch();
		if wake_ {
			wake(&self.state);
		}
		self.schedule_expiration(expires_at);
	}

	fn discard(&self, value: &Arc<T>, access: Access) {
		{
			let mut state = self.state.lock().unwrap();
			state.discard(value, access);
		}
		self.dispatch();
		wake(&self.state);
	}

	fn schedule_expiration(&self, expires_at: Instant) {
		self.expiration_sender.send(expires_at).ok();
	}
}

impl<T, E> Lease<T, E>
where
	T: Send + Sync + 'static,
	E: Send + 'static,
{
	fn into_exclusive(mut self) -> (Arc<T>, usize) {
		let Access::Exclusive(id) = self.access else {
			unreachable!();
		};
		(self.value.take().unwrap(), id)
	}

	fn into_shared(mut self) -> Arc<T> {
		assert!(matches!(self.access, Access::Shared));
		self.value.take().unwrap()
	}
}

impl<T, E> Drop for Lease<T, E>
where
	T: Send + Sync + 'static,
	E: Send + 'static,
{
	fn drop(&mut self) {
		if let Some(value) = self.value.take() {
			self.pool.release(value, self.access);
		}
	}
}

async fn expiration_task<T, E>(
	state: Arc<Mutex<State<T, E>>>,
	mut expirations: tokio::sync::mpsc::UnboundedReceiver<Instant>,
) where
	T: Send + Sync + 'static,
	E: Send + 'static,
{
	let mut expires_at: Option<Instant> = None;
	loop {
		if let Some(deadline) = expires_at {
			let sleep = tokio::time::sleep_until(deadline.into());
			pin_mut!(sleep);
			tokio::select! {
				() = &mut sleep => {
					let mut state = state.lock().unwrap();
					state.prune_expired();
					expires_at = state.next_expiration();
				},
				option = expirations.recv() => {
					let Some(expiration) = option else {
						break;
					};
					expires_at = Some(expires_at.map_or(expiration, |expires_at| expires_at.min(expiration)));
				},
			}
		} else {
			let Some(expiration) = expirations.recv().await else {
				break;
			};
			expires_at = Some(expiration);
		}
	}
}

fn wake<T, E>(state: &Arc<Mutex<State<T, E>>>)
where
	T: Send + Sync + 'static,
	E: Send + 'static,
{
	loop {
		let mut state_ = state.lock().unwrap();
		if state_.entries.len() + state_.pending >= state_.options.max {
			return;
		}
		let Some(request) = state_.requests.pop() else {
			return;
		};
		state_.pending += 1;
		let pending = Pending {
			active: true,
			epoch: state_.epoch,
			state: state.clone(),
		};
		match request.sender.send(Response::Create(pending)) {
			Ok(()) => return,
			Err(Response::Create(mut pending)) => {
				pending.active = false;
				state_.pending -= 1;
			},
			Err(Response::Lease(_)) => unreachable!(),
		}
	}
}

impl<T, E> State<T, E>
where
	T: Send + Sync + 'static,
	E: Send + 'static,
{
	fn add(&mut self, value: T) -> Instant {
		let expires_at = self.expires_at();
		let id = self.next_id();
		let entry = Entry {
			expires_at,
			expired: false,
			id,
			shared: 0,
			value: Some(Arc::new(value)),
		};
		self.entries.push(entry);
		expires_at
	}

	fn add_and_lease(&mut self, value: T, kind: Kind) -> (Arc<T>, Access) {
		let expires_at = self.expires_at();
		let id = self.next_id();
		let value = Arc::new(value);
		match kind {
			Kind::Exclusive => {
				let entry = Entry {
					expires_at,
					expired: false,
					id,
					shared: 0,
					value: None,
				};
				self.entries.push(entry);
				(value, Access::Exclusive(id))
			},
			Kind::Shared => {
				let entry = Entry {
					expires_at,
					expired: false,
					id,
					shared: 1,
					value: Some(value.clone()),
				};
				self.entries.push(entry);
				(value, Access::Shared)
			},
		}
	}

	fn lease(&mut self, kind: Kind) -> Option<(Arc<T>, Access)> {
		match kind {
			Kind::Exclusive => {
				let index = self
					.entries
					.iter()
					.enumerate()
					.filter(|(_, entry)| {
						!entry.expired && entry.value.is_some() && entry.shared == 0
					})
					.max_by_key(|(_, entry)| entry.expires_at)
					.map(|(index, _)| index)?;
				let entry = &mut self.entries[index];
				Some((entry.value.take().unwrap(), Access::Exclusive(entry.id)))
			},
			Kind::Shared => {
				let index = self
					.entries
					.iter()
					.enumerate()
					.find(|(_, entry)| {
						!entry.expired
							&& entry.value.is_some()
							&& entry.shared > 0 && entry.shared < self.options.shared
					})
					.map(|(index, _)| index)
					.or_else(|| {
						self.entries
							.iter()
							.enumerate()
							.filter(|(_, entry)| {
								!entry.expired && entry.value.is_some() && entry.shared == 0
							})
							.max_by_key(|(_, entry)| entry.expires_at)
							.map(|(index, _)| index)
					})?;
				let entry = &mut self.entries[index];
				entry.shared += 1;
				Some((entry.value.as_ref().unwrap().clone(), Access::Shared))
			},
		}
	}

	fn expires_at(&self) -> Instant {
		Instant::now() + self.options.ttl.unwrap_or_default()
	}

	fn next_id(&mut self) -> usize {
		let id = self.next_id;
		self.next_id += 1;
		id
	}

	fn prune_expired(&mut self) {
		if self.options.ttl.is_none() {
			return;
		}
		let now = Instant::now();
		while self.entries.len() > self.options.min {
			let Some(index) =
				self.entries
					.iter()
					.enumerate()
					.filter(|(_, entry)| {
						!entry.expired
							&& entry.value.is_some()
							&& entry.shared == 0 && now >= entry.expires_at
					})
					.min_by_key(|(_, entry)| entry.expires_at)
					.map(|(index, _)| index)
			else {
				break;
			};
			self.entries.remove(index);
		}
	}

	fn next_expiration(&self) -> Option<Instant> {
		self.options.ttl?;
		if self.entries.len() <= self.options.min {
			return None;
		}
		self.entries
			.iter()
			.filter(|entry| !entry.expired && entry.value.is_some() && entry.shared == 0)
			.map(|entry| entry.expires_at)
			.min()
	}

	fn clear(&mut self) {
		self.epoch += 1;
		self.pending = 0;
		for entry in &mut self.entries {
			entry.expired = true;
		}
		self.entries
			.retain(|entry| entry.value.is_none() || entry.shared > 0);
	}

	fn discard(&mut self, value: &Arc<T>, access: Access) {
		match access {
			Access::Exclusive(id) => {
				if let Some(index) = self.empty_entry_index(id) {
					self.entries.remove(index);
				}
			},
			Access::Shared => {
				if let Some(index) = self.entries.iter().position(|entry| {
					entry
						.value
						.as_ref()
						.is_some_and(|entry_value| Arc::ptr_eq(entry_value, value))
				}) {
					let entry = &mut self.entries[index];
					entry.expired = true;
					entry.shared -= 1;
					if entry.shared == 0 {
						self.entries.remove(index);
					}
				}
			},
		}
	}

	fn release(&mut self, value: Arc<T>, access: Access) -> (Instant, bool) {
		let expires_at = self.expires_at();
		let mut wake = false;
		match access {
			Access::Exclusive(id) => {
				if let Some(index) = self.empty_entry_index(id) {
					let entry = &mut self.entries[index];
					if entry.expired {
						wake = true;
						self.entries.remove(index);
					} else {
						entry.value = Some(value);
						entry.expires_at = expires_at;
					}
				}
			},
			Access::Shared => {
				if let Some(index) = self.entries.iter().position(|entry| {
					entry
						.value
						.as_ref()
						.is_some_and(|entry_value| Arc::ptr_eq(entry_value, &value))
				}) {
					let entry = &mut self.entries[index];
					entry.shared -= 1;
					if entry.shared == 0 && entry.expired {
						self.entries.remove(index);
						wake = true;
					} else if entry.shared == 0 {
						entry.expires_at = expires_at;
					}
				}
			},
		}
		(expires_at, wake)
	}

	fn empty_entry_index(&self, id: usize) -> Option<usize> {
		self.entries
			.iter()
			.position(|entry| entry.id == id && entry.value.is_none())
	}
}

impl<T, E> Drop for Pending<T, E>
where
	T: Send + Sync + 'static,
	E: Send + 'static,
{
	fn drop(&mut self) {
		if self.active {
			{
				let mut state = self.state.lock().unwrap();
				if self.epoch == state.epoch {
					state.pending -= 1;
				}
			}
			wake(&self.state);
		}
	}
}

impl<T, E> Clone for Pool<T, E>
where
	T: Send + Sync + 'static,
	E: Send + 'static,
{
	fn clone(&self) -> Self {
		Self {
			create: self.create.clone(),
			expiration_sender: self.expiration_sender.clone(),
			expiration_task: self.expiration_task.clone(),
			state: self.state.clone(),
		}
	}
}

impl<T, E> Deref for ExclusiveGuard<T, E>
where
	T: Send + Sync + 'static,
	E: Send + 'static,
{
	type Target = T;

	fn deref(&self) -> &Self::Target {
		self.value.as_ref().unwrap()
	}
}

impl<T, E> DerefMut for ExclusiveGuard<T, E>
where
	T: Send + Sync + 'static,
	E: Send + 'static,
{
	fn deref_mut(&mut self) -> &mut Self::Target {
		Arc::get_mut(self.value.as_mut().unwrap()).unwrap()
	}
}

impl<T, E> AsRef<T> for ExclusiveGuard<T, E>
where
	T: Send + Sync + 'static,
	E: Send + 'static,
{
	fn as_ref(&self) -> &T {
		self
	}
}

impl<T, E> AsMut<T> for ExclusiveGuard<T, E>
where
	T: Send + Sync + 'static,
	E: Send + 'static,
{
	fn as_mut(&mut self) -> &mut T {
		self
	}
}

impl<T, E> Drop for ExclusiveGuard<T, E>
where
	T: Send + Sync + 'static,
	E: Send + 'static,
{
	fn drop(&mut self) {
		if let Some(value) = self.value.take() {
			self.pool.release(value, Access::Exclusive(self.id));
		}
	}
}

impl<T, E> ExclusiveGuard<T, E>
where
	T: Send + Sync + 'static,
	E: Send + 'static,
{
	pub fn discard(mut self) {
		if let Some(value) = self.value.take() {
			self.pool.discard(&value, Access::Exclusive(self.id));
		}
	}
}

impl<T, E> Deref for SharedGuard<T, E>
where
	T: Send + Sync + 'static,
	E: Send + 'static,
{
	type Target = T;

	fn deref(&self) -> &Self::Target {
		self.value.as_ref().unwrap()
	}
}

impl<T, E> AsRef<T> for SharedGuard<T, E>
where
	T: Send + Sync + 'static,
	E: Send + 'static,
{
	fn as_ref(&self) -> &T {
		self
	}
}

impl<T, E> Drop for SharedGuard<T, E>
where
	T: Send + Sync + 'static,
	E: Send + 'static,
{
	fn drop(&mut self) {
		if let Some(value) = self.value.take() {
			self.pool.release(value, Access::Shared);
		}
	}
}

impl<T, E> SharedGuard<T, E>
where
	T: Send + Sync + 'static,
	E: Send + 'static,
{
	pub fn discard(mut self) {
		if let Some(value) = self.value.take() {
			self.pool.discard(&value, Access::Shared);
		}
	}
}

impl<T, E> PartialEq for Request<T, E>
where
	T: Send + Sync + 'static,
	E: Send + 'static,
{
	fn eq(&self, other: &Self) -> bool {
		self.priority == other.priority && self.order == other.order
	}
}

impl<T, E> Eq for Request<T, E>
where
	T: Send + Sync + 'static,
	E: Send + 'static,
{
}

impl<T, E> PartialOrd for Request<T, E>
where
	T: Send + Sync + 'static,
	E: Send + 'static,
{
	fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
		Some(self.cmp(other))
	}
}

impl<T, E> Ord for Request<T, E>
where
	T: Send + Sync + 'static,
	E: Send + 'static,
{
	fn cmp(&self, other: &Self) -> std::cmp::Ordering {
		self.priority
			.cmp(&other.priority)
			.then_with(|| self.order.cmp(&other.order).reverse())
	}
}

#[cfg(test)]
mod tests {
	use {
		super::{Kind, Options, Pool, Priority, Request, Response, wake},
		std::{
			sync::{
				Arc,
				atomic::{AtomicUsize, Ordering},
			},
			time::Duration,
		},
	};

	type Error = &'static str;

	struct DropCounter(Arc<AtomicUsize>);

	impl Drop for DropCounter {
		fn drop(&mut self) {
			self.0.fetch_add(1, Ordering::SeqCst);
		}
	}

	fn options(max: usize) -> Options {
		Options {
			min: 0,
			max,
			shared: 1,
			ttl: None,
		}
	}

	// A pool without a time to live does not spawn the expiration task and can be constructed outside of an async runtime.
	#[test]
	fn pool_without_ttl_can_be_created_without_a_runtime() {
		let _pool = Pool::new(options(1), || async { Ok::<_, Error>(0) });
	}

	// Dropping an exclusive guard returns the value to the pool, preserving any mutations, so that it can be leased again.
	#[tokio::test]
	async fn exclusive_returns_value_on_drop() {
		let pool = Pool::new(options(1), || async { Ok::<_, Error>(0) });
		{
			let mut guard = pool.get_exclusive(Priority::default()).await.unwrap();
			*guard = 1;
			assert_eq!(*guard, 1);
			assert_eq!(pool.available(), 0);
		}
		assert_eq!(pool.available(), 1);
		assert_eq!(*pool.get_exclusive(Priority::default()).await.unwrap(), 1);
	}

	// Adding values does not exceed the maximum, so the first added value is the one that is leased.
	#[tokio::test]
	async fn add_respects_max() {
		let pool = Pool::new(options(1), || async { Ok::<_, Error>(2) });
		pool.add(0);
		pool.add(1);
		assert_eq!(*pool.get_exclusive(Priority::default()).await.unwrap(), 0);
	}

	// Adding a value while a creation is pending does not exceed the maximum capacity, so the added value is dropped rather than admitted.
	#[tokio::test]
	async fn add_respects_pending_capacity() {
		let (sender, receiver) = tokio::sync::oneshot::channel::<()>();
		let receiver = Arc::new(std::sync::Mutex::new(Some(receiver)));
		let pool = Pool::new(options(1), {
			let receiver = receiver.clone();
			move || {
				let receiver = receiver.clone();
				async move {
					let receiver = receiver.lock().unwrap().take().unwrap();
					receiver.await.unwrap();
					Ok::<_, Error>(1)
				}
			}
		});
		let guard = tokio::spawn({
			let pool = pool.clone();
			async move { pool.get_exclusive(Priority::default()).await.unwrap() }
		});
		tokio::task::yield_now().await;
		pool.add(0);
		sender.send(()).unwrap();
		let guard = guard.await.unwrap();
		assert_eq!(*guard, 1);
		drop(guard);
		assert_eq!(pool.available(), 1);
	}

	// A new request does not trigger a creation while an earlier queued request is waiting, so the queued request is the one that is woken to create.
	#[tokio::test]
	async fn new_requests_do_not_create_before_queued_requests() {
		let pool = Pool::new(options(1), || async { Ok::<_, Error>(0) });
		let (sender, receiver) = tokio::sync::oneshot::channel();
		{
			let mut state = pool.state.lock().unwrap();
			let request = Request {
				kind: Kind::Exclusive,
				order: state.order,
				priority: Priority::High,
				sender,
			};
			state.order += 1;
			state.requests.push(request);
		}
		let low = tokio::spawn({
			let pool = pool.clone();
			async move { pool.get_exclusive(Priority::Low).await.unwrap() }
		});
		let response = tokio::time::timeout(Duration::from_secs(1), receiver)
			.await
			.unwrap()
			.unwrap();
		assert!(matches!(response, Response::Create(_)));
		low.abort();
	}

	// When a leased value is dispatched to a request whose receiver has been dropped, the value is returned to the pool rather than lost.
	#[tokio::test]
	async fn cancelled_lease_receiver_returns_value() {
		let pool = Pool::new(options(1), || async { Ok::<_, Error>(0) });
		pool.add(0);
		let (sender, receiver) = tokio::sync::oneshot::channel();
		{
			let mut state = pool.state.lock().unwrap();
			let request = Request {
				kind: Kind::Exclusive,
				order: state.order,
				priority: Priority::default(),
				sender,
			};
			state.order += 1;
			state.requests.push(request);
		}
		pool.dispatch();
		assert_eq!(pool.available(), 0);
		drop(receiver);
		assert_eq!(pool.available(), 1);
	}

	// A single value can be shared up to the configured shared limit, and a further shared request waits until a shared guard is dropped.
	#[tokio::test]
	async fn shared_guards_respect_the_limit() {
		let mut options = options(1);
		options.shared = 2;
		let pool = Pool::new(options, || async { Ok::<_, Error>(0) });
		let a = pool.get_shared(Priority::default()).await.unwrap();
		let b = pool.get_shared(Priority::default()).await.unwrap();
		let c = tokio::spawn({
			let pool = pool.clone();
			async move { pool.get_shared(Priority::default()).await.unwrap() }
		});
		tokio::task::yield_now().await;
		assert!(!c.is_finished());
		drop(a);
		let c = c.await.unwrap();
		assert_eq!(*b, 0);
		assert_eq!(*c, 0);
	}

	// A shared waiter that arrives during another request's creation shares the newly created value once it becomes available rather than creating a second value.
	#[tokio::test]
	async fn shared_waiters_can_use_a_newly_created_value() {
		let mut options = options(1);
		options.shared = 2;
		let (sender, receiver) = tokio::sync::oneshot::channel::<()>();
		let receiver = Arc::new(std::sync::Mutex::new(Some(receiver)));
		let pool = Pool::new(options, {
			let receiver = receiver.clone();
			move || {
				let receiver = receiver.clone();
				async move {
					let receiver = receiver.lock().unwrap().take().unwrap();
					receiver.await.unwrap();
					Ok::<_, Error>(0)
				}
			}
		});
		let first = tokio::spawn({
			let pool = pool.clone();
			async move { pool.get_shared(Priority::default()).await.unwrap() }
		});
		tokio::task::yield_now().await;
		let second = tokio::spawn({
			let pool = pool.clone();
			async move { pool.get_shared(Priority::default()).await.unwrap() }
		});
		tokio::task::yield_now().await;
		assert!(!second.is_finished());
		sender.send(()).unwrap();
		let first = first.await.unwrap();
		let second = second.await.unwrap();
		assert_eq!(*first, 0);
		assert_eq!(*second, 0);
	}

	// An exclusive request cannot acquire a value that is currently held by a shared guard, so it waits until the shared guard is dropped.
	#[tokio::test]
	async fn shared_and_exclusive_access_exclude_each_other() {
		let pool = Pool::new(options(1), || async { Ok::<_, Error>(0) });
		let shared = pool.get_shared(Priority::default()).await.unwrap();
		let exclusive = tokio::spawn({
			let pool = pool.clone();
			async move { pool.get_exclusive(Priority::default()).await.unwrap() }
		});
		tokio::task::yield_now().await;
		assert!(!exclusive.is_finished());
		drop(shared);
		assert_eq!(*exclusive.await.unwrap(), 0);
	}

	// A later shared request does not jump ahead of an earlier queued exclusive request to share an active value, so the exclusive request is served first.
	#[tokio::test]
	async fn shared_requests_do_not_bypass_queued_exclusive_requests() {
		let mut options = options(1);
		options.shared = 2;
		let pool = Pool::new(options, || async { Ok::<_, Error>(0) });
		let shared = pool.get_shared(Priority::default()).await.unwrap();
		let exclusive = tokio::spawn({
			let pool = pool.clone();
			async move { pool.get_exclusive(Priority::High).await.unwrap() }
		});
		tokio::task::yield_now().await;
		let shared_ = tokio::spawn({
			let pool = pool.clone();
			async move { pool.get_shared(Priority::Low).await.unwrap() }
		});
		tokio::task::yield_now().await;
		assert!(!exclusive.is_finished());
		assert!(!shared_.is_finished());
		drop(shared);
		let exclusive = exclusive.await.unwrap();
		assert!(!shared_.is_finished());
		drop(exclusive);
		assert_eq!(*shared_.await.unwrap(), 0);
	}

	// Waiters are served by descending priority and, within the same priority, in first in first out order.
	#[tokio::test]
	async fn priority_and_fifo_order_are_preserved() {
		let pool = Pool::new(options(1), || async { Ok::<_, Error>(0) });
		let guard = pool.get_exclusive(Priority::default()).await.unwrap();
		let low = tokio::spawn({
			let pool = pool.clone();
			async move {
				let guard = pool.get_exclusive(Priority::Low).await.unwrap();
				*guard
			}
		});
		let high_a = tokio::spawn({
			let pool = pool.clone();
			async move {
				let guard = pool.get_exclusive(Priority::High).await.unwrap();
				*guard
			}
		});
		let high_b = tokio::spawn({
			let pool = pool.clone();
			async move {
				let guard = pool.get_exclusive(Priority::High).await.unwrap();
				*guard
			}
		});
		drop(guard);
		assert_eq!(high_a.await.unwrap(), 0);
		assert_eq!(high_b.await.unwrap(), 0);
		assert_eq!(low.await.unwrap(), 0);
	}

	// A cancelled waiter is skipped when a value becomes available, so the next live waiter receives the value.
	#[tokio::test]
	async fn cancelled_waiters_are_skipped() {
		let pool = Pool::new(options(1), || async { Ok::<_, Error>(0) });
		let guard = pool.get_exclusive(Priority::default()).await.unwrap();
		let cancelled = tokio::spawn({
			let pool = pool.clone();
			async move { pool.get_exclusive(Priority::High).await.unwrap() }
		});
		let next = tokio::spawn({
			let pool = pool.clone();
			async move { pool.get_exclusive(Priority::Low).await.unwrap() }
		});
		cancelled.abort();
		drop(guard);
		assert_eq!(*next.await.unwrap(), 0);
	}

	// The pool creates at most the maximum number of values, so a further request waits rather than creating an additional value.
	#[tokio::test]
	async fn creation_stops_at_max() {
		let created = Arc::new(AtomicUsize::new(0));
		let pool = Pool::new(options(2), {
			let created = created.clone();
			move || {
				let created = created.clone();
				async move { Ok::<_, Error>(created.fetch_add(1, Ordering::SeqCst)) }
			}
		});
		let a = pool.get_exclusive(Priority::default()).await.unwrap();
		let b = pool.get_exclusive(Priority::default()).await.unwrap();
		let c = tokio::spawn({
			let pool = pool.clone();
			async move { pool.get_exclusive(Priority::default()).await.unwrap() }
		});
		tokio::task::yield_now().await;
		assert!(!c.is_finished());
		assert_eq!(created.load(Ordering::SeqCst), 2);
		drop(a);
		assert!(*c.await.unwrap() <= 1);
		drop(b);
	}

	// A shared request reuses an already active value with available shared capacity rather than creating a new value.
	#[tokio::test]
	async fn shared_reuses_active_value_before_creating() {
		let mut options = options(2);
		options.shared = 2;
		let created = Arc::new(AtomicUsize::new(0));
		let pool = Pool::new(options, {
			let created = created.clone();
			move || {
				let created = created.clone();
				async move { Ok::<_, Error>(created.fetch_add(1, Ordering::SeqCst)) }
			}
		});
		let a = pool.get_shared(Priority::default()).await.unwrap();
		let b = pool.get_shared(Priority::default()).await.unwrap();
		assert_eq!(*a, 0);
		assert_eq!(*b, 0);
		assert_eq!(created.load(Ordering::SeqCst), 1);
	}

	// Discarding an exclusive guard removes its value and frees capacity, so the next request creates a fresh value.
	#[tokio::test]
	async fn discarded_exclusive_guard_frees_capacity() {
		let created = Arc::new(AtomicUsize::new(0));
		let pool = Pool::new(options(1), {
			let created = created.clone();
			move || {
				let created = created.clone();
				async move { Ok::<_, Error>(created.fetch_add(1, Ordering::SeqCst)) }
			}
		});
		let guard = pool.get_exclusive(Priority::default()).await.unwrap();
		assert_eq!(*guard, 0);
		guard.discard();
		assert_eq!(*pool.get_exclusive(Priority::default()).await.unwrap(), 1);
	}

	// Discarding a shared guard prevents the value from being reused, so the next shared request creates a fresh value.
	#[tokio::test]
	async fn discarded_shared_guard_is_not_reused() {
		let mut options = options(1);
		options.shared = 2;
		let created = Arc::new(AtomicUsize::new(0));
		let pool = Pool::new(options, {
			let created = created.clone();
			move || {
				let created = created.clone();
				async move { Ok::<_, Error>(created.fetch_add(1, Ordering::SeqCst)) }
			}
		});
		let guard = pool.get_shared(Priority::default()).await.unwrap();
		assert_eq!(*guard, 0);
		guard.discard();
		assert_eq!(*pool.get_shared(Priority::default()).await.unwrap(), 1);
	}

	// A value with a discarded shared guard is removed only once all of its other shared guards are dropped, so a waiter is served only afterward.
	#[tokio::test]
	async fn discarded_shared_guard_waits_for_other_shared_guards() {
		let mut options = options(1);
		options.shared = 2;
		let created = Arc::new(AtomicUsize::new(0));
		let pool = Pool::new(options, {
			let created = created.clone();
			move || {
				let created = created.clone();
				async move { Ok::<_, Error>(created.fetch_add(1, Ordering::SeqCst)) }
			}
		});
		let a = pool.get_shared(Priority::default()).await.unwrap();
		let b = pool.get_shared(Priority::default()).await.unwrap();
		a.discard();
		let next = tokio::spawn({
			let pool = pool.clone();
			async move { pool.get_shared(Priority::default()).await.unwrap() }
		});
		tokio::task::yield_now().await;
		assert!(!next.is_finished());
		drop(b);
		assert_eq!(*next.await.unwrap(), 1);
	}

	// Clearing the pool immediately drops available values and retires active values so that they are dropped once their guards are released.
	#[tokio::test]
	async fn clear_drops_available_values_and_retires_active_values() {
		let mut options = options(2);
		options.shared = 2;
		let drops = Arc::new(AtomicUsize::new(0));
		let pool = Pool::new(options, || async { Err::<DropCounter, Error>("unused") });
		pool.add(DropCounter(drops.clone()));
		pool.add(DropCounter(drops.clone()));
		let guard = pool.get_shared(Priority::default()).await.unwrap();
		pool.clear();
		assert_eq!(drops.load(Ordering::SeqCst), 1);
		drop(guard);
		assert_eq!(drops.load(Ordering::SeqCst), 2);
	}

	// After clearing, each still active exclusive guard retains its capacity slot, so a new request can create immediately only once a retired guard is dropped.
	#[tokio::test]
	async fn clear_retains_capacity_for_each_active_exclusive_guard() {
		let created = Arc::new(AtomicUsize::new(0));
		let pool = Pool::new(options(2), {
			let created = created.clone();
			move || {
				let created = created.clone();
				async move { Ok::<_, Error>(created.fetch_add(1, Ordering::SeqCst)) }
			}
		});
		let a = pool.get_exclusive(Priority::default()).await.unwrap();
		let b = pool.get_exclusive(Priority::default()).await.unwrap();
		pool.clear();
		drop(a);
		let next_a = tokio::spawn({
			let pool = pool.clone();
			async move { pool.get_exclusive(Priority::default()).await.unwrap() }
		});
		let next_b = tokio::spawn({
			let pool = pool.clone();
			async move { pool.get_exclusive(Priority::default()).await.unwrap() }
		});
		tokio::task::yield_now().await;
		assert!(next_a.is_finished());
		assert!(!next_b.is_finished());
		drop(b);
		assert_eq!(*next_a.await.unwrap(), 2);
		assert_eq!(*next_b.await.unwrap(), 3);
	}

	// Dropping an exclusive guard that was retired by a clear does not repopulate the entry created after the clear, so subsequent requests create fresh values.
	#[tokio::test]
	async fn cleared_exclusive_guard_does_not_repopulate_new_exclusive_entry() {
		let created = Arc::new(AtomicUsize::new(0));
		let pool = Pool::new(options(2), {
			let created = created.clone();
			move || {
				let created = created.clone();
				async move { Ok::<_, Error>(created.fetch_add(1, Ordering::SeqCst)) }
			}
		});
		let old_a = pool.get_exclusive(Priority::default()).await.unwrap();
		let old_b = pool.get_exclusive(Priority::default()).await.unwrap();
		pool.clear();
		drop(old_a);
		let new = pool.get_exclusive(Priority::default()).await.unwrap();
		assert_eq!(*new, 2);
		new.discard();
		drop(old_b);
		assert_eq!(*pool.get_exclusive(Priority::default()).await.unwrap(), 3);
	}

	// A creation that completes after a clear is discarded and its value dropped, so the original request retries and creates a new value.
	#[tokio::test]
	async fn clear_discards_pending_creation() {
		let (sender, receiver) = tokio::sync::oneshot::channel::<()>();
		let receiver = Arc::new(std::sync::Mutex::new(Some(receiver)));
		let created = Arc::new(AtomicUsize::new(0));
		let drops = Arc::new(AtomicUsize::new(0));
		let pool = Pool::new(options(1), {
			let created = created.clone();
			let drops = drops.clone();
			let receiver = receiver.clone();
			move || {
				let created = created.clone();
				let drops = drops.clone();
				let receiver = receiver.clone();
				async move {
					let index = created.fetch_add(1, Ordering::SeqCst);
					if index == 0 {
						let receiver = receiver.lock().unwrap().take().unwrap();
						receiver.await.unwrap();
					}
					Ok::<_, Error>(DropCounter(drops))
				}
			}
		});
		let guard = tokio::spawn({
			let pool = pool.clone();
			async move { pool.get_exclusive(Priority::default()).await.unwrap() }
		});
		tokio::task::yield_now().await;
		pool.clear();
		sender.send(()).unwrap();
		let guard = guard.await.unwrap();
		assert_eq!(created.load(Ordering::SeqCst), 2);
		assert_eq!(drops.load(Ordering::SeqCst), 1);
		drop(guard);
		assert_eq!(drops.load(Ordering::SeqCst), 1);
	}

	// Clearing releases the capacity reserved by a pending creation, so a new request can proceed without waiting for the retired creation.
	#[tokio::test]
	async fn clear_releases_capacity_from_pending_creation() {
		let (_sender, receiver) = tokio::sync::oneshot::channel::<()>();
		let receiver = Arc::new(std::sync::Mutex::new(Some(receiver)));
		let created = Arc::new(AtomicUsize::new(0));
		let pool = Pool::new(options(1), {
			let created = created.clone();
			let receiver = receiver.clone();
			move || {
				let created = created.clone();
				let receiver = receiver.clone();
				async move {
					let index = created.fetch_add(1, Ordering::SeqCst);
					if index == 0 {
						let receiver = receiver.lock().unwrap().take().unwrap();
						receiver.await.unwrap();
					}
					Ok::<_, Error>(index)
				}
			}
		});
		let retired = tokio::spawn({
			let pool = pool.clone();
			async move { pool.get_exclusive(Priority::default()).await.unwrap() }
		});
		tokio::task::yield_now().await;
		pool.clear();
		let replacement = tokio::time::timeout(
			Duration::from_secs(1),
			pool.get_exclusive(Priority::default()),
		)
		.await
		.unwrap()
		.unwrap();
		assert_eq!(*replacement, 1);
		retired.abort();
	}

	// An error from a creation that completes after a clear is discarded rather than returned, so the request retries and ultimately succeeds.
	#[tokio::test]
	async fn clear_discards_errors_from_pending_creation() {
		let (sender, receiver) = tokio::sync::oneshot::channel::<()>();
		let receiver = Arc::new(std::sync::Mutex::new(Some(receiver)));
		let attempts = Arc::new(AtomicUsize::new(0));
		let pool = Pool::new(options(1), {
			let attempts = attempts.clone();
			let receiver = receiver.clone();
			move || {
				let attempts = attempts.clone();
				let receiver = receiver.clone();
				async move {
					if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
						let receiver = receiver.lock().unwrap().take().unwrap();
						receiver.await.unwrap();
						Err("failed")
					} else {
						Ok(1)
					}
				}
			}
		});
		let guard = tokio::spawn({
			let pool = pool.clone();
			async move { pool.get_exclusive(Priority::default()).await }
		});
		tokio::task::yield_now().await;
		pool.clear();
		sender.send(()).unwrap();
		let guard = tokio::time::timeout(Duration::from_secs(1), guard)
			.await
			.unwrap()
			.unwrap()
			.unwrap();
		assert_eq!(*guard, 1);
		assert_eq!(attempts.load(Ordering::SeqCst), 2);
	}

	// A value created under a zero time to live is not pruned before the requesting checkout receives it.
	#[tokio::test]
	async fn zero_ttl_does_not_expire_a_newly_created_value_before_checkout() {
		let mut options = options(1);
		options.ttl = Some(Duration::ZERO);
		let pool = Pool::new(options, || async { Ok::<_, Error>(0) });
		assert_eq!(*pool.get_exclusive(Priority::default()).await.unwrap(), 0);
	}

	// When several values are available, an exclusive lease reuses the value with the most recent expiration first.
	#[tokio::test]
	async fn ttl_reuse_is_most_recent_first() {
		let pool = Pool::new(options(2), || async { Ok::<_, Error>(0) });
		pool.add(1);
		pool.add(2);
		let a = pool.get_exclusive(Priority::default()).await.unwrap();
		let b = pool.get_exclusive(Priority::default()).await.unwrap();
		drop(a);
		tokio::time::sleep(Duration::from_millis(1)).await;
		drop(b);
		assert_eq!(*pool.get_exclusive(Priority::default()).await.unwrap(), 1);
	}

	// Expiration prunes expired values down to the configured minimum, leaving exactly the minimum number available.
	#[tokio::test]
	async fn ttl_expiration_drops_to_min() {
		let mut options = options(3);
		options.min = 1;
		options.ttl = Some(Duration::from_millis(1));
		let pool = Pool::new(options, || async { Ok::<_, Error>(0) });
		pool.add(1);
		pool.add(2);
		pool.add(3);
		tokio::time::sleep(Duration::from_millis(2)).await;
		assert_eq!(pool.available(), 1);
	}

	// When pruning down to the minimum, expiration keeps the most recently added values and discards the older ones.
	#[tokio::test]
	async fn ttl_expiration_keeps_the_most_recent_min_values() {
		let mut options = options(2);
		options.min = 1;
		options.ttl = Some(Duration::from_millis(1));
		let pool = Pool::new(options, || async { Ok::<_, Error>(0) });
		pool.add(1);
		tokio::time::sleep(Duration::from_millis(2)).await;
		pool.add(2);
		tokio::time::sleep(Duration::from_millis(2)).await;
		assert_eq!(*pool.get_exclusive(Priority::default()).await.unwrap(), 2);
	}

	// The background expiration task prunes expired values down to the minimum without any further pool activity.
	#[tokio::test]
	async fn ttl_task_expires_values_without_pool_activity() {
		let mut options = options(3);
		options.min = 1;
		options.ttl = Some(Duration::from_millis(1));
		let drops = Arc::new(AtomicUsize::new(0));
		let pool = Pool::new(options, || async { Err::<DropCounter, Error>("unused") });
		pool.add(DropCounter(drops.clone()));
		pool.add(DropCounter(drops.clone()));
		pool.add(DropCounter(drops.clone()));
		tokio::time::sleep(Duration::from_millis(10)).await;
		assert_eq!(drops.load(Ordering::SeqCst), 2);
	}

	// The background expiration task does not hold a strong reference to the pool, so dropping the pool drops its values even with a long time to live.
	#[tokio::test]
	async fn ttl_task_does_not_keep_the_pool_alive() {
		let mut options = options(1);
		options.ttl = Some(Duration::from_mins(1));
		let drops = Arc::new(AtomicUsize::new(0));
		{
			let pool = Pool::new(options, || async { Err::<DropCounter, Error>("unused") });
			pool.add(DropCounter(drops.clone()));
		}
		tokio::task::yield_now().await;
		assert_eq!(drops.load(Ordering::SeqCst), 1);
	}

	// After expiring one value, the background expiration task reschedules and continues to expire the next value at its later deadline.
	#[tokio::test]
	async fn ttl_task_continues_to_the_next_expiration() {
		let mut options = options(3);
		options.ttl = Some(Duration::from_millis(20));
		let drops = Arc::new(AtomicUsize::new(0));
		let pool = Pool::new(options, || async { Err::<DropCounter, Error>("unused") });
		pool.add(DropCounter(drops.clone()));
		tokio::time::sleep(Duration::from_millis(5)).await;
		pool.add(DropCounter(drops.clone()));
		tokio::time::sleep(Duration::from_millis(50)).await;
		assert_eq!(drops.load(Ordering::SeqCst), 2);
	}

	// A creation error does not consume a capacity slot, so a subsequent request can create a value successfully.
	#[tokio::test]
	async fn factory_errors_do_not_consume_capacity() {
		let attempts = Arc::new(AtomicUsize::new(0));
		let pool = Pool::new(options(1), {
			let attempts = attempts.clone();
			move || {
				let attempts = attempts.clone();
				async move {
					if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
						Err("failed")
					} else {
						Ok(1)
					}
				}
			}
		});
		assert!(matches!(
			pool.get_exclusive(Priority::default()).await,
			Err("failed")
		));
		assert_eq!(*pool.get_exclusive(Priority::default()).await.unwrap(), 1);
	}

	// When a request that was creating a value is cancelled, the freed capacity wakes a waiting request so that it can create the value instead.
	#[tokio::test]
	async fn cancelled_creation_wakes_a_waiter() {
		let attempts = Arc::new(AtomicUsize::new(0));
		let (_sender, receiver) = tokio::sync::oneshot::channel::<()>();
		let receiver = Arc::new(std::sync::Mutex::new(Some(receiver)));
		let pool = Pool::new(options(1), {
			let attempts = attempts.clone();
			let receiver = receiver.clone();
			move || {
				let attempts = attempts.clone();
				let receiver = receiver.clone();
				async move {
					if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
						let receiver = receiver.lock().unwrap().take().unwrap();
						receiver.await.unwrap();
					}
					Ok::<_, Error>(1)
				}
			}
		});
		let cancelled = tokio::spawn({
			let pool = pool.clone();
			async move { pool.get_exclusive(Priority::default()).await.unwrap() }
		});
		tokio::task::yield_now().await;
		let waiter = tokio::spawn({
			let pool = pool.clone();
			async move { pool.get_exclusive(Priority::default()).await.unwrap() }
		});
		tokio::task::yield_now().await;
		cancelled.abort();
		let guard = tokio::time::timeout(Duration::from_secs(1), waiter)
			.await
			.unwrap()
			.unwrap();
		assert_eq!(*guard, 1);
	}

	// When a waiter drops the creation response it was sent, the reserved capacity is released and the next queued waiter is woken to create.
	#[tokio::test]
	async fn cancelled_creation_response_wakes_a_waiter() {
		let pool = Pool::new(options(1), || async { Ok::<_, Error>(0) });
		let (first_sender, first_receiver) = tokio::sync::oneshot::channel();
		let (second_sender, second_receiver) = tokio::sync::oneshot::channel();
		{
			let mut state = pool.state.lock().unwrap();
			let first = Request {
				kind: Kind::Exclusive,
				order: state.order,
				priority: Priority::High,
				sender: first_sender,
			};
			state.order += 1;
			let second = Request {
				kind: Kind::Exclusive,
				order: state.order,
				priority: Priority::High,
				sender: second_sender,
			};
			state.order += 1;
			state.requests.push(first);
			state.requests.push(second);
		}
		wake(&pool.state);
		let response = first_receiver.await.unwrap();
		assert!(matches!(response, Response::Create(_)));
		drop(response);
		let response = tokio::time::timeout(Duration::from_secs(1), second_receiver)
			.await
			.unwrap()
			.unwrap();
		assert!(matches!(response, Response::Create(_)));
	}

	// A creation error is returned to the request that initiated the creation, and the freed capacity wakes a waiter that then creates successfully.
	#[tokio::test]
	async fn factory_errors_are_returned_to_the_creator_and_wake_a_waiter() {
		let attempts = Arc::new(AtomicUsize::new(0));
		let pool = Pool::new(options(1), {
			let attempts = attempts.clone();
			move || {
				let attempts = attempts.clone();
				async move {
					if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
						tokio::task::yield_now().await;
						Err("failed")
					} else {
						Ok(1)
					}
				}
			}
		});
		let creator = tokio::spawn({
			let pool = pool.clone();
			async move { pool.get_exclusive(Priority::default()).await }
		});
		tokio::task::yield_now().await;
		let waiter = tokio::spawn({
			let pool = pool.clone();
			async move { pool.get_exclusive(Priority::default()).await.unwrap() }
		});
		assert!(matches!(creator.await.unwrap(), Err("failed")));
		assert_eq!(*waiter.await.unwrap(), 1);
	}

	// When a creating request is cancelled, the waiters retain their relative order so that the earliest queued waiter is served first.
	#[tokio::test]
	async fn cancelled_creation_preserves_waiter_order() {
		let attempts = Arc::new(AtomicUsize::new(0));
		let (_sender, receiver) = tokio::sync::oneshot::channel::<()>();
		let receiver = Arc::new(std::sync::Mutex::new(Some(receiver)));
		let pool = Pool::new(options(1), {
			let attempts = attempts.clone();
			let receiver = receiver.clone();
			move || {
				let attempts = attempts.clone();
				let receiver = receiver.clone();
				async move {
					let attempt = attempts.fetch_add(1, Ordering::SeqCst);
					if attempt == 0 {
						let receiver = receiver.lock().unwrap().take().unwrap();
						receiver.await.unwrap();
					}
					Ok::<_, Error>(attempt)
				}
			}
		});
		let cancelled = tokio::spawn({
			let pool = pool.clone();
			async move { pool.get_exclusive(Priority::default()).await.unwrap() }
		});
		tokio::task::yield_now().await;
		let first = tokio::spawn({
			let pool = pool.clone();
			async move { pool.get_exclusive(Priority::High).await.unwrap() }
		});
		let second = tokio::spawn({
			let pool = pool.clone();
			async move { pool.get_exclusive(Priority::High).await.unwrap() }
		});
		tokio::task::yield_now().await;
		cancelled.abort();
		let first = tokio::time::timeout(Duration::from_secs(1), first)
			.await
			.unwrap()
			.unwrap();
		assert_eq!(*first, 1);
		assert!(!second.is_finished());
		drop(first);
		assert_eq!(*second.await.unwrap(), 1);
	}
}
