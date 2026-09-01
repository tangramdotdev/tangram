use {
	futures::{SinkExt as _, StreamExt as _, channel::mpsc, channel::oneshot},
	std::{
		collections::HashMap,
		convert::Infallible,
		future::Future,
		ops::ControlFlow,
		sync::{
			Arc, Mutex,
			atomic::{AtomicUsize, Ordering},
		},
	},
	tangram_client::prelude::*,
};

pub(crate) type LmdbError = Infallible;
pub(crate) type Receiver<E> = mpsc::Receiver<Message<E>>;
type Response<E> = Result<ControlFlow<Output, E>, tg::Error>;

#[derive(Clone, Debug)]
pub(crate) enum Request {
	Group {
		group: tg::group::Id,
	},
	GroupMembers {
		after: Option<Vec<u8>>,
		group: tg::group::Id,
		limit: usize,
	},
	Id {
		id: tg::Id,
	},
	MemberGroups {
		after: Option<Vec<u8>>,
		limit: usize,
		member: tg::Id,
	},
	MemberOrganizations {
		after: Option<Vec<u8>>,
		limit: usize,
		member: tg::Id,
	},
	ObjectChildren {
		after: Option<Vec<u8>>,
		limit: usize,
		object: tg::object::Id,
	},
	ObjectParents {
		after: Option<Vec<u8>>,
		limit: usize,
		object: tg::object::Id,
	},
	ObjectProcesses {
		after: Option<Vec<u8>>,
		limit: usize,
		object: tg::object::Id,
	},
	OrganizationMembers {
		after: Option<Vec<u8>>,
		limit: usize,
		organization: tg::organization::Id,
	},
	OwnerSandboxes {
		after: Option<Vec<u8>>,
		limit: usize,
		owner: tg::Principal,
	},
	Process {
		process: tg::process::Id,
	},
	ProcessChildren {
		after: Option<Vec<u8>>,
		limit: usize,
		process: tg::process::Id,
	},
	ProcessGrants {
		after: Option<Vec<u8>>,
		limit: usize,
		process: tg::process::Id,
	},
	ProcessObjects {
		after: Option<Vec<u8>>,
		limit: usize,
		process: tg::process::Id,
	},
	ProcessParents {
		after: Option<Vec<u8>>,
		limit: usize,
		process: tg::process::Id,
	},
	ResourceGrants {
		after: Option<Vec<u8>>,
		limit: usize,
		resource: tg::Id,
	},
	SandboxOwner {
		sandbox: tg::sandbox::Id,
	},
	SandboxProcesses {
		after: Option<Vec<u8>>,
		limit: usize,
		sandbox: tg::sandbox::Id,
	},
	Specifier {
		specifier: tg::Specifier,
	},
	SubjectGrants {
		after: Option<Vec<u8>>,
		limit: usize,
		subject: tg::authorization::Subject,
	},
	Tag {
		tag: tg::tag::Id,
	},
	TargetTags {
		after: Option<Vec<u8>>,
		limit: usize,
		target: tg::Id,
	},
}

#[derive(Clone, Debug)]
pub(crate) enum Output {
	Grants {
		after: Option<Vec<u8>>,
		grants: Vec<crate::grant::Fact>,
	},
	Group(Option<crate::group::Group>),
	Id(Option<tg::Id>),
	Ids {
		after: Option<Vec<u8>>,
		ids: Vec<tg::Id>,
	},
	MemberGroups {
		after: Option<Vec<u8>>,
		groups: Vec<tg::group::Id>,
	},
	MemberOrganizations {
		after: Option<Vec<u8>>,
		organizations: Vec<tg::organization::Id>,
	},
	ObjectProcesses {
		after: Option<Vec<u8>>,
		processes: Vec<(tg::process::Id, crate::process::object::Kind)>,
	},
	Process(Option<crate::process::Process>),
	ProcessObjects {
		after: Option<Vec<u8>>,
		objects: Vec<(tg::object::Id, crate::process::object::Kind)>,
	},
	SandboxOwner(Option<tg::Principal>),
	Tag(Option<crate::tag::Tag>),
	Tags {
		after: Option<Vec<u8>>,
		tags: Vec<tg::tag::Id>,
	},
}

pub(crate) struct Message<E> {
	pub request: Request,
	pub sender: oneshot::Sender<Result<ControlFlow<Output, E>, tg::Error>>,
}

#[derive(Clone, Eq, Hash, PartialEq)]
enum CacheKey {
	Group(tg::group::Id),
	Id(tg::Id),
	MemberGroups {
		after: Option<Vec<u8>>,
		limit: usize,
		member: tg::Id,
	},
	MemberOrganizations {
		after: Option<Vec<u8>>,
		limit: usize,
		member: tg::Id,
	},
	ObjectParents {
		after: Option<Vec<u8>>,
		limit: usize,
		object: tg::object::Id,
	},
	ObjectProcesses {
		after: Option<Vec<u8>>,
		limit: usize,
		object: tg::object::Id,
	},
	Process(tg::process::Id),
	ProcessObjects {
		after: Option<Vec<u8>>,
		limit: usize,
		process: tg::process::Id,
	},
	ProcessParents {
		after: Option<Vec<u8>>,
		limit: usize,
		process: tg::process::Id,
	},
	ResourceGrants {
		after: Option<Vec<u8>>,
		limit: usize,
		resource: tg::Id,
	},
	SandboxOwner(tg::sandbox::Id),
	Specifier(tg::Specifier),
	Tag(tg::tag::Id),
	TargetTags {
		after: Option<Vec<u8>>,
		limit: usize,
		target: tg::Id,
	},
}

enum CacheEntry<E> {
	Pending(Vec<oneshot::Sender<Option<Response<E>>>>),
	Ready(Response<E>),
}

pub(crate) struct Cache<E> {
	entries: Arc<Mutex<HashMap<CacheKey, CacheEntry<E>>>>,
}

struct CacheMissGuard<E> {
	cache: Cache<E>,
	key: Option<CacheKey>,
}

pub(crate) struct Client<E> {
	cache: Cache<E>,
	concurrency: usize,
	reads: Arc<AtomicUsize>,
	sender: mpsc::Sender<Message<E>>,
}

#[cfg(test)]
#[must_use]
pub(crate) fn channel<E>(concurrency: usize) -> (Client<E>, Receiver<E>) {
	channel_with_cache(concurrency, Cache::new())
}

#[must_use]
pub(crate) fn channel_with_cache<E>(
	concurrency: usize,
	cache: Cache<E>,
) -> (Client<E>, Receiver<E>) {
	let concurrency = concurrency.max(1);
	let (sender, receiver) = mpsc::channel(concurrency);
	let client = Client {
		cache,
		concurrency,
		reads: Arc::new(AtomicUsize::new(0)),
		sender,
	};

	(client, receiver)
}

pub(crate) async fn serve<E, F, Fut>(receiver: Receiver<E>, concurrency: usize, handler: F)
where
	F: Clone + Fn(Request) -> Fut,
	Fut: Future<Output = Response<E>>,
{
	let concurrency = concurrency.max(1);
	receiver
		.for_each_concurrent(concurrency, move |message| {
			let handler = handler.clone();
			async move {
				let response = handler(message.request).await;
				message.sender.send(response).ok();
			}
		})
		.await;
}

impl Request {
	#[must_use]
	fn cache_key(&self) -> Option<CacheKey> {
		let key = match self {
			Self::Group { group } => CacheKey::Group(group.clone()),
			Self::Id { id } => CacheKey::Id(id.clone()),
			Self::MemberGroups {
				after,
				limit,
				member,
			} => CacheKey::MemberGroups {
				after: after.clone(),
				limit: *limit,
				member: member.clone(),
			},
			Self::MemberOrganizations {
				after,
				limit,
				member,
			} => CacheKey::MemberOrganizations {
				after: after.clone(),
				limit: *limit,
				member: member.clone(),
			},
			Self::ObjectParents {
				after,
				limit,
				object,
			} => CacheKey::ObjectParents {
				after: after.clone(),
				limit: *limit,
				object: object.clone(),
			},
			Self::ObjectProcesses {
				after,
				limit,
				object,
			} => CacheKey::ObjectProcesses {
				after: after.clone(),
				limit: *limit,
				object: object.clone(),
			},
			Self::Process { process } => CacheKey::Process(process.clone()),
			Self::ProcessObjects {
				after,
				limit,
				process,
			} => CacheKey::ProcessObjects {
				after: after.clone(),
				limit: *limit,
				process: process.clone(),
			},
			Self::ProcessParents {
				after,
				limit,
				process,
			} => CacheKey::ProcessParents {
				after: after.clone(),
				limit: *limit,
				process: process.clone(),
			},
			Self::ResourceGrants {
				after,
				limit,
				resource,
			} => CacheKey::ResourceGrants {
				after: after.clone(),
				limit: *limit,
				resource: resource.clone(),
			},
			Self::SandboxOwner { sandbox } => CacheKey::SandboxOwner(sandbox.clone()),
			Self::Specifier { specifier } => CacheKey::Specifier(specifier.clone()),
			Self::Tag { tag } => CacheKey::Tag(tag.clone()),
			Self::TargetTags {
				after,
				limit,
				target,
			} => CacheKey::TargetTags {
				after: after.clone(),
				limit: *limit,
				target: target.clone(),
			},
			Self::GroupMembers { .. }
			| Self::ObjectChildren { .. }
			| Self::OwnerSandboxes { .. }
			| Self::OrganizationMembers { .. }
			| Self::ProcessChildren { .. }
			| Self::ProcessGrants { .. }
			| Self::SandboxProcesses { .. }
			| Self::SubjectGrants { .. } => return None,
		};

		Some(key)
	}
}

impl Output {
	pub(crate) fn into_grants(self) -> tg::Result<(Option<Vec<u8>>, Vec<crate::grant::Fact>)> {
		let Self::Grants { after, grants } = self else {
			return Err(tg::error!("received a non-grant authorization fact"));
		};

		Ok((after, grants))
	}

	pub(crate) fn into_group(self) -> tg::Result<Option<crate::group::Group>> {
		let Self::Group(group) = self else {
			return Err(tg::error!("received a non-group authorization fact"));
		};

		Ok(group)
	}

	pub(crate) fn into_id(self) -> tg::Result<Option<tg::Id>> {
		let Self::Id(id) = self else {
			return Err(tg::error!("received a non-ID authorization fact"));
		};

		Ok(id)
	}

	pub(crate) fn into_ids(self) -> tg::Result<(Option<Vec<u8>>, Vec<tg::Id>)> {
		let Self::Ids { after, ids } = self else {
			return Err(tg::error!("received a non-ID-page authorization fact"));
		};

		Ok((after, ids))
	}

	pub(crate) fn into_member_groups(self) -> tg::Result<(Option<Vec<u8>>, Vec<tg::group::Id>)> {
		let Self::MemberGroups { after, groups } = self else {
			return Err(tg::error!("received a non-member-group authorization fact"));
		};

		Ok((after, groups))
	}

	pub(crate) fn into_member_organizations(
		self,
	) -> tg::Result<(Option<Vec<u8>>, Vec<tg::organization::Id>)> {
		let Self::MemberOrganizations {
			after,
			organizations,
		} = self
		else {
			return Err(tg::error!(
				"received a non-member-organization authorization fact"
			));
		};

		Ok((after, organizations))
	}

	pub(crate) fn into_object_processes(
		self,
	) -> tg::Result<(
		Option<Vec<u8>>,
		Vec<(tg::process::Id, crate::process::object::Kind)>,
	)> {
		let Self::ObjectProcesses { after, processes } = self else {
			return Err(tg::error!(
				"received a non-object-process authorization fact"
			));
		};

		Ok((after, processes))
	}

	pub(crate) fn into_process(self) -> tg::Result<Option<crate::process::Process>> {
		let Self::Process(process) = self else {
			return Err(tg::error!("received a non-process authorization fact"));
		};

		Ok(process)
	}

	pub(crate) fn into_process_objects(
		self,
	) -> tg::Result<(
		Option<Vec<u8>>,
		Vec<(tg::object::Id, crate::process::object::Kind)>,
	)> {
		let Self::ProcessObjects { after, objects } = self else {
			return Err(tg::error!(
				"received a non-process-object authorization fact"
			));
		};

		Ok((after, objects))
	}

	pub(crate) fn into_sandbox_owner(self) -> tg::Result<Option<tg::Principal>> {
		let Self::SandboxOwner(owner) = self else {
			return Err(tg::error!(
				"received a non-sandbox-owner authorization fact"
			));
		};

		Ok(owner)
	}

	pub(crate) fn into_tag(self) -> tg::Result<Option<crate::tag::Tag>> {
		let Self::Tag(tag) = self else {
			return Err(tg::error!("received a non-tag authorization fact"));
		};

		Ok(tag)
	}

	pub(crate) fn into_tags(self) -> tg::Result<(Option<Vec<u8>>, Vec<tg::tag::Id>)> {
		let Self::Tags { after, tags } = self else {
			return Err(tg::error!("received a non-tag-list authorization fact"));
		};

		Ok((after, tags))
	}
}

impl<E> Cache<E> {
	#[must_use]
	pub(crate) fn new() -> Self {
		Self {
			entries: Arc::new(Mutex::new(HashMap::new())),
		}
	}
}

impl<E> Clone for Cache<E> {
	fn clone(&self) -> Self {
		Self {
			entries: self.entries.clone(),
		}
	}
}

impl<E> CacheMissGuard<E>
where
	E: Clone,
{
	fn publish(&mut self, response: &Response<E>) {
		let key = self.key.take().unwrap();
		let waiters = {
			let mut entries = self.cache.entries.lock().unwrap();
			let previous = entries.insert(key, CacheEntry::Ready(response.clone()));
			let Some(CacheEntry::Pending(waiters)) = previous else {
				unreachable!();
			};

			waiters
		};
		for waiter in waiters {
			waiter.send(Some(response.clone())).ok();
		}
	}
}

impl<E> Drop for CacheMissGuard<E> {
	fn drop(&mut self) {
		let Some(key) = self.key.take() else {
			return;
		};
		let waiters = {
			let mut entries = self.cache.entries.lock().unwrap();
			let Some(CacheEntry::Pending(waiters)) = entries.remove(&key) else {
				unreachable!();
			};

			waiters
		};
		for waiter in waiters {
			waiter.send(None).ok();
		}
	}
}

impl<E> Client<E>
where
	E: Clone + Send + Sync + 'static,
{
	#[must_use]
	pub(crate) fn concurrency(&self) -> usize {
		self.concurrency
	}

	#[must_use]
	pub(crate) fn reads(&self) -> usize {
		self.reads.load(Ordering::Relaxed)
	}

	pub(crate) async fn read(&self, request: Request) -> Response<E> {
		let Some(key) = request.cache_key() else {
			self.record_read(&request);

			return Self::request(self.sender.clone(), request).await;
		};
		let mut miss = loop {
			let receiver = {
				let mut entries = self.cache.entries.lock().unwrap();
				match entries.entry(key.clone()) {
					std::collections::hash_map::Entry::Occupied(mut entry) => match entry.get_mut()
					{
						CacheEntry::Pending(waiters) => {
							let (sender, receiver) = oneshot::channel();
							waiters.push(sender);

							receiver
						},
						CacheEntry::Ready(response) => return response.clone(),
					},
					std::collections::hash_map::Entry::Vacant(entry) => {
						entry.insert(CacheEntry::Pending(Vec::new()));

						break CacheMissGuard {
							cache: self.cache.clone(),
							key: Some(key.clone()),
						};
					},
				}
			};
			match receiver.await {
				Err(_) | Ok(None) => (),
				Ok(Some(response)) => return response,
			}
		};
		self.record_read(&request);
		let response = Self::request(self.sender.clone(), request).await;
		miss.publish(&response);

		response
	}

	fn record_read(&self, request: &Request) {
		self.reads.fetch_add(1, Ordering::Relaxed);
		if let Request::ObjectParents { object, .. } = request {
			tracing::debug!(%object, "read object parents for authorization");
		}
	}

	async fn request(
		mut request_sender: mpsc::Sender<Message<E>>,
		request: Request,
	) -> Response<E> {
		let (sender, receiver) = oneshot::channel();
		let message = Message { request, sender };
		request_sender
			.send(message)
			.await
			.map_err(|error| tg::error!(!error, "failed to send an authorization fact request"))?;
		let response = receiver.await.map_err(|error| {
			tg::error!(!error, "failed to receive an authorization fact response")
		})??;

		Ok(response)
	}
}

impl<E> Clone for Client<E> {
	fn clone(&self) -> Self {
		Self {
			cache: self.cache.clone(),
			concurrency: self.concurrency,
			reads: self.reads.clone(),
			sender: self.sender.clone(),
		}
	}
}

#[cfg(test)]
mod tests {
	use {
		super::*,
		std::{
			sync::{
				Arc,
				atomic::{AtomicUsize, Ordering},
			},
			time::Duration,
		},
		tokio::sync::Barrier,
	};

	#[derive(Clone, Copy, Debug, Eq, PartialEq)]
	enum Retry {
		Retry,
	}

	#[tokio::test]
	async fn cancellation_does_not_retain_the_channel() {
		let requests = Arc::new(AtomicUsize::new(0));
		let cache = Cache::new();
		let (client, receiver) = channel_with_cache::<LmdbError>(1, cache.clone());
		let provide = serve(receiver, 1, {
			let requests = requests.clone();
			move |_| {
				let index = requests.fetch_add(1, Ordering::SeqCst);
				async move {
					tokio::task::yield_now().await;
					if index == 0 {
						return Err(tg::error!("an expected fact error"));
					}

					Ok(ControlFlow::Break(Output::Id(None)))
				}
			}
		});
		let authorize = async move {
			let first = client.read(Request::Id {
				id: tg::user::Id::new().into(),
			});
			let second = client.read(Request::Id {
				id: tg::user::Id::new().into(),
			});

			futures::try_join!(first, second)
		};
		let result = tokio::time::timeout(
			Duration::from_secs(1),
			futures::future::join(authorize, provide),
		)
		.await
		.unwrap();
		drop(cache);

		assert!(result.0.is_err());
		assert_eq!(requests.load(Ordering::SeqCst), 2);
	}

	#[tokio::test]
	async fn the_consumer_is_the_concurrency_limit() {
		const CONCURRENCY: usize = 2;
		const TOTAL: usize = 8;

		let active = Arc::new(AtomicUsize::new(0));
		let barrier = Arc::new(Barrier::new(CONCURRENCY));
		let maximum = Arc::new(AtomicUsize::new(0));
		let (client, receiver) = channel::<LmdbError>(CONCURRENCY);
		let provide = serve(receiver, CONCURRENCY, {
			let active = active.clone();
			let barrier = barrier.clone();
			let maximum = maximum.clone();
			move |_| {
				let active = active.clone();
				let barrier = barrier.clone();
				let maximum = maximum.clone();
				async move {
					let current = active.fetch_add(1, Ordering::SeqCst) + 1;
					maximum.fetch_max(current, Ordering::SeqCst);
					barrier.wait().await;
					active.fetch_sub(1, Ordering::SeqCst);

					Ok(ControlFlow::Break(Output::Id(None)))
				}
			}
		});
		let authorize = async move {
			let reads = (0..TOTAL).map(|_| {
				let client = client.clone();
				async move {
					let id = tg::Id::from(tg::user::Id::new());

					client.read(Request::Id { id }).await
				}
			});

			futures::future::try_join_all(reads).await
		};
		let (responses, ()) = futures::future::join(authorize, provide).await;

		assert_eq!(responses.unwrap().len(), TOTAL);
		assert_eq!(maximum.load(Ordering::SeqCst), CONCURRENCY);
	}

	#[tokio::test]
	async fn direct_fact_requests_are_deduplicated() {
		let requests = Arc::new(AtomicUsize::new(0));
		let (client, receiver) = channel::<LmdbError>(2);
		let provide = serve(receiver, 2, {
			let requests = requests.clone();
			move |_| {
				let requests = requests.clone();
				async move {
					requests.fetch_add(1, Ordering::SeqCst);

					Ok(ControlFlow::Break(Output::Id(None)))
				}
			}
		});
		let authorize = async move {
			let id = tg::Id::from(tg::user::Id::new());
			let request = Request::Id { id };
			let first = client.read(request.clone());
			let second = client.read(request);

			futures::try_join!(first, second)
		};
		let (responses, ()) = futures::future::join(authorize, provide).await;

		let (first, second) = responses.unwrap();
		assert!(matches!(first, ControlFlow::Break(Output::Id(None))));
		assert!(matches!(second, ControlFlow::Break(Output::Id(None))));
		assert_eq!(requests.load(Ordering::SeqCst), 1);
	}

	#[tokio::test]
	async fn fact_cache_is_shared_across_channels() {
		let requests = Arc::new(AtomicUsize::new(0));
		let cache = Cache::<LmdbError>::new();
		let (first_client, first_receiver) = channel_with_cache(1, cache.clone());
		let (second_client, second_receiver) = channel_with_cache(1, cache);
		let first_provider = serve(first_receiver, 1, {
			let requests = requests.clone();
			move |_| {
				let requests = requests.clone();
				async move {
					requests.fetch_add(1, Ordering::SeqCst);

					Ok(ControlFlow::Break(Output::Id(None)))
				}
			}
		});
		let second_provider = serve(second_receiver, 1, {
			let requests = requests.clone();
			move |_| {
				let requests = requests.clone();
				async move {
					requests.fetch_add(1, Ordering::SeqCst);

					Ok(ControlFlow::Break(Output::Id(None)))
				}
			}
		});
		let authorize = async move {
			let id = tg::Id::from(tg::user::Id::new());
			let request = Request::Id { id };
			let first = first_client.read(request.clone());
			let second = second_client.read(request);

			futures::try_join!(first, second)
		};
		let providers = futures::future::join(first_provider, second_provider);
		let (responses, ((), ())) = futures::future::join(authorize, providers).await;

		let (first, second) = responses.unwrap();
		assert!(matches!(first, ControlFlow::Break(Output::Id(None))));
		assert!(matches!(second, ControlFlow::Break(Output::Id(None))));
		assert_eq!(requests.load(Ordering::SeqCst), 1);
	}

	#[tokio::test]
	async fn parent_pages_are_cached() {
		let requests = Arc::new(AtomicUsize::new(0));
		let (client, receiver) = channel::<LmdbError>(2);
		let provide = serve(receiver, 2, {
			let requests = requests.clone();
			move |_| {
				let requests = requests.clone();
				async move {
					requests.fetch_add(1, Ordering::SeqCst);

					Ok(ControlFlow::Break(Output::Ids {
						after: None,
						ids: Vec::new(),
					}))
				}
			}
		});
		let authorize = async move {
			let object = tg::object::Id::new(tg::object::Kind::Blob, &vec![0].into());
			let object_request = Request::ObjectParents {
				after: None,
				limit: 1,
				object,
			};
			let first = client.read(object_request.clone()).await?;
			assert!(matches!(first, ControlFlow::Break(Output::Ids { .. })));
			let second = client.read(object_request).await?;
			assert!(matches!(second, ControlFlow::Break(Output::Ids { .. })));
			let process_request = Request::ProcessParents {
				after: None,
				limit: 1,
				process: tg::process::Id::new(),
			};
			let third = client.read(process_request.clone()).await?;
			assert!(matches!(third, ControlFlow::Break(Output::Ids { .. })));
			client.read(process_request).await
		};
		let (response, ()) = futures::future::join(authorize, provide).await;

		let response = response.unwrap();
		assert!(matches!(response, ControlFlow::Break(Output::Ids { .. })));
		assert_eq!(requests.load(Ordering::SeqCst), 2);
	}

	#[tokio::test]
	async fn object_child_pages_are_not_cached() {
		let requests = Arc::new(AtomicUsize::new(0));
		let (client, receiver) = channel::<LmdbError>(2);
		let provide = serve(receiver, 2, {
			let requests = requests.clone();
			move |_| {
				let requests = requests.clone();
				async move {
					requests.fetch_add(1, Ordering::SeqCst);

					Ok(ControlFlow::Break(Output::Ids {
						after: None,
						ids: Vec::new(),
					}))
				}
			}
		});
		let authorize = async move {
			let object = tg::object::Id::new(tg::object::Kind::Blob, &vec![0].into());
			let request = Request::ObjectChildren {
				after: None,
				limit: 1,
				object,
			};
			let first = client.read(request.clone()).await?;
			assert!(matches!(first, ControlFlow::Break(Output::Ids { .. })));
			client.read(request).await
		};
		let (response, ()) = futures::future::join(authorize, provide).await;

		let response = response.unwrap();
		assert!(matches!(response, ControlFlow::Break(Output::Ids { .. })));
		assert_eq!(requests.load(Ordering::SeqCst), 2);
	}

	#[tokio::test]
	async fn retry_errors_cross_the_channel_as_control_flow() {
		let (client, receiver) = channel::<Retry>(1);
		let provide = serve(receiver, 1, |_| async {
			Ok(ControlFlow::Continue(Retry::Retry))
		});
		let authorize = async move {
			let id = tg::Id::from(tg::user::Id::new());

			client.read(Request::Id { id }).await
		};
		let (response, ()) = futures::future::join(authorize, provide).await;

		assert!(matches!(
			response.unwrap(),
			ControlFlow::Continue(Retry::Retry)
		));
	}
}
