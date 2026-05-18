use {
	crate::{Delivery, Error, Message, Payload},
	async_broadcast as broadcast,
	bytes::{BufMut as _, Bytes, BytesMut},
	futures::{StreamExt as _, future},
	std::{
		os::unix::fs::FileTypeExt as _,
		path::{Path, PathBuf},
		sync::Arc,
		time::Duration,
	},
	tangram_futures::task::Task,
	tokio::io::unix::AsyncFd,
};

const MAX_DATAGRAM_SIZE: usize = 65_536;
const SOCKET_TIMEOUT: Duration = Duration::from_millis(100);

#[derive(Clone)]
pub struct Messenger {
	state: Arc<State>,
}

struct State {
	#[expect(dead_code)]
	guard: tangram_util::io::unix::Guard,
	id: String,
	path: PathBuf,
	receiver: broadcast::InactiveReceiver<(String, Bytes)>,
	#[expect(dead_code)]
	task: Task<()>,
	sender: broadcast::Sender<(String, Bytes)>,
}

impl Messenger {
	pub async fn new(path: PathBuf) -> Result<Self, Error> {
		tokio::fs::create_dir_all(&path)
			.await
			.map_err(Error::other)?;

		let bound = tokio::task::spawn_blocking({
			let path = path.clone();
			move || bind(&path)
		})
		.await
		.map_err(Error::other)??;
		let id = bound.id;
		let guard = bound.guard;
		let socket = bound.socket;
		let socket = AsyncFd::new(socket).map_err(Error::other)?;

		let (mut sender, receiver) = broadcast::broadcast(1_000_000);
		sender.set_overflow(true);
		sender.set_await_active(false);

		let task = spawn_task(socket, sender.clone());

		let messenger = Self {
			state: Arc::new(State {
				guard,
				id,
				path,
				receiver: receiver.deactivate(),
				task,
				sender,
			}),
		};

		Ok(messenger)
	}
}

impl crate::Messenger for Messenger {
	async fn publish<T>(&self, subject: String, payload: T) -> Result<(), Error>
	where
		T: Payload,
	{
		let payload = payload.serialize()?;
		self.state
			.sender
			.try_broadcast((subject.clone(), payload.clone()))
			.ok();

		let path = self.state.path.clone();
		let id = self.state.id.clone();
		tokio::task::spawn_blocking(move || publish(&subject, &payload, &path, &id))
			.await
			.map_err(Error::other)??;
		Ok(())
	}

	async fn subscribe<T>(
		&self,
		subject: String,
	) -> Result<impl futures::Stream<Item = Result<Message<T>, Error>> + Send + 'static, Error>
	where
		T: Payload,
	{
		self.subscribe_with_delivery(subject, Delivery::All).await
	}

	async fn subscribe_with_delivery<T>(
		&self,
		subject: String,
		_delivery: Delivery,
	) -> Result<impl futures::Stream<Item = Result<Message<T>, Error>> + Send + 'static, Error>
	where
		T: Payload,
	{
		let stream =
			self.state
				.receiver
				.activate_cloned()
				.filter_map(move |(subject_, payload)| {
					if subject_ != subject {
						return future::ready(None);
					}
					let message = T::deserialize(payload)
						.map(|payload| Message {
							subject: subject_,
							payload,
						})
						.map_err(Error::deserialization);
					future::ready(Some(message))
				});
		Ok(stream)
	}
}

struct BindOutput {
	guard: tangram_util::io::unix::Guard,
	id: String,
	socket: rustix::fd::OwnedFd,
}

enum BindError {
	Retry,
	Other(Error),
}

fn bind(path: &Path) -> Result<BindOutput, Error> {
	let mut last_error = None;
	for _ in 0..16 {
		let name = random_name();
		let entry_path = path.join(&name);

		let result = bind_datagram(&entry_path, &name);

		match result {
			Ok(bound) => return Ok(bound),
			Err(BindError::Retry) => {
				last_error = Some(retry_error());
			},
			Err(BindError::Other(error)) => last_error = Some(error),
		}
	}
	Err(last_error.unwrap_or_else(retry_error))
}

fn retry_error() -> Error {
	Error::other(std::io::Error::from_raw_os_error(
		rustix::io::Errno::ADDRINUSE.raw_os_error(),
	))
}

fn random_name() -> String {
	format!("{:016x}", rand::random::<u64>())
}

fn bind_datagram(entry_path: &Path, name: &str) -> Result<BindOutput, BindError> {
	let (socket, guard) = tangram_util::io::unix::bind_datagram(entry_path).map_err(|error| {
		if error.kind() == std::io::ErrorKind::AddrInUse {
			BindError::Retry
		} else {
			BindError::Other(Error::other(error))
		}
	})?;
	Ok(BindOutput {
		guard,
		id: name.to_owned(),
		socket,
	})
}

fn spawn_task(
	socket: AsyncFd<rustix::fd::OwnedFd>,
	sender: broadcast::Sender<(String, Bytes)>,
) -> Task<()> {
	Task::spawn(move |_| async move {
		let mut buffer = vec![0; MAX_DATAGRAM_SIZE];
		loop {
			let Ok(mut guard) = socket.readable().await else {
				break;
			};
			loop {
				match guard.try_io(|socket| receive(socket.get_ref(), &mut buffer)) {
					Ok(Ok(Some((subject, payload)))) => {
						sender.try_broadcast((subject, payload)).ok();
					},
					Ok(Ok(None)) => {},
					Ok(Err(error)) if error.kind() == std::io::ErrorKind::Interrupted => {},
					Ok(Err(_)) | Err(_) => break,
				}
			}
		}
	})
}

fn receive(
	socket: &rustix::fd::OwnedFd,
	buffer: &mut [u8],
) -> std::io::Result<Option<(String, Bytes)>> {
	rustix::net::recvfrom(socket, &mut *buffer, rustix::net::RecvFlags::empty())
		.map(|(length, _, _)| decode(&buffer[..length]))
		.map_err(|error| std::io::Error::from_raw_os_error(error.raw_os_error()))
}

fn publish(subject: &str, payload: &Bytes, path: &Path, id: &str) -> Result<(), Error> {
	let Ok(entries) = std::fs::read_dir(path) else {
		return Ok(());
	};
	let mut send_socket = None;
	let mut message = None;
	for entry in entries {
		let Ok(entry) = entry else {
			continue;
		};
		let peer = entry.path();
		if peer.file_name().and_then(|name| name.to_str()) == Some(id) {
			continue;
		}
		let Ok(file_type) = entry.file_type() else {
			continue;
		};
		let (address_path, symlink_target) = if file_type.is_socket() {
			(peer.clone(), None)
		} else if file_type.is_symlink() {
			let Ok(target) = tangram_util::io::unix::resolve(&peer) else {
				std::fs::remove_file(&peer).ok();
				continue;
			};
			let target = target.into_owned();
			(target.clone(), Some(target))
		} else {
			continue;
		};
		let Ok(address) = rustix::net::SocketAddrUnix::new(address_path.as_path()) else {
			continue;
		};
		if send_socket.is_none() {
			let socket_ = rustix::net::socket(
				rustix::net::AddressFamily::UNIX,
				rustix::net::SocketType::DGRAM,
				None,
			)
			.map_err(Error::other)?;
			rustix::net::sockopt::set_socket_timeout(
				&socket_,
				rustix::net::sockopt::Timeout::Send,
				Some(SOCKET_TIMEOUT),
			)
			.map_err(Error::other)?;
			send_socket = Some(socket_);
		}
		if message.is_none() {
			message = Some(encode(subject, payload)?);
		}
		let socket = send_socket.as_ref().unwrap();
		let message = message.as_ref().unwrap();
		let result =
			rustix::net::sendto(socket, message, rustix::net::SendFlags::empty(), &address);
		if let Err(
			rustix::io::Errno::CONNREFUSED | rustix::io::Errno::NOENT | rustix::io::Errno::NOTSOCK,
		) = result
		{
			cleanup_peer(&peer, symlink_target.as_deref());
		}
	}
	Ok(())
}

fn cleanup_peer(peer: &Path, symlink_target: Option<&Path>) {
	if let Some(target) = symlink_target {
		cleanup_peer_target(peer, target);
	}
	std::fs::remove_file(peer).ok();
}

fn cleanup_peer_target(peer: &Path, target: &Path) {
	let same_name = peer
		.file_name()
		.is_some_and(|name| target.file_name() == Some(name));
	let target_is_socket = std::fs::symlink_metadata(target)
		.ok()
		.is_some_and(|metadata| metadata.file_type().is_socket());
	if same_name && target_is_socket {
		std::fs::remove_file(target).ok();
	}
}

fn encode(subject: &str, payload: &Bytes) -> Result<Bytes, Error> {
	let subject = subject.as_bytes();
	let subject_length = u32::try_from(subject.len()).map_err(|_| {
		Error::other(std::io::Error::new(
			std::io::ErrorKind::InvalidInput,
			"message too large",
		))
	})?;
	let length = 4usize
		.checked_add(subject.len())
		.and_then(|length| length.checked_add(payload.len()))
		.ok_or_else(|| {
			Error::other(std::io::Error::new(
				std::io::ErrorKind::InvalidInput,
				"message too large",
			))
		})?;
	if length > MAX_DATAGRAM_SIZE {
		return Err(Error::other(std::io::Error::new(
			std::io::ErrorKind::InvalidInput,
			"message too large",
		)));
	}
	let mut bytes = BytesMut::with_capacity(length);
	bytes.put_u32(subject_length);
	bytes.extend_from_slice(subject);
	bytes.extend_from_slice(payload);
	Ok(bytes.freeze())
}

fn decode(bytes: &[u8]) -> Option<(String, Bytes)> {
	let subject_length = u32::from_be_bytes(bytes.get(..4)?.try_into().ok()?) as usize;
	let subject_start = 4;
	let subject_end = subject_start + subject_length;
	let subject = std::str::from_utf8(bytes.get(subject_start..subject_end)?)
		.ok()?
		.to_owned();
	let payload = Bytes::copy_from_slice(bytes.get(subject_end..)?);
	Some((subject, payload))
}

#[cfg(test)]
mod tests {
	use {
		super::*,
		crate::Messenger as _,
		futures::TryStreamExt as _,
		serde::{Deserialize, Serialize},
		std::time::Duration,
	};

	#[tokio::test]
	async fn subscribes_only_to_matching_subject() {
		let path = temp_path();
		let messenger = Messenger::new(path.clone()).await.unwrap();
		let mut stream = messenger
			.subscribe::<Bytes>("subject1".into())
			.await
			.unwrap();

		messenger
			.publish("subject2".into(), Bytes::from_static(b"skip"))
			.await
			.unwrap();
		messenger
			.publish("subject1".into(), Bytes::from_static(b"hello"))
			.await
			.unwrap();

		let message = stream.try_next().await.unwrap().unwrap();
		assert_eq!(message.payload, Bytes::from_static(b"hello"));
		cleanup(path);
	}

	#[tokio::test]
	async fn subscribes_with_typed_payloads() {
		let path = temp_path();
		let messenger = Messenger::new(path.clone()).await.unwrap();
		let mut stream = messenger
			.subscribe::<crate::payload::Json<Event>>("subject".into())
			.await
			.unwrap();

		messenger
			.publish(
				"subject".into(),
				crate::payload::Json(Event {
					value: "hello".into(),
				}),
			)
			.await
			.unwrap();

		let message = stream.try_next().await.unwrap().unwrap();
		assert_eq!(
			message.payload.0,
			Event {
				value: "hello".into(),
			}
		);
		cleanup(path);
	}

	#[tokio::test]
	async fn sends_between_messengers() {
		let path = temp_path();
		let sender = Messenger::new(path.clone()).await.unwrap();
		let receiver = Messenger::new(path.clone()).await.unwrap();
		let mut stream = receiver.subscribe::<Bytes>("subject".into()).await.unwrap();

		sender
			.publish("subject".into(), Bytes::from_static(b"hello"))
			.await
			.unwrap();

		let message = timeout_next(&mut stream).await;
		assert_eq!(message.payload, Bytes::from_static(b"hello"));
		cleanup(path);
	}

	#[tokio::test]
	async fn sends_between_messengers_with_long_paths() {
		let path = long_temp_path();
		let sender = Messenger::new(path.clone()).await.unwrap();
		let receiver = Messenger::new(path.clone()).await.unwrap();
		let mut stream = receiver.subscribe::<Bytes>("subject".into()).await.unwrap();

		let entry_path = sender.state.path.join(&sender.state.id);
		let socket_path = std::fs::read_link(&entry_path).unwrap();
		assert_ne!(entry_path, socket_path);
		assert!(
			std::fs::symlink_metadata(&entry_path)
				.unwrap()
				.file_type()
				.is_symlink()
		);

		sender
			.publish("subject".into(), Bytes::from_static(b"hello"))
			.await
			.unwrap();

		let message = timeout_next(&mut stream).await;
		assert_eq!(message.payload, Bytes::from_static(b"hello"));
		drop(sender);
		drop(receiver);
		cleanup(path);
	}

	#[tokio::test]
	async fn drops_indirect_socket_and_symlink() {
		let path = long_temp_path();
		let messenger = Messenger::new(path.clone()).await.unwrap();
		let entry_path = messenger.state.path.join(&messenger.state.id);
		let socket_path = std::fs::read_link(&entry_path).unwrap();

		assert_ne!(entry_path, socket_path);
		assert!(std::fs::symlink_metadata(&entry_path).is_ok());
		assert!(std::fs::symlink_metadata(&socket_path).is_ok());

		drop(messenger);

		assert!(std::fs::symlink_metadata(&entry_path).is_err());
		assert!(std::fs::symlink_metadata(&socket_path).is_err());
		cleanup(path);
	}

	#[tokio::test]
	async fn does_not_receive_duplicate_self_delivery() {
		let path = temp_path();
		let messenger = Messenger::new(path.clone()).await.unwrap();
		let mut stream = messenger
			.subscribe::<Bytes>("subject".into())
			.await
			.unwrap();

		messenger
			.publish("subject".into(), Bytes::from_static(b"hello"))
			.await
			.unwrap();

		let message = timeout_next(&mut stream).await;
		assert_eq!(message.payload, Bytes::from_static(b"hello"));
		assert!(
			tokio::time::timeout(Duration::from_millis(150), stream.try_next())
				.await
				.is_err()
		);
		cleanup(path);
	}

	#[tokio::test]
	async fn broadcasts_to_multiple_local_subscribers() {
		let path = temp_path();
		let messenger = Messenger::new(path.clone()).await.unwrap();
		let mut stream1 = messenger
			.subscribe::<Bytes>("subject".into())
			.await
			.unwrap();
		let mut stream2 = messenger
			.subscribe::<Bytes>("subject".into())
			.await
			.unwrap();

		messenger
			.publish("subject".into(), Bytes::from_static(b"hello"))
			.await
			.unwrap();

		assert_eq!(
			timeout_next(&mut stream1).await.payload,
			Bytes::from_static(b"hello")
		);
		assert_eq!(
			timeout_next(&mut stream2).await.payload,
			Bytes::from_static(b"hello")
		);
		cleanup(path);
	}

	#[tokio::test]
	async fn delivery_one_broadcasts() {
		let path = temp_path();
		let messenger = Messenger::new(path.clone()).await.unwrap();
		let mut stream1 = messenger
			.subscribe_with_delivery::<Bytes>("subject".into(), Delivery::One)
			.await
			.unwrap();
		let mut stream2 = messenger
			.subscribe_with_delivery::<Bytes>("subject".into(), Delivery::One)
			.await
			.unwrap();

		messenger
			.publish("subject".into(), Bytes::from_static(b"hello"))
			.await
			.unwrap();

		assert_eq!(
			timeout_next(&mut stream1).await.payload,
			Bytes::from_static(b"hello")
		);
		assert_eq!(
			timeout_next(&mut stream2).await.payload,
			Bytes::from_static(b"hello")
		);
		cleanup(path);
	}

	#[tokio::test]
	async fn removes_stale_socket_files() {
		let path = temp_path();
		tokio::fs::create_dir_all(&path).await.unwrap();
		let stale_path = path.join("stale");
		let socket = rustix::net::socket(
			rustix::net::AddressFamily::UNIX,
			rustix::net::SocketType::DGRAM,
			None,
		)
		.unwrap();
		let address = rustix::net::SocketAddrUnix::new(stale_path.as_path()).unwrap();
		rustix::net::bind(&socket, &address).unwrap();
		drop(socket);
		assert!(stale_path.exists());

		let messenger = Messenger::new(path.clone()).await.unwrap();
		messenger
			.publish("subject".into(), Bytes::from_static(b"hello"))
			.await
			.unwrap();

		assert!(!stale_path.exists());
		cleanup(path);
	}

	#[tokio::test]
	async fn removes_stale_symlinks_and_target_sockets() {
		let path = temp_path();
		tokio::fs::create_dir_all(&path).await.unwrap();
		let name = random_name();
		let stale_path = path.join(&name);
		let target_path = PathBuf::from("/tmp").join(name);
		std::fs::remove_file(&target_path).ok();
		let socket = rustix::net::socket(
			rustix::net::AddressFamily::UNIX,
			rustix::net::SocketType::DGRAM,
			None,
		)
		.unwrap();
		let address = rustix::net::SocketAddrUnix::new(target_path.as_path()).unwrap();
		rustix::net::bind(&socket, &address).unwrap();
		drop(socket);
		std::os::unix::fs::symlink(&target_path, &stale_path).unwrap();
		assert!(std::fs::symlink_metadata(&stale_path).is_ok());
		assert!(std::fs::symlink_metadata(&target_path).is_ok());

		let messenger = Messenger::new(path.clone()).await.unwrap();
		messenger
			.publish("subject".into(), Bytes::from_static(b"hello"))
			.await
			.unwrap();

		assert!(std::fs::symlink_metadata(&stale_path).is_err());
		assert!(std::fs::symlink_metadata(&target_path).is_err());
		cleanup(path);
	}

	#[derive(Debug, Deserialize, PartialEq, Serialize)]
	struct Event {
		value: String,
	}

	fn temp_path() -> PathBuf {
		PathBuf::from("/tmp").join(format!("tangram-messenger-test-{}", rand::random::<u64>()))
	}

	fn long_temp_path() -> PathBuf {
		let mut path = temp_path();
		while rustix::net::SocketAddrUnix::new(path.join("0000000000000000").as_path()).is_ok() {
			path = path.join("long");
		}
		path
	}

	fn cleanup(path: PathBuf) {
		std::fs::remove_dir_all(path).ok();
	}

	async fn timeout_next<S>(stream: &mut S) -> Message<Bytes>
	where
		S: futures::Stream<Item = Result<Message<Bytes>, Error>> + Unpin,
	{
		tokio::time::timeout(Duration::from_secs(2), stream.try_next())
			.await
			.unwrap()
			.unwrap()
			.unwrap()
	}
}
