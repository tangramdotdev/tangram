use {
	super::{bind_listener, resolve},
	crate::fs::Temp,
	std::{fs, io::ErrorKind},
	tokio::io::{AsyncReadExt as _, AsyncWriteExt as _},
};

#[tokio::test]
async fn bind_listener_path_boundaries() {
	let capacity = if cfg!(target_os = "macos") { 104 } else { 108 };
	for length in [capacity - 1, capacity, capacity + 1] {
		// Create a socket path with the requested byte length and a short filename.
		let temp = Temp::new_in("/tmp").unwrap();
		let padding = length - temp.path().join("socket").as_os_str().len() - 1;
		let directory = temp.path().join("x".repeat(padding));
		fs::create_dir_all(&directory).unwrap();
		let path = directory.join("socket");
		assert_eq!(path.as_os_str().len(), length);

		// Bind the listener and resolve the path used by clients.
		let (listener, guard) = bind_listener(&path).unwrap();
		let indirect = length >= capacity;
		assert_eq!(guard.indirect(), indirect);
		assert_eq!(guard.entry(), path);
		let socket_path = guard.path().to_owned();
		let resolved = resolve(&path).unwrap();
		assert_eq!(resolved.as_ref(), socket_path);

		// Exchange data through the resolved socket.
		let mut client = tokio::net::UnixStream::connect(resolved.as_ref())
			.await
			.unwrap();
		let (mut stream, _) = listener.accept().await.unwrap();
		client.write_all(b"hello").await.unwrap();
		let mut bytes = [0; 5];
		stream.read_exact(&mut bytes).await.unwrap();
		assert_eq!(&bytes, b"hello");

		// Remove the socket, its symlink, and its temporary directory when the guard drops.
		drop((client, stream, listener, guard));
		assert_eq!(
			fs::symlink_metadata(&path).unwrap_err().kind(),
			ErrorKind::NotFound,
		);
		assert!(!socket_path.exists());
		if indirect {
			assert!(!socket_path.parent().unwrap().exists());
		}
	}
}
