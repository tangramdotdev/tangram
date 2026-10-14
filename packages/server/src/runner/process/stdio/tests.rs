use super::*;

fn pipe() -> Pipe {
	Pipe {
		buffer_chunks: 16 * 1024,
		buffer_size: 8 * 1024 * 1024,
		buffered: BTreeMap::new(),
		chunks: VecDeque::new(),
		combined_position: 0,
		eof: BTreeSet::new(),
		error: None,
		input_ended: false,
		inputs: [
			tg::process::stdio::Stream::Stderr,
			tg::process::stdio::Stream::Stdout,
		]
		.into_iter()
		.map(|stream| Input {
			buffered_chunks: 0,
			buffered_length: 0,
			ended: false,
			inner: stream::pending().boxed(),
			stream,
		})
		.collect(),
		next_input: 0,
		progress_stream: tg::process::stdio::Stream::Stderr,
		sources: BTreeMap::new(),
		stderr_position: 0,
		stdout_position: 0,
		streams: [
			tg::process::stdio::Stream::Stderr,
			tg::process::stdio::Stream::Stdout,
		]
		.into(),
	}
}

fn read(
	arg: tg::process::stdio::read::Arg,
) -> (Read, tokio::sync::mpsc::Receiver<tg::Result<ServerMessage>>) {
	let (sender, receiver) = tokio::sync::mpsc::channel(1);
	let Ok(read) = Read::new(arg, sender) else {
		panic!("expected a valid read");
	};
	(read, receiver)
}

fn stdout() -> tg::process::stdio::read::Arg {
	tg::process::stdio::read::Arg {
		streams: vec![tg::process::stdio::Stream::Stdout],
		..Default::default()
	}
}

async fn recv(
	receiver: &mut tokio::sync::mpsc::Receiver<tg::Result<ServerMessage>>,
) -> tg::Result<ServerMessage> {
	tokio::time::timeout(std::time::Duration::from_secs(1), receiver.recv())
		.await
		.unwrap()
		.unwrap()
}

#[test]
fn an_idle_read_does_not_block_another_stream() {
	let mut pipe = pipe();
	pipe.push(
		Bytes::from_static(b"stderr"),
		tg::process::stdio::Stream::Stderr,
	);
	let (mut stdout, _stdout) = read(stdout());
	let arg = tg::process::stdio::read::Arg {
		streams: vec![tg::process::stdio::Stream::Stderr],
		..Default::default()
	};
	let (mut stderr, _stderr) = read(arg);
	assert!(pipe.read(&mut stdout).unwrap().is_none());
	let Some(ServerMessage::Notification(Event::Chunk(chunk))) = pipe.read(&mut stderr).unwrap()
	else {
		panic!("expected the stderr chunk");
	};
	assert_eq!(chunk.bytes, b"stderr".as_slice());
}

#[test]
fn a_read_stops_at_its_limit() {
	let mut pipe = pipe();
	pipe.push(
		Bytes::from_static(b"hello world"),
		tg::process::stdio::Stream::Stdout,
	);
	let arg = tg::process::stdio::read::Arg {
		length: Some(5),
		..stdout()
	};
	let (mut read, _receiver) = read(arg);
	let Some(ServerMessage::Notification(Event::Chunk(chunk))) = pipe.read(&mut read).unwrap()
	else {
		panic!("expected a chunk");
	};
	assert_eq!(chunk.bytes, b"hello".as_slice());
	assert!(matches!(
		pipe.read(&mut read).unwrap(),
		Some(ServerMessage::Response(Output::Limit { position: 5 }))
	));
}

#[test]
fn a_read_times_out_without_output() {
	let mut pipe = pipe();
	let arg = tg::process::stdio::read::Arg {
		timeout: Some(std::time::Duration::ZERO),
		..stdout()
	};
	let (mut read, _receiver) = read(arg);
	assert!(matches!(
		pipe.read(&mut read).unwrap(),
		Some(ServerMessage::Response(Output::Timeout { position: 0 }))
	));
}

#[test]
fn eof_does_not_hide_a_gap_in_a_pipe() {
	let mut pipe = pipe();
	pipe.stdout_position = 6;
	pipe.combined_position = 6;
	pipe.eof.insert(tg::process::stdio::Stream::Stdout);
	let (mut read, _receiver) = read(stdout());
	assert!(pipe.read(&mut read).is_err());
	read.position = 6;
	assert!(matches!(
		pipe.read(&mut read).unwrap(),
		Some(ServerMessage::Response(Output::End(_)))
	));
}

#[tokio::test]
async fn a_full_stdout_buffer_does_not_block_stderr() {
	let mut pipe = pipe();
	pipe.push(
		Bytes::from(vec![0; pipe.buffer_size]),
		tg::process::stdio::Stream::Stdout,
	);
	let input = pipe
		.inputs
		.iter_mut()
		.find(|input| input.stream == tg::process::stdio::Stream::Stdout)
		.unwrap();
	input.inner = stream::iter([InputEvent::Progress(Some(Ok(Bytes::from_static(
		b"not polled",
	))))])
	.boxed();
	let input = pipe
		.inputs
		.iter_mut()
		.find(|input| input.stream == tg::process::stdio::Stream::Stderr)
		.unwrap();
	input.inner = stream::iter([InputEvent::Progress(Some(Ok(Bytes::from_static(
		b"stderr",
	))))])
	.boxed();
	pipe.next_input = 1;
	tokio::time::timeout(std::time::Duration::from_secs(1), pipe.fill())
		.await
		.unwrap();
	let arg = tg::process::stdio::read::Arg {
		streams: vec![tg::process::stdio::Stream::Stderr],
		..Default::default()
	};
	let (mut read, _receiver) = read(arg);
	let Some(ServerMessage::Notification(Event::Chunk(chunk))) = pipe.read(&mut read).unwrap()
	else {
		panic!("expected the stderr chunk");
	};
	assert_eq!(chunk.bytes, b"stderr".as_slice());
}

#[test]
fn completion_detects_chunks_lost_before_the_terminal_response() {
	for length in [None, Some(4)] {
		let mut pipe = pipe();
		pipe.push(
			Bytes::from_static(b"lost"),
			tg::process::stdio::Stream::Stdout,
		);
		pipe.eof = pipe.streams.clone();
		let arg = tg::process::stdio::read::Arg { length, ..stdout() };
		let (mut read, _receiver) = read(arg);
		assert!(matches!(
			pipe.read(&mut read).unwrap(),
			Some(ServerMessage::Notification(Event::Chunk(_)))
		));
		let Some(ServerMessage::Response(output)) = pipe.read(&mut read).unwrap() else {
			panic!("expected the terminal response");
		};
		assert!(
			output
				.validate(&[tg::process::stdio::Stream::Stdout], 0)
				.is_err()
		);
		assert!(
			output
				.validate(&[tg::process::stdio::Stream::Stdout], 4)
				.is_ok()
		);
	}
}

#[tokio::test]
async fn drained_streams_keep_eof_available() {
	let mut pipe = pipe();
	pipe.eof = pipe.streams.clone();
	let (sender, receiver) = tokio::sync::mpsc::channel(16);
	let task = tokio::spawn(Session::run_process_stdio_pipe_task(
		pipe,
		receiver,
		std::time::Duration::from_secs(10),
	));
	for stream in [
		tg::process::stdio::Stream::Stdout,
		tg::process::stdio::Stream::Stderr,
	] {
		let arg = tg::process::stdio::read::Arg {
			streams: vec![stream],
			..Default::default()
		};
		let (read_sender, mut output) = tokio::sync::mpsc::channel(1);
		let message = Message::Read {
			arg,
			sender: read_sender,
		};
		sender.send(message).await.unwrap();
		assert!(matches!(
			recv(&mut output).await.unwrap(),
			ServerMessage::Response(Output::End(_))
		));
	}

	// A reader that reconnects after draining receives EOF again.
	let arg = tg::process::stdio::read::Arg {
		streams: vec![tg::process::stdio::Stream::Stdout],
		..Default::default()
	};
	let (read_sender, mut output) = tokio::sync::mpsc::channel(1);
	let message = Message::Read {
		arg,
		sender: read_sender,
	};
	sender.send(message).await.unwrap();
	assert!(matches!(
		recv(&mut output).await.unwrap(),
		ServerMessage::Response(Output::End(_))
	));

	drop(sender);
	tokio::time::timeout(std::time::Duration::from_secs(1), task)
		.await
		.unwrap()
		.unwrap()
		.unwrap();
}

#[tokio::test]
async fn drained_streams_are_released_after_the_time_to_live() {
	let mut pipe = pipe();
	pipe.eof = pipe.streams.clone();
	let (sender, receiver) = tokio::sync::mpsc::channel(16);
	let task = tokio::spawn(Session::run_process_stdio_pipe_task(
		pipe,
		receiver,
		std::time::Duration::from_millis(50),
	));
	let arg = tg::process::stdio::read::Arg {
		streams: vec![
			tg::process::stdio::Stream::Stderr,
			tg::process::stdio::Stream::Stdout,
		],
		..Default::default()
	};
	let (read_sender, mut output) = tokio::sync::mpsc::channel(1);
	let message = Message::Read {
		arg,
		sender: read_sender,
	};
	sender.send(message).await.unwrap();
	assert!(matches!(
		recv(&mut output).await.unwrap(),
		ServerMessage::Response(Output::End(_))
	));
	tokio::time::timeout(std::time::Duration::from_secs(1), task)
		.await
		.unwrap()
		.unwrap()
		.unwrap();
}

#[tokio::test]
async fn a_full_sink_holds_back_the_read_and_a_new_read_supersedes_it() {
	let mut pipe = pipe();
	for _ in 0..3 {
		pipe.push(Bytes::from_static(b"a"), tg::process::stdio::Stream::Stdout);
	}
	let (sender, receiver) = tokio::sync::mpsc::channel(16);
	let task = tokio::spawn(Session::run_process_stdio_pipe_task(
		pipe,
		receiver,
		std::time::Duration::from_secs(10),
	));

	// The sink holds one message, so the pipe sends only the first chunk.
	let (read_sender, mut first) = tokio::sync::mpsc::channel(1);
	let message = Message::Read {
		arg: stdout(),
		sender: read_sender,
	};
	sender.send(message).await.unwrap();
	tokio::time::sleep(std::time::Duration::from_millis(50)).await;
	assert_eq!(first.len(), 1);

	// A new read of the same stream supersedes the first, and resumes from its own position.
	let (read_sender, mut second) = tokio::sync::mpsc::channel(1);
	let arg = tg::process::stdio::read::Arg {
		position: Some(std::io::SeekFrom::Start(1)),
		..stdout()
	};
	let message = Message::Read {
		arg,
		sender: read_sender,
	};
	sender.send(message).await.unwrap();
	// The first read may be sent more chunks before the new read supersedes it, but it ends.
	loop {
		let message = tokio::time::timeout(std::time::Duration::from_secs(1), first.recv())
			.await
			.unwrap();
		match message {
			Some(Ok(ServerMessage::Notification(Event::Chunk(_)))) => (),
			Some(Err(_)) | None => break,
			Some(Ok(_)) => panic!("expected a chunk or the end of the read"),
		}
	}
	for position in 1..3 {
		let ServerMessage::Notification(Event::Chunk(chunk)) = recv(&mut second).await.unwrap()
		else {
			panic!("expected a chunk");
		};
		assert_eq!(chunk.stream_position, position);
	}
	task.abort();
	task.await.ok();
}

#[test]
fn sent_chunks_are_kept_until_acknowledged() {
	let mut pipe = pipe();
	pipe.push(
		Bytes::from_static(b"ab"),
		tg::process::stdio::Stream::Stderr,
	);
	pipe.push(
		Bytes::from_static(b"cd"),
		tg::process::stdio::Stream::Stdout,
	);
	pipe.push(
		Bytes::from_static(b"ef"),
		tg::process::stdio::Stream::Stdout,
	);

	// A read of stdout is sent both of its chunks, and they are kept.
	let (mut first, _receiver) = read(stdout());
	for _ in 0..2 {
		assert!(matches!(
			pipe.read(&mut first).unwrap(),
			Some(ServerMessage::Notification(Event::Chunk(_)))
		));
	}
	assert_eq!(pipe.chunks.len(), 3);

	// An acknowledgment in stdout's coordinates releases only the chunks it covers.
	pipe.acknowledge(2, &[tg::process::stdio::Stream::Stdout]);
	assert_eq!(pipe.chunks.len(), 2);

	// A new read resumes from the kept bytes.
	let arg = tg::process::stdio::read::Arg {
		position: Some(std::io::SeekFrom::Start(3)),
		..stdout()
	};
	let (mut second, _receiver) = read(arg);
	let Some(ServerMessage::Notification(Event::Chunk(chunk))) = pipe.read(&mut second).unwrap()
	else {
		panic!("expected a chunk");
	};
	assert_eq!(chunk.stream_position, 3);
	assert_eq!(chunk.bytes, Bytes::from_static(b"f"));
}
