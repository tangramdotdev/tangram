use super::*;

fn reader() -> Reader {
	Reader {
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

#[test]
fn an_idle_read_does_not_block_another_stream() {
	let mut reader = reader();
	reader.push(
		Bytes::from_static(b"stderr"),
		tg::process::stdio::Stream::Stderr,
	);
	let arg = tg::process::stdio::read::Arg {
		streams: vec![tg::process::stdio::Stream::Stdout],
		..Default::default()
	};
	let mut stdout = Read::new(arg).unwrap();
	let arg = tg::process::stdio::read::Arg {
		streams: vec![tg::process::stdio::Stream::Stderr],
		..Default::default()
	};
	let mut stderr = Read::new(arg).unwrap();
	assert!(reader.read(&mut stdout).unwrap().is_none());
	let Some(tg::process::stdio::read::ServerMessage::Notification(
		tg::process::stdio::read::Event::Chunk(chunk),
	)) = reader.read(&mut stderr).unwrap()
	else {
		panic!("expected the stderr chunk");
	};
	assert_eq!(chunk.bytes, b"stderr".as_slice());
}

#[test]
fn pipes_stream_a_range_until_the_window_or_limit() {
	let mut reader = reader();
	reader.push(
		Bytes::from(vec![0; tg::process::stdio::flow::CHUNK_SIZE * 65]),
		tg::process::stdio::Stream::Stdout,
	);
	let arg = tg::process::stdio::read::Arg {
		length: Some(i64::try_from(tg::process::stdio::flow::CHUNK_SIZE * 65).unwrap()),
		streams: vec![tg::process::stdio::Stream::Stdout],
		..Default::default()
	};
	let mut read = Read::new(arg).unwrap();
	for _ in 0..64 {
		assert!(matches!(
			reader.read(&mut read).unwrap(),
			Some(tg::process::stdio::read::ServerMessage::Notification(_))
		));
	}
	assert!(reader.read(&mut read).unwrap().is_none());
	let progress = tg::process::stdio::read::Progress {
		consumed: tg::process::stdio::flow::WINDOW / 2,
	};
	read.window.update(progress).unwrap();
	assert!(matches!(
		reader.read(&mut read).unwrap(),
		Some(tg::process::stdio::read::ServerMessage::Notification(_))
	));
	assert!(matches!(
		reader.read(&mut read).unwrap(),
		Some(tg::process::stdio::read::ServerMessage::Response(
			tg::process::stdio::read::Output::Limit { .. }
		))
	));
}

#[test]
fn eof_does_not_hide_a_gap_in_a_pipe() {
	let mut reader = reader();
	reader.stdout_position = 6;
	reader.combined_position = 6;
	reader.eof.insert(tg::process::stdio::Stream::Stdout);
	let arg = tg::process::stdio::read::Arg {
		streams: vec![tg::process::stdio::Stream::Stdout],
		..Default::default()
	};
	let mut read = Read::new(arg).unwrap();
	assert!(reader.read(&mut read).is_err());
	read.position = 6;
	assert!(matches!(
		reader.read(&mut read).unwrap(),
		Some(tg::process::stdio::read::ServerMessage::Response(
			tg::process::stdio::read::Output::End(_)
		))
	));
}

#[test]
fn a_read_times_out_while_its_window_is_full() {
	let mut reader = reader();
	reader.push(
		Bytes::from(vec![0; flow::CHUNK_SIZE * 65]),
		tg::process::stdio::Stream::Stdout,
	);
	let arg = tg::process::stdio::read::Arg {
		streams: vec![tg::process::stdio::Stream::Stdout],
		timeout: Some(std::time::Duration::ZERO),
		..Default::default()
	};
	let mut read = Read::new(arg).unwrap();
	for _ in 0..flow::MAX_CHUNKS {
		assert!(matches!(
			reader.read(&mut read).unwrap(),
			Some(ServerMessage::Notification(_))
		));
	}
	assert!(matches!(
		reader.read(&mut read).unwrap(),
		Some(ServerMessage::Response(Output::Timeout { .. }))
	));
}

#[tokio::test]
async fn a_full_stdout_buffer_does_not_block_stderr() {
	let mut reader = reader();
	reader.push(
		Bytes::from(vec![0; BUFFER_CAPACITY]),
		tg::process::stdio::Stream::Stdout,
	);
	let input = reader
		.inputs
		.iter_mut()
		.find(|input| input.stream == tg::process::stdio::Stream::Stdout)
		.unwrap();
	input.inner = stream::iter([InputEvent::Progress(Some(Ok(Bytes::from_static(
		b"not polled",
	))))])
	.boxed();
	let input = reader
		.inputs
		.iter_mut()
		.find(|input| input.stream == tg::process::stdio::Stream::Stderr)
		.unwrap();
	input.inner = stream::iter([InputEvent::Progress(Some(Ok(Bytes::from_static(
		b"stderr",
	))))])
	.boxed();
	reader.next_input = 1;
	tokio::time::timeout(std::time::Duration::from_secs(1), reader.fill())
		.await
		.unwrap();
	let arg = tg::process::stdio::read::Arg {
		streams: vec![tg::process::stdio::Stream::Stderr],
		..Default::default()
	};
	let mut read = Read::new(arg).unwrap();
	let Some(ServerMessage::Notification(Event::Chunk(chunk))) = reader.read(&mut read).unwrap()
	else {
		panic!("expected the stderr chunk");
	};
	assert_eq!(chunk.bytes, b"stderr".as_slice());
}

#[tokio::test]
async fn draining_stdout_keeps_stderr_eof_available() {
	let mut reader = reader();
	reader.eof = reader.streams.clone();
	let (sender, mut output) = tokio::sync::mpsc::channel(16);
	let control = crate::control::Stream::new(
		stream::pending().boxed(),
		sender,
		crate::control::stream_options(),
	);
	let (sender, receiver) = tokio::sync::mpsc::channel(16);
	let task = tokio::spawn(Session::run_process_control_output_reader_task(
		reader,
		receiver,
		control.sender(),
	));
	for stream in [
		tg::process::stdio::Stream::Stdout,
		tg::process::stdio::Stream::Stderr,
	] {
		let arg = tg::process::stdio::read::Arg {
			streams: vec![stream],
			..Default::default()
		};
		sender
			.send(Message::Read {
				arg,
				id: stream.to_string(),
			})
			.await
			.unwrap();
		let response = tokio::time::timeout(std::time::Duration::from_secs(1), output.recv())
			.await
			.unwrap()
			.unwrap();
		let tg::process::control::ClientMessage::Response(response) = response else {
			panic!("expected a read response");
		};
		assert_eq!(response.id, stream.to_string());
		assert!(matches!(
			response.output,
			Some(tg::process::control::ClientResponseOutput::Read(
				Output::End(_)
			))
		));
	}
	tokio::time::timeout(std::time::Duration::from_secs(1), task)
		.await
		.unwrap()
		.unwrap()
		.unwrap();
}

#[test]
fn completion_detects_chunks_lost_before_the_terminal_response() {
	for length in [None, Some(4)] {
		let mut reader = reader();
		reader.push(
			Bytes::from_static(b"lost"),
			tg::process::stdio::Stream::Stdout,
		);
		reader.eof = reader.streams.clone();
		let arg = tg::process::stdio::read::Arg {
			length,
			streams: vec![tg::process::stdio::Stream::Stdout],
			..Default::default()
		};
		let mut read = Read::new(arg).unwrap();
		assert!(matches!(
			reader.read(&mut read).unwrap(),
			Some(ServerMessage::Notification(Event::Chunk(_)))
		));
		let Some(ServerMessage::Response(output)) = reader.read(&mut read).unwrap() else {
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
async fn reconnecting_ends_reads_with_lost_progress() {
	let mut reader = reader();
	reader.push(
		Bytes::from(vec![0; flow::CHUNK_SIZE * (flow::MAX_CHUNKS + 1)]),
		tg::process::stdio::Stream::Stdout,
	);
	reader.eof.insert(tg::process::stdio::Stream::Stdout);
	let (sender, mut output) = tokio::sync::mpsc::channel(flow::CHANNEL_CAPACITY);
	let control = crate::control::Stream::new(
		stream::pending().boxed(),
		sender,
		crate::control::stream_options(),
	);
	let (sender, receiver) = tokio::sync::mpsc::channel(16);
	let task = tokio::spawn(Session::run_process_control_output_reader_task(
		reader,
		receiver,
		control.sender(),
	));

	// Fill stdout's window without delivering progress, and leave stderr idle.
	for stream in [
		tg::process::stdio::Stream::Stderr,
		tg::process::stdio::Stream::Stdout,
	] {
		let arg = tg::process::stdio::read::Arg {
			streams: vec![stream],
			..Default::default()
		};
		sender
			.send(Message::Read {
				arg,
				id: stream.to_string(),
			})
			.await
			.unwrap();
	}
	for _ in 0..flow::MAX_CHUNKS {
		let message = tokio::time::timeout(std::time::Duration::from_secs(1), output.recv())
			.await
			.unwrap()
			.unwrap();
		assert!(matches!(
			message,
			tg::process::control::ClientMessage::Notification(_)
		));
	}
	assert!(output.try_recv().is_err());

	// Reconnection must terminate both reads even when no further progress can arrive.
	sender.send(Message::Reconnect).await.unwrap();
	for id in ["stderr", "stdout"] {
		let message = tokio::time::timeout(std::time::Duration::from_secs(1), output.recv())
			.await
			.unwrap()
			.unwrap();
		let tg::process::control::ClientMessage::Response(response) = message else {
			panic!("expected a read response");
		};
		assert_eq!(response.id, id);
		assert!(response.error.is_some());
		assert!(response.output.is_none());
	}

	// A fresh read can consume the remaining bytes from the caller's position.
	let arg = tg::process::stdio::read::Arg {
		position: Some(std::io::SeekFrom::Start(flow::WINDOW)),
		streams: vec![tg::process::stdio::Stream::Stdout],
		..Default::default()
	};
	sender
		.send(Message::Read {
			arg,
			id: "resumed".into(),
		})
		.await
		.unwrap();
	let message = tokio::time::timeout(std::time::Duration::from_secs(1), output.recv())
		.await
		.unwrap()
		.unwrap();
	let tg::process::control::ClientMessage::Notification(
		tg::process::control::ClientNotification::Read(notification),
	) = message
	else {
		panic!("expected the remaining stdout chunk");
	};
	let Event::Chunk(chunk) = notification.event else {
		panic!("expected the remaining stdout chunk");
	};
	assert_eq!(chunk.stream_position, flow::WINDOW);
	assert_eq!(chunk.bytes.len(), flow::CHUNK_SIZE);
	let message = tokio::time::timeout(std::time::Duration::from_secs(1), output.recv())
		.await
		.unwrap()
		.unwrap();
	let tg::process::control::ClientMessage::Response(response) = message else {
		panic!("expected a read response");
	};
	assert_eq!(response.id, "resumed");
	assert!(matches!(
		response.output,
		Some(tg::process::control::ClientResponseOutput::Read(
			Output::End(_)
		))
	));
	task.abort();
	task.await.ok();
}
