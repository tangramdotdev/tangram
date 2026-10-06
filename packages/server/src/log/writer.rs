use {
	super::{Entry, Index},
	crate::Session,
	std::{borrow::Cow, io::Cursor},
	tangram_cache::log,
	tangram_client::prelude::*,
	tangram_futures::{task::Task, write::Ext as _},
	tokio::io::AsyncWriteExt as _,
};

pub(crate) struct Writer {
	index: Index,
	input: tokio::io::DuplexStream,
	length: u64,
	session: Session,
	task: Task<tg::Result<tg::write::Output>>,
}

impl Writer {
	#[must_use]
	pub(crate) fn new(session: &Session) -> Self {
		let (input, reader) = tokio::io::duplex(64 * 1024);
		let task = Task::spawn({
			let session = session.clone();
			move |_| async move {
				let arg = tg::write::Arg {
					checkout_pointers: Some(true),
				};
				session.write(arg, reader).await
			}
		});
		Self {
			index: Index::default(),
			input,
			length: 0,
			session: session.clone(),
			task,
		}
	}

	pub(crate) async fn write(&mut self, chunk: &tg::process::stdio::Chunk) -> tg::Result<()> {
		let timestamp = chunk
			.timestamp
			.ok_or_else(|| tg::error!("missing a log timestamp"))?;
		let entry = log::read::Entry {
			bytes: Cow::Borrowed(&chunk.bytes),
			position: chunk.combined_position,
			stream: chunk.stream,
			stream_position: chunk.stream_position,
			timestamp,
		};
		let bytes = tangram_serialize::to_vec(&entry)
			.map_err(|error| tg::error!(!error, "failed to serialize a log entry"))?;
		let length = u64::try_from(bytes.len()).unwrap();
		let entry = Entry {
			blob_length: length,
			blob_position: self.length,
			combined_position: chunk.combined_position,
			stream: chunk.stream,
			stream_position: chunk.stream_position,
		};
		let position = u32::try_from(self.index.entries.len())
			.map_err(|error| tg::error!(!error, "too many log entries"))?;
		match chunk.stream {
			tg::process::stdio::Stream::Stderr => self.index.stderr.push(position),
			tg::process::stdio::Stream::Stdin => return Err(tg::error!("invalid log stream")),
			tg::process::stdio::Stream::Stdout => self.index.stdout.push(position),
		}
		self.input
			.write_all(&bytes)
			.await
			.map_err(|error| tg::error!(!error, "failed to write a log entry"))?;
		self.index.entries.push(entry);
		self.length += length;
		Ok(())
	}

	pub(crate) async fn end(self) -> tg::Result<tg::Referent<tg::blob::Id>> {
		let Self {
			index,
			input,
			length,
			session,
			task,
		} = self;
		drop(input);
		let entries = task
			.wait()
			.await
			.map_err(|error| tg::error!(!error, "the log blob write task panicked"))??;
		let index = tangram_serialize::to_vec(&index)
			.map_err(|error| tg::error!(!error, "failed to serialize the log index"))?;
		let mut bytes = vec![0];
		bytes
			.write_uvarint(u64::try_from(index.len()).unwrap())
			.await
			.unwrap();
		bytes.extend_from_slice(&index);
		let header_length = u64::try_from(bytes.len()).unwrap();
		let arg = tg::write::Arg {
			checkout_pointers: Some(true),
		};
		let header = session.write(arg, Cursor::new(bytes)).await?;
		let header = tg::blob::Child {
			blob: tg::Blob::with_referent(header.blob),
			length: header_length,
		};
		let entries = tg::blob::Child {
			blob: tg::Blob::with_referent(entries.blob),
			length,
		};
		let blob = tg::blob::Builder::new().children([header, entries]).build();
		blob.store_with_instance(&session).await?;
		let blob = blob.to_referent();

		Ok(blob)
	}
}
