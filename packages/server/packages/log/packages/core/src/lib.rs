use {
	std::{borrow::Cow, io::Cursor},
	tangram_cache::log,
	tangram_client::prelude::*,
	tangram_futures::{task::Task, write::Ext as _},
	tokio::io::AsyncWriteExt as _,
};

pub struct Writer<I> {
	index: Index,
	input: tokio::io::DuplexStream,
	length: u64,
	instance: I,
	task: Task<tg::Result<tg::write::Output>>,
}

#[derive(Clone, Debug, Default, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct Index {
	#[tangram_serialize(id = 0)]
	pub entries: Vec<Entry>,

	#[tangram_serialize(id = 1)]
	pub stdout: Vec<u32>,

	#[tangram_serialize(id = 2)]
	pub stderr: Vec<u32>,
}

#[derive(Clone, Copy, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct Entry {
	#[tangram_serialize(id = 0)]
	pub blob_position: u64,

	#[tangram_serialize(id = 1)]
	pub blob_length: u64,

	#[tangram_serialize(id = 2)]
	pub combined_position: u64,

	#[tangram_serialize(id = 3)]
	pub stream: tg::process::stdio::Stream,

	#[tangram_serialize(id = 4)]
	pub stream_position: u64,
}

impl<I> Writer<I>
where
	I: tg::Instance,
{
	#[must_use]
	pub fn new(instance: &I) -> Self {
		let (input, reader) = tokio::io::duplex(64 * 1024);
		let task = Task::spawn({
			let instance = instance.clone();
			move |_| async move {
				let arg = tg::write::Arg {
					checkout_pointers: Some(true),
				};
				instance.write(arg, reader).await
			}
		});
		Self {
			index: Index::default(),
			input,
			length: 0,
			instance: instance.clone(),
			task,
		}
	}

	pub async fn write(&mut self, chunk: &tg::process::stdio::Chunk) -> tg::Result<()> {
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

	pub async fn end(self) -> tg::Result<tg::Referent<tg::blob::Id>> {
		let Self {
			index,
			input,
			length,
			instance,
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
		let header = instance.write(arg, Cursor::new(bytes)).await?;
		let header = tg::blob::Child {
			blob: tg::Blob::with_referent(header.blob),
			length: header_length,
		};
		let entries = tg::blob::Child {
			blob: tg::Blob::with_referent(entries.blob),
			length,
		};
		let blob = tg::blob::Builder::new().children([header, entries]).build();
		blob.store_with_instance(&instance).await?;
		let blob = blob.to_referent();

		Ok(blob)
	}
}
