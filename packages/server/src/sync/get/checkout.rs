use {
	crate::{Session, temp::Temp},
	bytes::Bytes,
	indexmap::IndexMap,
	std::{
		collections::{BTreeMap, HashMap, HashSet, VecDeque},
		io::SeekFrom,
		path::PathBuf,
		sync::{Arc, Mutex},
	},
	tangram_client::prelude::*,
	tangram_index::prelude::*,
	tangram_store::prelude::*,
	tokio::io::{AsyncReadExt as _, AsyncSeekExt as _, AsyncWriteExt as _},
};

const LOAD_BATCH_SIZE: usize = 64;
const MAX_READ_HANDLES: usize = 32;
const MAX_WRITE_HANDLES: usize = 32;

struct State {
	blobs: HashMap<tg::blob::Id, Blob>,
	files: HashMap<tg::file::Id, File>,
	graph: Arc<Mutex<super::super::graph::Graph>>,
	loads: VecDeque<tg::blob::Id>,
	paths: HashSet<Arc<PathBuf>>,
	queued_loads: HashMap<tg::blob::Id, bool>,
	read_handles: IndexMap<Arc<PathBuf>, Handle>,
	ready: VecDeque<(tg::blob::Id, Pointer)>,
	receiver: tokio::sync::mpsc::Receiver<ObjectNode>,
	store_sender: tokio::sync::mpsc::Sender<super::store::ObjectNode>,
	pointers: HashMap<tg::blob::Id, Vec<Pointer>>,
	write_handles: IndexMap<tg::file::Id, Handle>,
}

struct Blob {
	data: BlobData,
	length: u64,
	metadata: Option<tg::object::Metadata>,
	published: bool,
	put: [u8; 16],
	received: bool,
	registered: bool,
	storage: Option<tangram_index::object::Storage>,
	transferred_bytes: u64,
}

enum BlobData {
	Branch {
		bytes: Option<Bytes>,
		children: Arc<[tg::blob::data::Child]>,
	},
	Leaf {
		bytes: Option<Bytes>,
		source: Option<Source>,
	},
}

#[derive(Clone)]
struct Source {
	path: Arc<PathBuf>,
	position: u64,
}

struct File {
	dependencies: Vec<tg::Id>,
	executable: bool,
	path: Arc<PathBuf>,
	pending: usize,
	root: tg::blob::Id,
	sources: Vec<tg::blob::Id>,
	temp: Temp,
}

struct Handle {
	file: tokio::fs::File,
	position: u64,
}

#[derive(Clone)]
struct Pointer {
	file: tg::file::Id,
	position: u64,
}

pub struct ObjectNode {
	pub bytes: Option<Bytes>,
	pub id: tg::blob::Id,
	pub metadata: Option<tg::object::Metadata>,
	pub put: [u8; 16],
}

impl Session {
	pub(super) async fn sync_get_checkout(
		&self,
		graph: Arc<Mutex<super::super::graph::Graph>>,
		receiver: tokio::sync::mpsc::Receiver<ObjectNode>,
		store_sender: tokio::sync::mpsc::Sender<super::store::ObjectNode>,
	) -> tg::Result<()> {
		if !self.sync_get_checkout_pointers_enabled() {
			return Ok(());
		}
		let mut state = State {
			blobs: HashMap::new(),
			files: HashMap::new(),
			graph,
			loads: VecDeque::new(),
			paths: HashSet::new(),
			queued_loads: HashMap::new(),
			read_handles: IndexMap::new(),
			ready: VecDeque::new(),
			receiver,
			store_sender,
			pointers: HashMap::new(),
			write_handles: IndexMap::new(),
		};
		while let Some(node) = state.receiver.recv().await {
			self.sync_get_checkout_handle_node(&mut state, node).await?;
			self.sync_get_checkout_process_pending(&mut state).await?;
		}

		// Reconcile targets which were added to the graph after their shared blob arrived.
		let targets = state.graph.lock().unwrap().finish_checkout_input();
		for (root, files) in targets {
			if !state.blobs.get(&root).is_some_and(|blob| blob.registered) {
				continue;
			}
			for file in files {
				self.sync_get_checkout_create_file(&mut state, root.clone(), file)
					.await?;
			}
			self.sync_get_checkout_process_pending(&mut state).await?;
		}

		// Create default files for the remaining received roots.
		let mut roots = {
			let graph = state.graph.lock().unwrap();
			state
				.blobs
				.iter()
				.filter(|(id, blob)| {
					blob.registered && !blob.published && !graph.checkout_blob_contained(id)
				})
				.map(|(id, _)| id.clone())
				.collect::<Vec<_>>()
		};
		roots.sort();
		self.sync_get_checkout_create_default_files(&mut state, roots)
			.await?;

		if !state.pointers.is_empty() {
			return Err(tg::error!("a checkout was missing blob objects"));
		}
		if !state.files.is_empty() {
			return Err(tg::error!("a checkout was incomplete"));
		}
		state.graph.lock().unwrap().finish_checkout();

		Ok(())
	}

	async fn sync_get_checkout_create_default_files(
		&self,
		state: &mut State,
		roots: Vec<tg::blob::Id>,
	) -> tg::Result<()> {
		for root in roots {
			if state.blobs.get(&root).unwrap().published {
				continue;
			}
			let file = Self::sync_get_checkout_default_file(&root)?;
			self.sync_get_checkout_create_file(state, root, file)
				.await?;
			self.sync_get_checkout_process_pending(state).await?;
		}
		if let Some((id, _)) = state
			.blobs
			.iter()
			.find(|(_, blob)| blob.registered && !blob.published)
		{
			return Err(tg::error!(%id, "a received blob was not published"));
		}

		Ok(())
	}

	async fn sync_get_checkout_handle_node(
		&self,
		state: &mut State,
		node: ObjectNode,
	) -> tg::Result<()> {
		let (contained, files) = {
			let graph = state.graph.lock().unwrap();
			let contained = graph.checkout_blob_contained(&node.id);
			let files = graph
				.checkout_files
				.get(&node.id)
				.cloned()
				.unwrap_or_default();

			(contained, files)
		};
		let pointed = state.pointers.contains_key(&node.id);
		if files.is_empty() && !contained && !pointed {
			let file = Self::sync_get_checkout_default_file(&node.id)?;
			self.sync_get_checkout_create_file(state, node.id.clone(), file)
				.await?;
		} else {
			for file in files {
				self.sync_get_checkout_create_file(state, node.id.clone(), file)
					.await?;
			}
		}

		match node.bytes {
			Some(bytes) => {
				Self::sync_get_checkout_insert_received(
					state,
					&node.id,
					bytes,
					node.metadata,
					node.put,
				)?;
			},
			None => Self::sync_get_checkout_queue_load(state, node.id, true),
		}

		Ok(())
	}

	fn sync_get_checkout_default_file(
		blob: &tg::blob::Id,
	) -> tg::Result<super::super::graph::CheckoutFile> {
		let data = tg::file::Data::Node(tg::file::data::Node {
			contents: Some(blob.clone()),
			dependencies: BTreeMap::new(),
			executable: false,
			module: None,
		});
		let id = tg::file::Id::new(&data.serialize()?);
		let file = super::super::graph::CheckoutFile {
			dependencies: Vec::new(),
			executable: false,
			id,
		};

		Ok(file)
	}

	async fn sync_get_checkout_create_file(
		&self,
		state: &mut State,
		root: tg::blob::Id,
		file: super::super::graph::CheckoutFile,
	) -> tg::Result<()> {
		let artifact = tg::artifact::Id::from(file.id.clone());
		let complete = state
			.graph
			.lock()
			.unwrap()
			.checkouts
			.contains_key(&artifact);
		if complete || state.files.contains_key(&file.id) {
			return Ok(());
		}
		let temp = Temp::new(&self.server);
		if state.write_handles.len() >= MAX_WRITE_HANDLES {
			state.write_handles.shift_remove_index(0);
		}
		let handle = tokio::fs::File::create_new(temp.path())
			.await
			.map_err(|error| tg::error!(!error, "failed to create the checkout temp file"))?;
		let id = file.id.clone();
		let path = Arc::new(temp.path().to_owned());
		let assembly = File {
			dependencies: file.dependencies,
			executable: file.executable,
			path,
			pending: 1,
			root: root.clone(),
			sources: Vec::new(),
			temp,
		};
		state.files.insert(id.clone(), assembly);
		let handle = Handle {
			file: handle,
			position: 0,
		};
		Self::sync_get_checkout_put_write_handle(state, id.clone(), handle);
		Self::sync_get_checkout_add_pointer(
			state,
			root,
			Pointer {
				file: id,
				position: 0,
			},
		);

		Ok(())
	}

	fn sync_get_checkout_insert_received(
		state: &mut State,
		id: &tg::blob::Id,
		bytes: Bytes,
		metadata: Option<tg::object::Metadata>,
		put: [u8; 16],
	) -> tg::Result<()> {
		if let Some(blob) = state.blobs.get_mut(id) {
			blob.registered = true;
			return Ok(());
		}
		let transferred_bytes = u64::try_from(bytes.len()).unwrap();
		let data = tg::blob::Data::deserialize(bytes.clone())?;
		let length = data.length();
		let data = match data {
			tg::blob::Data::Branch(branch) => BlobData::Branch {
				bytes: Some(bytes),
				children: branch.children.into(),
			},
			tg::blob::Data::Leaf(leaf) => BlobData::Leaf {
				bytes: Some(leaf.bytes),
				source: None,
			},
		};
		let blob = Blob {
			data,
			length,
			metadata,
			published: false,
			put,
			received: true,
			registered: true,
			storage: None,
			transferred_bytes,
		};
		Self::sync_get_checkout_insert_blob(state, id, blob);

		Ok(())
	}

	async fn sync_get_checkout_process_pending(&self, state: &mut State) -> tg::Result<()> {
		loop {
			self.sync_get_checkout_process_ready(state).await?;
			if state.loads.is_empty() {
				break;
			}
			self.sync_get_checkout_load_existing_batch(state).await?;
		}

		Ok(())
	}

	async fn sync_get_checkout_load_existing_batch(&self, state: &mut State) -> tg::Result<()> {
		let mut ids = Vec::with_capacity(LOAD_BATCH_SIZE);
		while ids.len() < LOAD_BATCH_SIZE
			&& let Some(id) = state.loads.pop_front()
		{
			let registered = state.queued_loads.remove(&id).unwrap();
			if !state.blobs.contains_key(&id) {
				ids.push((id, registered));
			} else if registered {
				state.blobs.get_mut(&id).unwrap().registered = true;
			}
		}
		if ids.is_empty() {
			return Ok(());
		}

		let object_ids = ids
			.iter()
			.map(|(id, _)| id.clone().into())
			.collect::<Vec<_>>();
		let arg = tangram_store::object::get::batch::Arg {
			ids: object_ids.clone(),
		};
		let objects_future = async {
			self.server
				.store
				.try_get_object_batch(arg)
				.await
				.map_err(|error| tg::error!(!error, "failed to get the existing blobs"))
		};
		let index_future = async {
			self.server
				.index
				.try_get_objects(&object_ids)
				.await
				.map_err(|error| {
					tg::error!(!error, "failed to get the existing blob index entries")
				})
		};
		let (objects, indexes) = tokio::try_join!(objects_future, index_future)?;

		for (((id, registered), output), index) in
			std::iter::zip(std::iter::zip(ids, objects), indexes)
		{
			let Some(object) = output.object else {
				if registered {
					return Err(tg::error!(%id, "expected the existing blob to exist"));
				}
				continue;
			};
			let metadata = index.as_ref().map(|object| object.metadata.clone());
			let storage = index.map(|object| object.storage);
			let (data, graph_data, length) = if let Some(bytes) = object.bytes {
				let bytes = Bytes::from(bytes.into_owned());
				let data = tg::blob::Data::deserialize(bytes.clone())?;
				let length = data.length();
				let blob = match &data {
					tg::blob::Data::Branch(branch) => BlobData::Branch {
						bytes: Some(bytes),
						children: branch.children.clone().into(),
					},
					tg::blob::Data::Leaf(leaf) => BlobData::Leaf {
						bytes: Some(leaf.bytes.clone()),
						source: None,
					},
				};
				(blob, data, length)
			} else {
				let pointer = object.checkout_pointer.ok_or_else(
					|| tg::error!(%id, "the existing blob had no bytes or checkout pointer"),
				)?;
				let length = object
					.length
					.ok_or_else(|| tg::error!(%id, "the existing blob had no length"))?;
				let data = tg::blob::Data::Leaf(tg::blob::data::Leaf {
					bytes: Bytes::new(),
				});
				let mut path = self
					.server
					.checkout_path()
					.join(pointer.artifact.to_string());
				if let Some(subpath) = pointer.path {
					path.push(subpath);
				}
				let path = Self::sync_get_checkout_intern_path(state, path);
				let source = Source {
					path,
					position: pointer.position,
				};
				let blob = BlobData::Leaf {
					bytes: None,
					source: Some(source),
				};
				(blob, data, length)
			};
			let blob = Blob {
				data,
				length,
				metadata: metadata.clone(),
				published: false,
				put: uuid::Uuid::now_v7().into_bytes(),
				received: false,
				registered,
				storage: storage.clone(),
				transferred_bytes: 0,
			};
			let object_id = id.clone().into();
			let data = tg::object::Data::Blob(graph_data);
			let arg = super::super::graph::UpdateObjectLocalArg {
				data: Some(&data),
				id: &object_id,
				marked: None,
				metadata,
				permissions: None,
				put: Some(blob.put),
				requested: None,
				storage,
			};
			{
				let mut graph = state.graph.lock().unwrap();
				graph.update_object_local(arg);
				graph.update_checkout_object(&object_id, &data);
			}
			Self::sync_get_checkout_insert_blob(state, &id, blob);
		}

		Ok(())
	}

	async fn sync_get_checkout_process_ready(&self, state: &mut State) -> tg::Result<()> {
		while let Some((id, pointer)) = state.ready.pop_front() {
			let (branch, leaf, received) = {
				let blob = state.blobs.get_mut(&id).unwrap();
				let received = blob.received;
				match &mut blob.data {
					BlobData::Branch { children, .. } => (Some(children.clone()), None, received),
					BlobData::Leaf { bytes, source } => {
						let leaf = match bytes.take() {
							Some(bytes) => tg::Either::Left(bytes),
							None => tg::Either::Right(source.clone().unwrap()),
						};
						(None, Some(leaf), received)
					},
				}
			};

			if let Some(children) = branch {
				Self::sync_get_checkout_decrement_pending(state, &pointer.file)?;
				let mut position = pointer.position;
				for child in children.iter() {
					Self::sync_get_checkout_increment_pending(state, &pointer.file)?;
					let child_pointer = Pointer {
						file: pointer.file.clone(),
						position,
					};
					Self::sync_get_checkout_add_pointer(state, child.blob.clone(), child_pointer);
					if !received && !state.blobs.contains_key(&child.blob) {
						Self::sync_get_checkout_queue_load(state, child.blob.clone(), false);
					}
					position += child.length;
				}
			} else if let Some(leaf) = leaf {
				match leaf {
					tg::Either::Left(bytes) => {
						Self::sync_get_checkout_write_bytes(state, &pointer, &bytes).await?;
					},
					tg::Either::Right(source) => {
						let length = state.blobs.get(&id).unwrap().length;
						Self::sync_get_checkout_copy_source(state, &pointer, &source, length)
							.await?;
					},
				}
				let path = state.files.get(&pointer.file).unwrap().path.clone();
				let source = Source {
					path,
					position: pointer.position,
				};
				let BlobData::Leaf { source: target, .. } =
					&mut state.blobs.get_mut(&id).unwrap().data
				else {
					unreachable!();
				};
				if target.is_none() {
					*target = Some(source);
					state
						.files
						.get_mut(&pointer.file)
						.unwrap()
						.sources
						.push(id.clone());
				}
				Self::sync_get_checkout_decrement_pending(state, &pointer.file)?;
			}

			if state.files.get(&pointer.file).unwrap().pending == 0 {
				self.sync_get_checkout_complete_file(state, &pointer.file)
					.await?;
			}
		}

		Ok(())
	}

	fn sync_get_checkout_queue_load(state: &mut State, id: tg::blob::Id, registered: bool) {
		if let Some(blob) = state.blobs.get_mut(&id) {
			blob.registered |= registered;
			return;
		}
		if let Some(existing) = state.queued_loads.get_mut(&id) {
			*existing |= registered;
			return;
		}
		state.queued_loads.insert(id.clone(), registered);
		state.loads.push_back(id);
	}

	fn sync_get_checkout_insert_blob(state: &mut State, id: &tg::blob::Id, blob: Blob) {
		state.blobs.insert(id.clone(), blob);
		if let Some(pointers) = state.pointers.remove(id) {
			state
				.ready
				.extend(pointers.into_iter().map(|pointer| (id.clone(), pointer)));
		}
	}

	fn sync_get_checkout_add_pointer(state: &mut State, id: tg::blob::Id, pointer: Pointer) {
		if state.blobs.contains_key(&id) {
			state.ready.push_back((id, pointer));
		} else {
			state.pointers.entry(id).or_default().push(pointer);
		}
	}

	fn sync_get_checkout_increment_pending(state: &mut State, id: &tg::file::Id) -> tg::Result<()> {
		let file = state
			.files
			.get_mut(id)
			.ok_or_else(|| tg::error!("expected the checkout file to exist"))?;
		file.pending += 1;

		Ok(())
	}

	fn sync_get_checkout_decrement_pending(state: &mut State, id: &tg::file::Id) -> tg::Result<()> {
		let file = state
			.files
			.get_mut(id)
			.ok_or_else(|| tg::error!("expected the checkout file to exist"))?;
		file.pending = file
			.pending
			.checked_sub(1)
			.ok_or_else(|| tg::error!("the checkout pending count underflowed"))?;

		Ok(())
	}

	async fn sync_get_checkout_write_bytes(
		state: &mut State,
		pointer: &Pointer,
		bytes: &[u8],
	) -> tg::Result<()> {
		let mut handle = Self::sync_get_checkout_take_write_handle(state, &pointer.file).await?;
		if handle.position != pointer.position {
			handle
				.file
				.seek(SeekFrom::Start(pointer.position))
				.await
				.map_err(|error| tg::error!(!error, "failed to seek the checkout temp file"))?;
			handle.position = pointer.position;
		}
		handle
			.file
			.write_all(bytes)
			.await
			.map_err(|error| tg::error!(!error, "failed to write the checkout temp file"))?;
		handle.position += u64::try_from(bytes.len()).unwrap();
		Self::sync_get_checkout_put_write_handle(state, pointer.file.clone(), handle);

		Ok(())
	}

	async fn sync_get_checkout_copy_source(
		state: &mut State,
		pointer: &Pointer,
		source: &Source,
		length: u64,
	) -> tg::Result<()> {
		let path = source.path.clone();
		let position = source.position;
		let mut source = Self::sync_get_checkout_take_read_handle(state, &path).await?;
		if source.position != position {
			source
				.file
				.seek(SeekFrom::Start(position))
				.await
				.map_err(|error| tg::error!(!error, "failed to seek the checkout source"))?;
			source.position = position;
		}
		let mut destination =
			Self::sync_get_checkout_take_write_handle(state, &pointer.file).await?;
		if destination.position != pointer.position {
			destination
				.file
				.seek(SeekFrom::Start(pointer.position))
				.await
				.map_err(|error| tg::error!(!error, "failed to seek the checkout temp file"))?;
			destination.position = pointer.position;
		}
		let copied = tokio::io::copy(&mut (&mut source.file).take(length), &mut destination.file)
			.await
			.map_err(|error| tg::error!(!error, "failed to copy the checkout bytes"))?;
		source.position += copied;
		destination.position += copied;
		Self::sync_get_checkout_put_read_handle(state, path, source);
		Self::sync_get_checkout_put_write_handle(state, pointer.file.clone(), destination);
		if copied != length {
			return Err(tg::error!(
				actual = %copied,
				expected = %length,
				"copied an invalid length"
			));
		}

		Ok(())
	}

	async fn sync_get_checkout_take_read_handle(
		state: &mut State,
		path: &Arc<PathBuf>,
	) -> tg::Result<Handle> {
		if let Some(handle) = state.read_handles.shift_remove(path) {
			return Ok(handle);
		}
		if state.read_handles.len() >= MAX_READ_HANDLES {
			state.read_handles.shift_remove_index(0);
		}
		let file = tokio::fs::File::open(path.as_ref())
			.await
			.map_err(|error| tg::error!(!error, "failed to open the checkout source"))?;
		let handle = Handle { file, position: 0 };

		Ok(handle)
	}

	async fn sync_get_checkout_take_write_handle(
		state: &mut State,
		id: &tg::file::Id,
	) -> tg::Result<Handle> {
		if let Some(handle) = state.write_handles.shift_remove(id) {
			return Ok(handle);
		}
		if state.write_handles.len() >= MAX_WRITE_HANDLES {
			state.write_handles.shift_remove_index(0);
		}
		let path = state.files.get(id).unwrap().temp.path();
		let file = tokio::fs::OpenOptions::new()
			.write(true)
			.open(path)
			.await
			.map_err(|error| tg::error!(!error, "failed to open the checkout temp file"))?;
		let handle = Handle { file, position: 0 };

		Ok(handle)
	}

	fn sync_get_checkout_put_read_handle(state: &mut State, path: Arc<PathBuf>, handle: Handle) {
		if state.read_handles.len() >= MAX_READ_HANDLES {
			state.read_handles.shift_remove_index(0);
		}
		state.read_handles.insert(path, handle);
	}

	fn sync_get_checkout_put_write_handle(state: &mut State, id: tg::file::Id, handle: Handle) {
		if state.write_handles.len() >= MAX_WRITE_HANDLES {
			state.write_handles.shift_remove_index(0);
		}
		state.write_handles.insert(id, handle);
	}

	async fn sync_get_checkout_complete_file(
		&self,
		state: &mut State,
		id: &tg::file::Id,
	) -> tg::Result<()> {
		if let Some(mut handle) = state.write_handles.shift_remove(id) {
			handle
				.file
				.flush()
				.await
				.map_err(|error| tg::error!(!error, "failed to flush the checkout temp file"))?;
		}
		let file = state
			.files
			.remove(id)
			.ok_or_else(|| tg::error!("expected the checkout file to exist"))?;
		let File {
			dependencies,
			executable,
			path: source_path,
			pending: _,
			root,
			sources,
			temp,
		} = file;
		let temp_path = temp.path().to_owned();
		state.read_handles.shift_remove(&source_path);
		#[cfg(unix)]
		if executable {
			use std::os::unix::fs::PermissionsExt as _;
			let permissions = std::fs::Permissions::from_mode(0o755);
			tokio::fs::set_permissions(&temp_path, permissions)
				.await
				.map_err(|error| tg::error!(!error, "failed to set checkout permissions"))?;
		}
		let path = self.server.checkout_path().join(id.to_string());
		match tangram_util::fs::rename_noreplace(&temp_path, &path).await {
			Ok(()) => {},
			Err(error)
				if matches!(
					error.kind(),
					std::io::ErrorKind::AlreadyExists
						| std::io::ErrorKind::IsADirectory
						| std::io::ErrorKind::PermissionDenied
				) => {},
			Err(error) => return Err(tg::error!(!error, "failed to publish the checkout file")),
		}
		let path = Self::sync_get_checkout_intern_path(state, path);
		for id in sources {
			let blob = state.blobs.get_mut(&id).unwrap();
			let BlobData::Leaf {
				source: Some(source),
				..
			} = &mut blob.data
			else {
				continue;
			};
			if source.path == source_path {
				source.path = path.clone();
			}
		}
		let artifact = tg::artifact::Id::from(id.clone());
		state
			.graph
			.lock()
			.unwrap()
			.checkouts
			.entry(artifact.clone())
			.or_insert(dependencies);
		Self::sync_get_checkout_publish(state, &root, artifact).await?;

		Ok(())
	}

	fn sync_get_checkout_intern_path(state: &mut State, path: PathBuf) -> Arc<PathBuf> {
		if let Some(path) = state.paths.get(&path) {
			return path.clone();
		}
		let path = Arc::new(path);
		state.paths.insert(path.clone());

		path
	}

	async fn sync_get_checkout_publish(
		state: &mut State,
		root: &tg::blob::Id,
		artifact: tg::artifact::Id,
	) -> tg::Result<()> {
		let mut stack = vec![(root.clone(), 0)];
		while let Some((id, position)) = stack.pop() {
			let blob = state.blobs.get_mut(&id).unwrap();
			if blob.published {
				continue;
			}
			blob.published = true;
			let (bytes, children) = match &mut blob.data {
				BlobData::Branch { bytes, children } => {
					let bytes = bytes
						.take()
						.ok_or_else(|| tg::error!(%id, "the branch blob had no bytes"))?;
					(Some(bytes), Some(children.clone()))
				},
				BlobData::Leaf { .. } => (None, None),
			};
			if let Some(children) = children {
				let mut child_position = position;
				let mut children_with_positions = Vec::with_capacity(children.len());
				for child in children.iter() {
					children_with_positions.push((child.blob.clone(), child_position));
					child_position += child.length;
				}
				stack.extend(children_with_positions.into_iter().rev());
			}
			let pointer = tangram_store::object::checkout::Pointer {
				artifact: artifact.clone(),
				length: blob.length,
				path: None,
				position,
			};
			let node = super::store::ObjectNode {
				bytes,
				checkout_pointer: Some(pointer),
				id: id.clone().into(),
				length: Some(blob.length),
				metadata: blob.metadata.take(),
				put: blob.put,
				storage: blob.storage.take(),
				transferred_bytes: blob.transferred_bytes,
			};
			state
				.store_sender
				.send(node)
				.await
				.map_err(|_| tg::error!("failed to send the object to the store task"))?;
			state
				.graph
				.lock()
				.unwrap()
				.checkout_objects
				.insert(id.into(), artifact.clone());
		}

		Ok(())
	}
}
