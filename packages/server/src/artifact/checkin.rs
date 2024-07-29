use crate::{tmp::Tmp, Server};
use futures::{
	future::BoxFuture,
	stream::{self, FuturesUnordered},
	FutureExt as _, Stream, StreamExt as _, TryStreamExt as _,
};
use indoc::formatdoc;
use num::ToPrimitive;
use std::sync::Mutex;
use std::{
	collections::BTreeMap,
	os::unix::fs::PermissionsExt as _,
	path::PathBuf,
	sync::{
		atomic::{AtomicU64, Ordering},
		Arc,
	},
};
use tangram_client::{self as tg, handle::Ext as _};
use tangram_database::{self as db, prelude::*};
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};
use time::format_description::well_known::Rfc3339;
use tokio_stream::wrappers::IntervalStream;

mod graph;
mod lock;
struct State {
	count: ProgressState,
	weight: ProgressState,
	visited: Mutex<BTreeMap<tg::Path, InnerOutput>>,
	graph: Mutex<graph::Graph>,
}

struct ProgressState {
	current: AtomicU64,
	total: Option<AtomicU64>,
}

#[derive(Clone, Debug)]
struct InnerOutput {
	artifact_id: Option<tg::artifact::Id>,
	graph_id: graph::Id,
	path: tg::path::Path,
	data: Option<Arc<tg::artifact::Data>>,
	count: Option<u64>,
	weight: Option<u64>,
	lock: Option<Lock>,
}

#[derive(Clone, Debug)]
enum Lock {
	File,
	Xattr,
}

impl Server {
	pub async fn check_in_artifact(
		&self,
		arg: tg::artifact::checkin::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::artifact::checkin::Event>>> {
		// Create the state.
		let count = ProgressState {
			current: AtomicU64::new(0),
			total: None,
		};
		let weight = ProgressState {
			current: AtomicU64::new(0),
			total: None,
		};
		let graph = Mutex::new(graph::Graph::default());
		let visited = Mutex::new(BTreeMap::new());
		let state = Arc::new(State {
			count,
			weight,
			graph,
			visited,
		});

		// Spawn the task.
		let (result_sender, result_receiver) = tokio::sync::oneshot::channel();
		tokio::spawn({
			let server = self.clone();
			let arg = arg.clone();
			let state = state.clone();
			async move {
				let result = server.check_in_artifact_task(arg, &state).await;
				result_sender.send(result).ok();
			}
		});

		// Create the stream.
		let interval = std::time::Duration::from_millis(100);
		let interval = tokio::time::interval(interval);
		let result = result_receiver.map(Result::unwrap).shared();
		let stream = IntervalStream::new(interval)
			.map(move |_| {
				let current = state
					.count
					.current
					.load(std::sync::atomic::Ordering::Relaxed);
				let total = state
					.count
					.total
					.as_ref()
					.map(|total| total.load(std::sync::atomic::Ordering::Relaxed));
				let count = tg::Progress { current, total };
				let current = state
					.weight
					.current
					.load(std::sync::atomic::Ordering::Relaxed);
				let total = state
					.weight
					.total
					.as_ref()
					.map(|total| total.load(std::sync::atomic::Ordering::Relaxed));
				let weight = tg::Progress { current, total };
				let progress = tg::artifact::checkin::Progress { count, weight };
				Ok(tg::artifact::checkin::Event::Progress(progress))
			})
			.take_until(result.clone())
			.chain(stream::once(result.map(|result| match result {
				Ok(id) => Ok(tg::artifact::checkin::Event::End(id)),
				Err(error) => Err(error),
			})));

		Ok(stream)
	}

	/// Attempt to store an artifact in the database.
	async fn check_in_artifact_task(
		&self,
		mut arg: tg::artifact::checkin::Arg,
		state: &State,
	) -> tg::Result<tg::artifact::Id> {
		// If this is a checkin of a path in the checkouts directory, then retrieve the corresponding artifact.
		let checkouts_path = self.checkouts_path().try_into()?;
		if let Some(path) = arg.path.diff(&checkouts_path).filter(tg::Path::is_internal) {
			let id = path
				.components()
				.get(1)
				.ok_or_else(|| tg::error!("cannot check in the checkouts directory"))?
				.try_unwrap_normal_ref()
				.ok()
				.ok_or_else(|| tg::error!("invalid path"))?
				.parse::<tg::artifact::Id>()?;
			let path = tg::Path::with_components(path.components().iter().skip(2).cloned());
			if path.components().len() == 1 {
				return Ok(id);
			}
			let artifact = tg::Artifact::with_id(id);
			let directory = artifact
				.try_unwrap_directory()
				.ok()
				.ok_or_else(|| tg::error!("invalid path"))?;
			let artifact = directory.get(self, &path).await?;
			let id = artifact.id(self).await?;
			return Ok(id);
		}

		// TODO: handle destructive checkins.
		// Copy or rename the file system object to the temp.
		// let tmp = Tmp::new(self);
		// if arg.destructive {
		// 	// If this is a destructive checkin, then attempt to rename the file system object to the temp. Otherwise, copy it.
		// 	match tokio::fs::rename(&arg.path, &tmp.path).await {
		// 		Ok(()) => (),
		// 		Err(error) if error.raw_os_error() == Some(libc::EXDEV) => {
		// 			copy_all(arg.path.as_ref(), &tmp.path).await?;
		// 		},
		// 		Err(source) => {
		// 			return Err(tg::error!(!source, "failed to rename file"));
		// 		},
		// 	}
		// } else {
		// 	// Otherwise, copy .
		// 	copy_all(arg.path.as_ref(), &tmp.path)
		// 		.await
		// 		.map_err(|source| tg::error!(!source, %path = arg.path, "failed to copy file"))?;
		// }
		// arg.path = tmp.path.clone().try_into()?;

		// Check in the artifact.
		self.check_in_artifact_inner(&arg, state).await?;

		// Get the output.
		let output = state
			.visited
			.lock()
			.unwrap()
			.get(&arg.path)
			.ok_or_else(|| tg::error!("invalid object graph"))?
			.clone();

		// Get the artifact ID.
		let artifact = output
			.artifact_id
			.ok_or_else(|| tg::error!("invalid object graph"))?;

		// Create hard links to files.
		let output = state
			.visited
			.lock()
			.unwrap()
			.values()
			.cloned()
			.collect::<Vec<_>>();
		for output in output {
			let artifact_id = output
				.artifact_id
				.as_ref()
				.ok_or_else(|| tg::error!("incomplete object graph"))?;

			if matches!(artifact_id, tg::artifact::Id::File(_)) {
				let src = &output.path;
				let dst = self.checkouts_path().join(artifact_id.to_string());
				tokio::fs::hard_link(src, &dst).await.ok();
			}
		}

		// Rename from the temp path to the checkout path.
		// let root_path = self.checkouts_path().join(artifact.to_string());
		// match tokio::fs::rename(&tmp.path, &root_path).await {
		// 	Ok(()) => (),
		// 	Err(error) if matches!(error.raw_os_error(), Some(libc::EEXIST | libc::ENOTEMPTY)) => {
		// 		return Ok(artifact);
		// 	},
		// 	Err(source) => {
		// 		return Err(tg::error!(!source, "failed to rename the temp"));
		// 	},
		// }

		Ok(artifact)
	}

	async fn check_in_artifact_inner(
		&self,
		arg: &tg::artifact::checkin::Arg,
		state: &State,
	) -> tg::Result<()> {
		// Check if we've visited this path already.
		if state.visited.lock().unwrap().contains_key(&arg.path) {
			return Ok(());
		}

		// Get the metadata for the file system object at the path.
		let metadata = tokio::fs::symlink_metadata(&arg.path).await.map_err(
			|source| tg::error!(!source, %path = arg.path, "failed to get the metadata for the path"),
		)?;

		// Create the output.
		let graph_id = state.graph.lock().unwrap().next_id();
		let output = InnerOutput {
			graph_id,
			artifact_id: None,
			path: arg.path.clone(),
			data: None,
			count: None,
			weight: None,
			lock: None,
		};
		state
			.visited
			.lock()
			.unwrap()
			.insert(arg.path.clone(), output);

		// Call the appropriate function for the file system object at the path.
		if metadata.is_dir() {
			self.check_in_directory(&arg, &metadata, state)
				.await
				.map_err(
					|source| tg::error!(!source, %path = arg.path, "failed to check in the directory"),
				)?
		} else if metadata.is_file() {
			self.check_in_file(arg, &metadata, state).await.map_err(
				|source| tg::error!(!source, %path = arg.path, "failed to check in the file"),
			)?
		} else if metadata.is_symlink() {
			self.check_in_symlink(arg, &metadata, state).await.map_err(
				|source| tg::error!(!source, %path = arg.path, "failed to check in the symlink"),
			)?
		} else {
			let file_type = metadata.file_type();
			return Err(tg::error!(
				%path = arg.path,
				?file_type,
				"invalid file type"
			));
		};

		// TODO: Update file times.
		// tokio::task::spawn_blocking({
		// 	let path = arg.path.clone();
		// 	move || {
		// 		let epoch = filetime::FileTime::from_system_time(std::time::SystemTime::UNIX_EPOCH);
		// 		filetime::set_symlink_file_times(path, epoch, epoch)
		// 			.map_err(|source| tg::error!(!source, "failed to set the modified time"))?;
		// 		Ok::<_, tg::Error>(())
		// 	}
		// })
		// .await
		// .unwrap()?;

		// Update the state.
		let visited = state.visited.lock().unwrap();
		let output = visited.get(&arg.path).unwrap();
		if let Some(count) = output.count {
			state.count.current.fetch_add(count, Ordering::Relaxed);
		}
		if let Some(weight) = output.weight {
			state.weight.current.fetch_add(weight, Ordering::Relaxed);
		}

		Ok(())
	}

	fn check_in_directory<'a>(
		&'a self,
		arg: &'a tg::artifact::checkin::Arg,
		metadata: &'a std::fs::Metadata,
		state: &'a State,
	) -> BoxFuture<'a, tg::Result<()>> {
		Box::pin(self.check_in_directory_inner(arg, metadata, state))
	}

	async fn check_in_directory_inner(
		&self,
		arg: &tg::artifact::checkin::Arg,
		_metadata: &std::fs::Metadata,
		state: &State,
	) -> tg::Result<()> {
		// If a root module path exists, check in as a file.
		'a: {
			let permit = self.file_descriptor_semaphore.acquire().await;
			let Ok(Some(path)) =
				tg::artifact::module::try_get_root_module_path_for_path(arg.path.as_ref()).await
			else {
				break 'a;
			};
			let path = arg.path.clone().join(path);
			let metadata = tokio::fs::metadata(&path)
				.await
				.map_err(|source| tg::error!(!source, %path, "failed to get the file metadata"))?;
			drop(permit);
			if !metadata.is_file() {
				break 'a;
			}
			let arg = tg::artifact::checkin::Arg {
				path,
				..arg.clone()
			};
			return self.check_in_file(&arg, &metadata, state).await;
		}

		// Read the directory.
		let names = {
			let _permit = self.file_descriptor_semaphore.acquire().await;
			let mut read_dir = tokio::fs::read_dir(&arg.path)
				.await
				.map_err(|source| tg::error!(!source, "failed to read the directory"))?;
			let mut names = Vec::new();
			while let Some(entry) = read_dir
				.next_entry()
				.await
				.map_err(|source| tg::error!(!source, "failed to get the directory entry"))?
			{
				let name = entry
					.file_name()
					.to_str()
					.ok_or_else(|| {
						let name = entry.file_name();
						tg::error!(?name, "all file names must be valid UTF-8")
					})?
					.to_owned();
				names.push(name);
			}
			names
		};

		// Recurse into the directory's entries.
		let entries = names
			.iter()
			.map(|name| async {
				let mut arg = arg.clone();
				arg.path = arg.path.clone().join(name.clone());
				self.check_in_artifact_inner(&arg, state).await?;
				let id = state
					.visited
					.lock()
					.unwrap()
					.get(&arg.path)
					.ok_or_else(|| tg::error!("invalid graph"))?
					.artifact_id
					.clone()
					.ok_or_else(|| tg::error!("cycle detected when checking in directory"))?;
				Ok::<_, tg::Error>((name.clone(), id))
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect::<BTreeMap<_, _>>()
			.await?;

		// Compute the count.
		let count = std::iter::empty()
			.chain(std::iter::once(Some(1)))
			.chain(entries.iter().map(|(name, _)| {
				let path = arg.path.clone().join(name.clone());
				state.visited.lock().unwrap().get(&path)?.count
			}))
			.sum::<Option<u64>>();

		// Compute the weight of the children.
		let weight = std::iter::empty()
			.chain(entries.iter().map(|(name, _)| {
				let path = arg.path.clone().join(name.clone());
				state.visited.lock().unwrap().get(&path)?.weight
			}))
			.sum::<Option<u64>>();

		// Create the directory.
		let data = tg::directory::Data { entries };
		let bytes = data.serialize()?;
		let id = tg::artifact::Id::from(tg::directory::Id::new(&bytes));
		let data = Arc::new(data.into());

		// Add the length of this directory to the weight.
		let weight = weight.map(|w| w + bytes.len().to_u64().unwrap());

		// Get the output.
		let mut visited = state.visited.lock().unwrap();
		let output = visited
			.get_mut(&arg.path)
			.ok_or_else(|| tg::error!("invalid check in state"))?;

		// Update the state.
		output.artifact_id.replace(id);
		output.data.replace(data);
		output.count = count;
		output.weight = weight;

		Ok(())
	}

	async fn check_in_file(
		&self,
		arg: &tg::artifact::checkin::Arg,
		metadata: &std::fs::Metadata,
		state: &State,
	) -> tg::Result<()> {
		// Create the blob without writing to disk/database.
		let _permit = self.file_descriptor_semaphore.acquire().await;
		let file = tokio::fs::File::open(&arg.path)
			.await
			.map_err(|source| tg::error!(!source, "failed to open the file"))?;

		// Create the output.
		let output = self
			.create_blob_inner(file, None)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the contents"))?;

		// Determine if the file is executable.
		let executable = (metadata.permissions().mode() & 0o111) != 0;

		// Get the dependencies.
		let dependencies: Option<tg::file::data::Dependencies> = None;
		let dependencies_metadata = if let Some(dependencies) = &dependencies {
			Some(
				dependencies
					.children()
					.iter()
					.map(|id| async { self.get_object_metadata(id).await })
					.collect::<FuturesUnordered<_>>()
					.try_collect::<Vec<_>>()
					.await?,
			)
		} else {
			None
		};

		// Create the file data.
		let data = tg::file::Data {
			contents: output.blob.clone(),
			executable,
			dependencies: dependencies.clone(),
			metadata: None,
		};
		let bytes = data.serialize()?;
		let id = tg::artifact::Id::from(tg::file::Id::new(&bytes));
		let data = Arc::new(data.into());

		// Install a symlink in the blobs directory.
		let src = PathBuf::from("../checkouts").join(id.to_string());
		let dst = self.blobs_path().join(output.blob.to_string());
		match tokio::fs::symlink(src, dst).await {
			Ok(()) => (),
			Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => (),
			Err(source) => {
				return Err(tg::error!(
					!source,
					"failed to install symlink into checkouts directory"
				))
			},
		}

		// Compute the count and weight.
		let dependencies_count = dependencies_metadata
			.iter()
			.flatten()
			.map(|metadata| metadata.count);
		let dependencies_weight = dependencies_metadata
			.iter()
			.flatten()
			.map(|metadata| metadata.weight);
		let count = std::iter::empty()
			.chain(std::iter::once(Some(1)))
			.chain(std::iter::once(Some(output.count)))
			.chain(dependencies_count)
			.sum::<Option<u64>>();
		let weight = std::iter::empty()
			.chain(std::iter::once(Some(bytes.len().to_u64().unwrap())))
			.chain(std::iter::once(Some(output.weight)))
			.chain(dependencies_weight)
			.sum::<Option<u64>>();

		// Get the output.
		let mut visited = state.visited.lock().unwrap();
		let output = visited
			.get_mut(&arg.path)
			.ok_or_else(|| tg::error!("invalid check in state"))?;

		// Update the state.
		output.artifact_id.replace(id);
		output.data.replace(data);
		output.count = count;
		output.weight = weight;

		Ok(())
	}

	async fn check_in_symlink(
		&self,
		arg: &tg::artifact::checkin::Arg,
		_metadata: &std::fs::Metadata,
		state: &State,
	) -> tg::Result<()> {
		// Read the target from the symlink.
		let target = tokio::fs::read_link(&arg.path).await.map_err(
			|source| tg::error!(!source, %path = arg.path, r#"failed to read the symlink at path"#,),
		)?;

		// Unrender the target.
		let target = target
			.to_str()
			.ok_or_else(|| tg::error!("the symlink target must be valid UTF-8"))?;
		let artifacts_path = self.artifacts_path();
		let artifacts_path = artifacts_path
			.to_str()
			.ok_or_else(|| tg::error!("the artifacts path must be valid UTF-8"))?;
		let target = tg::template::Data::unrender(artifacts_path, target)?;

		// Get the artifact and path.
		let (artifact, path) = if target.components.len() == 1 {
			let path = target.components[0]
				.try_unwrap_string_ref()
				.ok()
				.ok_or_else(|| tg::error!("invalid symlink"))?
				.clone();
			let path = path
				.parse()
				.map_err(|source| tg::error!(!source, "invalid symlink"))?;
			(None, Some(path))
		} else if target.components.len() == 2 {
			let artifact = target.components[0]
				.try_unwrap_artifact_ref()
				.ok()
				.ok_or_else(|| tg::error!("invalid symlink"))?
				.clone();
			let path = target.components[1]
				.try_unwrap_string_ref()
				.ok()
				.ok_or_else(|| tg::error!("invalid sylink"))?
				.clone();
			let path = &path[1..];
			let path = path
				.parse()
				.map_err(|source| tg::error!(!source, "invalid symlink"))?;
			(Some(artifact), Some(path))
		} else {
			return Err(tg::error!("invalid symlink"));
		};

		// Create the symlink.
		let data = tg::symlink::Data {
			artifact: artifact.clone(),
			path,
		};
		let bytes = data.serialize()?;
		let id = tg::artifact::Id::from(tg::symlink::Id::new(&bytes));
		let data = Arc::new(data.into());

		let count = if let Some(artifact) = &artifact {
			let metadata = self.get_object_metadata(&artifact.clone().into()).await?;
			metadata.count.map(|count| 1 + count)
		} else {
			Some(1)
		};
		let weight = if let Some(artifact) = &artifact {
			let metadata = self.get_object_metadata(&artifact.clone().into()).await?;
			metadata
				.weight
				.map(|weight| bytes.len().to_u64().unwrap() + weight)
		} else {
			Some(bytes.len().to_u64().unwrap())
		};

		// Get the output.
		let mut visited = state.visited.lock().unwrap();
		let output = visited
			.get_mut(&arg.path)
			.ok_or_else(|| tg::error!("invalid check in state"))?;

		// Update the state.
		output.artifact_id.replace(id);
		output.data.replace(data);
		output.count = count;
		output.weight = weight;

		Ok(())
	}

	pub(crate) fn try_store_artifact_future(
		&self,
		id: &tg::artifact::Id,
	) -> BoxFuture<'static, tg::Result<bool>> {
		let server = self.clone();
		let id = id.clone();
		Box::pin(async move { server.try_store_artifact_inner(&id).await })
	}

	pub(crate) async fn try_store_artifact_inner(&self, id: &tg::artifact::Id) -> tg::Result<bool> {
		// Check if the artifact exists in the checkouts directory.
		let permit = self.file_descriptor_semaphore.acquire().await.unwrap();
		let path = self.checkouts_path().join(id.to_string());
		let exists = tokio::fs::try_exists(&path)
			.await
			.map_err(|source| tg::error!(!source, "failed to check if the file exists"))?;
		if !exists {
			return Ok(false);
		}
		drop(permit);

		// Create the state.
		let count = ProgressState {
			current: AtomicU64::new(0),
			total: None,
		};
		let weight = ProgressState {
			current: AtomicU64::new(0),
			total: None,
		};
		let graph = Mutex::new(graph::Graph::default());
		let visited = Mutex::new(BTreeMap::new());
		let state = Arc::new(State {
			count,
			weight,
			graph,
			visited,
		});

		// Check in the artifact.
		let arg = tg::artifact::checkin::Arg {
			destructive: false,
			locked: true,
			path: path.try_into()?,
		};
		self.check_in_artifact_inner(&arg, &state).await?;
		let artifact_id = state
			.visited
			.lock()
			.unwrap()
			.get(&arg.path)
			.ok_or_else(|| tg::error!("invalid graph"))?
			.artifact_id
			.clone()
			.ok_or_else(|| tg::error!("invalid graph"))?;

		if &artifact_id != id {
			return Err(tg::error!("corrupted internal checkout"));
		}

		// Get a database connection.
		let mut connection = self
			.database
			.connection(db::Priority::Low)
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Begin a transaction.
		let transaction = connection
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

		// Collect the output.
		let output = state
			.visited
			.lock()
			.unwrap()
			.values()
			.cloned()
			.collect::<Vec<_>>();

		// Insert into the database.
		for output in output {
			// Validate output.
			let id = output
				.artifact_id
				.ok_or_else(|| tg::error!("invalid graph"))?;
			let data = output.data.ok_or_else(|| tg::error!("invalid graph"))?;
			let bytes = data.serialize()?;
			let count = output.count;
			let weight = output.weight;

			// Insert.
			let p = transaction.p();
			let statement = formatdoc!(
				"
					insert into objects (id, bytes, complete, count, weight, touched_at)
					values ({p}1, {p}2, {p}3, {p}4, {p}5, {p}6)
					on conflict (id) do update set touched_at = {p}6;
				"
			);
			let now = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();
			let params = db::params![id, bytes, 1, count, weight, now];
			transaction
				.execute(statement, params)
				.await
				.map_err(|source| {
					tg::error!(!source, "failed to put the artifact into the database")
				})?;
		}

		// Commit the transaction.
		transaction
			.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

		Ok(true)
	}
}

impl Server {
	pub(crate) async fn handle_check_in_artifact_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let arg = request.json().await?;
		let stream = handle.check_in_artifact(arg).await?;
		let sse = stream.map(|result| match result {
			Ok(tg::artifact::checkin::Event::Progress(progress)) => {
				let data = serde_json::to_string(&progress).unwrap();
				let event = tangram_http::sse::Event {
					data,
					..Default::default()
				};
				Ok::<_, tg::Error>(event)
			},
			Ok(tg::artifact::checkin::Event::End(artifact)) => {
				let event = "end".to_owned();
				let data = serde_json::to_string(&artifact).unwrap();
				let event = tangram_http::sse::Event {
					event: Some(event),
					data,
					..Default::default()
				};
				Ok::<_, tg::Error>(event)
			},
			Err(error) => {
				let data = serde_json::to_string(&error).unwrap();
				let event = "error".to_owned();
				let event = tangram_http::sse::Event {
					data,
					event: Some(event),
					..Default::default()
				};
				Ok::<_, tg::Error>(event)
			},
		});
		let body = Outgoing::sse(sse);
		let response = http::Response::builder().ok().body(body).unwrap();
		Ok(response)
	}
}

async fn copy_all(from: &std::path::Path, to: &std::path::Path) -> tg::Result<()> {
	let mut stack = vec![(from.to_owned(), to.to_owned())];
	while let Some((from, to)) = stack.pop() {
		let metadata = tokio::fs::symlink_metadata(&from).await.map_err(
			|source| tg::error!(!source, %path = from.display(), "failed to get file metadata"),
		)?;
		let file_type = metadata.file_type();
		if file_type.is_dir() {
			tokio::fs::create_dir_all(&to).await.map_err(
				|source| tg::error!(!source, %path = to.display(), "failed to create directory"),
			)?;
			let mut entries = tokio::fs::read_dir(&from).await.map_err(
				|source| tg::error!(!source, %path = from.display(), "failed to read directory"),
			)?;
			while let Some(entry) = entries
				.next_entry()
				.await
				.map_err(|source| tg::error!(!source, "failed to get directory entry"))?
			{
				let from = from.join(entry.file_name());
				let to = to.join(entry.file_name());
				stack.push((from, to));
			}
		} else if file_type.is_file() {
			tokio::fs::copy(&from, &to).await.map_err(
				|source| tg::error!(!source, %from = from.display(), %to = to.display(), "failed to copy file"),
			)?;
		} else if file_type.is_symlink() {
			let target = tokio::fs::read_link(&from).await.map_err(
				|source| tg::error!(!source, %path = from.display(), "failed to read link"),
			)?;
			tokio::fs::symlink(&target, &to)
				.await
				.map_err(|source| tg::error!(!source, %src = target.display(), %dst = to.display(), "failed to create symlink"))?;
		} else {
			return Err(tg::error!(%path = from.display(), "invalid file type"))?;
		}
	}
	Ok(())
}
