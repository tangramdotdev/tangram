use super::{node::Indicator, Method};
use futures::{
	future::{self, BoxFuture},
	stream, FutureExt as _, StreamExt as _, TryStreamExt as _,
};
use num::ToPrimitive as _;
use std::{collections::BTreeMap, fmt::Write as _, pin::pin};
use tangram_client as tg;
use tangram_either::Either;

#[derive(Default)]
pub struct Provider {
	pub build_children: Option<Method<tokio::sync::mpsc::UnboundedSender<Self>, ()>>,
	pub item: Option<Either<tg::Build, tg::Value>>,
	pub log: Option<Method<tokio::sync::watch::Sender<String>, ()>>,
	pub name: Option<String>,
	pub object_children: Option<Method<(), Vec<Self>>>,
	pub status: Option<Method<tokio::sync::watch::Sender<Indicator>, ()>>,
	pub title: Option<Method<(), String>>,
}

impl Provider {
	pub fn array<H>(handle: &H, name: Option<String>, value: &[tg::Value]) -> Self
	where
		H: tg::Handle,
	{
		let title = Box::new(|()| {
			let fut = async move { String::new() };
			Box::pin(fut) as BoxFuture<'static, _>
		});

		let object_children = {
			let handle = handle.clone();
			let value = value.to_owned();
			Box::new(move |()| {
				let handle = handle.clone();
				let value = value.clone();
				let fut = async move {
					value
						.iter()
						.enumerate()
						.map(|(index, value)| Self::value(&handle, Some(index.to_string()), value))
						.collect::<Vec<_>>()
				};
				fut.boxed()
			})
		};

		Self {
			item: Some(Either::Right(tg::Value::Array(value.to_owned()))),
			name,
			title: Some(title),
			object_children: Some(object_children),
			..Default::default()
		}
	}

	#[allow(clippy::needless_pass_by_value)]
	pub fn build<H>(
		handle: &H,
		parent: Option<tg::Build>,
		name: Option<String>,
		build: tg::Build,
	) -> Self
	where
		H: tg::Handle,
	{
		let title = {
			let handle = handle.clone();
			let build = build.clone();
			Box::new(move |()| {
				let parent = parent.clone();
				let handle = handle.clone();
				let build = build.clone();
				let fut = async move {
					Self::build_title(&handle, parent, build)
						.await
						.unwrap_or_else(|error| format!("error: {error}"))
				};
				Box::pin(fut) as BoxFuture<'static, _>
			})
		};

		let build_children = {
			let handle = handle.clone();
			let build = build.clone();
			Box::new(move |sender: tokio::sync::mpsc::UnboundedSender<Self>| {
				let handle = handle.clone();
				// let parent = parent
				let build = build.clone();
				let sender = sender.clone();
				let fut = async move {
					let Ok(mut children) = build
						.children(&handle, tg::build::children::get::Arg::default())
						.await
					else {
						return;
					};
					while let Some(child) = children.next().await {
						let Ok(child) = child else {
							continue;
						};
						let child = Self::build(&handle, Some(build.clone()), None, child);
						sender.send(child).ok();
					}
				};
				fut.boxed()
			})
		};

		let object_children = {
			let handle = handle.clone();
			let build = build.clone();
			Box::new(move |()| {
				let handle = handle.clone();
				let build = build.clone();
				let fut = async move {
					let Ok(target) = build.target(&handle).await else {
						return Vec::new();
					};
					let child = Self::object(&handle, Some("target".to_owned()), &target.into());
					vec![child]
				};
				fut.boxed()
			})
		};

		let status = {
			let handle = handle.clone();
			let build = build.clone();
			Box::new(move |watch: tokio::sync::watch::Sender<_>| {
				let handle = handle.clone();
				let build = build.clone();
				let fut = async move {
					let Ok(mut status) = build.status(&handle).await else {
						return;
					};
					while let Some(status) = status.next().await {
						let Ok(status) = status else {
							break;
						};
						let indicator = match status {
							tg::build::Status::Created => Indicator::Created,
							tg::build::Status::Dequeued => Indicator::Dequeued,
							tg::build::Status::Finished => break,
							tg::build::Status::Started => Indicator::Started,
						};
						watch.send(indicator).ok();
					}
					let outcome = build.outcome(&handle).await;
					let indicator = match outcome {
						Ok(tg::build::Outcome::Canceled) => Indicator::Canceled,
						Ok(tg::build::Outcome::Failed(_)) | Err(_) => Indicator::Failed,
						Ok(tg::build::Outcome::Succeeded(_)) => Indicator::Succeeded,
					};
					watch.send(indicator).ok();
				};
				fut.boxed()
			})
		};

		let log = {
			let handle = handle.clone();
			let build = build.clone();
			Box::new(move |watch: tokio::sync::watch::Sender<String>| {
				let handle = handle.clone();
				let build = build.clone();
				let fut = async move {
					let mut last_line = String::new();
					let Ok(mut log) = build
						.log(
							&handle,
							tg::build::log::get::Arg {
								position: Some(std::io::SeekFrom::Start(0)),
								..Default::default()
							},
						)
						.await
					else {
						return;
					};
					while let Some(chunk) = log.next().await {
						let Ok(chunk) = chunk else {
							break;
						};
						let chunk = String::from_utf8_lossy(&chunk.bytes);
						last_line.push_str(&chunk);
						last_line = last_line
							.lines()
							.last()
							.unwrap_or(last_line.as_str())
							.to_owned();
						watch.send(last_line.clone()).ok();
						last_line.push('\n');
					}
				};
				fut.boxed()
			})
		};

		Self {
			item: Some(Either::Left(build.clone())),
			name,
			title: Some(title),
			build_children: Some(build_children),
			object_children: Some(object_children),
			status: Some(status),
			log: Some(log),
		}
	}

	async fn build_title<H>(
		handle: &H,
		parent: Option<tg::Build>,
		build: tg::Build,
	) -> tg::Result<String>
	where
		H: tg::Handle,
	{
		let mut title = String::new();
		let target = build.target(handle).await?;
		let host = target.host(handle).await?;

		// If this is a builtin, use the first arg.
		if host.as_str() == "builtin" {
			let name = target
				.args(handle)
				.await?
				.first()
				.and_then(|arg| arg.try_unwrap_string_ref().ok())
				.cloned()
				.ok_or_else(|| tg::error!("expected a string"))?;
			write!(title, "{name}").unwrap();
			return Ok(title);
		}

		'a: {
			let Some(parent) = parent else {
				break 'a;
			};

			// Get the parent's executable.
			let parent_executable = parent
				.target(handle)
				.await?
				.executable(handle)
				.await?
				.clone()
				.ok_or_else(|| tg::error!("expected the parent's target to have an executable"))?;
			let parent_executable_referent = match parent_executable {
				tg::target::Executable::Artifact(_) => {
					break 'a;
				},
				tg::target::Executable::Module(module) => module.referent,
			};
			let tg::Referent { item, subpath, .. } = parent_executable_referent;
			let artifact = tg::Artifact::try_from(item)
				.ok()
				.ok_or_else(|| tg::error!("expected an artifact"))?;
			let parent_executable_file = match artifact {
				tg::Artifact::Directory(directory) => {
					let subpath = subpath.ok_or_else(|| tg::error!("expected a subpath"))?;
					let artifact = directory.get(handle, subpath).await?;
					match artifact {
						tg::Artifact::Directory(_) => {
							return Err(tg::error!("expected a file"));
						},
						tg::Artifact::File(file) => file,
						tg::Artifact::Symlink(symlink) => symlink
							.resolve(handle)
							.await?
							.try_unwrap_file()
							.ok()
							.ok_or_else(|| tg::error!("expected a file"))?,
					}
				},
				tg::Artifact::File(file) => file,
				tg::Artifact::Symlink(symlink) => symlink
					.resolve(handle)
					.await?
					.try_unwrap_file()
					.ok()
					.ok_or_else(|| tg::error!("expected a file"))?,
			};

			// Get the build's executable.
			let executable = build
				.target(handle)
				.await?
				.executable(handle)
				.await?
				.clone()
				.ok_or_else(|| tg::error!("expected the target to have an executable"))?;
			let executable_referent = match executable {
				tg::target::Executable::Artifact(_) => {
					break 'a;
				},
				tg::target::Executable::Module(module) => module.referent,
			};
			let tg::Referent {
				item,
				path,
				subpath,
				tag,
			} = executable_referent;
			let executable_referent = tg::Referent {
				item: item.id(handle).await?,
				path,
				subpath,
				tag,
			};

			// Try to find the build in the parent executable's dependencies.
			if let Some(reference) = pin!(stream::iter(
				parent_executable_file
					.dependencies(handle)
					.await?
					.into_iter()
					.map(Ok),
			)
			.try_filter_map(|(reference, referent)| {
				let executable_referent = executable_referent.clone();
				async move {
					let referent = tg::Referent {
						item: referent.item.id(handle).await?,
						path: referent.path.clone(),
						subpath: referent.subpath.clone(),
						tag: referent.tag.clone(),
					};
					Ok::<_, tg::Error>((referent == executable_referent).then_some(reference))
				}
			}))
			.try_next()
			.await?
			{
				write!(title, "{reference}").unwrap();
			}
		}

		if host.as_str() == "js" {
			let name = target
				.args(handle)
				.await?
				.first()
				.and_then(|arg| arg.try_unwrap_string_ref().ok())
				.cloned();
			if let Some(name) = name {
				write!(title, "#{name}").unwrap();
			}
		}

		Ok(title)
	}

	pub fn map<H>(handle: &H, name: Option<String>, value: &BTreeMap<String, tg::Value>) -> Self
	where
		H: tg::Handle,
	{
		let title = Box::new(|()| {
			let fut = async move { String::new() };
			Box::pin(fut) as BoxFuture<'static, _>
		});

		let object_children = {
			let handle = handle.clone();
			let value = value.clone();
			Box::new(move |()| {
				let handle = handle.clone();
				let value = value.clone();
				let fut = async move {
					value
						.iter()
						.map(|(name, value)| Self::value(&handle, Some(name.clone()), value))
						.collect::<Vec<_>>()
				};
				fut.boxed()
			})
		};

		Self {
			item: Some(Either::Right(tg::Value::Map(value.clone()))),
			name,
			title: Some(title),
			object_children: Some(object_children),
			..Default::default()
		}
	}

	pub fn mutation<H>(_handle: &H, name: Option<String>, value: &tg::Mutation) -> Self
	where
		H: tg::Handle,
	{
		let title = Box::new(move |()| {
			let fut = async move { "mutation".to_owned() };
			Box::pin(fut) as BoxFuture<'static, _>
		});

		Self {
			item: Some(Either::Right(tg::Value::Mutation(value.clone()))),
			name,
			title: Some(title),
			..Default::default()
		}
	}

	pub fn object<H>(handle: &H, name: Option<String>, object: &tg::Object) -> Self
	where
		H: tg::Handle,
	{
		let title = {
			let handle = handle.clone();
			let object = object.clone();
			Box::new(move |()| {
				let handle = handle.clone();
				let object = object.clone();
				let fut = async move {
					match object.id(&handle).await {
						Ok(id) => id.to_string(),
						Err(error) => error.to_string(),
					}
				};
				Box::pin(fut) as BoxFuture<'static, _>
			})
		};

		let object_children = {
			let handle = handle.clone();
			let object = object.clone();
			Box::new(move |()| {
				let handle = handle.clone();
				let object = object.clone();
				let fut = async move {
					Self::object_children(&handle, &object)
						.await
						.into_iter()
						.map(|(name, value)| Self::value(&handle, name, &value))
						.collect::<Vec<_>>()
				};
				fut.boxed()
			})
		};

		Self {
			item: Some(Either::Right(tg::Value::Object(object.clone()))),
			name,
			title: Some(title),
			object_children: Some(object_children),
			..Default::default()
		}
	}

	async fn object_children<H>(handle: &H, object: &tg::Object) -> Vec<(Option<String>, tg::Value)>
	where
		H: tg::Handle,
	{
		let Ok(object) = object.object(handle).await else {
			return Vec::new();
		};
		match object {
			tg::object::Object::Branch(object) => {
				let branches = object
					.children
					.iter()
					.map(|child| {
						let child = match &child.blob {
							tg::Blob::Branch(branch) => {
								tg::Value::Object(tg::Object::Branch(branch.clone()))
							},
							tg::Blob::Leaf(leaf) => {
								tg::Value::Object(tg::Object::Leaf(leaf.clone()))
							},
						};
						(None, child)
					})
					.collect();
				branches
			},
			tg::object::Object::Leaf(_) => Vec::new(),
			tg::object::Object::Directory(object) => match object.as_ref() {
				tg::directory::Object::Graph { graph, node } => {
					vec![
						(
							Some("node".into()),
							tg::Value::Number(node.to_f64().unwrap()),
						),
						(None, tg::Value::Object(tg::Object::Graph(graph.clone()))),
					]
				},
				tg::directory::Object::Normal { entries } => entries
					.iter()
					.map(|(name, child)| {
						(Some(name.clone()), tg::Value::Object(child.clone().into()))
					})
					.collect(),
			},
			tg::object::Object::File(object) => match object.as_ref() {
				tg::file::Object::Graph { graph, node } => {
					vec![
						(
							Some("node".into()),
							tg::Value::Number(node.to_f64().unwrap()),
						),
						(None, tg::Value::Object(tg::Object::Graph(graph.clone()))),
					]
				},
				tg::file::Object::Normal {
					contents,
					dependencies,
					..
				} => {
					let mut children = vec![(
						Some("contents".into()),
						tg::Value::Object(contents.clone().into()),
					)];
					if !dependencies.is_empty() {
						let dependencies = dependencies
							.iter()
							.map(|(reference, referent)| {
								let mut name = reference.to_string();
								if let Some(tag) = &referent.tag {
									write!(name, "@{tag}").unwrap();
								}
								(name, tg::Value::Object(referent.item.clone()))
							})
							.collect();
						children.push((Some("dependencies".into()), tg::Value::Map(dependencies)));
					}
					children
				},
			},
			tg::object::Object::Symlink(object) => match object.as_ref() {
				tg::symlink::Object::Graph { graph, node } => {
					vec![
						(
							Some("node".into()),
							tg::Value::Number(node.to_f64().unwrap()),
						),
						(None, tg::Value::Object(tg::Object::Graph(graph.clone()))),
					]
				},
				tg::symlink::Object::Normal { artifact, .. } => {
					let mut children = Vec::new();
					if let Some(child) = artifact {
						children.push((None, tg::Value::Object(child.clone().into())));
					}
					children
				},
			},
			tg::object::Object::Graph(graph) => graph
				.nodes
				.iter()
				.enumerate()
				.map(|(index, node)| match node {
					tg::graph::Node::Directory(directory) => {
						let entries = directory
							.entries
							.iter()
							.map(|(name, entry)| {
								let child = match entry {
									Either::Left(index) => {
										tg::Value::Number(index.to_f64().unwrap())
									},
									Either::Right(artifact) => {
										tg::Value::Object(artifact.clone().into())
									},
								};
								(name.clone(), child)
							})
							.collect();
						let value = [("entries".to_owned(), tg::Value::Map(entries))]
							.into_iter()
							.collect();
						(Some(index.to_string()), tg::Value::Map(value))
					},
					tg::graph::Node::File(file) => {
						let mut value = BTreeMap::new();
						let contents = tg::Value::Object(file.contents.clone().into());
						value.insert("contents".into(), contents);

						if !file.dependencies.is_empty() {
							let dependencies = file
								.dependencies
								.iter()
								.map(|(reference, referent)| {
									let mut value = BTreeMap::new();
									let object = match &referent.item {
										Either::Left(index) => {
											tg::Value::Number(index.to_f64().unwrap())
										},
										Either::Right(object) => tg::Value::Object(object.clone()),
									};
									value.insert("object".to_owned(), object);
									if let Some(tag) = &referent.tag {
										value.insert(
											"tag".to_owned(),
											tg::Value::String(tag.to_string()),
										);
									}
									(reference.to_string(), tg::Value::Map(value))
								})
								.collect();
							value.insert("dependencies".into(), tg::Value::Map(dependencies));
						}
						(Some(index.to_string()), tg::Value::Map(value))
					},
					tg::graph::Node::Symlink(symlink) => {
						let mut value = BTreeMap::new();
						if let Some(artifact) = &symlink.artifact {
							let object = match artifact {
								Either::Left(index) => tg::Value::Number(index.to_f64().unwrap()),
								Either::Right(artifact) => {
									tg::Value::Object(artifact.clone().into())
								},
							};
							value.insert("artifact".into(), object);
						}
						(Some(index.to_string()), tg::Value::Map(value))
					},
				})
				.collect(),
			tg::object::Object::Target(target) => {
				let mut children = vec![
					(Some("args".into()), tg::Value::Array(target.args.clone())),
					(Some("env".into()), tg::Value::Map(target.env.clone())),
				];
				if let Some(executable) = &target.executable {
					let object = match executable {
						tg::target::Executable::Artifact(artifact) => Some(artifact.clone().into()),
						tg::target::Executable::Module(module) => {
							Some(module.referent.item.clone())
						},
					};
					if let Some(object) = object {
						children.push((Some("executable".into()), object.into()));
					}
				}
				children
			},
		}
	}

	pub fn value<H>(handle: &H, name: Option<String>, value: &tg::Value) -> Self
	where
		H: tg::Handle,
	{
		match value {
			tg::Value::Array(value) => Self::array(handle, name, value),
			tg::Value::Map(value) => Self::map(handle, name, value),
			tg::Value::Mutation(value) => Self::mutation(handle, name, value),
			tg::Value::Object(value) => Self::object(handle, name, value),
			value => {
				let value_ = value.clone();
				let title = Box::new(move |()| {
					let fut = future::ready(value_.to_string());
					Box::pin(fut) as BoxFuture<'static, _>
				});
				Self {
					item: Some(Either::Right(value.clone())),
					name,
					title: Some(title),
					..Default::default()
				}
			},
		}
	}
}
