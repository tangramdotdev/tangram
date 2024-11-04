use crate::{Config, Server};
use bytes::Bytes;
use futures::TryStreamExt;
use futures::{stream, stream::FuturesUnordered, FutureExt as _};
use std::fmt::Debug;
use std::{collections::BTreeMap, panic::AssertUnwindSafe};
use tangram_client as tg;
use tangram_client::handle::Ext;
use tangram_either::Either;
use tangram_temp::Temp;
use tg::tag::Component;
use tokio_stream::StreamExt;

#[tokio::test]
async fn test_objects() -> tg::Result<()> {
	let temp = Temp::new();
	let options = Config::with_path(temp.path().to_owned());
	let server = Server::start(options).await?;
	let result = AssertUnwindSafe(async {
		let file_1 = tg::file::Builder::new("child of root dir")
			.executable(true)
			.build();
		let file_1_id = file_1.store(&server).await.unwrap();

		let file_2 = tg::file::Builder::new("child one of dir three")
			.executable(true)
			.build();
		let file_2_id = file_2.store(&server).await.unwrap();

		let file_3 = tg::file::Builder::new("child two of dir three")
			.executable(true)
			.build();
		let file_3_id = file_3.store(&server).await.unwrap();

		let file_4 = tg::file::Builder::new("child of dir two")
			.executable(true)
			.build();
		let file_4_id = file_4.store(&server).await.unwrap();

		let child_dir_one = tg::directory::Builder::with_entries(BTreeMap::new()).build();
		let child_dir_one_id = child_dir_one.store(&server).await.unwrap();

		let child_dir_two =
			tg::directory::Builder::with_entries(dir_contents(vec![file_1])).build();
		let child_dir_two_id = child_dir_two.store(&server).await.unwrap();

		let child_dir_three =
			tg::directory::Builder::with_entries(dir_contents(vec![file_2, file_3])).build();
		let child_dir_three_id = child_dir_three.store(&server).await.unwrap();

		let root_contents: Vec<tg::Artifact> = vec![
			child_dir_one.into(),
			child_dir_two.clone().into(),
			child_dir_three.into(),
			file_4.into(),
		];
		let root_dir = tg::directory::Builder::with_entries(dir_contents(root_contents)).build();
		let root_dir_id = root_dir.store(&server).await.unwrap();

		index_manually(&server, root_dir_id.clone().into(), root_dir.into()).await;

		let tag_components = vec![Component::new("abc".to_owned())];
		let tag = tg::tag::Tag::with_components(tag_components);
		let arg = tg::tag::put::Arg {
			force: false,
			item: Either::Right(child_dir_two_id.clone().into()),
			remote: None,
		};
		server.put_tag(&tag, arg).await.unwrap();

		let tag_components = vec![Component::new("bcd".to_owned())];
		let tag = tg::tag::Tag::with_components(tag_components);
		let arg = tg::tag::put::Arg {
			force: false,
			item: Either::Right(file_2_id.clone().into()),
			remote: None,
		};
		server.put_tag(&tag, arg).await.unwrap();

		server.clean().await.unwrap();

		assert_object_presence(
			vec![
				(root_dir_id, false),
				(child_dir_one_id, false),
				(child_dir_two_id, true),
				(child_dir_three_id, false),
			],
			&server,
		)
		.await;

		assert_object_presence(
			vec![
				(file_1_id, true),
				(file_2_id, true),
				(file_3_id, false),
				(file_4_id, false),
			],
			&server,
		)
		.await;

		Ok::<_, tg::Error>(())
	})
	.catch_unwind()
	.await;
	server.stop();
	server.wait().await;
	result.unwrap()
}

#[tokio::test]
async fn test_builds() -> tg::Result<()> {
	let temp = Temp::new();
	let options = Config::with_path(temp.path().to_owned());
	let server = Server::start(options).await?;
	let result = AssertUnwindSafe(async {
		let child = put_build(&server, Vec::new()).await.unwrap();
		let tagged_child = put_build(&server, Vec::new()).await.unwrap();
		let parent = put_build(&server, Vec::new()).await.unwrap();
		let tagged_parent = put_build(&server, vec![child.clone(), tagged_child.clone()])
			.await
			.unwrap();
		let grandparent = put_build(&server, vec![parent.clone(), tagged_parent.clone()])
			.await
			.unwrap();

		let tag_components = vec![Component::new("abc".to_owned())];
		let tag = tg::tag::Tag::with_components(tag_components);
		let arg = tg::tag::put::Arg {
			force: false,
			item: Either::Left(tagged_child.clone()),
			remote: None,
		};
		server.put_tag(&tag, arg).await.unwrap();

		let tag_components = vec![Component::new("bcd".to_owned())];
		let tag = tg::tag::Tag::with_components(tag_components);
		let arg = tg::tag::put::Arg {
			force: false,
			item: Either::Left(tagged_parent.clone()),
			remote: None,
		};
		server.put_tag(&tag, arg).await.unwrap();

		server.clean().await.unwrap();

		assert_build_presence(
			vec![
				(grandparent.clone(), false),
				(parent.clone(), false),
				(tagged_parent.clone(), true),
				(child.clone(), true),
				(tagged_child.clone(), true),
			],
			&server,
		)
		.await;
		// Ensure that links to removed builds remain.
		assert_build_children(
			&server,
			tagged_parent,
			vec![(child, true), (tagged_child, true)],
		)
		.await;

		Ok::<_, tg::Error>(())
	})
	.catch_unwind()
	.await;
	server.stop();
	server.wait().await;
	result.unwrap()
}

async fn put_build(
	server: &Server,
	build_children: Vec<tg::build::Id>,
) -> tg::Result<tg::build::Id> {
	let id = tg::build::Id::new();
	let arg = tg::build::put::Arg {
		id: id.clone(),
		children: build_children,
		depth: 1,
		host: "host".to_string(),
		log: None,
		outcome: None,
		retry: tg::build::Retry::Succeeded,
		status: tg::build::Status::Finished,
		target: tg::target::Id::new(&Bytes::from("target id")),
		created_at: time::OffsetDateTime::now_utc(),
		dequeued_at: None,
		started_at: None,
		finished_at: None,
	};
	server.put_build(&id.clone(), arg).await?;
	Ok(id)
}

async fn index_manually(server: &Server, id: tg::object::Id, artifact: tg::Artifact) {
	match artifact {
		tg::artifact::Artifact::Directory(dir) => {
			dir.entries(server)
				.await
				.unwrap()
				.values()
				.map(|artifact| async move {
					index_manually(
						server,
						artifact.id(server).await.unwrap().into(),
						artifact.clone(),
					)
					.await;
				})
				.collect::<FuturesUnordered<_>>()
				.collect::<()>()
				.await;
			server.index_object(&id).await.unwrap();
		},
		tg::artifact::Artifact::File(_file) => {
			server.index_object(&id).await.unwrap();
		},
		tg::artifact::Artifact::Symlink(_sym) => todo!(),
	}
}

async fn assert_build_children<T>(server: &Server, parent: T, children: Vec<(T, bool)>)
where
	T: Into<tg::build::Id> + Debug + Clone,
{
	let parent = parent.clone().into();
	let arg = tg::build::children::get::Arg::default();
	let present_children: Vec<tg::build::Id> = server
		.try_get_build_children(&parent, arg)
		.await
		.unwrap()
		.ok_or_else(|| tg::error!("expected the build to exist"))
		.unwrap()
		.map_ok(|chunk| stream::iter(chunk.data).map(Ok::<_, tg::Error>))
		.try_flatten()
		.try_collect()
		.await
		.unwrap();
	for (id, present) in children {
		if present {
			assert!(
				present_children.contains(&id.clone().into()),
				"Expected the link between build {:?} and child {:?} to be present!",
				parent.clone(),
				id.clone()
			);
		} else {
			assert!(
				!present_children.contains(&id.clone().into()),
				"Expected the link between build {:?} and child {:?} to be absent!",
				parent.clone(),
				id.clone()
			);
		}
	}
}

async fn assert_build_presence<T>(ids: Vec<(T, bool)>, server: &Server)
where
	T: Into<tg::build::Id> + Debug + Clone,
{
	ids.into_iter()
		.map(|(id, present)| async move {
			let output = server.try_get_build(&id.clone().into()).await.unwrap();

			if present {
				assert!(
					output.is_some(),
					"Expected build {:?} to be present!",
					id.clone()
				);
			} else {
				assert!(
					output.is_none(),
					"Expected build {:?} to be absent!",
					id.clone()
				);
			}
		})
		.collect::<FuturesUnordered<_>>()
		.collect::<Vec<_>>()
		.await;
}

async fn assert_object_presence<T>(ids: Vec<(T, bool)>, server: &Server)
where
	T: Into<tg::object::Id> + Debug + Clone,
{
	ids.into_iter()
		.map(|(id, present)| async move {
			let output = server
				.try_get_object_local_database(&id.clone().into())
				.await
				.unwrap();

			if present {
				assert!(
					output.is_some(),
					"Expected object {:?} to be present!",
					id.clone()
				);
			} else {
				assert!(
					output.is_none(),
					"Expected object {:?} to be absent!",
					id.clone()
				);
			}
		})
		.collect::<FuturesUnordered<_>>()
		.collect::<Vec<_>>()
		.await;
}

fn dir_contents<T>(contents: Vec<T>) -> BTreeMap<String, tg::Artifact>
where
	T: Into<tg::Artifact>,
{
	contents
		.into_iter()
		.enumerate()
		.map(|(i, artifact)| (i.to_string(), artifact.into()))
		.collect()
}
