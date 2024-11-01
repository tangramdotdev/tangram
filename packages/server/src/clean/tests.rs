use crate::{Config, Server};
use futures::{stream::FuturesUnordered, FutureExt as _};
use std::fmt::Debug;
use std::{collections::BTreeMap, panic::AssertUnwindSafe};
use tangram_client as tg;
use tangram_either::Either;
use tangram_temp::Temp;
use tg::tag::Component;
use tokio_stream::StreamExt;

#[tokio::test]
async fn test() -> tg::Result<()> {
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

		assert_presence(
			vec![
				(root_dir_id, false),
				(child_dir_one_id, false),
				(child_dir_two_id, true),
				(child_dir_three_id, false),
			],
			&server,
		)
		.await;

		assert_presence(
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

async fn assert_presence<T>(ids: Vec<(T, bool)>, server: &Server)
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
				assert!(output.is_some(), "Expected {:?} to be present!", id.clone());
			} else {
				assert!(output.is_none(), "Expected {:?} to be absent!", id.clone());
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
