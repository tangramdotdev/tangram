use crate::{Config, Server};
use bytes::Bytes;
use futures::{
	stream::{self, FuturesUnordered},
	FutureExt as _, TryStreamExt as _,
};
use std::{collections::BTreeMap, fmt::Debug, panic::AssertUnwindSafe};
use tangram_client as tg;
use tangram_client::handle::Ext as _;
use tangram_either::Either;
use tangram_temp::Temp;
use tg::tag::Component;

#[tokio::test]
async fn test_objects() -> tg::Result<()> {
	let temp = Temp::new();
	let options = Config::with_path(temp.path().to_owned());
	let server = Server::start(options).await?;
	let result = AssertUnwindSafe(async {
		// Create the directory.
		let a = tg::directory! {
			b: tg::directory! {
				f: tg::file!("f"),
			},
			c: tg::directory! {
				g: tg::file!("g"),
			},
			d: tg::directory! {
				h: tg::file!("h"),
			},
			e: tg::file!("e"),
		};
		let b = a.get(&server, "b").await?;
		let c = a.get(&server, "c").await?;
		let d = a.get(&server, "d").await?;
		let e = a.get(&server, "e").await?;
		let f = a.get(&server, "b/f").await?;
		let g = a.get(&server, "c/g").await?;
		let h = a.get(&server, "d/h").await?;

		let a = a.id(&server).await?.into();
		let b = b.id(&server).await?;
		let c = c.id(&server).await?;
		let d = d.id(&server).await?;
		let e = e.id(&server).await?;
		let f = f.id(&server).await?;
		let g = g.id(&server).await?;
		let h = h.id(&server).await?;

		// Index the object.
		server.index_object_recursive(a.clone().into()).await?;

		// Tag c.
		let tag = "c".parse()?;
		let arg = tg::tag::put::Arg {
			force: false,
			item: Either::Right(c.clone().into()),
			remote: None,
		};
		server.put_tag(&tag, arg).await?;

		// Tag h.
		let tag = "h".parse()?;
		let arg = tg::tag::put::Arg {
			force: false,
			item: Either::Right(h.clone().into()),
			remote: None,
		};
		server.put_tag(&tag, arg).await?;

		// Clean.
		server.clean().await?;

		// Assert.
		assert_object_presence(
			&server,
			vec![
				(a, false),
				(b, false),
				(c, true),
				(d, true),
				(e, false),
				(f, false),
				(g, false),
				(h, true),
			],
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
		let child = create_test_build(&server, Vec::new()).await.unwrap();
		let tagged_child = create_test_build(&server, Vec::new()).await.unwrap();
		let parent = create_test_build(&server, Vec::new()).await.unwrap();
		let tagged_parent = create_test_build(&server, vec![child.clone(), tagged_child.clone()])
			.await
			.unwrap();
		let grandparent = create_test_build(&server, vec![parent.clone(), tagged_parent.clone()])
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

async fn create_test_build(
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

async fn assert_object_presence(
	server: &Server,
	artifacts: Vec<(tg::artifact::Id, bool)>,
) -> tg::Result<()> {
	artifacts
		.into_iter()
		.map(|(artifact, present)| async move {
			let output = server.try_get_object(artifact.state()).await?;
			if present {
				assert!(
					output.is_some(),
					r#"expected object "{:?}" to be present"#,
					id.clone()
				);
			} else {
				assert!(
					output.is_none(),
					r#"expected object "{:?}" to be absent"#,
					id.clone()
				);
			}
			Ok::<_, tg::Error>(())
		})
		.collect::<FuturesUnordered<_>>()
		.try_collect::<()>()
		.await;
}
