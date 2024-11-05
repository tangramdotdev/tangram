use crate::{Config, Server};
use bytes::Bytes;
use futures::{
	stream::{self, FuturesUnordered},
	FutureExt as _, StreamExt as _, TryStreamExt as _,
};
use std::{fmt::Debug, panic::AssertUnwindSafe};
use tangram_client as tg;
use tangram_either::Either;
use tangram_temp::Temp;
use tg::{directory, file, handle::Ext as _, symlink};

#[tokio::test]
#[allow(clippy::many_single_char_names)]
async fn test_builds() -> tg::Result<()> {
	let temp = Temp::new();
	let options = Config::with_path(temp.path().to_owned());
	let server = Server::start(options).await?;
	let result = AssertUnwindSafe(async {
		// Create the builds.
		let e = create_test_build(&server, Vec::new()).await?;
		let d = create_test_build(&server, Vec::new()).await?;
		let c = create_test_build(&server, Vec::new()).await?;
		let b = create_test_build(&server, vec![e.clone(), d.clone()]).await?;
		let a = create_test_build(&server, vec![c.clone(), b.clone()]).await?;

		// Tag d.
		let tag = "d"
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the tag"))?;
		let arg = tg::tag::put::Arg {
			force: false,
			item: Either::Left(d.clone()),
			remote: None,
		};
		server.put_tag(&tag, arg).await?;

		// Tag b.
		let tag = "b"
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the tag"))?;
		let arg = tg::tag::put::Arg {
			force: false,
			item: Either::Left(b.clone()),
			remote: None,
		};
		server.put_tag(&tag, arg).await?;

		// Clean.
		server.clean().await?;

		// Assert.
		assert_build_presence(
			vec![
				(a.clone(), false),
				(c.clone(), false),
				(b.clone(), true),
				(e.clone(), true),
				(d.clone(), true),
			],
			&server,
		)
		.await?;

		Ok::<_, tg::Error>(())
	})
	.catch_unwind()
	.await;
	server.stop();
	server.wait().await;
	result.unwrap()
}

#[tokio::test]
#[allow(clippy::many_single_char_names)]
async fn test_objects() -> tg::Result<()> {
	let temp = Temp::new();
	let options = Config::with_path(temp.path().to_owned());
	let server = Server::start(options).await?;
	let result = AssertUnwindSafe(async {
		// Create the directory.
		let a = directory! {
			"b" => directory! {
				"f" => file!("f"),
			},
			"c" => directory! {
				"g" => file!("g"),
			},
			"d" => directory! {
				"h" => file!("h"),
			},
			"e" => symlink!(None, Some("d".into())),
		};

		// Get the artifacts.
		let b = a.get(&server, "b").await?;
		let c = a.get(&server, "c").await?;
		let d = a.get(&server, "d").await?;
		let e = a.get(&server, "e").await?;
		let f = a.get(&server, "b/f").await?;
		let g = a.get(&server, "c/g").await?;
		let h = a.get(&server, "d/h").await?;

		// Get the IDs.
		let a = a.id(&server).await?;
		let b = b.id(&server).await?;
		let c = c.id(&server).await?;
		let d = d.id(&server).await?;
		let e = e.id(&server).await?;
		let f = f.id(&server).await?;
		let g = g.id(&server).await?;
		let h = h.id(&server).await?;

		// Index the root object.
		server.index_object_recursive(&a.clone().into()).await?;

		// Tag c.
		let tag = "c"
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the tag"))?;
		let arg = tg::tag::put::Arg {
			force: false,
			item: Either::Right(c.clone().into()),
			remote: None,
		};
		server.put_tag(&tag, arg).await?;

		// Tag h.
		let tag = "h"
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the tag"))?;
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
				(a.clone().into(), false),
				(b, false),
				(c, true),
				(d, false),
				(e, false),
				(f, false),
				(g, true),
				(h, true),
			],
		)
		.await?;

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

async fn assert_build_presence<T>(ids: Vec<(T, bool)>, server: &Server) -> tg::Result<()>
where
	T: Into<tg::build::Id> + Debug + Clone,
{
	ids.into_iter()
		.map(|(id, present)| async move {
			let output = server.try_get_build(&id.clone().into()).await?;
			if present {
				assert!(
					output.is_some(),
					r#"expected build "{:?}" to be present"#,
					id.clone()
				);
			} else {
				assert!(
					output.is_none(),
					r#"expected build "{:?}" to be absent"#,
					id.clone()
				);
			}
			Ok::<_, tg::Error>(())
		})
		.collect::<FuturesUnordered<_>>()
		.try_collect::<Vec<_>>()
		.await?;
	Ok(())
}

async fn assert_object_presence(
	server: &Server,
	artifacts: Vec<(tg::artifact::Id, bool)>,
) -> tg::Result<()> {
	artifacts
		.into_iter()
		.map(|(id, present)| async move {
			let output = server.try_get_object(&id.clone().into()).await?;
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
		.try_collect::<Vec<_>>()
		.await?;
	Ok(())
}
