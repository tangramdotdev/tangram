use crate::{util::fs::cleanup, Config, Server};
use futures::FutureExt as _;
use insta::assert_yaml_snapshot;
use std::{panic::AssertUnwindSafe, str::FromStr};
use tangram_client as tg;
use tangram_temp::Temp;

#[tokio::test]
async fn get_no_results() -> tg::Result<()> {
	let temp = Temp::new();
	let options = Config::with_path(temp.path().to_owned());
	let server = Server::start(options).await?;
	let result = AssertUnwindSafe(async {
		let pattern = tg::tag::Pattern::from_str("test")?;
		let output = server.try_get_tag(&pattern).await?;
		assert!(output.is_none());
		Ok::<_, tg::Error>(())
	})
	.catch_unwind()
	.await;
	cleanup(temp, server).await;
	result.unwrap()
}

#[tokio::test]
async fn list_no_results() -> tg::Result<()> {
	let temp = Temp::new();
	let options = Config::with_path(temp.path().to_owned());
	let server = Server::start(options).await?;
	let result = AssertUnwindSafe(async {
		let arg = tg::tag::list::Arg {
			length: None,
			pattern: tg::tag::Pattern::from_str("test")?,
			remote: None,
		};
		let output = server.list_tags(arg).await?;
		assert_yaml_snapshot!(output, @"[]");
		Ok::<_, tg::Error>(())
	})
	.catch_unwind()
	.await;
	cleanup(temp, server).await;
	result.unwrap()
}

#[tokio::test]
async fn single_tag() -> tg::Result<()> {
	let temp = Temp::new();
	let options = Config::with_path(temp.path().to_owned());
	let server = Server::start(options).await?;
	let result = AssertUnwindSafe(async {
		let file = tg::File::with_contents("test");
		let file_id = file.id(&server).await?;

		let tag = tg::Tag::from_str("test").unwrap();
		let put_arg = tg::tag::put::Arg {
			force: false,
			item: tangram_either::Either::Right(file_id.into()),
			remote: None,
		};
		server.put_tag(&tag, put_arg).await?;

		let pattern = tg::tag::Pattern::from_str("test")?;
		let get_output = server.try_get_tag(&pattern).await?;
		assert_yaml_snapshot!(get_output, @r###"
  tag: test
  item: fil_01gtq62nh8tjjx5h9v0vn7k5gdr07p3es3wypse70hymnzn3dgrw8g
  "###);

		let list_arg = tg::tag::list::Arg {
			length: None,
			pattern,
			remote: None,
		};
		let output = server.list_tags(list_arg).await?;
		assert_yaml_snapshot!(output, @r###"
  - tag: test
    item: fil_01gtq62nh8tjjx5h9v0vn7k5gdr07p3es3wypse70hymnzn3dgrw8g
  "###);
		Ok::<_, tg::Error>(())
	})
	.catch_unwind()
	.await;
	cleanup(temp, server).await;
	result.unwrap()
}

#[tokio::test]
async fn remote_put() -> tg::Result<()> {
	let remote_temp = Temp::new();
	let remote_config = Config::with_path(remote_temp.path().to_owned());
	let remote = Server::start(remote_config).await?;

	let server_temp = Temp::new();
	let mut server_config = Config::with_path(server_temp.path().to_owned());
	server_config.remotes = [(
		"default".to_owned(),
		crate::config::Remote {
			url: remote.url().clone(),
		},
	)]
	.into();
	let server = Server::start(server_config).await?;

	let result = AssertUnwindSafe(async {
		let file = tg::File::with_contents("test");
		let file_id = file.id(&server).await?;

		let tag = tg::Tag::from_str("test").unwrap();
		let put_arg = tg::tag::put::Arg {
			force: false,
			item: tangram_either::Either::Right(file_id.clone().into()),
			remote: Some("default".to_string()),
		};
		server.put_tag(&tag, put_arg).await?;

		let pattern = tg::tag::Pattern::from_str("test")?;
		let local_get_output = server.try_get_tag(&pattern).await?;
		assert_yaml_snapshot!(local_get_output, @r###"
  tag: test
  item: fil_01gtq62nh8tjjx5h9v0vn7k5gdr07p3es3wypse70hymnzn3dgrw8g
  "###);
		let remote_get_output = remote.try_get_tag(&pattern).await?;
		assert_eq!(local_get_output, remote_get_output);
		Ok::<_, tg::Error>(())
	})
	.catch_unwind()
	.await;
	cleanup(server_temp, server).await;
	cleanup(remote_temp, remote).await;
	result.unwrap()
}
