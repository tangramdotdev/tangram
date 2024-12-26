use crate::{util::fs::cleanup, Config, Server};
use futures::FutureExt as _;
use insta::assert_json_snapshot;
use std::panic::AssertUnwindSafe;
use tangram_client::{self as tg, handle::Ext as _};
use tangram_temp::Temp;

#[tokio::test]
async fn list_no_results() -> tg::Result<()> {
	let temp = Temp::new();
	let options = Config::with_path(temp.path().to_owned());
	let server = Server::start(options).await?;
	let result = AssertUnwindSafe(async {
		let arg = tg::tag::list::Arg {
			length: None,
			pattern: "test".parse().unwrap(),
			remote: None,
			reverse: false,
		};
		let output = server.list_tags(arg).await?;
		assert_json_snapshot!(output, @"[]");
		Ok::<_, tg::Error>(())
	})
	.catch_unwind()
	.await;
	cleanup(temp, server).await;
	result.unwrap()
}

#[tokio::test]
async fn get_no_results() -> tg::Result<()> {
	let temp = Temp::new();
	let options = Config::with_path(temp.path().to_owned());
	let server = Server::start(options).await?;
	let result = AssertUnwindSafe(async {
		let pattern = "test".parse().unwrap();
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
async fn single() -> tg::Result<()> {
	let temp = Temp::new();
	let options = Config::with_path(temp.path().to_owned());
	let server = Server::start(options).await?;
	let result = AssertUnwindSafe(async {
		let file = tg::File::with_contents("test");
		let id = file.id(&server).await?;

		let tag = "test".parse().unwrap();
		let arg = tg::tag::put::Arg {
			force: false,
			item: tangram_either::Either::Right(id.into()),
			remote: None,
		};
		server.put_tag(&tag, arg).await?;

		let pattern = "test".parse().unwrap();
		let output = server.try_get_tag(&pattern).await?;
		assert_json_snapshot!(output, @r#"
  {
    "tag": "test",
    "item": "fil_01gtq62nh8tjjx5h9v0vn7k5gdr07p3es3wypse70hymnzn3dgrw8g"
  }
  "#);

		let arg = tg::tag::list::Arg {
			length: None,
			pattern,
			remote: None,
			reverse: false,
		};
		let output = server.list_tags(arg).await?;
		assert_json_snapshot!(output, @r#"
  [
    {
      "tag": "test",
      "item": "fil_01gtq62nh8tjjx5h9v0vn7k5gdr07p3es3wypse70hymnzn3dgrw8g"
    }
  ]
  "#);

		Ok::<_, tg::Error>(())
	})
	.catch_unwind()
	.await;

	cleanup(temp, server).await;

	result.unwrap()
}

#[tokio::test]
async fn multiple() -> tg::Result<()> {
	let temp = Temp::new();
	let options = Config::with_path(temp.path().to_owned());
	let server = Server::start(options).await?;
	let result = AssertUnwindSafe(async {
		let file = tg::File::with_contents("test");
		let id = file.id(&server).await?;

		// Put tags.
		let tags = [
			"foo",
			"bar",
			"test",
			"test/1.0.0",
			"test/1.1.0",
			"test/1.2.0",
			"test/10.0.0",
			"test/hello",
			"test/world",
		]
		.iter()
		.map(|tag| tag.parse().unwrap());
		for tag in tags {
			let arg = tg::tag::put::Arg {
				force: false,
				item: tangram_either::Either::Right(id.clone().into()),
				remote: None,
			};
			server.put_tag(&tag, arg).await?;
		}

		let pattern = "test".parse().unwrap();
		let arg = tg::tag::list::Arg {
			length: None,
			pattern,
			remote: None,
			reverse: false,
		};
		let output = server.list_tags(arg).await?;
		assert_json_snapshot!(output, @r#"
  [
    {
      "tag": "test",
      "item": "fil_01gtq62nh8tjjx5h9v0vn7k5gdr07p3es3wypse70hymnzn3dgrw8g"
    }
  ]
  "#);

		let pattern = "test/*".parse().unwrap();
		let arg = tg::tag::list::Arg {
			length: None,
			pattern,
			remote: None,
			reverse: false,
		};
		let output = server.list_tags(arg).await?;
		assert_json_snapshot!(output, @r#"
  [
    {
      "tag": "test/hello",
      "item": "fil_01gtq62nh8tjjx5h9v0vn7k5gdr07p3es3wypse70hymnzn3dgrw8g"
    },
    {
      "tag": "test/world",
      "item": "fil_01gtq62nh8tjjx5h9v0vn7k5gdr07p3es3wypse70hymnzn3dgrw8g"
    },
    {
      "tag": "test/1.0.0",
      "item": "fil_01gtq62nh8tjjx5h9v0vn7k5gdr07p3es3wypse70hymnzn3dgrw8g"
    },
    {
      "tag": "test/1.1.0",
      "item": "fil_01gtq62nh8tjjx5h9v0vn7k5gdr07p3es3wypse70hymnzn3dgrw8g"
    },
    {
      "tag": "test/1.2.0",
      "item": "fil_01gtq62nh8tjjx5h9v0vn7k5gdr07p3es3wypse70hymnzn3dgrw8g"
    },
    {
      "tag": "test/10.0.0",
      "item": "fil_01gtq62nh8tjjx5h9v0vn7k5gdr07p3es3wypse70hymnzn3dgrw8g"
    }
  ]
  "#);

		let pattern = "test".parse().unwrap();
		let output = server.get_tag(&pattern).await?;
		assert_json_snapshot!(output, @r#"
  {
    "tag": "test",
    "item": "fil_01gtq62nh8tjjx5h9v0vn7k5gdr07p3es3wypse70hymnzn3dgrw8g"
  }
  "#);

		let pattern = "test/^1".parse().unwrap();
		let output = server.get_tag(&pattern).await?;
		assert_json_snapshot!(output, @r#"
  {
    "tag": "test/1.2.0",
    "item": "fil_01gtq62nh8tjjx5h9v0vn7k5gdr07p3es3wypse70hymnzn3dgrw8g"
  }
  "#);

		let pattern = "test/^10".parse().unwrap();
		let output = server.get_tag(&pattern).await?;
		assert_json_snapshot!(output, @r#"
  {
    "tag": "test/10.0.0",
    "item": "fil_01gtq62nh8tjjx5h9v0vn7k5gdr07p3es3wypse70hymnzn3dgrw8g"
  }
  "#);

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
		let id = file.id(&server).await?;

		let tag = "test".parse().unwrap();
		let arg = tg::tag::put::Arg {
			force: false,
			item: tangram_either::Either::Right(id.clone().into()),
			remote: Some("default".to_string()),
		};
		server.put_tag(&tag, arg).await?;

		let pattern = "test".parse().unwrap();
		let local_get_output = server.try_get_tag(&pattern).await?;
		assert_json_snapshot!(local_get_output, @r#"
  {
    "tag": "test",
    "item": "fil_01gtq62nh8tjjx5h9v0vn7k5gdr07p3es3wypse70hymnzn3dgrw8g"
  }
  "#);
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
