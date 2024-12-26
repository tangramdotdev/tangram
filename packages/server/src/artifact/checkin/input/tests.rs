use super::Graph;
use crate::{util::fs::cleanup, Config, Server};
use futures::FutureExt;
use indoc::indoc;
use insta::assert_snapshot;
use std::panic::AssertUnwindSafe;
use tangram_client as tg;
use tangram_temp::{self as temp, Temp};

#[tokio::test]
async fn directory() -> tg::Result<()> {
	test(
		temp::directory! {
			"file.txt" => "hello, world!"
		},
		"",
		|graph| {
			assert_snapshot!(graph.to_string(), @r"
				0 
					(reference: ./file.txt, node: 1)
				1 file.txt
			");
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn file() -> tg::Result<()> {
	test(
		temp::directory! {
			"file.txt" => "hello, world!"
		},
		"file.txt",
		|graph| {
			assert_snapshot!(graph.to_string(), @"0");
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn symlink() -> tg::Result<()> {
	test(
		temp::directory! {
			"file.txt" => "hello, world!",
			"link" => temp::symlink!("file.txt"),
		},
		"link",
		|graph| {
			assert_snapshot!(graph.to_string(), @r"
				0 
					(reference: ./file.txt, path: ../file.txt)
			");
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn package() -> tg::Result<()> {
	test(
		temp::directory! {
			"tangram.ts" =>  indoc!(r#"
				import * as a from "./a.tg.ts";
				import * as b from "b";
			"#),
			"a.tg.ts" => "",
		},
		"",
		|graph| {
			assert_snapshot!(graph.to_string(), @r"
				0 
					(reference: ./a.tg.ts, node: 1)
					(reference: ./tangram.ts, node: 2)
				1 a.tg.ts
				2 tangram.ts
					(reference: ./a.tg.ts, node: 0, path: , subpath: a.tg.ts)
					(reference: b)
			");
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

async fn test<F>(artifact: impl Into<temp::Artifact>, path: &str, assertions: F) -> tg::Result<()>
where
	F: Fn(Graph) -> tg::Result<()>,
{
	let temp = Temp::new();
	let options = Config::with_path(temp.path().to_owned());
	let server = Server::start(options).await?;
	let result = AssertUnwindSafe(async {
		let temp = Temp::new();
		artifact.into().to_path(temp.path()).await.unwrap();
		let arg = tg::artifact::checkin::Arg {
			cache: false,
			path: temp.path().join(path),
			destructive: false,
			deterministic: false,
			ignore: true,
			locked: true,
			lockfile: true,
		};
		let input = server.create_input_graph(arg, None).await?;
		assertions(input)
	})
	.catch_unwind()
	.await;
	cleanup(temp, server).await;
	result.unwrap()
}
