use crate::{util::fs::cleanup, Config, Server};
use futures::FutureExt;
use insta::{assert_json_snapshot, assert_snapshot};
use std::{future::Future, panic::AssertUnwindSafe, path::Path};
use tangram_client as tg;
use tangram_temp::{self as temp, Temp};

#[allow(dead_code)]
struct TestArtifact {
	// The server the artifact exists upon.
	server: Server,

	// The tangram artifact.
	artifact: tg::Artifact,

	// The artifact that was checked in.
	checkin: temp::Artifact,

	// The lockfile created at checkin.
	checkin_lockfile: Option<tg::Lockfile>,

	// The artifact that was checked out.
	checkout: temp::Artifact,

	// The lockfile created at checkout.
	checkout_lockfile: Option<tg::Lockfile>,
}

#[tokio::test]
async fn directory() -> tg::Result<()> {
	test(
		temp::directory! {
			"hello.txt" => "hello, world!",
		},
		"".as_ref(),
		|a, b| async move {
			assert_eq!(a.id().await, b.id().await);
			let output_a = a.output().await;
			let output_b = b.output().await;
			assert_eq!(&output_a, &output_b);
			assert_snapshot!(&output_a, @r#"
            tg.directory({
            	"hello.txt": tg.file({
            		"contents": tg.leaf("hello, world!"),
            	}),
            })
            "#);
			Ok(())
		},
	)
	.await
}

#[tokio::test]
async fn file() -> tg::Result<()> {
	test(
		temp::file!("hello, world!"),
		"".as_ref(),
		|a, b| async move {
			assert_eq!(a.id().await, b.id().await);
			let output_a = a.output().await;
			let output_b = b.output().await;
			assert_eq!(&output_a, &output_b);
			assert_snapshot!(&output_a, @r#"
            tg.file({
            	"contents": tg.leaf("hello, world!"),
            })
            "#);
			Ok(())
		},
	)
	.await
}

#[tokio::test]
async fn symlink() -> tg::Result<()> {
	test(
		temp::directory! {
			"link" => temp::symlink!("hello.txt"),
			"hello.txt" => "hello, world!",
		},
		"link".as_ref(),
		|a, b| async move {
			assert_eq!(a.id().await, b.id().await);
			let output_a = a.output().await;
			let output_b = b.output().await;
			assert_eq!(&output_a, &output_b);
			assert_snapshot!(&output_a, @r#"
            tg.symlink({
            	"target": "hello.txt",
            })
            "#);
			Ok(())
		},
	)
	.await
}

#[tokio::test]
async fn path_dependency() -> tg::Result<()> {
	test(
		temp::directory! {
			"foo" => temp::directory! {
				"tangram.ts" => r#"import * as bar from "../bar""#,
			},
			"bar" => temp::directory! {
				"tangram.ts" => r#""#,
			},
		},
		"foo".as_ref(),
		|a, b| async move {
			assert_eq!(a.id().await, b.id().await);
			let output_a = a.output().await;
			let output_b = b.output().await;
			assert_eq!(&output_a, &output_b);
			assert_snapshot!(&output_a, @r#"
   tg.directory({
   	"tangram.ts": tg.file({
   		"contents": tg.leaf("import * as bar from \"../bar\""),
   		"dependencies": {
   			"../bar": {
   				"item": tg.directory({
   					"tangram.ts": tg.file({
   						"contents": tg.leaf(""),
   					}),
   				}),
   				"path": "../bar",
   				"subpath": "tangram.ts",
   			},
   		},
   	}),
   })
   "#);
			assert_json_snapshot!(a.checkin_lockfile.as_ref().unwrap(), @r#"
   {
     "nodes": [
       {
         "kind": "directory",
         "entries": {
           "tangram.ts": 1
         },
         "id": "dir_01n1phz6j39tdffmkqy9eggavj3m4ds272z3t9fx63s74y73h789g0"
       },
       {
         "kind": "file",
         "dependencies": {
           "../bar": {
             "item": 2,
             "path": "../bar",
             "subpath": "tangram.ts"
           }
         },
         "id": "fil_01tnqb2gzw6j7sdeqeptfyfgcgp812b5wkvk91ke02b29gercr0hag"
       },
       {
         "kind": "directory",
         "entries": {
           "tangram.ts": 3
         },
         "id": "dir_01xgt4hh9nhgacbsa204wnne5346kmfd2eh3ddqqde8gyr6ma2jcj0"
       },
       {
         "kind": "file",
         "id": "fil_010kectq93xrz0cdy3bvkb43sdx2b0exppwwdfcy34ve5aktn8z260"
       }
     ]
   }
   "#);
			assert_json_snapshot!(b.checkout_lockfile.as_ref().unwrap(), @r#"
   {
     "nodes": [
       {
         "kind": "directory",
         "entries": {
           "tangram.ts": 1
         },
         "id": "dir_01n1phz6j39tdffmkqy9eggavj3m4ds272z3t9fx63s74y73h789g0"
       },
       {
         "kind": "file",
         "dependencies": {
           "../bar": {
             "item": 2,
             "path": "../bar",
             "subpath": "tangram.ts"
           }
         },
         "id": "fil_01tnqb2gzw6j7sdeqeptfyfgcgp812b5wkvk91ke02b29gercr0hag"
       },
       {
         "kind": "directory",
         "entries": {
           "tangram.ts": 3
         },
         "id": "dir_01xgt4hh9nhgacbsa204wnne5346kmfd2eh3ddqqde8gyr6ma2jcj0"
       },
       {
         "kind": "file",
         "id": "fil_010kectq93xrz0cdy3bvkb43sdx2b0exppwwdfcy34ve5aktn8z260"
       }
     ]
   }
   "#);
			Ok(())
		},
	)
	.await
}

#[tokio::test]
async fn cyclic_path_dependency() -> tg::Result<()> {
	test(
		temp::directory! {
			"foo" => temp::directory! {
				"tangram.ts" => r#"import * as bar from "../bar""#,
			},
			"bar" => temp::directory! {
				"tangram.ts" => r#"import * as foo from "../foo""#,
			},
		},
		"foo".as_ref(),
		|a, b| async move {
			assert_eq!(a.id().await, b.id().await);
			let output_a = a.output().await;
			let output_b = b.output().await;
			assert_eq!(&output_a, &output_b);
			assert_snapshot!(&output_a, @r#"
   tg.directory({
   	"graph": tg.graph({
   		"nodes": [
   			{
   				"kind": "directory",
   				"entries": {
   					"tangram.ts": 1,
   				},
   			},
   			{
   				"kind": "file",
   				"contents": tg.leaf("import * as bar from \"../bar\""),
   				"dependencies": {
   					"../bar": {
   						"item": 2,
   						"path": "../bar",
   						"subpath": "tangram.ts",
   					},
   				},
   			},
   			{
   				"kind": "directory",
   				"entries": {
   					"tangram.ts": 3,
   				},
   			},
   			{
   				"kind": "file",
   				"contents": tg.leaf("import * as foo from \"../foo\""),
   				"dependencies": {
   					"../foo": {
   						"item": 0,
   						"path": "",
   						"subpath": "tangram.ts",
   					},
   				},
   			},
   		],
   	}),
   	"node": 0,
   })
   "#);
			assert_json_snapshot!(a.checkin_lockfile.as_ref().unwrap(), @r#"
      {
        "nodes": [
          {
            "kind": "directory",
            "entries": {
              "tangram.ts": 1
            },
            "id": "dir_014q009v7jzv5686j2f0y4j17m53rsev0gy814qkchmrrnsycf32xg"
          },
          {
            "kind": "file",
            "contents": "lef_01pqttaksgrf3n76tqrrhb6c96tyafzhrex2jgy54ht8419s6wpg2g",
            "dependencies": {
              "../bar": {
                "item": 2,
                "path": "../bar",
                "subpath": "tangram.ts"
              }
            },
            "id": "fil_019ah53qck1p9xxe0jxvb0vxwdnt4680bfj0xtj9etawagthsrh1e0"
          },
          {
            "kind": "directory",
            "entries": {
              "tangram.ts": 3
            },
            "id": "dir_01w261pda787rg6f89jxqzevkbt30bbbe1zwnsnjav6mxdx7gcs27g"
          },
          {
            "kind": "file",
            "contents": "lef_01fnhktwqxcgtzkra7arsx7d50rgmaycmnqxhrt58s0yb9xkg5ydjg",
            "dependencies": {
              "../foo": {
                "item": 0,
                "path": "",
                "subpath": "tangram.ts"
              }
            },
            "id": "fil_0157mddgf6en44fc11ercy1b022135je68p0h3x3hka1syke5abvz0"
          }
        ]
      }
      "#);
			assert_json_snapshot!(b.checkout_lockfile.as_ref().unwrap(), @r#"
   {
     "nodes": [
       {
         "kind": "directory",
         "entries": {
           "tangram.ts": 1
         },
         "id": "dir_014q009v7jzv5686j2f0y4j17m53rsev0gy814qkchmrrnsycf32xg"
       },
       {
         "kind": "file",
         "contents": "lef_01pqttaksgrf3n76tqrrhb6c96tyafzhrex2jgy54ht8419s6wpg2g",
         "dependencies": {
           "../bar": {
             "item": 2,
             "path": "../bar",
             "subpath": "tangram.ts"
           }
         },
         "id": "fil_019ah53qck1p9xxe0jxvb0vxwdnt4680bfj0xtj9etawagthsrh1e0"
       },
       {
         "kind": "directory",
         "entries": {
           "tangram.ts": 3
         },
         "id": "dir_01w261pda787rg6f89jxqzevkbt30bbbe1zwnsnjav6mxdx7gcs27g"
       },
       {
         "kind": "file",
         "contents": "lef_01fnhktwqxcgtzkra7arsx7d50rgmaycmnqxhrt58s0yb9xkg5ydjg",
         "dependencies": {
           "../foo": {
             "item": 0,
             "path": "",
             "subpath": "tangram.ts"
           }
         },
         "id": "fil_0157mddgf6en44fc11ercy1b022135je68p0h3x3hka1syke5abvz0"
       }
     ]
   }
   "#);

			Ok(())
		},
	)
	.await
}

#[tokio::test]
async fn symlink_roundtrip() -> tg::Result<()> {
	let temp = Temp::new();
	let config = Config::with_path(temp.path().to_owned());
	let server = Server::start(config).await?;

	let result = AssertUnwindSafe(async {
		let dependency_file = tg::file!("contents");
		let dependency_dir = tg::directory! {
			"dependency" => dependency_file
		};
		let dependency_symlink =
			tg::Symlink::with_artifact_and_subpath(dependency_dir.into(), None);

		let file = tg::File::with_object(tg::file::Object::Normal {
			contents: tg::Blob::with_reader(&server, b"c".as_slice()).await?,
			dependencies: [(
				tg::Reference::with_object(&dependency_symlink.id(&server).await?.into()),
				tg::Referent {
					item: dependency_symlink.clone().into(),
					path: None,
					subpath: None,
					tag: None,
				},
			)]
			.into_iter()
			.collect(),
			executable: false,
		});

		// Check out the artifact.
		let temp = Temp::new();
		tokio::fs::create_dir_all(temp.path())
			.await
			.map_err(|source| tg::error!(!source, "failed to create temp path"))?;
		let arg = tg::artifact::checkout::Arg {
			force: false,
			lockfile: true,
			path: Some(temp.path().join("file")),
		};
		let path = tg::Artifact::from(file.clone())
			.check_out(&server, arg)
			.await?;

		// Check the artifact back in.
		let arg = tg::artifact::checkin::Arg {
			deterministic: true,
			destructive: false,
			ignore: true,
			locked: true,
			lockfile: true,
			path,
		};
		let artifact = tg::Artifact::check_in(&server, arg)
			.await?
			.try_unwrap_file()
			.map_err(|_| tg::error!("expected a file"))?;
		assert_eq!(artifact.id(&server).await?, file.id(&server).await?);
		Ok::<_, tg::Error>(())
	})
	.catch_unwind()
	.await;
	cleanup(temp, server).await;
	result.unwrap()
}

async fn test<F, Fut>(
	checkin: impl Into<temp::Artifact>,
	path: &Path,
	assertions: F,
) -> tg::Result<()>
where
	F: FnOnce(TestArtifact, TestArtifact) -> Fut,
	Fut: Future<Output = tg::Result<()>>,
{
	let checkin = checkin.into();

	// Create the first server.
	let temp1 = Temp::new();
	let config = Config::with_path(temp1.path().to_owned());
	let server1 = Server::start(config.clone()).await?;

	// Create the second server.
	let temp2 = Temp::new();
	let config = Config::with_path(temp2.path().to_owned());
	let server2 = Server::start(config.clone()).await?;

	// Run the test.
	let result = AssertUnwindSafe(async {
		// Create an initial temp.
		let temp = Temp::new();

		// Realize the artifact.
		checkin.to_path(temp.path()).await.unwrap();

		// Check in the artifact to the first server.
		let path = temp.path().join(path);
		let arg = tg::artifact::checkin::Arg {
			path: path.clone(),
			destructive: false,
			deterministic: false,
			ignore: true,
			locked: false,
			lockfile: true,
		};
		let artifact = tg::Artifact::check_in(&server1, arg).await?;
		let checkin_lockfile = tg::Lockfile::try_read(&path.join(tg::package::LOCKFILE_FILE_NAME))
			.await
			.ok()
			.flatten();

		// Check the artifact out from the first server.
		let temp = Temp::new();
		let arg = tg::artifact::checkout::Arg {
			path: Some(temp.path().to_owned()),
			force: false,
			lockfile: true,
		};
		let path = artifact.check_out(&server1, arg).await?;
		let checkout = temp::Artifact::with_path(&path).await?;
		let checkout_lockfile = tg::Lockfile::try_read(&path.join(tg::package::LOCKFILE_FILE_NAME))
			.await
			.ok()
			.flatten();

		// Create the test data for the first server.
		let artifact1 = TestArtifact {
			server: server1.clone(),
			artifact,
			checkin,
			checkin_lockfile,
			checkout,
			checkout_lockfile,
		};

		// Check the artifact into a second server.
		let checkin = temp::Artifact::with_path(&path).await?;
		let arg = tg::artifact::checkin::Arg {
			path: path.clone(),
			destructive: false,
			deterministic: false,
			ignore: true,
			locked: false,
			lockfile: true,
		};
		let artifact = tg::Artifact::check_in(&server2, arg).await?;
		let checkin_lockfile = tg::Lockfile::try_read(&path.join(tg::package::LOCKFILE_FILE_NAME))
			.await
			.ok()
			.flatten();

		// Check it out.
		let temp = Temp::new();
		let arg = tg::artifact::checkout::Arg {
			path: Some(temp.path().to_owned()),
			force: false,
			lockfile: true,
		};
		let path = artifact.check_out(&server2, arg).await?;

		let checkout = temp::Artifact::with_path(&path).await?;
		let checkout_lockfile = tg::Lockfile::try_read(&path.join(tg::package::LOCKFILE_FILE_NAME))
			.await
			.ok()
			.flatten();

		let artifact2 = TestArtifact {
			server: server2.clone(),
			artifact,
			checkin,
			checkin_lockfile,
			checkout,
			checkout_lockfile,
		};

		assertions(artifact1, artifact2).await
	})
	.catch_unwind()
	.await;
	// Cleanup the servers.
	cleanup(temp1, server1).await;
	cleanup(temp2, server2).await;
	result.unwrap()
}

impl TestArtifact {
	async fn output(&self) -> String {
		let object: tg::Object = self.artifact.clone().into();
		object.load_recursive(&self.server).await.unwrap();
		let value = tg::Value::Object(object);
		let options = tg::value::print::Options {
			recursive: true,
			style: tg::value::print::Style::Pretty { indentation: "\t" },
		};
		value.print(options)
	}

	async fn id(&self) -> tg::artifact::Id {
		self.artifact.id(&self.server).await.unwrap()
	}
}
