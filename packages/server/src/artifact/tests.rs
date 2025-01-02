use crate::{util::fs::cleanup, Config, Server};
use futures::FutureExt as _;
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
				"tangram.ts" => "",
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

		let object: tg::Object = file.clone().into();
		object.load_recursive(&server).await.unwrap();
		let value = tg::Value::Object(object);
		let options = tg::value::print::Options {
			recursive: true,
			style: tg::value::print::Style::Pretty { indentation: "\t" },
		};
		let output1 = value.print(options);
		assert_snapshot!(output1, @r#"
  tg.file({
  	"contents": tg.leaf("c"),
  	"dependencies": {
  		"sym_01ky91btcmtmxv1973agar5b43tjvrq0qty6wcynkq3n0s4mynh880": {
  			"item": tg.symlink({
  				"artifact": tg.directory({
  					"dependency": tg.file({
  						"contents": tg.leaf("contents"),
  					}),
  				}),
  			}),
  		},
  	},
  })
  "#);

		// Check out the artifact.
		let temp = Temp::new();
		tokio::fs::create_dir_all(temp.path())
			.await
			.map_err(|source| tg::error!(!source, "failed to create temp path"))?;
		let arg = tg::artifact::checkout::Arg {
			dependencies: false,
			force: false,
			lockfile: true,
			path: Some(temp.path().join("file")),
		};
		let path = tg::Artifact::from(file.clone())
			.check_out(&server, arg)
			.await?;

		let temp::Artifact::File(temp_file) = temp::Artifact::with_path(&path).await? else {
			return Err(tg::error!("expected a file"));
		};
		assert_json_snapshot!(temp_file, @r#"
  {
    "contents": "c",
    "executable": false,
    "xattrs": {
      "user.tangram.lock": "{\"nodes\":[{\"kind\":\"file\",\"dependencies\":{\"sym_01ky91btcmtmxv1973agar5b43tjvrq0qty6wcynkq3n0s4mynh880\":{\"item\":\"sym_01ky91btcmtmxv1973agar5b43tjvrq0qty6wcynkq3n0s4mynh880\"}},\"id\":\"fil_016eea5b1xt939meb567ckn2yhwhhvcgc0vg65h0zqssg6dw569pbg\"}]}"
    }
  }
  "#);

		// Check the artifact back in.
		let arg = tg::artifact::checkin::Arg {
			cache: false,
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

		let object: tg::Object = artifact.clone().into();
		object.load_recursive(&server).await.unwrap();
		let value = tg::Value::Object(object);
		let options = tg::value::print::Options {
			recursive: true,
			style: tg::value::print::Style::Pretty { indentation: "\t" },
		};
		let output2 = value.print(options);
		assert_snapshot!(output2, @r#"
  tg.file({
  	"contents": tg.leaf("c"),
  	"dependencies": {
  		"sym_01ky91btcmtmxv1973agar5b43tjvrq0qty6wcynkq3n0s4mynh880": {
  			"item": tg.symlink({
  				"artifact": tg.directory({
  					"dependency": tg.file({
  						"contents": tg.leaf("contents"),
  					}),
  				}),
  			}),
  		},
  	},
  })
  "#);

		assert_eq!(output1, output2);
		assert_eq!(artifact.id(&server).await?, file.id(&server).await?);
		Ok::<_, tg::Error>(())
	})
	.catch_unwind()
	.await;
	cleanup(temp, server).await;
	result.unwrap()
}

#[tokio::test]
async fn file_with_object_dependencies() -> tg::Result<()> {
	// Create the first server.
	let temp1 = Temp::new();
	let config = Config::with_path(temp1.path().to_owned());
	let server1 = Server::start(config.clone()).await?;

	// Create the second server.
	// let temp2 = Temp::new();
	// let config = Config::with_path(temp1.path().to_owned());
	// let server2 = Server::start(config.clone()).await?;

	// Run the test.
	let result = AssertUnwindSafe(async {
		// Create the dependency artifact and obejct.
		let artifact = tg::file!("hello, world!");
		let file = artifact.id(&server1).await?;
		let blob = artifact.contents(&server1).await?.id(&server1).await?;
		eprintln!("{file}, {blob}");

		// Create a file that depends on the artifact and object by ID.
		let temp = Temp::new();
		let package: temp::Artifact = temp::directory! {
			"tangram.ts" => format!("
				import file from \"{file}\";
				import blob from \"{blob}\";
			"),
		}
		.into();
		package
			.to_path(temp.path())
			.await
			.map_err(|source| tg::error!(!source, "failed to create the temp"))?;

		// Check in the file.
		let arg = tg::artifact::checkin::Arg {
			cache: false,
			destructive: false,
			deterministic: false,
			ignore: false,
			locked: false,
			lockfile: true,
			path: temp.path().to_owned(),
		};
		let directory = tg::Artifact::check_in(&server1, arg).await?;

		// Validate the lockfile.
		let lockfile = tg::Lockfile::try_read(&temp.path().join(tg::package::LOCKFILE_FILE_NAME))
			.await?
			.ok_or_else(|| tg::error!("expected a lockfile"))?;
		assert_json_snapshot!(lockfile, @r#"
  {
    "nodes": [
      {
        "kind": "directory",
        "entries": {
          "tangram.ts": 1
        },
        "id": "dir_01mwk7bw8b1fft3k8zywea75cqvwfrpsrcbhejv7yr9w3y6s1tmms0"
      },
      {
        "kind": "file",
        "dependencies": {
          "fil_01my5jh8zn8r1jmpm8j6286w0v65pqe95wmn116pvf6k18rkbbacf0": {
            "item": "fil_01my5jh8zn8r1jmpm8j6286w0v65pqe95wmn116pvf6k18rkbbacf0"
          },
          "lef_01be9a1a2fqh8ab33myhrqqg6nyg1jgaq4snyqme2327pkrgca2qc0": {
            "item": "lef_01be9a1a2fqh8ab33myhrqqg6nyg1jgaq4snyqme2327pkrgca2qc0"
          }
        },
        "id": "fil_01av35e5gkymwwx5dk6wn04tm1ydrq5wrancyagfg0dhfnt3jzbbjg"
      }
    ]
  }
  "#);

  		// Check out the file within the artifact.
		let temp = Temp::new();
  		let file = directory.unwrap_directory_ref().get(&server1, "tangram.ts").await?;
		let arg = tg::artifact::checkout::Arg {
			dependencies: false,
			force: false,
			lockfile: true,
			path: Some(temp.path().to_owned()),
		};
		let path = file.check_out(&server1, arg).await?;

		// Get the result.
		let artifact = temp::Artifact::with_path(&path).await.map_err(|source| tg::error!(!source, "failed to get the artifact"))?;

		assert_json_snapshot!(artifact, @r#"
  {
    "kind": "file",
    "contents": "\n\t\t\t\timport file from \"fil_01my5jh8zn8r1jmpm8j6286w0v65pqe95wmn116pvf6k18rkbbacf0\";\n\t\t\t\timport blob from \"lef_01be9a1a2fqh8ab33myhrqqg6nyg1jgaq4snyqme2327pkrgca2qc0\";\n\t\t\t",
    "executable": false,
    "xattrs": {
      "user.tangram.lock": "{\"nodes\":[{\"kind\":\"file\",\"dependencies\":{\"fil_01my5jh8zn8r1jmpm8j6286w0v65pqe95wmn116pvf6k18rkbbacf0\":{\"item\":\"fil_01my5jh8zn8r1jmpm8j6286w0v65pqe95wmn116pvf6k18rkbbacf0\"},\"lef_01be9a1a2fqh8ab33myhrqqg6nyg1jgaq4snyqme2327pkrgca2qc0\":{\"item\":\"lef_01be9a1a2fqh8ab33myhrqqg6nyg1jgaq4snyqme2327pkrgca2qc0\"}},\"id\":\"fil_01av35e5gkymwwx5dk6wn04tm1ydrq5wrancyagfg0dhfnt3jzbbjg\"}]}"
    }
  }
  "#);

		Ok::<_, tg::Error>(())
	})
	.catch_unwind()
	.await;

	cleanup(temp1, server1).await;
	// cleanup(temp2, server2).await;
	result.unwrap()
}

#[tokio::test]
async fn file_with_transitive_object_dependencies() -> tg::Result<()> {
	// Create the first server.
	let temp1 = Temp::new();
	let config = Config::with_path(temp1.path().to_owned());
	let server = Server::start(config.clone()).await?;

	// Run the test.
	let result = AssertUnwindSafe(async {
		let transitive_dep = tg::file!("transitive");
		
		// Create the dependency artifact and object.
		let file_dep = tg::File::with_object(tg::file::Object::Normal {
			contents: "hello".into(),
			dependencies: [(
				tg::Reference::with_object(&transitive_dep.id(&server).await?.into()),
				tg::Referent {
					item: transitive_dep.clone().into(),
					path: None,
					subpath: None,
					tag: None,
				},
			)]
			.into_iter()
			.collect(),
			executable: false,
		});

		let dep_dir = tg::directory!{ "entry" => file_dep };
		let orig = tg::File::with_object(tg::file::Object::Normal {
			contents: "hello".into(),
			dependencies: [(
				tg::Reference::with_object(&dep_dir.id(&server).await?.into()),
				tg::Referent {
					item: dep_dir.clone().into(),
					path: None,
					subpath: None,
					tag: None,
				},
			)]
			.into_iter()
			.collect(),
			executable: false,
		});
		
		let orig_id = orig.id(&server).await?;

  		// Check out the file within the artifact.
		let temp = Temp::new();
		let arg = tg::artifact::checkout::Arg {
			dependencies: false,
			force: false,
			lockfile: true,
			path: Some(temp.path().to_owned()),
		};
		let path = tg::Artifact::from(orig).check_out(&server, arg).await?;

		// Get the result.
		let artifact = temp::Artifact::with_path(&path).await.map_err(|source| tg::error!(!source, "failed to get the artifact"))?;

		assert_json_snapshot!(artifact, @r#"
  {
    "kind": "file",
    "contents": "hello",
    "executable": false,
    "xattrs": {
      "user.tangram.lock": "{\"nodes\":[{\"kind\":\"file\",\"dependencies\":{\"dir_01s8c95696nppdhy29sdjqxbzdmhyw53g807z2ddm47dt9w5xbts9g\":{\"item\":1}},\"id\":\"fil_015g2z6mnxhe1wtz6f9vgv1cjxpxvvmmph32a7n18153n74hrj33n0\"},{\"kind\":\"directory\",\"entries\":{\"entry\":2},\"id\":\"dir_01s8c95696nppdhy29sdjqxbzdmhyw53g807z2ddm47dt9w5xbts9g\"},{\"kind\":\"file\",\"dependencies\":{\"fil_010ahk1drre093nerbycshqfpb5vzcjptq94jyfw5r5y9j7jhfx1mg\":{\"item\":\"fil_010ahk1drre093nerbycshqfpb5vzcjptq94jyfw5r5y9j7jhfx1mg\"}},\"id\":\"fil_01jwz4z27dcyft5q87egvz5r7mh1k67v7q0sey4ffb63nqbpdhj21g\"}]}"
    }
  }
  "#);

	// Check back in the file.
		let arg = tg::artifact::checkin::Arg {
			cache: false,
			destructive: false,
			deterministic: false,
			ignore: false,
			locked: false,
			lockfile: false,
			path: temp.path().to_owned(),
		};
		let roundtrip = tg::Artifact::check_in(&server, arg).await?.try_unwrap_file().unwrap();
		let roundtrip_id = roundtrip.id(&server).await?;
		assert_eq!(orig_id, roundtrip_id);
	

		Ok::<_, tg::Error>(())
	})
	.catch_unwind()
	.await;

	cleanup(temp1, server).await;
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
			cache: false,
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
			dependencies: true,
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
			cache: false,
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
			dependencies: true,
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
