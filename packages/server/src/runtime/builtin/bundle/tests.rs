use crate::{util::fs::cleanup, Config, Server};
use futures::{Future, FutureExt as _};
use insta::assert_snapshot;
use std::panic::AssertUnwindSafe;
use tangram_client::{self as tg};
use tangram_temp::Temp;

/// Test bundling a file with no dependencies.
#[tokio::test]
async fn file_no_dependencies() -> tg::Result<()> {
	test(tg::file!("hello!"), |_, _object, output| async move {
		assert_snapshot!(output, @r#"
  tg.file({
  	"contents": tg.leaf("hello!"),
  })
  "#);
		Ok::<_, tg::Error>(())
	})
	.await
}

/// Test bundling a directory that contains no files with dependencies
#[tokio::test]
async fn directory_no_dependencies() -> tg::Result<()> {
	test(
		tg::directory! {
			"file" => tg::file!("hello"),
			"link" => tg::symlink!("link")
		},
		|_, _object, output| async move {
			assert_snapshot!(output, @r#"
   tg.directory({
   	"file": tg.file({
   		"contents": tg.leaf("hello"),
   	}),
   	"link": tg.symlink({
   		"target": "link",
   	}),
   })
   "#);
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

/// Test bundling an executable file with a dependency.
#[tokio::test]
async fn executable_file_with_dependency() -> tg::Result<()> {
	let temp = Temp::new();
	let mut options = Config::with_path(temp.path().to_owned());
	options.build = Some(crate::config::Build::default());
	options.build_heartbeat_monitor = Some(crate::config::BuildHeartbeatMonitor::default());
	let server = Server::start(options).await?;
	let result = AssertUnwindSafe(async {
		let dependency = tg::file!("dependency");
		let file = tg::File::with_object(tg::file::Object::Normal {
			contents: tg::Blob::from("f"),
			dependencies: [(
				tg::Reference::with_object(&dependency.id(&server).await?.into()),
				tg::Referent {
					item: dependency.clone().into(),
					path: None,
					subpath: None,
					tag: None,
				},
			)]
			.into_iter()
			.collect(),
			executable: true,
		});
		let bundled = tg::Artifact::from(file).bundle(&server).await?;
		let object = tg::Object::from(bundled.clone());
		object.load_recursive(&server).await?;
		let value = tg::Value::from(bundled.clone());
		let options = tg::value::print::Options {
			recursive: true,
			style: tg::value::print::Style::Pretty { indentation: "\t" },
		};
		let output = value.print(options);
		assert_snapshot!(output, @r#"
  tg.directory({
  	".tangram": tg.directory({
  		"artifacts": tg.directory({
  			"fil_01gkrw51xnwqmtdqg7eww1yzcgvwjber106q9j96z94zdgkr49073g": tg.file({
  				"contents": tg.leaf("dependency"),
  			}),
  		}),
  		"run": tg.file({
  			"contents": tg.leaf("f"),
  			"executable": true,
  		}),
  	}),
  })
  "#);
		Ok::<_, tg::Error>(())
	})
	.catch_unwind()
	.await;
	cleanup(temp, server).await;
	result.unwrap()
}

/// Test bundling a directory that contains files with dependencies.
#[tokio::test]
async fn directory_containing_file_with_file_dependency() -> tg::Result<()> {
	let temp = Temp::new();
	let mut options = Config::with_path(temp.path().to_owned());
	options.build = Some(crate::config::Build::default());
	options.build_heartbeat_monitor = Some(crate::config::BuildHeartbeatMonitor::default());
	let server = Server::start(options).await?;
	let result = AssertUnwindSafe(async {
		let dependency = tg::file!("dependency");
		let file = tg::File::with_object(tg::file::Object::Normal {
			contents: tg::Blob::from("f"),
			dependencies: [(
				tg::Reference::with_object(&dependency.id(&server).await?.into()),
				tg::Referent {
					item: dependency.clone().into(),
					path: None,
					subpath: None,
					tag: None,
				},
			)]
			.into_iter()
			.collect(),
			executable: false,
		});
		let dir = tg::directory! {
			"file" => file
		};
		let bundled = tg::Artifact::from(dir).bundle(&server).await?;
		let object = tg::Object::from(bundled.clone());
		object.load_recursive(&server).await?;
		let value = tg::Value::from(bundled.clone());
		let options = tg::value::print::Options {
			recursive: true,
			style: tg::value::print::Style::Pretty { indentation: "\t" },
		};
		let output = value.print(options);
		assert_snapshot!(output, @r#"
  tg.directory({
  	".tangram": tg.directory({
  		"artifacts": tg.directory({
  			"fil_01gkrw51xnwqmtdqg7eww1yzcgvwjber106q9j96z94zdgkr49073g": tg.file({
  				"contents": tg.leaf("dependency"),
  			}),
  		}),
  	}),
  	"file": tg.file({
  		"contents": tg.leaf("f"),
  	}),
  })
  "#);
		Ok::<_, tg::Error>(())
	})
	.catch_unwind()
	.await;
	cleanup(temp, server).await;
	result.unwrap()
}

/// Test bundling dependencies that contain target symlinks.
#[tokio::test]
async fn directory_containing_file_with_directory_dependency_target_symlink() -> tg::Result<()> {
	let temp = Temp::new();
	let mut options = Config::with_path(temp.path().to_owned());
	options.build = Some(crate::config::Build::default());
	options.build_heartbeat_monitor = Some(crate::config::BuildHeartbeatMonitor::default());
	let server = Server::start(options).await?;
	let result = AssertUnwindSafe(async {
		let dependency = tg::directory! {
			"dep" => tg::file!("dependency"),
			"link" => tg::symlink!("dep")
		};
		let file = tg::File::with_object(tg::file::Object::Normal {
			contents: tg::Blob::from("f"),
			dependencies: [(
				tg::Reference::with_object(&dependency.id(&server).await?.into()),
				tg::Referent {
					item: dependency.clone().into(),
					path: None,
					subpath: None,
					tag: None,
				},
			)]
			.into_iter()
			.collect(),
			executable: false,
		});
		let dir = tg::directory! {
			"file" => file,
			"link" => tg::symlink!("file")
		};
		let bundled = tg::Artifact::from(dir).bundle(&server).await?;
		let object = tg::Object::from(bundled.clone());
		object.load_recursive(&server).await?;
		let value = tg::Value::from(bundled.clone());
		let options = tg::value::print::Options {
			recursive: true,
			style: tg::value::print::Style::Pretty { indentation: "\t" },
		};
		let output = value.print(options);
		assert_snapshot!(output, @r#"
  tg.directory({
  	".tangram": tg.directory({
  		"artifacts": tg.directory({
  			"dir_01neb60wpaemmf2p87vzyvbs1r4c06a664fek83685stzycqp4t9z0": tg.directory({
  				"dep": tg.file({
  					"contents": tg.leaf("dependency"),
  				}),
  				"link": tg.symlink({
  					"target": "dep",
  				}),
  			}),
  		}),
  	}),
  	"file": tg.file({
  		"contents": tg.leaf("f"),
  	}),
  	"link": tg.symlink({
  		"target": "file",
  	}),
  })
  "#);
		Ok::<_, tg::Error>(())
	})
	.catch_unwind()
	.await;
	cleanup(temp, server).await;
	result.unwrap()
}

/// Test bundling dependencies that contain artifact/path symlinks.
#[tokio::test]
async fn directory_containing_file_with_directory_dependency_artifact_path_symlink(
) -> tg::Result<()> {
	let temp = Temp::new();
	let mut options = Config::with_path(temp.path().to_owned());
	options.build = Some(crate::config::Build::default());
	options.build_heartbeat_monitor = Some(crate::config::BuildHeartbeatMonitor::default());
	let server = Server::start(options).await?;
	let result = AssertUnwindSafe(async {
		let inner_dir = tg::directory! {
			"a" => tg::file!("a")
		};
		let dependency = tg::directory! {
			"dep" => tg::file!("dependency"),
			"link" => tg::Symlink::with_artifact_and_subpath(inner_dir.clone().into(), Some("a".into()))
		};
		let file = tg::File::with_object(tg::file::Object::Normal {
			contents: tg::Blob::from("f"),
			dependencies: [(
				tg::Reference::with_object(&dependency.id(&server).await?.into()),
				tg::Referent {
					item: dependency.clone().into(),
					path: None,
					subpath: None,
					tag: None,
				},
			)]
			.into_iter()
			.collect(),
			executable: false,
		});
		let dir = tg::directory! {
			"file" => file,
			"link" => tg::symlink!("file")
		};
		let bundled = tg::Artifact::from(dir).bundle(&server).await?;
		let object = tg::Object::from(bundled.clone());
		object.load_recursive(&server).await?;
		let value = tg::Value::from(bundled.clone());
		let options = tg::value::print::Options {
			recursive: true,
			style: tg::value::print::Style::Pretty { indentation: "\t" },
		};
		let output = value.print(options);
		assert_snapshot!(output, @r#"
  tg.directory({
  	".tangram": tg.directory({
  		"artifacts": tg.directory({
  			"dir_010tcemg45skky72hzj6brbq9pqdm6anjc3y1n1367faavg0wvb5cg": tg.directory({
  				"dep": tg.file({
  					"contents": tg.leaf("dependency"),
  				}),
  				"link": tg.symlink({
  					"target": "../../../.tangram/artifacts/dir_015hd3d0r0njfxz43hby36168cdz80tbc7wgpnp105bmray9wv7wng/a",
  				}),
  			}),
  			"dir_015hd3d0r0njfxz43hby36168cdz80tbc7wgpnp105bmray9wv7wng": tg.directory({
  				"a": tg.file({
  					"contents": tg.leaf("a"),
  				}),
  			}),
  		}),
  	}),
  	"file": tg.file({
  		"contents": tg.leaf("f"),
  	}),
  	"link": tg.symlink({
  		"target": "file",
  	}),
  })
  "#);
		Ok::<_, tg::Error>(())
	})
	.catch_unwind()
	.await;
	cleanup(temp, server).await;
	result.unwrap()
}

async fn test<F, Fut>(artifact: impl Into<tg::Artifact>, assertions: F) -> tg::Result<()>
where
	F: FnOnce(Server, tg::Artifact, String) -> Fut,
	Fut: Future<Output = tg::Result<()>>,
{
	let artifact = artifact.into();
	let temp = Temp::new();
	let mut options = Config::with_path(temp.path().to_owned());
	options.build = Some(crate::config::Build::default());
	options.build_heartbeat_monitor = Some(crate::config::BuildHeartbeatMonitor::default());
	let server = Server::start(options).await?;
	let result = AssertUnwindSafe(async {
		let bundled = artifact.bundle(&server).await?;
		let object = tg::Object::from(bundled.clone());
		object.load_recursive(&server).await?;
		let value = tg::Value::from(bundled.clone());
		let options = tg::value::print::Options {
			recursive: true,
			style: tg::value::print::Style::Pretty { indentation: "\t" },
		};
		let output = value.print(options);
		(assertions)(server.clone(), bundled, output).await?;
		Ok::<_, tg::Error>(())
	})
	.catch_unwind()
	.await;
	cleanup(temp, server).await;
	result.unwrap()
}
