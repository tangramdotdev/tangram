use {
	indoc::indoc,
	insta::assert_snapshot,
	tangram_cli_test::{Server, assert_success},
	tangram_temp::{self as temp, Temp},
};

const TG: &str = env!("CARGO_BIN_EXE_tangram");

#[tokio::test]
async fn object() {
	let server = Server::new(TG).await.unwrap();
	let temp = Temp::new();
	let directory = temp::directory! {
		"tangram.ts" => temp::file!("export default () => 42;"),
	};
	directory.to_path(temp.path()).await.unwrap();
	let stderr = test(&server, vec![temp.path().display().to_string()], Vec::new()).await;
	assert_snapshot!(stderr, @r"
	dir_01kv4fvampbgahwqgd7z0ctaq1eff5bv4mavrr982f9b1vmft06bpg
	└╴entries: map
	  └╴tangram.ts: fil_01c3d141vk7v44j4krd8800sc11z2ddfyr4x7xp8z8r778r4rb4qr0
	    └╴contents: blb_01mdez7rn5622ncqxr3the1thtqwp9tv919f5xyaj021mbp0egfa40
	");
}

#[tokio::test]
async fn package() {
	let server = Server::new(TG).await.unwrap();
	let temp = Temp::new();
	let foo = temp::directory! {
		"tangram.ts" => temp::file!("// foo"),
	}
	.into();
	let bar = temp::directory! {
		"tangram.ts" => temp::file!(r#"import * as foo from "foo""#),
	}
	.into();

	let objects = vec![(Some("foo".into()), foo), (Some("bar".into()), bar)];
	let directory = temp::directory! {
		"tangram.ts" => temp::file!(indoc!(r#"
            import * as foo from "foo";
            import * as bar from "bar";
        "#)),
	};
	directory.to_path(temp.path()).await.unwrap();
	let stderr = test(
		&server,
		vec![temp.path().display().to_string(), "--mode=package".into()],
		objects,
	)
	.await;
	assert_snapshot!(stderr, @r"
	package: dir_01f97dr5h20x9by01c4gwk46e0x9hnc40fedsbffykfe3h21bhy5q0
	├╴bar: dir_0130zet0544m0nx01je1zqyy399fyeyn6m27mf3krfnvs2pj7qsac0
	│ └╴foo: dir_01vddbk4t4h179fppw4pe9bz6prhh3vt7s1qeaa0fqgakbpbf4stp0
	└╴foo: dir_01vddbk4t4h179fppw4pe9bz6prhh3vt7s1qeaa0fqgakbpbf4stp0
	");
}

#[tokio::test]
async fn tag() {
	let server = Server::new(TG).await.unwrap();
	let foo = temp::directory! {
		"tangram.ts" => temp::file!("// tree/of/tags/foo"),
	}
	.into();
	let bar = temp::directory! {
		"tangram.ts" => temp::file!(r#"import * as foo from "tree/of/tags/foo""#),
	}
	.into();

	let objects = vec![
		(Some("tree/of/tags/foo".into()), foo),
		(Some("tree/of/tags/bar".into()), bar),
	];

	let stderr = test(&server, vec!["tree".into(), "--mode=tag".into()], objects).await;
	assert_snapshot!(stderr, @r#"
	tree
	└╴tree/of
	  └╴tree/of/tags
	    ├╴tree/of/tags/bar: dir_01apgd3f4my7he6be8a0kckf32bpw01k4ez11pbhfhzzsm6chy9dgg
	    │ └╴entries: map
	    │   └╴tangram.ts: fil_014461y623bj37gwnaf682c81vh8rwps2bjj0sa7222gncqqe6vj10
	    │     ├╴contents: blb_0171ytyz9bccy0k15hrep1as9ccybpy2ny0bjq2cmshydz9265zbz0
	    │     └╴dependencies: map
	    │       └╴tree/of/tags/foo: map
	    │         ├╴item: dir_01x6rye7cfh2jjq6f58ffrs4s6pwb25e4xepm61z9nkg7hq3dky7y0
	    │         └╴tag: "tree/of/tags/foo"
	    └╴tree/of/tags/foo: dir_01x6rye7cfh2jjq6f58ffrs4s6pwb25e4xepm61z9nkg7hq3dky7y0
	      └╴entries: map
	        └╴tangram.ts: fil_01kvs4ge7sm2h03a0dc4fbbz4g4m8rg0b04mvxregc85v90bgs56z0
	          └╴contents: blb_0194cvce5k7jhd4ywjr3b9k3ax0gqv8af172q1mk1y287bwz6gaarg
	"#);
}

async fn test(
	server: &Server,
	args: Vec<String>,
	objects: Vec<(Option<String>, temp::Artifact)>,
) -> String {
	// Tag the objects.
	for (tag, artifact) in objects {
		let temp = Temp::new();
		artifact.to_path(temp.path()).await.unwrap();
		let mut command = server.tg();
		if let Some(tag) = tag {
			// Tag the dependency
			command.arg("tag").arg(tag);
		} else {
			// Check in the artifact
			command.arg("checkin");
		}
		command.arg(temp.path());
		let output = command.output().await.unwrap();
		assert_success!(output);
	}

	// Print the tree.
	let output = server.tg().arg("tree").args(args).output().await.unwrap();
	assert_success!(output);
	String::from_utf8(output.stderr).unwrap()
}
