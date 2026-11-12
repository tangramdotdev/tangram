use {
	super::Ignorer,
	indoc::indoc,
	pretty_assertions::assert_eq,
	tangram_util::{self as util, artifact::Artifact},
};

// The ignorer applies the global pattern and per-directory gitignore files to classify each path as ignored or not.
#[tokio::test]
async fn test() {
	let temp = tangram_util::fs::Temp::new().unwrap();
	std::fs::create_dir(temp.path()).unwrap();
	let root = temp.path().join("root");
	let artifact = Artifact::from(util::directory! {
		".DS_Store" => util::file!(""),
		".gitignore" => util::file!(indoc!("
			foo
			foo.txt
		")),
		"foo" => util::directory! {
			"foo.txt" => util::file!(""),
		},
		"bar" => util::directory! {
			"bar.txt" => util::file!(""),
		},
		"foo.txt" => util::file!(""),
		"bar.txt" => util::file!(""),
		"directory" => util::directory! {},
	});
	artifact.to_path(&root).await.unwrap();
	let file_names = vec![".gitignore".into()];
	let global = indoc!(
		"
			.DS_Store
		"
	);
	let mut ignorer = Ignorer::new(file_names, Some(global)).unwrap();
	let right = vec![
		(".DS_Store", true),
		(".gitignore", false),
		("foo", true),
		("foo/foo.txt", true),
		("bar", false),
		("bar/bar.txt", false),
		("foo.txt", true),
		("bar.txt", false),
	];
	let mut left = Vec::new();
	for (path, _) in &right {
		let matches = ignorer.matches(None, &root.join(path), None).unwrap();
		left.push((*path, matches));
	}
	assert_eq!(left, right);
}

// Matching a directory uses its parents' ignore files even after its children have been matched.
#[tokio::test]
async fn cached_directory() {
	let temp = tangram_util::fs::Temp::new().unwrap();
	std::fs::create_dir(temp.path()).unwrap();
	let root = temp.path().join("root");
	let artifact = Artifact::from(util::directory! {
		".gitignore" => util::file!("/ignored"),
		"ignored" => util::directory! {
			"file.txt" => util::file!(""),
		},
		"visible" => util::directory! {
			".gitignore" => util::file!("/visible"),
			"file.txt" => util::file!(""),
		},
	});
	artifact.to_path(&root).await.unwrap();
	let paths = [
		("ignored/file.txt", false),
		("ignored", true),
		("visible/file.txt", false),
		("visible", false),
	];
	for root_path in [None, Some(root.as_path())] {
		let file_names = vec![".gitignore".into()];
		let mut ignorer = Ignorer::new(file_names, None).unwrap();
		for (path, expected) in paths {
			let actual = ignorer.matches(root_path, &root.join(path), None).unwrap();
			assert_eq!(actual, expected, "{path}");
		}
	}
}
