use {
	super::Ignorer,
	indoc::indoc,
	pretty_assertions::assert_eq,
	tangram_util::{self as util, artifact::Artifact},
};

#[tokio::test]
async fn test() {
	let temp = tempfile::TempDir::new().unwrap();
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
	let mut matcher = Ignorer::new(file_names, Some(global)).unwrap();
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
		let matches = matcher.matches(None, &root.join(path), None).unwrap();
		left.push((*path, matches));
	}
	assert_eq!(left, right);
}
