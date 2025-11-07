use {
	super::Ignorer,
	indoc::indoc,
	pretty_assertions::assert_eq,
	tangram_temp::{self as temp, Temp},
};

#[tokio::test]
async fn test() {
	let temp = Temp::new();
	let artifact = temp::Artifact::from(temp::directory! {
		".DS_Store" => temp::file!(""),
		".gitignore" => temp::file!(indoc!("
			foo
			foo.txt
		")),
		"foo" => temp::directory! {
			"foo.txt" => temp::file!(""),
		},
		"bar" => temp::directory! {
			"bar.txt" => temp::file!(""),
		},
		"foo.txt" => temp::file!(""),
		"bar.txt" => temp::file!(""),
		"directory" => temp::directory! {},
	});
	artifact.to_path(temp.path()).await.unwrap();
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
		let matches = matcher
			.matches(None, &temp.path().join(path), None)
			.unwrap();
		left.push((*path, matches));
	}
	assert_eq!(left, right);
}
