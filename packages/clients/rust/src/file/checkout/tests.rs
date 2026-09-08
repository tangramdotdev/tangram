use {super::*, std::os::unix::fs::symlink, tangram_util::fs::Temp};

#[test]
fn absence() {
	let temp = Temp::new().unwrap();
	std::fs::write(&temp, "ordinary file").unwrap();
	assert_eq!(read(&temp).unwrap(), Output::default());
	xattr::set(&temp, tg::file::DEPENDENCIES_XATTR_NAME, b"[]").unwrap();
	let output = read(&temp).unwrap();
	assert_eq!(output.dependencies, Some(Vec::new()));
	assert_eq!(output.token, None);
}

#[test]
fn invalid_metadata() {
	for (name, value) in [
		(tg::file::DEPENDENCIES_XATTR_NAME, "[\"./dependency\""),
		(tg::file::TOKEN_XATTR_NAME, "invalid"),
	] {
		let temp = Temp::new().unwrap();
		std::fs::write(&temp, "file").unwrap();
		xattr::set(&temp, name, value.as_bytes()).unwrap();
		assert!(read(&temp).is_err());
	}
}

#[test]
fn invalid_path() {
	let temp = Temp::new().unwrap();
	assert!(read(&temp).is_err());
	std::fs::create_dir(&temp).unwrap();
	assert!(read(&temp).is_err());
}

#[test]
fn references_and_tokens() {
	let temp = Temp::new().unwrap();
	std::fs::write(&temp, "file").unwrap();
	let dependency_token = token("dependency");
	let token = token("file");
	let mut reference = "./dependency?location=remote&path=lib/injection#default"
		.parse::<tg::Reference>()
		.unwrap();
	let mut options = reference.options().clone();
	options.tokens.set_local(dependency_token);
	reference.set_options(options);
	let references = vec![
		reference.clone(),
		reference,
		"./unresolved".parse().unwrap(),
	];
	let xattrs = tg::file::dependencies_xattrs(&references, 64).unwrap();
	assert!(xattrs.len() > 10);
	for xattr in xattrs.into_iter().rev() {
		xattr::set(&temp, &xattr.name, &xattr.value).unwrap();
	}
	xattr::set(
		&temp,
		tg::file::TOKEN_XATTR_NAME,
		token.to_string().as_bytes(),
	)
	.unwrap();
	let link = Temp::new().unwrap();
	symlink(&temp, &link).unwrap();
	let output = read(&link).unwrap();
	assert_eq!(output.dependencies, Some(references));
	assert_eq!(output.token, Some(token));
}

fn token(contents: &str) -> tg::authorization::Token {
	let file = tg::File::with_contents(contents);
	let body = tg::authorization::token::Body {
		expires_at: 0,
		permissions: vec![tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Subtree,
		)],
		resource: file.id().into(),
	};
	let key = tg::authorization::token::PrivateKey::generate(
		"test",
		tg::authorization::token::Algorithm::Ed25519,
	)
	.unwrap();
	let mut token = tg::authorization::Token::sign(body, &key).unwrap();
	token.signature.fill(0);
	token
}
