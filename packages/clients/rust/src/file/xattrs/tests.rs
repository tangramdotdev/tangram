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
fn explicit_empty_dependencies() {
	let temp = Temp::new().unwrap();
	std::fs::write(&temp, "file").unwrap();
	let arg = Arg {
		dependencies: Some(&[]),
		required: &[],
		token: None,
	};
	write(&temp, arg, Options::default()).unwrap();
	assert_eq!(read(&temp).unwrap().dependencies, Some(Vec::new()));
	assert_eq!(
		xattr::get(&temp, tg::file::DEPENDENCIES_XATTR_NAME)
			.unwrap()
			.as_deref(),
		Some(b"[]".as_slice())
	);
}

#[test]
fn invalid_metadata() {
	for (name, value) in [
		(
			tg::file::DEPENDENCIES_XATTR_NAME,
			b"[\"./dependency\"".as_slice(),
		),
		(tg::file::TOKEN_XATTR_NAME, b"invalid".as_slice()),
		(tg::file::TOKEN_XATTR_NAME, b"\xff".as_slice()),
	] {
		let temp = Temp::new().unwrap();
		std::fs::write(&temp, "file").unwrap();
		xattr::set(&temp, name, value).unwrap();
		assert!(read(&temp).is_err());
	}
}

#[test]
fn invalid_path() {
	let temp = Temp::new().unwrap();
	assert!(read(&temp).is_err());
	let link = Temp::new().unwrap();
	symlink(&temp, &link).unwrap();
	assert!(read(&link).is_err());
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
	options.tokens.insert_local(dependency_token);
	reference.set_options(options);
	let references = vec![
		reference.clone(),
		reference,
		"./unresolved".parse().unwrap(),
	];
	let xattrs = dependencies_xattrs(&references, 64).unwrap();
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

#[test]
fn write_and_replace() {
	let temp = Temp::new().unwrap();
	std::fs::write(&temp, "file").unwrap();
	xattr::set(&temp, "user.example", b"preserved").unwrap();
	let link = Temp::new().unwrap();
	symlink(&temp, &link).unwrap();
	let references = vec!["./first".parse().unwrap(), "./second".parse().unwrap()];
	let token = token("file");
	let required = [
		(
			tg::file::LOCK_XATTR_NAME,
			Some(b"{\"nodes\":[]}".as_slice()),
		),
		(tg::file::MODULE_XATTR_NAME, Some(b"ts".as_slice())),
	];
	let arg = Arg {
		dependencies: Some(&references),
		required: &required,
		token: Some(&token),
	};
	let options = Options { max_value_size: 8 };
	write(&link, arg, options).unwrap();
	let output = read(&temp).unwrap();
	assert_eq!(output.dependencies.as_ref(), Some(&references));
	assert_eq!(output.token.as_ref(), Some(&token));
	assert!(
		xattr::get(&temp, "user.tangram.dependencies.0")
			.unwrap()
			.is_some()
	);
	for (name, value) in required {
		assert_eq!(xattr::get(&temp, name).unwrap().as_deref(), value);
	}

	let required = [(tg::file::MODULE_XATTR_NAME, None)];
	let arg = Arg {
		dependencies: Some(&references[..1]),
		required: &required,
		token: None,
	};
	write(&temp, arg, Options::default()).unwrap();
	let output = read(&temp).unwrap();
	assert_eq!(output.dependencies.as_deref(), Some(&references[..1]));
	assert_eq!(output.token, None);
	assert!(
		xattr::get(&temp, tg::file::MODULE_XATTR_NAME)
			.unwrap()
			.is_none()
	);
	assert!(
		xattr::get(&temp, tg::file::LOCK_XATTR_NAME)
			.unwrap()
			.is_some()
	);
	assert!(xattr::list(&temp).unwrap().all(|name| {
		!name
			.to_string_lossy()
			.starts_with("user.tangram.dependencies.")
	}));

	let arg = Arg {
		dependencies: None,
		required: &[],
		token: None,
	};
	write(&temp, arg, Options::default()).unwrap();
	assert_eq!(read(&temp).unwrap(), Output::default());
	assert_eq!(
		xattr::get(&temp, "user.example").unwrap().as_deref(),
		Some(b"preserved".as_slice())
	);
}

#[test]
fn zero_shard_size() {
	let temp = Temp::new().unwrap();
	std::fs::write(&temp, "file").unwrap();
	xattr::set(&temp, tg::file::DEPENDENCIES_XATTR_NAME, b"[]").unwrap();
	let arg = Arg {
		dependencies: None,
		required: &[],
		token: None,
	};
	let options = Options { max_value_size: 0 };
	assert!(write(&temp, arg, options).is_err());
	assert_eq!(
		xattr::get(&temp, tg::file::DEPENDENCIES_XATTR_NAME)
			.unwrap()
			.as_deref(),
		Some(b"[]".as_slice())
	);
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
