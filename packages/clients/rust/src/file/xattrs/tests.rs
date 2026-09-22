use {
	super::*,
	std::{ffi::OsString, os::unix::fs::symlink, path::PathBuf},
	tangram_util::fs::Temp,
};

#[test]
fn absence() {
	let temp = Temp::new().unwrap();
	std::fs::write(&temp, "ordinary file").unwrap();
	assert_eq!(read(&temp).unwrap(), Output::default());
	xattr::set(&temp, tg::file::xattrs::DEPENDENCIES_NAME, b"[]").unwrap();
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
		xattr::get(&temp, tg::file::xattrs::DEPENDENCIES_NAME)
			.unwrap()
			.as_deref(),
		Some(b"[]".as_slice())
	);
}

#[test]
fn invalid_metadata() {
	for (name, value) in [
		(
			tg::file::xattrs::DEPENDENCIES_NAME,
			b"[\"./dependency\"".as_slice(),
		),
		(tg::file::xattrs::TOKEN_NAME, b"invalid".as_slice()),
		(tg::file::xattrs::TOKEN_NAME, b"\xff".as_slice()),
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
	// Create the references with dependency tokens and query options.
	let temp = Temp::new().unwrap();
	std::fs::write(&temp, "file").unwrap();
	let dependency_token = token("dependency");
	let token = token("file");
	let mut reference = "./dependency?location=remote&path=lib/injection#default"
		.parse::<tg::Reference>()
		.unwrap();
	let mut options = reference.options().clone();
	options.tokens.insert_local_authorization(dependency_token);
	reference.set_options(options);
	let references = vec![
		reference.clone(),
		reference,
		"./unresolved".parse().unwrap(),
	];

	// Write the shards in reverse order to verify that their numeric indices determine the order.
	let xattrs = encode_dependencies(&references, 64).unwrap();
	assert!(xattrs.len() > 10);
	for xattr in xattrs.into_iter().rev() {
		xattr::set(&temp, &xattr.name, &xattr.value).unwrap();
	}
	xattr::set(
		&temp,
		tg::file::xattrs::TOKEN_NAME,
		token.to_string().as_bytes(),
	)
	.unwrap();

	// Read the metadata through a symlink.
	let link = Temp::new().unwrap();
	symlink(&temp, &link).unwrap();
	let output = read(&link).unwrap();
	assert_eq!(output.dependencies, Some(references));
	assert_eq!(output.token, Some(token));
}

#[test]
fn write_and_replace() {
	// Write the sharded dependencies and required attributes through a symlink.
	let temp = Temp::new().unwrap();
	std::fs::write(&temp, "file").unwrap();
	xattr::set(&temp, "user.example", b"preserved").unwrap();
	let link = Temp::new().unwrap();
	symlink(&temp, &link).unwrap();
	let references = vec!["./first".parse().unwrap(), "./second".parse().unwrap()];
	let token = token("file");
	let required = [
		Required::Lock(Some(b"{\"nodes\":[]}".as_slice())),
		Required::Module(Some(b"ts".as_slice())),
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
	for attribute in required {
		let (name, value) = attribute.parts();
		assert_eq!(xattr::get(&temp, name).unwrap().as_deref(), value);
	}

	// Replace the shards with an unsharded value and remove the module and token attributes.
	let required = [Required::Module(None)];
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
		xattr::get(&temp, tg::file::xattrs::MODULE_NAME)
			.unwrap()
			.is_none()
	);
	assert!(
		xattr::get(&temp, tg::file::xattrs::LOCK_NAME)
			.unwrap()
			.is_some()
	);
	assert!(xattr::list(&temp).unwrap().all(|name| {
		!name
			.to_string_lossy()
			.starts_with("user.tangram.dependencies.")
	}));

	// Remove the dependency metadata while preserving the unrelated attributes.
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
	xattr::set(&temp, tg::file::xattrs::DEPENDENCIES_NAME, b"[]").unwrap();
	let arg = Arg {
		dependencies: None,
		required: &[],
		token: None,
	};
	let options = Options { max_value_size: 0 };
	assert!(write(&temp, arg, options).is_err());
	assert_eq!(
		xattr::get(&temp, tg::file::xattrs::DEPENDENCIES_NAME)
			.unwrap()
			.as_deref(),
		Some(b"[]".as_slice())
	);
}

#[test]
fn json_round_trip() {
	let references = vec![tg::Reference::with_path(PathBuf::from("dependency"))];
	let xattrs = encode_dependencies(&references, 4).unwrap();
	assert!(xattrs.len() > 1);
	assert_eq!(xattrs[0].name, "user.tangram.dependencies.0");
	let value = xattrs
		.into_iter()
		.flat_map(|xattr| xattr.value)
		.collect::<Vec<_>>();
	assert_eq!(
		dependencies::deserialize_dependencies_xattr(&value).unwrap(),
		references
	);
}

#[test]
fn unsharded_round_trip() {
	let references = vec![tg::Reference::with_path(PathBuf::from("dependency"))];
	let xattrs = encode_dependencies(&references, usize::MAX).unwrap();
	assert_eq!(xattrs.len(), 1);
	assert_eq!(xattrs[0].name, "user.tangram.dependencies");
	assert_eq!(
		dependencies::deserialize_dependencies_xattr(&xattrs[0].value).unwrap(),
		references
	);
}

#[test]
fn invalid_shards() {
	for suffixes in [
		vec!["", ".0"],
		vec![".1"],
		vec![".0", ".2"],
		vec![".00"],
		vec![".+0"],
		vec![".invalid"],
		vec!["."],
		vec![".18446744073709551616"],
	] {
		let names = suffixes.into_iter().map(|suffix| {
			OsString::from(format!("{}{suffix}", tg::file::xattrs::DEPENDENCIES_NAME))
		});
		assert!(
			dependencies::try_read_dependencies_xattrs(names, |_| Ok(Some(b"[]".to_vec())))
				.is_err()
		);
	}
}

#[test]
fn unreadable_shards() {
	let names = [OsString::from(tg::file::xattrs::DEPENDENCIES_NAME)];
	assert!(dependencies::try_read_dependencies_xattrs(names.clone(), |_| Ok(None)).is_err());
	let result = dependencies::try_read_dependencies_xattrs(names, |_| {
		Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied))
	});
	assert!(result.is_err());
}

#[test]
fn named_metadata() {
	let temp = Temp::new().unwrap();
	std::fs::write(&temp, "file").unwrap();
	let references = vec!["./first".parse().unwrap(), "./second".parse().unwrap()];
	let token = token("file");
	write_lock(&temp, b"lock").unwrap();
	write_module(&temp, tg::module::Kind::Ts).unwrap();
	write_token(&temp, &token).unwrap();
	let options = Options { max_value_size: 4 };
	write_dependencies(&temp, &references, options).unwrap();
	assert_eq!(read_dependencies(&temp).unwrap(), Some(references));
	assert_eq!(read_token(&temp).unwrap(), Some(token));
	assert_eq!(read_module(&temp).unwrap(), Some(tg::module::Kind::Ts));
	assert_eq!(read_lock(&temp).unwrap(), Some(b"lock".to_vec()));
	write_dependencies(&temp, &[], Options::default()).unwrap();
	assert_eq!(read_dependencies(&temp).unwrap(), Some(Vec::new()));
	assert!(xattr::list(&temp).unwrap().all(|name| {
		!name
			.to_string_lossy()
			.starts_with(&format!("{DEPENDENCIES_NAME}."))
	}));
	remove_lock(&temp).unwrap();
	assert_eq!(read_lock(&temp).unwrap(), None);
}

#[test]
fn named_process_metadata() {
	let temp = Temp::new().unwrap();
	std::fs::write(&temp, "file").unwrap();
	let value = vec![42; 131_072];
	write_output(&temp, &value).unwrap();
	write_error(&temp, &value).unwrap();
	write_checksum(&temp, b"checksum").unwrap();
	assert_eq!(read_output(&temp).unwrap().as_ref(), Some(&value));
	assert_eq!(read_error(&temp).unwrap().as_ref(), Some(&value));
	write_output(&temp, b"output").unwrap();
	assert_eq!(read_output(&temp).unwrap(), Some(b"output".to_vec()));
	assert_eq!(read_error(&temp).unwrap(), Some(value));
	assert_eq!(read_checksum(&temp).unwrap(), Some(b"checksum".to_vec()));
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
	// Invalidate the signature to verify that reading the metadata does not verify the token.
	token.signature.fill(0);

	token
}
