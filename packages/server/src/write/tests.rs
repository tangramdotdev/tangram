use {super::*, std::error::Error as _};

#[test]
fn read_error_preserves_the_request_body_code() {
	let error = tg::error!(
		code = tg::error::Code::RequestBody,
		"failed to read the request body"
	);
	let error = std::io::Error::other(error);
	let error = read_error(error.into());
	let source = error.source().unwrap().downcast_ref::<tg::Error>().unwrap();
	assert!(matches!(source.code(), Some(tg::error::Code::RequestBody)));
}

#[test]
fn read_error_preserves_an_unclassified_io_error() {
	let error = std::io::Error::from(std::io::ErrorKind::PermissionDenied);
	let message = error.to_string();
	let error = read_error(error.into());
	let source = error.source().unwrap().downcast_ref::<tg::Error>().unwrap();
	assert!(source.code().is_none());
	assert_eq!(source.message().as_deref(), Some(message.as_str()));
}
