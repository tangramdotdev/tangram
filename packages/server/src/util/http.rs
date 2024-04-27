use bytes::Bytes;
use http_body_util::BodyExt as _;
use tangram_client as tg;

pub type Incoming = hyper::body::Incoming;

pub type Outgoing = http_body_util::combinators::UnsyncBoxBody<Bytes, tg::Error>;

/// Get a bearer token or cookie with the specified name from an HTTP request.
pub fn get_token<'a>(request: &'a http::Request<Incoming>, name: Option<&str>) -> Option<&'a str> {
	let bearer = request
		.headers()
		.get(http::header::AUTHORIZATION)
		.and_then(|authorization| authorization.to_str().ok())
		.and_then(|authorization| authorization.split_once(' '))
		.filter(|(name, _)| *name == "Bearer")
		.map(|(_, value)| value);
	let cookie = name.and_then(|name| {
		request
			.headers()
			.get(http::header::COOKIE)
			.and_then(|cookies| cookies.to_str().ok())
			.and_then(|cookies| {
				cookies
					.split("; ")
					.filter_map(|cookie| {
						let mut components = cookie.split('=');
						let key = components.next()?;
						let value = components.next()?;
						Some((key, value))
					})
					.find(|(key, _)| *key == name)
					.map(|(_, token)| token)
			})
	});
	bearer.or(cookie)
}

#[must_use]
pub fn empty() -> Outgoing {
	http_body_util::Empty::new()
		.map_err(|_| unreachable!())
		.boxed_unsync()
}

#[must_use]
pub fn full(chunk: impl Into<Bytes>) -> Outgoing {
	http_body_util::Full::new(chunk.into())
		.map_err(|_| unreachable!())
		.boxed_unsync()
}

/// 200
#[must_use]
pub fn ok() -> http::Response<Outgoing> {
	http::Response::builder()
		.status(http::StatusCode::OK)
		.body(empty())
		.unwrap()
}

/// 400
#[must_use]
pub fn bad_request() -> http::Response<Outgoing> {
	http::Response::builder()
		.status(http::StatusCode::BAD_REQUEST)
		.body(full("bad request"))
		.unwrap()
}

/// 401
#[must_use]
pub fn unauthorized() -> http::Response<Outgoing> {
	http::Response::builder()
		.status(http::StatusCode::UNAUTHORIZED)
		.body(full("unauthorized"))
		.unwrap()
}

/// 404
#[must_use]
pub fn not_found() -> http::Response<Outgoing> {
	http::Response::builder()
		.status(http::StatusCode::NOT_FOUND)
		.body(full("not found"))
		.unwrap()
}
