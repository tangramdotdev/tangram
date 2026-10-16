use {
	crate::Payload,
	bytes::{Buf as _, BufMut as _, Bytes, BytesMut},
	futures::future::BoxFuture,
	std::{sync::Arc, time::Duration},
};

mod client;
mod server;
#[cfg(test)]
mod tests;

pub use self::{
	client::{Clients, Request},
	server::{Incoming, Reply, Server, Status, serve},
};

/// The message types of a service: the requests that clients send, and the responses that the server sends back.
pub trait Protocol: Send + Sync + 'static {
	type Request: Payload;
	type Response: Payload;

	/// Whether a response answers a request. A client ignores a response that it does not accept and keeps waiting for one that it does.
	fn accepts(_request: &Self::Request, _response: &Self::Response) -> bool {
		true
	}
}

/// Receives the events of a service's server and clients, which are awaited before the service continues.
pub trait Observer<P: Protocol>: Send + Sync + 'static {
	fn observe<'a>(&'a self, event: Event<'a, P>) -> BoxFuture<'a, ()>;
}

/// An event of a service's server or clients.
pub enum Event<'a, P: Protocol> {
	/// A client received the server's acknowledgment of a request.
	Acknowledged {
		attempt: &'a str,
		id: &'a str,
		request: &'a P::Request,
	},

	/// A client established an attempt with a server, from a heartbeat response or from the acknowledgment of a request.
	Attempt { attempt: &'a str, client: &'a str },

	/// The server received a client's cancellation of a request.
	Cancelled { attempt: &'a str, id: &'a str },

	/// The server responded to a client's heartbeat.
	HeartbeatResponded {
		attempt: &'a str,
		client: &'a str,
		id: &'a str,
	},

	/// A client sent a heartbeat.
	HeartbeatSent { client: &'a str, id: &'a str },

	/// A client sent a request, under its current attempt or, if it has none, under the attempt that the request establishes.
	RequestSent {
		attempt: Option<&'a str>,
		id: &'a str,
		request: &'a P::Request,
	},

	/// A client received the server's response to a request.
	Responded { attempt: &'a str, id: &'a str },

	/// The server received a client's acknowledgment of a response.
	ResponseAcknowledged { attempt: &'a str, id: &'a str },

	/// A client started. A client is identified by the subject that its responses are published to.
	Started { client: &'a str, subject: &'a str },
}

/// An observer that ignores every event.
pub struct Ignore;

#[derive(Clone, Copy, Debug)]
pub struct Options {
	/// How long an attempt lives without a heartbeat, and how long an acknowledged response is retained.
	pub attempt_ttl: Duration,

	/// How often a client with pending requests sends a heartbeat.
	pub heartbeat_interval: Duration,

	/// How long a client without pending requests waits before it stops.
	pub idle_timeout: Duration,

	/// How long a client waits to re-register its requests after its attempt changes or expires.
	pub recovery_timeout: Duration,

	/// How long a client waits for its first acknowledgment of a request.
	pub request_timeout: Duration,

	/// How long to wait before retransmitting an unacknowledged message.
	pub retry_interval: Duration,
}

#[derive(Clone, Debug)]
pub enum Error {
	Closed,
	Messenger(Arc<crate::Error>),
	Options(&'static str),
	Stopped,
	Timeout,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Message {
	Ack {
		attempt: String,
		id: String,
	},
	Announce,
	Cancel {
		attempt: Option<String>,
		client: String,
		id: String,
	},
	Heartbeat {
		client: String,
		id: String,
	},
	HeartbeatResponse {
		attempt: String,
		id: String,
		ttl: Duration,
	},
	Request {
		attempt: Option<String>,
		client: String,
		id: String,
		request: Bytes,
	},
	RequestAck {
		attempt: String,
		id: String,
		ttl: Duration,
	},
	Response {
		attempt: String,
		id: String,
		response: Bytes,
	},
}

impl<P: Protocol> Observer<P> for Ignore {
	fn observe<'a>(&'a self, _event: Event<'a, P>) -> BoxFuture<'a, ()> {
		Box::pin(async {})
	}
}

impl Options {
	pub fn validate(&self) -> Result<(), Error> {
		if self.attempt_ttl.is_zero()
			|| self.heartbeat_interval.is_zero()
			|| self.retry_interval.is_zero()
		{
			return Err(Error::Options(
				"the attempt time to live, heartbeat interval, and retry interval must be greater than zero",
			));
		}
		Ok(())
	}

	fn tick(&self) -> Duration {
		std::cmp::min(self.heartbeat_interval, self.retry_interval)
	}
}

impl Error {
	fn messenger(error: crate::Error) -> Self {
		Self::Messenger(Arc::new(error))
	}
}

impl Payload for Message {
	fn serialize(&self) -> Result<Bytes, crate::Error> {
		let mut bytes = BytesMut::new();
		match self {
			Self::Ack { attempt, id } => {
				bytes.put_u8(0);
				put_string(&mut bytes, attempt);
				put_string(&mut bytes, id);
			},
			Self::Announce => {
				bytes.put_u8(1);
			},
			Self::Cancel {
				attempt,
				client,
				id,
			} => {
				bytes.put_u8(2);
				put_optional_string(&mut bytes, attempt.as_deref());
				put_string(&mut bytes, client);
				put_string(&mut bytes, id);
			},
			Self::Heartbeat { client, id } => {
				bytes.put_u8(3);
				put_string(&mut bytes, client);
				put_string(&mut bytes, id);
			},
			Self::HeartbeatResponse { attempt, id, ttl } => {
				bytes.put_u8(4);
				put_string(&mut bytes, attempt);
				put_string(&mut bytes, id);
				bytes.put_u64(ttl.as_millis().try_into().unwrap_or(u64::MAX));
			},
			Self::Request {
				attempt,
				client,
				id,
				request,
			} => {
				bytes.put_u8(5);
				put_optional_string(&mut bytes, attempt.as_deref());
				put_string(&mut bytes, client);
				put_string(&mut bytes, id);
				bytes.put_slice(request);
			},
			Self::RequestAck { attempt, id, ttl } => {
				bytes.put_u8(6);
				put_string(&mut bytes, attempt);
				put_string(&mut bytes, id);
				bytes.put_u64(ttl.as_millis().try_into().unwrap_or(u64::MAX));
			},
			Self::Response {
				attempt,
				id,
				response,
			} => {
				bytes.put_u8(7);
				put_string(&mut bytes, attempt);
				put_string(&mut bytes, id);
				bytes.put_slice(response);
			},
		}
		Ok(bytes.freeze())
	}

	fn deserialize(mut bytes: Bytes) -> Result<Self, crate::Error> {
		let invalid = || crate::Error::deserialization("invalid service message");
		if bytes.is_empty() {
			return Err(invalid());
		}
		let kind = bytes.get_u8();
		let message = match kind {
			0 => {
				let attempt = get_string(&mut bytes)?;
				let id = get_string(&mut bytes)?;
				end(&bytes)?;
				Self::Ack { attempt, id }
			},
			1 => {
				end(&bytes)?;
				Self::Announce
			},
			2 => {
				let attempt = get_optional_string(&mut bytes)?;
				let client = get_string(&mut bytes)?;
				let id = get_string(&mut bytes)?;
				end(&bytes)?;
				Self::Cancel {
					attempt,
					client,
					id,
				}
			},
			3 => {
				let client = get_string(&mut bytes)?;
				let id = get_string(&mut bytes)?;
				end(&bytes)?;
				Self::Heartbeat { client, id }
			},
			4 => {
				let attempt = get_string(&mut bytes)?;
				let id = get_string(&mut bytes)?;
				if bytes.remaining() != 8 {
					return Err(invalid());
				}
				let ttl = Duration::from_millis(bytes.get_u64());
				Self::HeartbeatResponse { attempt, id, ttl }
			},
			5 => {
				let attempt = get_optional_string(&mut bytes)?;
				let client = get_string(&mut bytes)?;
				let id = get_string(&mut bytes)?;
				Self::Request {
					attempt,
					client,
					id,
					request: bytes,
				}
			},
			6 => {
				let attempt = get_string(&mut bytes)?;
				let id = get_string(&mut bytes)?;
				if bytes.remaining() != 8 {
					return Err(invalid());
				}
				let ttl = Duration::from_millis(bytes.get_u64());
				Self::RequestAck { attempt, id, ttl }
			},
			7 => {
				let attempt = get_string(&mut bytes)?;
				let id = get_string(&mut bytes)?;
				Self::Response {
					attempt,
					id,
					response: bytes,
				}
			},
			_ => return Err(invalid()),
		};
		Ok(message)
	}
}

impl std::fmt::Display for Error {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Closed => write!(f, "the service closed"),
			Self::Messenger(_) => write!(f, "the service messenger failed"),
			Self::Options(message) => write!(f, "invalid service options: {message}"),
			Self::Stopped => write!(f, "the service stopped"),
			Self::Timeout => write!(f, "the service timed out"),
		}
	}
}

impl std::error::Error for Error {
	fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
		match self {
			Self::Messenger(error) => Some(error.as_ref()),
			Self::Closed | Self::Options(_) | Self::Stopped | Self::Timeout => None,
		}
	}
}

/// Generate a random ID for a client, a request, or an attempt.
fn id() -> String {
	const ENCODING: data_encoding::Encoding = data_encoding_macro::new_encoding! {
		symbols: "0123456789abcdefghjkmnpqrstvwxyz",
	};
	ENCODING.encode(uuid::Uuid::now_v7().as_bytes())
}

fn announce_subject(subject: &str) -> String {
	format!("{subject}.announce")
}

/// The subject that the responses to one client of a pool are published to, which identifies the client.
fn client_subject(subject: &str, pool: &str, client: &str) -> String {
	format!("{subject}.{pool}.{client}")
}

/// The subject that a pool subscribes to once for the responses to all of its clients.
fn pool_subject(subject: &str, pool: &str) -> String {
	format!("{subject}.{pool}.*")
}

fn server_subject(subject: &str) -> String {
	format!("{subject}.server")
}

fn put_string(bytes: &mut BytesMut, value: &str) {
	bytes.put_u32(value.len().try_into().unwrap());
	bytes.put_slice(value.as_bytes());
}

fn put_optional_string(bytes: &mut BytesMut, value: Option<&str>) {
	match value {
		Some(value) => {
			bytes.put_u8(1);
			put_string(bytes, value);
		},
		None => {
			bytes.put_u8(0);
		},
	}
}

fn end(bytes: &Bytes) -> Result<(), crate::Error> {
	if !bytes.is_empty() {
		return Err(crate::Error::deserialization("invalid service message"));
	}
	Ok(())
}

fn get_string(bytes: &mut Bytes) -> Result<String, crate::Error> {
	let invalid = || crate::Error::deserialization("invalid service message");
	if bytes.remaining() < 4 {
		return Err(invalid());
	}
	let length = usize::try_from(bytes.get_u32()).map_err(|_| invalid())?;
	if bytes.remaining() < length {
		return Err(invalid());
	}
	let value = bytes.split_to(length);
	String::from_utf8(value.to_vec()).map_err(|_| invalid())
}

fn get_optional_string(bytes: &mut Bytes) -> Result<Option<String>, crate::Error> {
	let invalid = || crate::Error::deserialization("invalid service message");
	if bytes.remaining() < 1 {
		return Err(invalid());
	}
	match bytes.get_u8() {
		0 => Ok(None),
		1 => get_string(bytes).map(Some),
		_ => Err(invalid()),
	}
}
