use crate::prelude::*;

/// Who may use a server's local socket, lock file, and checkouts.
#[derive(
	Clone,
	Copy,
	Debug,
	Default,
	Eq,
	PartialEq,
	derive_more::Display,
	derive_more::FromStr,
	serde::Deserialize,
	serde::Serialize,
)]
#[display(rename_all = "snake_case")]
#[from_str(rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum Access {
	/// The server's user and the members of the server's group.
	Group,

	/// Only the server's user.
	#[default]
	User,
}

/// The content of the lock file in a server's directory, which the server writes while it holds the lock on the file.
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Lock {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub manager: Option<Manager>,

	pub pid: u32,
}

/// The manager that starts and stops a server.
#[derive(
	Clone,
	Copy,
	Debug,
	Eq,
	PartialEq,
	derive_more::Display,
	derive_more::FromStr,
	serde::Deserialize,
	serde::Serialize,
)]
#[display(rename_all = "snake_case")]
#[from_str(rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum Manager {
	App,
	Cli,
	Openrc,
	Systemd,
}

impl Lock {
	/// Parse the content of a lock file, returning `None` if it is empty or invalid, because no server holds the lock.
	#[must_use]
	pub fn try_parse(bytes: &[u8]) -> Option<Self> {
		serde_json::from_slice(bytes).ok()
	}

	/// Serialize the lock to the content of a lock file.
	pub fn serialize(&self) -> tg::Result<Vec<u8>> {
		serde_json::to_vec(self).map_err(|error| tg::error!(!error, "failed to serialize the lock"))
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	// A lock file with a manager and a pid parses, and one without a manager parses with no manager.
	#[test]
	fn parse() {
		let lock = Lock::try_parse(br#"{"manager":"systemd","pid":42}"#);
		let expected = Lock {
			manager: Some(Manager::Systemd),
			pid: 42,
		};
		assert_eq!(lock, Some(expected));
		let lock = Lock::try_parse(br#"{"manager":null,"pid":42}"#);
		let expected = Lock {
			manager: None,
			pid: 42,
		};
		assert_eq!(lock, Some(expected));
		let lock = Lock::try_parse(br#"{"pid":42}"#);
		let expected = Lock {
			manager: None,
			pid: 42,
		};
		assert_eq!(lock, Some(expected));
	}

	// An empty or invalid lock file parses as no lock.
	#[test]
	fn parse_invalid() {
		assert_eq!(Lock::try_parse(b""), None);
		assert_eq!(Lock::try_parse(b"42"), None);
		assert_eq!(Lock::try_parse(b"not a pid"), None);
		assert_eq!(Lock::try_parse(br#"{"manager":"launchd","pid":42}"#), None);
		assert_eq!(Lock::try_parse(br#"{"manager":"cli","pid":-1}"#), None);
		assert_eq!(Lock::try_parse(br#"{"manager":"cli","pid":4"#), None);
	}

	// A serialized lock parses to the same lock, and a lock without a manager omits the field.
	#[test]
	fn serialize() {
		let lock = Lock {
			manager: Some(Manager::Cli),
			pid: 42,
		};
		let bytes = lock.serialize().unwrap();
		assert_eq!(bytes, br#"{"manager":"cli","pid":42}"#);
		assert_eq!(Lock::try_parse(&bytes), Some(lock));
		let lock = Lock {
			manager: None,
			pid: 42,
		};
		let bytes = lock.serialize().unwrap();
		assert_eq!(bytes, br#"{"pid":42}"#);
	}
}
