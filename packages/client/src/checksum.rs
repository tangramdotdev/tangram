use crate::{error, Error, Result, WrapErr};
use base64::Engine;

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(into = "String", try_from = "String")]
pub enum Checksum {
	Blake3(Box<[u8]>),
	Sha256(Box<[u8]>),
	Sha512(Box<[u8]>),
	Unsafe,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(into = "String", try_from = "String")]
pub enum Algorithm {
	Blake3,
	Sha256,
	Sha512,
	Unsafe,
}

pub enum Encoding {
	Base64,
	Hex,
}

impl Checksum {
	#[must_use]
	pub fn algorithm(&self) -> Algorithm {
		match self {
			Checksum::Unsafe => Algorithm::Unsafe,
			Checksum::Sha256(_) => Algorithm::Sha256,
			Checksum::Sha512(_) => Algorithm::Sha512,
			Checksum::Blake3(_) => Algorithm::Blake3,
		}
	}
}

impl std::fmt::Display for Checksum {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Checksum::Blake3(bytes) | Checksum::Sha256(bytes) | Checksum::Sha512(bytes) => {
				write!(f, "{}:{}", self.algorithm(), hex::encode(bytes))?;
			},
			Checksum::Unsafe => write!(f, "unsafe")?,
		}
		Ok(())
	}
}

impl std::str::FromStr for Checksum {
	type Err = Error;

	fn from_str(value: &str) -> Result<Self, Self::Err> {
		// Split on a ":" or "-".
		let mut components = if value.contains(':') {
			value.split(':')
		} else {
			value.split('-')
		};

		// Parse the algorithm.
		let algorithm = components
			.next()
			.unwrap()
			.parse()
			.wrap_err("Invalid algorithm.")?;

		Ok(match (algorithm, components.next()) {
			(Algorithm::Unsafe, None) => Checksum::Unsafe,
			(Algorithm::Blake3, Some(bytes)) if bytes.len() == 44 => {
				Checksum::Blake3(base64(bytes)?)
			},
			(Algorithm::Blake3, Some(bytes)) if bytes.len() == 64 => Checksum::Blake3(hex(bytes)?),
			(Algorithm::Sha256, Some(bytes)) if bytes.len() == 44 => {
				Checksum::Sha256(base64(bytes)?)
			},
			(Algorithm::Sha256, Some(bytes)) if bytes.len() == 64 => Checksum::Sha256(hex(bytes)?),
			(Algorithm::Sha512, Some(bytes)) if bytes.len() == 88 => {
				Checksum::Sha512(base64(bytes)?)
			},
			(Algorithm::Sha512, Some(bytes)) if bytes.len() == 128 => Checksum::Sha512(hex(bytes)?),
			_ => return Err(error!("Invalid checksum string length.")),
		})
	}
}

fn base64(bytes: impl AsRef<[u8]>) -> Result<Box<[u8]>> {
	Ok(base64::engine::general_purpose::STANDARD
		.decode(bytes)
		.ok()
		.wrap_err("Invalid base64 string.")?
		.into_boxed_slice())
}

fn hex(bytes: impl AsRef<[u8]>) -> Result<Box<[u8]>> {
	Ok(hex::decode(bytes.as_ref())
		.ok()
		.wrap_err("Invalid hex string.")?
		.into_boxed_slice())
}

impl From<Checksum> for String {
	fn from(value: Checksum) -> Self {
		value.to_string()
	}
}

impl TryFrom<String> for Checksum {
	type Error = Error;

	fn try_from(value: String) -> Result<Self, Self::Error> {
		value.parse()
	}
}

impl std::fmt::Display for Algorithm {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let system = match self {
			Algorithm::Unsafe => "unsafe",
			Algorithm::Sha256 => "sha256",
			Algorithm::Sha512 => "sha512",
			Algorithm::Blake3 => "blake3",
		};
		write!(f, "{system}")?;
		Ok(())
	}
}

impl std::str::FromStr for Algorithm {
	type Err = Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let system = match s {
			"unsafe" => Algorithm::Unsafe,
			"sha256" => Algorithm::Sha256,
			"sha512" => Algorithm::Sha512,
			"blake3" => Algorithm::Blake3,
			_ => return Err(error!(r#"Invalid algorithm "{s}"."#)),
		};
		Ok(system)
	}
}

impl From<Algorithm> for String {
	fn from(value: Algorithm) -> Self {
		value.to_string()
	}
}

impl TryFrom<String> for Algorithm {
	type Error = Error;

	fn try_from(value: String) -> std::result::Result<Self, Self::Error> {
		value.parse()
	}
}

#[derive(Debug)]
pub enum Writer {
	Unsafe,
	Blake3(Box<blake3::Hasher>),
	Sha256(sha2::Sha256),
	Sha512(sha2::Sha512),
}

impl Writer {
	#[must_use]
	pub fn new(algorithm: Algorithm) -> Writer {
		match algorithm {
			Algorithm::Unsafe => Writer::Unsafe,
			Algorithm::Blake3 => Writer::Blake3(Box::new(blake3::Hasher::new())),
			Algorithm::Sha256 => Writer::Sha256(sha2::Sha256::default()),
			Algorithm::Sha512 => Writer::Sha512(sha2::Sha512::default()),
		}
	}

	pub fn update(&mut self, data: impl AsRef<[u8]>) {
		match self {
			Writer::Unsafe => (),
			Writer::Blake3(hasher) => {
				hasher.update(data.as_ref());
			},
			Writer::Sha256(sha256) => {
				sha2::Digest::update(sha256, data);
			},
			Writer::Sha512(sha512) => {
				sha2::Digest::update(sha512, data);
			},
		}
	}

	#[must_use]
	pub fn finalize(self) -> Checksum {
		match self {
			Writer::Unsafe => Checksum::Unsafe,
			Writer::Blake3(hasher) => {
				let value = hasher.finalize();
				Checksum::Blake3(value.as_bytes().as_slice().into())
			},
			Writer::Sha256(sha256) => {
				let value = sha2::Digest::finalize(sha256);
				Checksum::Sha256(value.as_slice().into())
			},
			Writer::Sha512(sha512) => {
				let value = sha2::Digest::finalize(sha512);
				Checksum::Sha512(value.as_slice().into())
			},
		}
	}
}

impl std::io::Write for Writer {
	fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
		self.update(buf);
		Ok(buf.len())
	}

	fn flush(&mut self) -> std::io::Result<()> {
		Ok(())
	}
}

impl tokio::io::AsyncWrite for Writer {
	fn poll_write(
		mut self: std::pin::Pin<&mut Self>,
		_cx: &mut std::task::Context<'_>,
		buf: &[u8],
	) -> std::task::Poll<Result<usize, std::io::Error>> {
		self.update(buf);
		std::task::Poll::Ready(Ok(buf.len()))
	}

	fn poll_flush(
		self: std::pin::Pin<&mut Self>,
		_cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<Result<(), std::io::Error>> {
		std::task::Poll::Ready(Ok(()))
	}

	fn poll_shutdown(
		self: std::pin::Pin<&mut Self>,
		_cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<Result<(), std::io::Error>> {
		std::task::Poll::Ready(Ok(()))
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn blake3() {
		let data = "Hello, world!";
		let expected_checksum = Checksum::Blake3(
			[
				237, 229, 192, 177, 15, 46, 196, 151, 156, 105, 181, 47, 97, 228, 47, 245, 180, 19,
				81, 156, 224, 155, 224, 241, 77, 9, 141, 207, 229, 246, 249, 141,
			]
			.into(),
		);
		let expected_string =
			"blake3:ede5c0b10f2ec4979c69b52f61e42ff5b413519ce09be0f14d098dcfe5f6f98d";
		let mut writer = Writer::new(Algorithm::Blake3);
		writer.update(data);
		let checksum = writer.finalize();
		assert_eq!(checksum, expected_checksum);
		assert_eq!(&checksum.to_string(), expected_string);
		assert_eq!(
			checksum,
			expected_string
				.parse()
				.expect("Failed to parse blake3 string.")
		);
	}

	#[test]
	fn blake3_sri() {
		let expected_checksum = Checksum::Blake3(
			[
				237, 229, 192, 177, 15, 46, 196, 151, 156, 105, 181, 47, 97, 228, 47, 245, 180, 19,
				81, 156, 224, 155, 224, 241, 77, 9, 141, 207, 229, 246, 249, 141,
			]
			.into(),
		);
		let sri = "blake3-7eXAsQ8uxJecabUvYeQv9bQTUZzgm+DxTQmNz+X2+Y0=";
		let checksum: Checksum = sri.parse().expect("Failed to parse blake3 SRI.");
		assert_eq!(checksum, expected_checksum);
	}

	#[test]
	fn sha256() {
		let data = "Hello, world!";
		let expected_checksum = Checksum::Sha256(
			[
				49, 95, 91, 219, 118, 208, 120, 196, 59, 138, 192, 6, 78, 74, 1, 100, 97, 43, 31,
				206, 119, 200, 105, 52, 91, 252, 148, 199, 88, 148, 237, 211,
			]
			.into(),
		);
		let expected_string =
			"sha256:315f5bdb76d078c43b8ac0064e4a0164612b1fce77c869345bfc94c75894edd3";
		let mut writer = Writer::new(Algorithm::Sha256);
		writer.update(data);
		let checksum = writer.finalize();
		assert_eq!(checksum, expected_checksum);
		assert_eq!(&checksum.to_string(), expected_string);
		assert_eq!(
			checksum,
			expected_string
				.parse()
				.expect("Failed to parse sha256 string.")
		);
	}

	#[test]
	fn sha256_sri() {
		let expected_checksum = Checksum::Sha256(
			[
				49, 95, 91, 219, 118, 208, 120, 196, 59, 138, 192, 6, 78, 74, 1, 100, 97, 43, 31,
				206, 119, 200, 105, 52, 91, 252, 148, 199, 88, 148, 237, 211,
			]
			.into(),
		);
		let sri = "sha256-MV9b23bQeMQ7isAGTkoBZGErH853yGk0W/yUx1iU7dM=";
		let checksum: Checksum = sri.parse().expect("Failed to parse sha256 SRI.");
		assert_eq!(checksum, expected_checksum);
	}

	#[test]
	fn sha512() {
		let data = "Hello, world!";
		let expected_checksum = Checksum::Sha512(
			[
				193, 82, 124, 216, 147, 193, 36, 119, 61, 129, 25, 17, 151, 12, 143, 230, 232, 87,
				214, 223, 93, 201, 34, 107, 216, 161, 96, 97, 76, 12, 217, 99, 164, 221, 234, 43,
				148, 187, 125, 54, 2, 30, 249, 216, 101, 213, 206, 162, 148, 168, 45, 212, 154, 11,
				178, 105, 245, 31, 110, 122, 87, 247, 148, 33,
			]
			.into(),
		);
		let expected_string = "sha512:c1527cd893c124773d811911970c8fe6e857d6df5dc9226bd8a160614c0cd963a4ddea2b94bb7d36021ef9d865d5cea294a82dd49a0bb269f51f6e7a57f79421";
		let mut writer = Writer::new(Algorithm::Sha512);
		writer.update(data);
		let checksum = writer.finalize();
		assert_eq!(checksum, expected_checksum);
		assert_eq!(&checksum.to_string(), expected_string);
		assert_eq!(
			checksum,
			expected_string
				.parse()
				.expect("Failed to parse sha512 string.")
		);
	}

	#[test]
	fn sha512_sri() {
		let expected_checksum = Checksum::Sha512(
			[
				193, 82, 124, 216, 147, 193, 36, 119, 61, 129, 25, 17, 151, 12, 143, 230, 232, 87,
				214, 223, 93, 201, 34, 107, 216, 161, 96, 97, 76, 12, 217, 99, 164, 221, 234, 43,
				148, 187, 125, 54, 2, 30, 249, 216, 101, 213, 206, 162, 148, 168, 45, 212, 154, 11,
				178, 105, 245, 31, 110, 122, 87, 247, 148, 33,
			]
			.into(),
		);
		let sri = "sha512-wVJ82JPBJHc9gRkRlwyP5uhX1t9dySJr2KFgYUwM2WOk3eorlLt9NgIe+dhl1c6ilKgt1JoLsmn1H256V/eUIQ==";
		let checksum: Checksum = sri.parse().expect("Failed to parse sha512 SRI.");
		assert_eq!(checksum, expected_checksum);
	}
}
