use crate as tg;
use sha2::Digest;

#[derive(
	Clone, Debug, Eq, PartialEq, serde_with::DeserializeFromStr, serde_with::SerializeDisplay,
)]
pub struct Checksum {
	algorithm: Algorithm,
	body: Body,
}

#[derive(
	Clone, Copy, Debug, Eq, PartialEq, serde_with::DeserializeFromStr, serde_with::SerializeDisplay,
)]
pub enum Algorithm {
	Blake3,
	Sha256,
	Sha512,
}

#[derive(Clone, Debug)]
pub enum Body {
	None,
	Any,
	Bytes(Box<[u8]>),
}

pub enum Encoding {
	Base64,
	Hex,
}

#[derive(Debug)]
pub enum Writer {
	Blake3(Box<blake3::Hasher>),
	Sha256(Box<sha2::Sha256>),
	Sha512(Box<sha2::Sha512>),
}

impl Checksum {
	#[must_use]
	pub fn algorithm(&self) -> Algorithm {
		self.algorithm
	}
}

impl std::fmt::Display for Checksum {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}:{}", self.algorithm, self.body)?;
		Ok(())
	}
}

impl std::str::FromStr for Checksum {
	type Err = tg::Error;

	fn from_str(value: &str) -> tg::Result<Self, Self::Err> {
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
			.map_err(|source| tg::error!(!source, "invalid algorithm"))?;

		// Parse the body.
		let body = match (algorithm, components.next()) {
			(_, Some("none")) => Body::None,
			(_, Some("any")) => Body::Any,
			(Algorithm::Blake3, Some(body)) if body.len() == 44 => Body::Bytes(
				data_encoding::BASE64
					.decode(body.as_bytes())
					.map_err(|source| tg::error!(!source, "invalid body"))?
					.into(),
			),
			(Algorithm::Blake3, Some(body)) if body.len() == 64 => Body::Bytes(
				data_encoding::HEXLOWER
					.decode(body.as_bytes())
					.map_err(|source| tg::error!(!source, "invalid body"))?
					.into(),
			),
			(Algorithm::Sha256, Some(body)) if body.len() == 44 => Body::Bytes(
				data_encoding::BASE64
					.decode(body.as_bytes())
					.map_err(|source| tg::error!(!source, "invalid body"))?
					.into(),
			),
			(Algorithm::Sha256, Some(body)) if body.len() == 64 => Body::Bytes(
				data_encoding::HEXLOWER
					.decode(body.as_bytes())
					.map_err(|source| tg::error!(!source, "invalid body"))?
					.into(),
			),
			(Algorithm::Sha512, Some(body)) if body.len() == 88 => Body::Bytes(
				data_encoding::BASE64
					.decode(body.as_bytes())
					.map_err(|source| tg::error!(!source, "invalid body"))?
					.into(),
			),
			(Algorithm::Sha512, Some(body)) if body.len() == 128 => Body::Bytes(
				data_encoding::HEXLOWER
					.decode(body.as_bytes())
					.map_err(|source| tg::error!(!source, "invalid body"))?
					.into(),
			),
			_ => {
				return Err(tg::error!(%value, "invalid checksum"));
			},
		};

		// Create the checksum.
		let checksum = Self { algorithm, body };

		Ok(checksum)
	}
}

impl std::fmt::Display for Algorithm {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let algorithm = match self {
			Self::Blake3 => "blake3",
			Self::Sha256 => "sha256",
			Self::Sha512 => "sha512",
		};
		write!(f, "{algorithm}")?;
		Ok(())
	}
}

impl std::str::FromStr for Algorithm {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		let algorithm = match s {
			"sha256" => Self::Sha256,
			"sha512" => Self::Sha512,
			"blake3" => Self::Blake3,
			algorithm => return Err(tg::error!(%algorithm, "invalid algorithm")),
		};
		Ok(algorithm)
	}
}

impl std::fmt::Display for Body {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::None => {
				write!(f, "none")?;
			},
			Self::Any => {
				write!(f, "any")?;
			},
			Self::Bytes(bytes) => {
				write!(f, "{}", data_encoding::HEXLOWER.encode(bytes))?;
			},
		}
		Ok(())
	}
}

impl PartialEq for Body {
	fn eq(&self, other: &Self) -> bool {
		match (self, other) {
			(Self::None, _) | (_, Self::None) => false,
			(Self::Any, _) | (_, Self::Any) => true,
			(Self::Bytes(a), Self::Bytes(b)) => a == b,
		}
	}
}

impl Eq for Body {}

impl Writer {
	#[must_use]
	pub fn new(algorithm: Algorithm) -> Self {
		match algorithm {
			Algorithm::Blake3 => Self::Blake3(Box::new(blake3::Hasher::new())),
			Algorithm::Sha256 => Self::Sha256(Box::new(sha2::Sha256::new())),
			Algorithm::Sha512 => Self::Sha512(Box::new(sha2::Sha512::new())),
		}
	}

	pub fn update(&mut self, data: impl AsRef<[u8]>) {
		match self {
			Writer::Blake3(blake3) => {
				blake3.update(data.as_ref());
			},
			Writer::Sha256(sha256) => {
				sha2::Digest::update(sha256.as_mut(), data);
			},
			Writer::Sha512(sha512) => {
				sha2::Digest::update(sha512.as_mut(), data);
			},
		}
	}

	#[must_use]
	pub fn finalize(self) -> Checksum {
		let (algorithm, body) = match self {
			Writer::Blake3(hasher) => {
				let algorithm = Algorithm::Blake3;
				let bytes = hasher.finalize().as_bytes().as_slice().into();
				let body = Body::Bytes(bytes);
				(algorithm, body)
			},
			Writer::Sha256(sha256) => {
				let algorithm = Algorithm::Sha256;
				let bytes = sha2::Digest::finalize(*sha256).as_slice().into();
				let body = Body::Bytes(bytes);
				(algorithm, body)
			},
			Writer::Sha512(sha512) => {
				let algorithm = Algorithm::Sha512;
				let bytes = sha2::Digest::finalize(*sha512).as_slice().into();
				let body = Body::Bytes(bytes);
				(algorithm, body)
			},
		};
		Checksum { algorithm, body }
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
		let expected_checksum = Checksum {
			algorithm: Algorithm::Blake3,
			body: Body::Bytes(
				[
					237, 229, 192, 177, 15, 46, 196, 151, 156, 105, 181, 47, 97, 228, 47, 245, 180,
					19, 81, 156, 224, 155, 224, 241, 77, 9, 141, 207, 229, 246, 249, 141,
				]
				.into(),
			),
		};
		let expected_string =
			"blake3:ede5c0b10f2ec4979c69b52f61e42ff5b413519ce09be0f14d098dcfe5f6f98d";
		let mut writer = Writer::new(Algorithm::Blake3);
		writer.update(data.as_bytes());
		let checksum = writer.finalize();
		assert_eq!(checksum, expected_checksum);
		assert_eq!(&checksum.to_string(), expected_string);
		assert_eq!(checksum, expected_string.parse().unwrap());
	}

	#[test]
	fn blake3_sri() {
		let expected_checksum = Checksum {
			algorithm: Algorithm::Blake3,
			body: Body::Bytes(
				[
					237, 229, 192, 177, 15, 46, 196, 151, 156, 105, 181, 47, 97, 228, 47, 245, 180,
					19, 81, 156, 224, 155, 224, 241, 77, 9, 141, 207, 229, 246, 249, 141,
				]
				.into(),
			),
		};
		let sri = "blake3-7eXAsQ8uxJecabUvYeQv9bQTUZzgm+DxTQmNz+X2+Y0=";
		let checksum: Checksum = sri.parse().expect("failed to parse blake3 SRI");
		assert_eq!(checksum, expected_checksum);
	}

	#[test]
	fn sha256() {
		let data = "Hello, world!";
		let expected_checksum = Checksum {
			algorithm: Algorithm::Sha256,
			body: Body::Bytes(
				[
					49, 95, 91, 219, 118, 208, 120, 196, 59, 138, 192, 6, 78, 74, 1, 100, 97, 43,
					31, 206, 119, 200, 105, 52, 91, 252, 148, 199, 88, 148, 237, 211,
				]
				.into(),
			),
		};
		let expected_string =
			"sha256:315f5bdb76d078c43b8ac0064e4a0164612b1fce77c869345bfc94c75894edd3";
		let mut writer = Writer::new(Algorithm::Sha256);
		writer.update(data.as_bytes());
		let checksum = writer.finalize();
		assert_eq!(checksum, expected_checksum);
		assert_eq!(&checksum.to_string(), expected_string);
		assert_eq!(checksum, expected_string.parse().unwrap());
	}

	#[test]
	fn sha256_sri() {
		let expected_checksum = Checksum {
			algorithm: Algorithm::Sha256,
			body: Body::Bytes(
				[
					49, 95, 91, 219, 118, 208, 120, 196, 59, 138, 192, 6, 78, 74, 1, 100, 97, 43,
					31, 206, 119, 200, 105, 52, 91, 252, 148, 199, 88, 148, 237, 211,
				]
				.into(),
			),
		};
		let sri = "sha256-MV9b23bQeMQ7isAGTkoBZGErH853yGk0W/yUx1iU7dM=";
		let checksum: Checksum = sri.parse().expect("failed to parse sha256 SRI");
		assert_eq!(checksum, expected_checksum);
	}

	#[test]
	fn sha512() {
		let data = "Hello, world!";
		let expected_checksum = Checksum {
			algorithm: Algorithm::Sha512,
			body: Body::Bytes(
				[
					193, 82, 124, 216, 147, 193, 36, 119, 61, 129, 25, 17, 151, 12, 143, 230, 232,
					87, 214, 223, 93, 201, 34, 107, 216, 161, 96, 97, 76, 12, 217, 99, 164, 221,
					234, 43, 148, 187, 125, 54, 2, 30, 249, 216, 101, 213, 206, 162, 148, 168, 45,
					212, 154, 11, 178, 105, 245, 31, 110, 122, 87, 247, 148, 33,
				]
				.into(),
			),
		};
		let expected_string = "sha512:c1527cd893c124773d811911970c8fe6e857d6df5dc9226bd8a160614c0cd963a4ddea2b94bb7d36021ef9d865d5cea294a82dd49a0bb269f51f6e7a57f79421";
		let mut writer = Writer::new(Algorithm::Sha512);
		writer.update(data.as_bytes());
		let checksum = writer.finalize();
		assert_eq!(checksum, expected_checksum);
		assert_eq!(&checksum.to_string(), expected_string);
		assert_eq!(checksum, expected_string.parse().unwrap());
	}

	#[test]
	fn sha512_sri() {
		let expected_checksum = Checksum {
			algorithm: Algorithm::Sha512,
			body: Body::Bytes(
				[
					193, 82, 124, 216, 147, 193, 36, 119, 61, 129, 25, 17, 151, 12, 143, 230, 232,
					87, 214, 223, 93, 201, 34, 107, 216, 161, 96, 97, 76, 12, 217, 99, 164, 221,
					234, 43, 148, 187, 125, 54, 2, 30, 249, 216, 101, 213, 206, 162, 148, 168, 45,
					212, 154, 11, 178, 105, 245, 31, 110, 122, 87, 247, 148, 33,
				]
				.into(),
			),
		};
		let sri = "sha512-wVJ82JPBJHc9gRkRlwyP5uhX1t9dySJr2KFgYUwM2WOk3eorlLt9NgIe+dhl1c6ilKgt1JoLsmn1H256V/eUIQ==";
		let checksum: Checksum = sri.parse().unwrap();
		assert_eq!(checksum, expected_checksum);
	}
}
