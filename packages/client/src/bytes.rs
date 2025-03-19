use bytes::Bytes;
use std::ops::Deref;

#[derive(Clone, Debug, derive_more::From)]
pub enum Cow<'a> {
	Borrowed(&'a [u8]),
	Owned(Bytes),
}

impl Cow<'_> {
	pub fn into_owned(self) -> Bytes {
		match self {
			Self::Borrowed(slice) => Bytes::copy_from_slice(slice),
			Self::Owned(bytes) => bytes,
		}
	}

	pub fn as_slice(&self) -> &[u8] {
		match self {
			Self::Borrowed(slice) => slice,
			Self::Owned(bytes) => bytes.as_ref(),
		}
	}
}

impl AsRef<[u8]> for Cow<'_> {
	fn as_ref(&self) -> &[u8] {
		self.as_slice()
	}
}

impl Deref for Cow<'_> {
	type Target = [u8];

	fn deref(&self) -> &Self::Target {
		self.as_slice()
	}
}
