#[derive(
	Clone, Debug, Eq, Hash, PartialEq, PartialOrd, Ord, serde::Deserialize, serde::Serialize,
)]
#[serde(untagged)]
pub enum Either<L, R> {
	Left(L),
	Right(R),
}

impl<L, R> Either<L, R> {
	pub fn as_mut(&mut self) -> Either<&mut L, &mut R> {
		match self {
			Self::Left(left) => Either::Left(left),
			Self::Right(right) => Either::Right(right),
		}
	}

	pub fn as_ref(&self) -> Either<&L, &R> {
		match self {
			Self::Left(left) => Either::Left(left),
			Self::Right(right) => Either::Right(right),
		}
	}

	pub fn is_left(&self) -> bool {
		self.as_ref().left().is_some()
	}

	pub fn is_right(&self) -> bool {
		self.as_ref().right().is_some()
	}

	pub fn left(self) -> Option<L> {
		match self {
			Self::Left(left) => Some(left),
			Self::Right(_) => None,
		}
	}

	pub fn map_left<F, L2>(self, f: F) -> Either<L2, R>
	where
		F: FnOnce(L) -> L2,
	{
		match self {
			Self::Left(left) => Either::Left(f(left)),
			Self::Right(right) => Either::Right(right),
		}
	}

	pub fn map_right<F, R2>(self, f: F) -> Either<L, R2>
	where
		F: FnOnce(R) -> R2,
	{
		match self {
			Self::Left(left) => Either::Left(left),
			Self::Right(right) => Either::Right(f(right)),
		}
	}

	pub fn right(self) -> Option<R> {
		match self {
			Self::Left(_) => None,
			Self::Right(right) => Some(right),
		}
	}

	pub fn unwrap_left(self) -> L {
		self.left().unwrap()
	}

	pub fn unwrap_right(self) -> R {
		self.right().unwrap()
	}
}

impl<T> Either<T, T> {
	pub fn into_inner(self) -> T {
		for_both!(self, inner => inner)
	}
}

impl<L, R> Either<&L, &R>
where
	L: Clone,
	R: Clone,
{
	#[must_use]
	pub fn cloned(&self) -> Either<L, R> {
		match self {
			Either::Left(inner) => Either::Left((*inner).clone()),
			Either::Right(inner) => Either::Right((*inner).clone()),
		}
	}
}

impl<L, R> std::fmt::Display for Either<L, R>
where
	L: std::fmt::Display,
	R: std::fmt::Display,
{
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		for_both!(self, value => write!(f, "{value}"))
	}
}

impl<L, R> std::error::Error for Either<L, R>
where
	L: std::error::Error,
	R: std::error::Error,
{
	fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
		for_both!(self, value => value.source())
	}
}

impl<L, R> Iterator for Either<L, R>
where
	L: Iterator,
	R: Iterator<Item = L::Item>,
{
	type Item = L::Item;

	fn next(&mut self) -> Option<Self::Item> {
		for_both!(*self, ref mut inner => inner.next())
	}

	fn size_hint(&self) -> (usize, Option<usize>) {
		for_both!(*self, ref inner => inner.size_hint())
	}

	fn fold<Acc, G>(self, init: Acc, f: G) -> Acc
	where
		G: FnMut(Acc, Self::Item) -> Acc,
	{
		for_both!(self, inner => inner.fold(init, f))
	}

	fn for_each<F>(self, f: F)
	where
		F: FnMut(Self::Item),
	{
		for_both!(self, inner => inner.for_each(f));
	}

	fn count(self) -> usize {
		for_both!(self, inner => inner.count())
	}

	fn last(self) -> Option<Self::Item> {
		for_both!(self, inner => inner.last())
	}

	fn nth(&mut self, n: usize) -> Option<Self::Item> {
		for_both!(*self, ref mut inner => inner.nth(n))
	}

	fn collect<B>(self) -> B
	where
		B: std::iter::FromIterator<Self::Item>,
	{
		for_both!(self, inner => inner.collect())
	}

	fn partition<B, F>(self, f: F) -> (B, B)
	where
		B: Default + Extend<Self::Item>,
		F: FnMut(&Self::Item) -> bool,
	{
		for_both!(self, inner => inner.partition(f))
	}

	fn all<F>(&mut self, f: F) -> bool
	where
		F: FnMut(Self::Item) -> bool,
	{
		for_both!(*self, ref mut inner => inner.all(f))
	}

	fn any<F>(&mut self, f: F) -> bool
	where
		F: FnMut(Self::Item) -> bool,
	{
		for_both!(*self, ref mut inner => inner.any(f))
	}

	fn find<P>(&mut self, predicate: P) -> Option<Self::Item>
	where
		P: FnMut(&Self::Item) -> bool,
	{
		for_both!(*self, ref mut inner => inner.find(predicate))
	}

	fn find_map<B, F>(&mut self, f: F) -> Option<B>
	where
		F: FnMut(Self::Item) -> Option<B>,
	{
		for_both!(*self, ref mut inner => inner.find_map(f))
	}

	fn position<P>(&mut self, predicate: P) -> Option<usize>
	where
		P: FnMut(Self::Item) -> bool,
	{
		for_both!(*self, ref mut inner => inner.position(predicate))
	}
}

impl<L, R> AsRef<[u8]> for Either<L, R>
where
	L: AsRef<[u8]>,
	R: AsRef<[u8]>,
{
	fn as_ref(&self) -> &[u8] {
		for_both!(self, value => value.as_ref())
	}
}

impl<L, R> std::str::FromStr for Either<L, R>
where
	L: std::str::FromStr,
	R: std::str::FromStr,
{
	type Err = Either<L::Err, R::Err>;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		L::from_str(s)
			.map(Either::Left)
			.or_else(|_| R::from_str(s).map(Either::Right).map_err(Either::Right))
	}
}

impl<L, R> std::convert::TryFrom<String> for Either<L, R>
where
	L: std::convert::TryFrom<String>,
	R: std::convert::TryFrom<String>,
{
	type Error = Either<L::Error, R::Error>;

	fn try_from(s: String) -> Result<Self, Self::Error> {
		L::try_from(s.clone())
			.map(Either::Left)
			.or_else(|_| R::try_from(s).map(Either::Right).map_err(Either::Right))
	}
}

impl<L, R> tangram_serialize::Serialize for Either<L, R>
where
	L: tangram_serialize::Serialize,
	R: tangram_serialize::Serialize,
{
	fn serialize<W>(
		&self,
		serializer: &mut tangram_serialize::Serializer<W>,
	) -> std::result::Result<(), std::io::Error>
	where
		W: std::io::Write,
	{
		serializer.write_kind(tangram_serialize::Kind::Enum)?;
		match self {
			Either::Left(left) => {
				serializer.write_id(0)?;
				serializer.serialize(left)?;
			},
			Either::Right(right) => {
				serializer.write_id(1)?;
				serializer.serialize(right)?;
			},
		}
		Ok(())
	}
}

impl<L, R> tangram_serialize::Deserialize for Either<L, R>
where
	L: tangram_serialize::Deserialize,
	R: tangram_serialize::Deserialize,
{
	fn deserialize<R_>(
		deserializer: &mut tangram_serialize::Deserializer<R_>,
	) -> std::result::Result<Self, std::io::Error>
	where
		R_: std::io::Read + std::io::Seek,
	{
		deserializer.ensure_kind(tangram_serialize::Kind::Enum)?;
		let id = deserializer.read_id()?;
		match id {
			0 => {
				let left = deserializer.deserialize()?;
				Ok(Either::Left(left))
			},
			1 => {
				let right = deserializer.deserialize()?;
				Ok(Either::Right(right))
			},
			_ => Err(std::io::Error::other("invalid variant")),
		}
	}
}

#[macro_export]
macro_rules! for_both {
	($value:expr, $pattern:pat => $result:expr) => {
		match $value {
			$crate::Either::Left($pattern) => $result,
			$crate::Either::Right($pattern) => $result,
		}
	};
}
