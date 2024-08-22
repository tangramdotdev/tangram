#[derive(
	Clone, Debug, Eq, Hash, PartialEq, PartialOrd, Ord, serde::Deserialize, serde::Serialize,
)]
#[serde(untagged)]
pub enum Either<L, R> {
	Left(L),
	Right(R),
}

impl<L, R> Either<L, R> {
	pub fn as_ref(&self) -> Either<&L, &R> {
		match self {
			Self::Left(left) => Either::Left(left),
			Self::Right(right) => Either::Right(right),
		}
	}

	pub fn left(self) -> Option<L> {
		match self {
			Self::Left(left) => Some(left),
			Self::Right(_) => None,
		}
	}

	pub fn right(self) -> Option<R> {
		match self {
			Self::Left(_) => None,
			Self::Right(right) => Some(right),
		}
	}

	pub fn is_left(&self) -> bool {
		self.as_ref().left().is_some()
	}

	pub fn is_right(&self) -> bool {
		self.as_ref().right().is_some()
	}

	pub fn unwrap_left(self) -> L {
		self.left().unwrap()
	}

	pub fn unwrap_right(self) -> R {
		self.right().unwrap()
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

#[macro_export]
macro_rules! for_both {
	($value:expr, $pattern:pat => $result:expr) => {
		match $value {
			$crate::Either::Left($pattern) => $result,
			$crate::Either::Right($pattern) => $result,
		}
	};
}
