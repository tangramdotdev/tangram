use num::ToPrimitive as _;

pub trait Encode {
	fn encode<W>(&self, encoder: &mut Encoder<W>) -> Result<(), Error>
	where
		W: std::io::Write;
}

pub trait Decode
where
	Self: Sized,
{
	fn decode(decoder: &mut Decoder<'_>) -> Result<Self, Error>;
}

pub trait SizeHint {
	fn xdr_len(&self) -> usize;
}

pub struct Encoder<W> {
	output: W,
}

pub struct Decoder<'a> {
	input: &'a [u8],
}

#[derive(Debug)]
pub enum Error {
	UnexpectedEof,
	Utf8(std::str::Utf8Error),
	Io(std::io::Error),
	Custom(String),
}

impl<W> Encoder<W>
where
	W: std::io::Write,
{
	pub fn new(output: W) -> Self {
		Self { output }
	}

	pub fn encode_n<const N: usize>(&mut self, bytes: [u8; N]) -> Result<(), Error> {
		self.output.write_all(&bytes)?;
		Ok(())
	}

	pub fn encode_int(&mut self, value: i32) -> Result<(), Error> {
		self.encode_n(value.to_be_bytes())
	}

	pub fn encode_uint(&mut self, value: u32) -> Result<(), Error> {
		self.encode_n(value.to_be_bytes())
	}

	pub fn encode_hyper_int(&mut self, value: i64) -> Result<(), Error> {
		self.encode_n(value.to_be_bytes())
	}

	pub fn encode_hyper_uint(&mut self, value: u64) -> Result<(), Error> {
		self.encode_n(value.to_be_bytes())
	}

	pub fn encode_bool(&mut self, value: bool) -> Result<(), Error> {
		self.encode_int(value.into())
	}

	pub fn encode_opaque(&mut self, bytes: &[u8]) -> Result<(), Error> {
		let len = bytes.len().to_u32().unwrap().to_be_bytes();
		let pad = (4 - bytes.len() % 4) % 4;
		let pad = &([0u8; 4][0..pad]);
		self.output.write_all(&len)?;
		self.output.write_all(bytes)?;
		self.output.write_all(pad)?;
		Ok(())
	}

	pub fn encode_bytes(&mut self, bytes: &[u8]) -> Result<(), Error> {
		self.output.write_all(bytes)?;
		Ok(())
	}

	pub fn encode_str(&mut self, value: &str) -> Result<(), Error> {
		self.encode_opaque(value.as_bytes())?;
		Ok(())
	}

	pub fn encode<T>(&mut self, value: &T) -> Result<(), Error>
	where
		T: Encode,
	{
		value.encode(self)
	}

	pub fn encode_array<T>(&mut self, value: &[T]) -> Result<(), Error>
	where
		T: Encode,
	{
		self.encode_uint(value.len().to_u32().unwrap())?;
		for entity in value {
			self.encode(entity)?;
		}
		Ok(())
	}
}

impl<'d> Decoder<'d> {
	#[must_use]
	pub fn from_bytes(input: &'d [u8]) -> Self {
		Self { input }
	}

	pub fn decode_n<const N: usize>(&mut self) -> Result<[u8; N], Error> {
		if self.input.len() < N {
			return Err(Error::UnexpectedEof);
		}
		let (head, tail) = self.input.split_at(N);
		let mut result = [0u8; N];
		result.copy_from_slice(head);
		self.input = tail;
		Ok(result)
	}

	pub fn decode_int(&mut self) -> Result<i32, Error> {
		Ok(i32::from_be_bytes(self.decode_n()?))
	}

	pub fn decode_uint(&mut self) -> Result<u32, Error> {
		Ok(u32::from_be_bytes(self.decode_n()?))
	}

	pub fn decode_hyper_int(&mut self) -> Result<i64, Error> {
		Ok(i64::from_be_bytes(self.decode_n()?))
	}

	pub fn decode_hyper_uint(&mut self) -> Result<u64, Error> {
		Ok(u64::from_be_bytes(self.decode_n()?))
	}

	pub fn decode_bool(&mut self) -> Result<bool, Error> {
		let i = self.decode_int()?;
		Ok(i == 1)
	}

	pub fn decode_bytes(&mut self, count: usize) -> Result<&'d [u8], Error> {
		if self.input.len() < count {
			let len = self.input.len();
			tracing::error!(?count, ?len, "not enough data in input");
			return Err(Error::UnexpectedEof);
		}
		let (head, tail) = self.input.split_at(count);
		self.input = tail;
		Ok(head)
	}

	pub fn peek_bytes(&mut self, count: usize) -> Result<&'d [u8], Error> {
		if self.input.len() < count {
			return Err(Error::UnexpectedEof);
		}
		let (head, _) = self.input.split_at(count);
		Ok(head)
	}

	pub fn decode_str(&mut self) -> Result<&'d str, Error> {
		let bytes = self.decode_opaque()?;
		let result = std::str::from_utf8(bytes)?;
		Ok(result)
	}

	pub fn decode_opaque(&mut self) -> Result<&'d [u8], Error> {
		let buf_len = self.decode_uint()?.to_usize().unwrap();
		let pad_len = buf_len + (4 - (buf_len % 4)) % 4;
		let bytes = self.decode_bytes(pad_len)?;
		Ok(&bytes[0..buf_len])
	}

	pub fn decode<T>(&mut self) -> Result<T, Error>
	where
		T: Decode,
	{
		<T as Decode>::decode(self)
	}
}

impl Decode for i32 {
	fn decode(decoder: &mut Decoder<'_>) -> Result<Self, Error> {
		decoder.decode_int()
	}
}

impl Encode for i32 {
	fn encode<W>(&self, encoder: &mut Encoder<W>) -> Result<(), Error>
	where
		W: std::io::Write,
	{
		encoder.encode_int(*self)
	}
}

impl Decode for u32 {
	fn decode(decoder: &mut Decoder<'_>) -> Result<Self, Error> {
		decoder.decode_uint()
	}
}

impl Encode for u32 {
	fn encode<W>(&self, encoder: &mut Encoder<W>) -> Result<(), Error>
	where
		W: std::io::Write,
	{
		encoder.encode_uint(*self)
	}
}

impl Decode for i64 {
	fn decode(decoder: &mut Decoder<'_>) -> Result<Self, Error> {
		decoder.decode_hyper_int()
	}
}

impl Encode for i64 {
	fn encode<W>(&self, encoder: &mut Encoder<W>) -> Result<(), Error>
	where
		W: std::io::Write,
	{
		encoder.encode_hyper_int(*self)
	}
}

impl Decode for u64 {
	fn decode(decoder: &mut Decoder<'_>) -> Result<Self, Error> {
		decoder.decode_hyper_uint()
	}
}

impl Encode for u64 {
	fn encode<W>(&self, encoder: &mut Encoder<W>) -> Result<(), Error>
	where
		W: std::io::Write,
	{
		encoder.encode_hyper_uint(*self)
	}
}

impl Decode for bool {
	fn decode(decoder: &mut Decoder<'_>) -> Result<Self, Error> {
		decoder.decode_bool()
	}
}

impl Encode for bool {
	fn encode<W>(&self, encoder: &mut Encoder<W>) -> Result<(), Error>
	where
		W: std::io::Write,
	{
		encoder.encode_bool(*self)
	}
}

impl Decode for String {
	fn decode(decoder: &mut Decoder<'_>) -> Result<Self, Error> {
		decoder.decode_str().map(ToOwned::to_owned)
	}
}

impl Encode for String {
	fn encode<W>(&self, encoder: &mut Encoder<W>) -> Result<(), Error>
	where
		W: std::io::Write,
	{
		encoder.encode_str(self)
	}
}

impl Decode for Vec<u8> {
	fn decode(decoder: &mut Decoder<'_>) -> Result<Self, Error> {
		decoder.decode_opaque().map(ToOwned::to_owned)
	}
}

impl Encode for Vec<u8> {
	fn encode<W>(&self, encoder: &mut Encoder<W>) -> Result<(), Error>
	where
		W: std::io::Write,
	{
		encoder.encode_opaque(self)
	}
}

impl<T> Decode for Vec<T>
where
	T: Decode,
{
	fn decode(decoder: &mut Decoder<'_>) -> Result<Self, Error> {
		let num_entities = decoder.decode_uint()?.to_usize().unwrap();
		let mut result = Vec::with_capacity(num_entities);
		for _ in 0..num_entities {
			result.push(decoder.decode()?);
		}
		Ok(result)
	}
}

impl<T> Encode for Vec<T>
where
	T: Encode,
{
	fn encode<W>(&self, encoder: &mut Encoder<W>) -> Result<(), Error>
	where
		W: std::io::Write,
	{
		let num_entities = self.len().to_u32().unwrap();
		encoder.encode_uint(num_entities)?;
		for entity in self {
			encoder.encode(entity)?;
		}
		Ok(())
	}
}

impl Encode for () {
	fn encode<W>(&self, _encoder: &mut Encoder<W>) -> Result<(), Error>
	where
		W: std::io::Write,
	{
		Ok(())
	}
}

impl Decode for () {
	fn decode(_decoder: &mut Decoder<'_>) -> Result<Self, Error> {
		Ok(())
	}
}

impl<T> Encode for Option<T>
where
	T: Encode,
{
	fn encode<W>(&self, encoder: &mut Encoder<W>) -> Result<(), Error>
	where
		W: std::io::Write,
	{
		match self {
			Some(value) => {
				encoder.encode_int(1)?;
				encoder.encode(value)?;
			},
			None => encoder.encode_int(0)?,
		}
		Ok(())
	}
}

pub fn to_bytes<T>(arg: &T) -> Vec<u8>
where
	T: Encode,
{
	let mut bytes = Vec::new();
	let mut encoder = Encoder::new(&mut bytes);
	encoder.encode(arg).unwrap();
	bytes
}

pub fn from_bytes<T>(bytes: &[u8]) -> Result<T, Error>
where
	T: Decode,
{
	let mut decoder = Decoder::from_bytes(bytes);
	decoder.decode()
}

impl SizeHint for u32 {
	fn xdr_len(&self) -> usize {
		4
	}
}

impl SizeHint for u64 {
	fn xdr_len(&self) -> usize {
		8
	}
}

impl SizeHint for i32 {
	fn xdr_len(&self) -> usize {
		4
	}
}

impl SizeHint for i64 {
	fn xdr_len(&self) -> usize {
		8
	}
}

impl SizeHint for bool {
	fn xdr_len(&self) -> usize {
		4
	}
}

impl SizeHint for Vec<u8> {
	fn xdr_len(&self) -> usize {
		4 + self.len()
	}
}

impl SizeHint for String {
	fn xdr_len(&self) -> usize {
		4 + self.len()
	}
}

impl<const N: usize> SizeHint for [u8; N] {
	fn xdr_len(&self) -> usize {
		N
	}
}

impl<T> SizeHint for Vec<T>
where
	T: SizeHint,
{
	fn xdr_len(&self) -> usize {
		let total_len = self.iter().fold(0, |acc, val| acc + val.xdr_len());
		4 + total_len
	}
}

impl<T> Encode for &T
where
	T: Encode,
{
	fn encode<W>(&self, encoder: &mut Encoder<W>) -> Result<(), Error>
	where
		W: std::io::Write,
	{
		<T as Encode>::encode(self, encoder)
	}
}

impl From<std::str::Utf8Error> for Error {
	fn from(value: std::str::Utf8Error) -> Self {
		Self::Utf8(value)
	}
}

impl From<std::io::Error> for Error {
	fn from(value: std::io::Error) -> Self {
		Self::Io(value)
	}
}
