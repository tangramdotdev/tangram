use std::io::Read;

pub struct InspectReader<R, F>
where
	R: Read,
	F: FnMut(&[u8]),
{
	reader: R,
	f: F,
}

impl<R, F> InspectReader<R, F>
where
	R: Read,
	F: FnMut(&[u8]),
{
	pub fn new(reader: R, f: F) -> Self {
		Self { reader, f }
	}
}

impl<R, F> Read for InspectReader<R, F>
where
	R: Read,
	F: FnMut(&[u8]),
{
	fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
		let n = self.reader.read(buf)?;
		(self.f)(&buf[..n]);
		Ok(n)
	}
}
