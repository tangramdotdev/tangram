use super::xdr;
use num::ToPrimitive as _;
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _};

#[derive(Clone, Debug)]
pub struct Auth {
	pub flavor: AuthFlavor,
	pub opaque: Vec<u8>,
}

#[derive(Copy, Clone, Debug)]
#[repr(i32)]
pub enum AuthFlavor {
	None = 0,
	Sys = 1,
	Short = 2,
	Unknown(i32),
}

#[derive(Clone, Debug)]
pub struct Message {
	pub xid: u32,
	pub body: MessageBody,
}

#[derive(Clone, Debug)]
pub enum MessageBody {
	Call(CallBody),
	Reply(ReplyBody),
}

#[derive(Clone, Debug)]
pub struct CallBody {
	pub rpcvers: u32,
	pub prog: u32,
	pub vers: u32,
	pub proc: u32,
	pub cred: Auth,
	pub verf: Auth,
}

#[derive(Clone, Debug)]
pub enum ReplyBody {
	Accepted(ReplyAccepted),
	Rejected(ReplyRejected),
}

#[derive(Clone, Debug)]
pub struct ReplyAccepted {
	pub verf: Auth,
	pub stat: ReplyAcceptedStat,
}

#[derive(Clone, Debug)]
pub enum ReplyAcceptedStat {
	Success { results: Vec<u8> },
	ProgramMismatch { low: u32, high: u32 },
	ProgramUnavailable,
	ProcedureUnavailable,
	GarbageArgs,
	SystemError,
}

#[derive(Copy, Clone, Debug)]
pub enum ReplyRejected {
	RpcMismatch { low: u32, high: u32 },
	AuthError(AuthStat),
}

#[derive(Copy, Clone, Debug)]
#[repr(i32)]
pub enum AuthStat {
	Ok = 0,
	BadCred = 1,
	RejectedCred = 2,
	BadVerf = 3,
	RejectedVerf = 4,
	TooWeak = 5,
	InvalidResp = 6,
	Failed = 7,
}

impl xdr::Encode for Auth {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		encoder.encode(&self.flavor)?;
		encoder.encode_opaque(&self.opaque)?;
		Ok(())
	}
}

impl xdr::Decode for Auth {
	fn decode(decoder: &mut xdr::Decoder<'_>) -> Result<Self, xdr::Error> {
		let flavor = decoder.decode()?;
		let opaque = decoder.decode()?;
		Ok(Self { flavor, opaque })
	}
}

impl Default for Auth {
	fn default() -> Self {
		Self {
			flavor: AuthFlavor::None,
			opaque: Vec::default(),
		}
	}
}

impl xdr::Encode for AuthFlavor {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		let int = match self {
			Self::None => 0,
			Self::Sys => 1,
			Self::Short => 2,
			Self::Unknown(i) => *i,
		};
		encoder.encode_int(int)
	}
}

impl xdr::Decode for AuthFlavor {
	fn decode(decoder: &mut xdr::Decoder<'_>) -> Result<Self, xdr::Error> {
		match decoder.decode_int()? {
			0 => Ok(Self::None),
			1 => Ok(Self::Sys),
			2 => Ok(Self::Short),
			i => Ok(Self::Unknown(i)),
		}
	}
}

impl xdr::Encode for Message {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		encoder.encode_uint(self.xid)?;
		encoder.encode(&self.body)?;
		Ok(())
	}
}

impl xdr::Decode for Message {
	fn decode(decoder: &mut xdr::Decoder<'_>) -> Result<Self, xdr::Error> {
		let xid = decoder.decode_uint()?;
		let body = decoder.decode()?;
		Ok(Self { xid, body })
	}
}

impl xdr::Encode for MessageBody {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		match self {
			Self::Call(body) => {
				encoder.encode_int(0)?;
				encoder.encode(body)?;
			},
			Self::Reply(body) => {
				encoder.encode_int(1)?;
				encoder.encode(body)?;
			},
		}
		Ok(())
	}
}

impl xdr::Decode for MessageBody {
	fn decode(decoder: &mut xdr::Decoder<'_>) -> Result<Self, xdr::Error> {
		let tag = decoder.decode_int()?;
		match tag {
			0 => {
				let body = decoder.decode()?;
				Ok(Self::Call(body))
			},
			1 => {
				let body = decoder.decode()?;
				Ok(Self::Reply(body))
			},
			tag => Err(xdr::Error::Custom(format!(
				"expected a message body. Got {tag}"
			))),
		}
	}
}

impl xdr::Encode for CallBody {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		encoder.encode_uint(self.rpcvers)?;
		encoder.encode_uint(self.prog)?;
		encoder.encode_uint(self.vers)?;
		encoder.encode_uint(self.proc)?;
		encoder.encode(&self.cred)?;
		encoder.encode(&self.verf)?;
		Ok(())
	}
}

impl xdr::Decode for CallBody {
	fn decode(decoder: &mut xdr::Decoder<'_>) -> Result<Self, xdr::Error> {
		let rpcvers = decoder.decode_uint()?;
		let prog = decoder.decode_uint()?;
		let vers = decoder.decode_uint()?;
		let proc = decoder.decode_uint()?;
		let cred = decoder.decode()?;
		let verf = decoder.decode()?;
		Ok(Self {
			rpcvers,
			prog,
			vers,
			proc,
			cred,
			verf,
		})
	}
}

impl xdr::Encode for ReplyBody {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		match self {
			Self::Accepted(accepted) => {
				encoder.encode_int(0)?;
				encoder.encode(accepted)?;
			},
			Self::Rejected(rejected) => {
				encoder.encode_int(1)?;
				encoder.encode(rejected)?;
			},
		}
		Ok(())
	}
}

impl xdr::Decode for ReplyBody {
	fn decode(decoder: &mut xdr::Decoder<'_>) -> Result<Self, xdr::Error> {
		let tag = decoder.decode_int()?;
		match tag {
			0 => Ok(Self::Accepted(decoder.decode()?)),
			1 => Ok(Self::Rejected(decoder.decode()?)),
			tag => Err(xdr::Error::Custom(format!(
				"expected a reply body. Got {tag}"
			))),
		}
	}
}

impl xdr::Encode for ReplyAccepted {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		encoder.encode(&self.verf)?;
		encoder.encode(&self.stat)?;
		Ok(())
	}
}

impl xdr::Decode for ReplyAccepted {
	fn decode(decoder: &mut xdr::Decoder<'_>) -> Result<Self, xdr::Error> {
		let verf = decoder.decode()?;
		let stat = decoder.decode()?;
		Ok(Self { verf, stat })
	}
}

impl xdr::Encode for ReplyAcceptedStat {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		match self {
			Self::Success { results } => {
				encoder.encode_int(0)?;
				encoder.encode_bytes(results)?;
			},
			Self::ProgramMismatch { low, high } => {
				encoder.encode_int(1)?;
				encoder.encode_uint(*low)?;
				encoder.encode_uint(*high)?;
			},
			Self::ProgramUnavailable => {
				encoder.encode_int(2)?;
			},
			Self::ProcedureUnavailable => {
				encoder.encode_int(3)?;
			},
			Self::GarbageArgs => {
				encoder.encode_int(4)?;
			},
			Self::SystemError => {
				encoder.encode_int(5)?;
			},
		}
		Ok(())
	}
}

impl xdr::Decode for ReplyAcceptedStat {
	fn decode(decoder: &mut xdr::Decoder<'_>) -> Result<Self, xdr::Error> {
		let tag = decoder.decode_int()?;
		match tag {
			0 => Ok(Self::Success {
				results: decoder.decode_opaque()?.to_owned(),
			}),
			1 => Ok(Self::ProgramMismatch {
				low: decoder.decode_uint()?,
				high: decoder.decode_uint()?,
			}),
			2 => Ok(Self::ProgramUnavailable),
			3 => Ok(Self::ProcedureUnavailable),
			4 => Ok(Self::GarbageArgs),
			5 => Ok(Self::SystemError),
			tag => Err(xdr::Error::Custom(format!("expected a reply. Got {tag}"))),
		}
	}
}

impl xdr::Encode for ReplyRejected {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		match self {
			Self::RpcMismatch { low, high } => {
				encoder.encode_int(0)?;
				encoder.encode_uint(*low)?;
				encoder.encode_uint(*high)?;
			},
			Self::AuthError(stat) => encoder.encode(stat)?,
		}
		Ok(())
	}
}

impl xdr::Decode for ReplyRejected {
	fn decode(decoder: &mut xdr::Decoder<'_>) -> Result<Self, xdr::Error> {
		let tag = decoder.decode_int()?;
		match tag {
			0 => Ok(Self::RpcMismatch {
				low: decoder.decode_uint()?,
				high: decoder.decode_uint()?,
			}),
			1 => Ok(Self::AuthError(decoder.decode()?)),
			tag => Err(xdr::Error::Custom(format!("expected a reply. Got {tag}"))),
		}
	}
}

impl xdr::Encode for AuthStat {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		let int = match self {
			Self::Ok => 0,
			Self::BadCred => 1,
			Self::RejectedCred => 2,
			Self::BadVerf => 3,
			Self::RejectedVerf => 4,
			Self::TooWeak => 5,
			Self::InvalidResp => 6,
			Self::Failed => 7,
		};
		encoder.encode_int(int)
	}
}

impl xdr::Decode for AuthStat {
	fn decode(decoder: &mut xdr::Decoder<'_>) -> Result<Self, xdr::Error> {
		let int = decoder.decode_int()?;
		match int {
			0 => Ok(Self::Ok),
			1 => Ok(Self::BadCred),
			2 => Ok(Self::RejectedCred),
			3 => Ok(Self::BadVerf),
			4 => Ok(Self::RejectedVerf),
			5 => Ok(Self::TooWeak),
			6 => Ok(Self::InvalidResp),
			7 => Ok(Self::Failed),
			int => Err(xdr::Error::Custom(format!(
				"expected auth error. Got {int}"
			))),
		}
	}
}

pub async fn read_fragments<S>(stream: &mut S) -> std::io::Result<Vec<u8>>
where
	S: AsyncRead + Unpin,
{
	let mut buf = Vec::new();
	loop {
		let header = stream.read_u32().await?;
		let is_last_fragment = (header & 0x8000_0000) != 0;
		let len = (header & 0x7fff_ffff).to_usize().unwrap();
		let old_len = buf.len();
		buf.resize_with(old_len + len, || 0);
		stream.read_exact(&mut buf[old_len..]).await?;
		if is_last_fragment {
			break;
		}
	}
	Ok(buf)
}

pub async fn write_fragments<S>(stream: &mut S, fragments: &[u8]) -> std::io::Result<()>
where
	S: AsyncWrite + Unpin,
{
	let header = 0x8000_0000 | fragments.len().to_u32().unwrap();
	stream.write_all(&header.to_be_bytes()).await?;
	stream.write_all(fragments).await?;
	Ok(())
}

#[must_use]
pub fn success<T>(verf: Option<Auth>, value: T) -> ReplyBody
where
	T: xdr::Encode,
{
	let verf = verf.unwrap_or_default();
	let results = xdr::to_bytes(&value);
	let stat = ReplyAcceptedStat::Success { results };
	ReplyBody::Accepted(ReplyAccepted { verf, stat })
}

#[must_use]
pub fn error(verf: Option<Auth>, reason: ReplyAcceptedStat) -> ReplyBody {
	let verf = verf.unwrap_or_default();
	ReplyBody::Accepted(ReplyAccepted { verf, stat: reason })
}

#[must_use]
pub fn reject(reason: ReplyRejected) -> ReplyBody {
	ReplyBody::Rejected(reason)
}
