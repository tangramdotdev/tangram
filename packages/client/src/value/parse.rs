use crate as tg;
use winnow::{
	ascii::float,
	combinator::{alt, cut_err, delimited, opt, preceded, repeat, separated, separated_pair},
	prelude::*,
	token::{any, none_of, one_of, take, take_while},
};

type Input<'a> = winnow::stream::LocatingSlice<&'a str>;

pub fn parse(input: &str) -> tg::Result<tg::Value> {
	value(&mut Input::new(input))
		.map_err(|error| tg::error!("{}", error.into_inner().unwrap().to_string()))
}

fn value(input: &mut Input) -> ModalResult<tg::Value> {
	delimited(whitespace, value_inner, whitespace).parse_next(input)
}

fn value_inner(input: &mut Input) -> ModalResult<tg::Value> {
	alt((
		null.value(tg::Value::Null),
		bool_.map(tg::Value::Bool),
		number.map(tg::Value::Number),
		string.map(tg::Value::String),
		array.map(tg::Value::Array),
		map.map(tg::Value::Map),
		object.map(tg::Value::Object),
	))
	.parse_next(input)
}

fn null(input: &mut Input) -> ModalResult<()> {
	"null".value(()).parse_next(input)
}

fn bool_(input: &mut Input) -> ModalResult<bool> {
	alt(("true".value(true), "false".value(false))).parse_next(input)
}

fn number(input: &mut Input) -> ModalResult<f64> {
	float.parse_next(input)
}

fn string(input: &mut Input) -> ModalResult<String> {
	let string = repeat(0.., char_).fold(String::new, |mut s, c| {
		s.push(c);
		s
	});
	delimited('"', cut_err(string), '"').parse_next(input)
}

fn char_(input: &mut Input) -> ModalResult<char> {
	let c = none_of('\"').parse_next(input)?;
	if c == '\\' {
		let c = alt((
			any.try_map(|c| {
				let c = match c {
					'"' | '\\' | '/' => c,
					'b' => '\x08',
					'f' => '\x0C',
					'n' => '\n',
					'r' => '\r',
					't' => '\t',
					_ => return Err(tg::error!("invalid char")),
				};
				Ok(c)
			}),
			preceded('u', unicode_escape),
		))
		.parse_next(input)?;
		return Ok(c);
	}
	Ok(c)
}

fn unicode_escape(input: &mut Input) -> ModalResult<char> {
	alt((
		u16_hex
			.verify(|cp| !(0xD800..0xE000).contains(cp))
			.map(u32::from),
		separated_pair(u16_hex, "\\u", u16_hex)
			.verify(|(high, low)| (0xD800..0xDC00).contains(high) && (0xDC00..0xE000).contains(low))
			.map(|(high, low)| {
				let high_ten = u32::from(high) - 0xD800;
				let low_ten = u32::from(low) - 0xDC00;
				(high_ten << 10) + low_ten + 0x10000
			}),
	))
	.verify_map(std::char::from_u32)
	.parse_next(input)
}

fn u16_hex(input: &mut Input) -> ModalResult<u16> {
	take(4usize)
		.verify_map(|s| u16::from_str_radix(s, 16).ok())
		.parse_next(input)
}

fn array(input: &mut Input) -> ModalResult<tg::value::Array> {
	delimited(
		('[', whitespace),
		cut_err(separated(0.., value, (whitespace, ',', whitespace))),
		(whitespace, opt(","), whitespace, ']'),
	)
	.parse_next(input)
}

fn map(input: &mut Input) -> ModalResult<tg::value::Map> {
	delimited(
		('{', whitespace),
		cut_err(separated(
			0..,
			separated_pair(string, (whitespace, ':', whitespace), value),
			(whitespace, ',', whitespace),
		)),
		(whitespace, opt(","), whitespace, '}'),
	)
	.parse_next(input)
}

fn id(input: &mut Input) -> ModalResult<tg::id::Id> {
	(id_kind, "_", "0", id_body)
		.verify_map(|(kind, _, version, body)| match version {
			"0" => Some(tg::Id::V0(tg::id::V0 { kind, body })),
			_ => None,
		})
		.parse_next(input)
}

fn id_kind(input: &mut Input) -> ModalResult<tg::id::Kind> {
	alt((
		alt(("lef", "leaf")).value(tg::id::Kind::Leaf),
		alt(("bch", "branch")).value(tg::id::Kind::Branch),
		alt(("dir", "directory")).value(tg::id::Kind::Directory),
		alt(("fil", "file")).value(tg::id::Kind::File),
		alt(("sym", "symlink")).value(tg::id::Kind::Symlink),
		alt(("gph", "graph")).value(tg::id::Kind::Graph),
		alt(("cmd", "command")).value(tg::id::Kind::Command),
		alt(("pcs", "process")).value(tg::id::Kind::Process),
		alt(("usr", "user")).value(tg::id::Kind::User),
		alt(("tok", "token")).value(tg::id::Kind::Token),
		alt(("req", "request")).value(tg::id::Kind::Request),
	))
	.parse_next(input)
}

fn id_body(input: &mut Input) -> ModalResult<tg::id::Body> {
	alt((
		preceded("0", id_body_inner)
			.verify_map(|bytes| bytes.try_into().map(tg::id::Body::UuidV7).ok()),
		preceded("1", id_body_inner)
			.verify_map(|bytes| bytes.try_into().map(tg::id::Body::Blake3).ok()),
	))
	.parse_next(input)
}

fn id_body_inner(input: &mut Input) -> ModalResult<Vec<u8>> {
	const ENCODING: data_encoding::Encoding = data_encoding_macro::new_encoding! {
		symbols: "0123456789abcdefghjkmnpqrstvwxyz",
	};
	repeat(
		0..,
		one_of(|c| "0123456789abcdefghjkmnpqrstvwxyz".contains(c)),
	)
	.fold(String::new, |mut string, c| {
		string.push(c);
		string
	})
	.verify_map(|string| ENCODING.decode(string.as_bytes()).ok())
	.parse_next(input)
}

fn object(input: &mut Input) -> ModalResult<tg::Object> {
	id.verify_map(|id| id.try_into().map(tg::Object::with_id).ok())
		.parse_next(input)
}

fn whitespace(input: &mut Input) -> ModalResult<()> {
	take_while(0.., [' ', '\t', '\r', '\n'])
		.parse_next(input)
		.map(|_| ())
}
