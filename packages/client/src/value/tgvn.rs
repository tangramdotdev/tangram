use crate as tg;
use bytes::Bytes;
use either::Either;
use num::ToPrimitive;
use std::{collections::BTreeMap, sync::Arc};
use winnow::{
	combinator::{
		alt, cut_err, delimited, opt, preceded, repeat, separated, separated_pair, terminated,
	}, error::{AddContext, ErrorKind, ParserError}, stream::Location as _, token::{any, none_of, one_of, take, take_while}, PResult, Parser
};

#[derive(Debug, Clone)]
pub struct Error {
	location: usize,
	message: String,
}

struct Serializer {
	pretty: bool,
	indent: u32,
	buf: String,
}

type Stream<'a> = winnow::stream::Located<&'a str>;

impl std::fmt::Display for Error {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "parse error at character {}: {}", self.location, self.message)
	}
}

impl<'a, C> AddContext<Stream<'a>, C> for Error
where
	C: std::fmt::Display,
{
	fn add_context(
		mut self,
		input: &Stream<'a>,
		_token_start: &<Stream<'a> as winnow::stream::Stream>::Checkpoint,
		context: C,
	) -> Self {
		self.location = input.location();
		self.message = context.to_string();
		self
	}
}

impl<'a> ParserError<Stream<'a>> for Error {
	fn from_error_kind(input: &Stream<'a>, _kind: winnow::error::ErrorKind) -> Self {
		Error {
			location: input.location(),
			message: String::new(),
		}
	}

	fn append(
		self,
		_input: &Stream<'a>,
		_token_start: &<Stream<'a> as winnow::stream::Stream>::Checkpoint,
		_kind: ErrorKind,
	) -> Self {
		self
	}
}

impl tg::Value {
	pub fn to_tgvn_string(&self) -> String {
		let mut serializer = Serializer {
			buf: String::with_capacity(128),
			pretty: false,
			indent: 0,
		};
		serializer.value(self);
		serializer.buf
	}

	pub fn to_tgvn_string_pretty(&self) -> String {
		let mut serializer = Serializer {
			buf: String::with_capacity(128),
			pretty: true,
			indent: 0,
		};
		serializer.value(self);
		serializer.buf
	}
}

impl std::str::FromStr for tg::Value {
	type Err = tg::Error;
	fn from_str(input: &str) -> Result<Self, Self::Err> {
		let mut input = Stream::new(input);
		value(&mut input).map_err(|error| tg::error!("{}", error.into_inner().unwrap()))
	}
}

impl Serializer {
	fn value(&mut self, v: &tg::Value) {
		match v {
			tg::Value::Null => self.null(),
			tg::Value::Bool(v) => self.bool(*v),
			tg::Value::Number(v) => self.number(*v),
			tg::Value::String(v) => self.string(v),
			tg::Value::Array(v) => self.array(v),
			tg::Value::Map(v) => self.map(v),
			tg::Value::Object(v) => self.object(v),
			tg::Value::Bytes(v) => self.bytes(v),
			tg::Value::Path(v) => self.path(v),
			tg::Value::Mutation(v) => self.mutation(v),
			tg::Value::Template(v) => self.template(v),
		}
	}

	fn indent(&mut self) {
		if self.pretty {
			let indent = (0..self.indent).map(|_| '\t');
			self.buf.extend(indent);
		}
	}

	fn null(&mut self) {
		self.buf += "null";
	}

	fn bool(&mut self, v: bool) {
		self.buf += if v { "true" } else { "false" };
	}

	fn number(&mut self, v: f64) {
		self.buf += &v.to_string();
	}

	fn string(&mut self, v: &str) {
		self.buf += &format!("{v:#?}");
	}

	fn array(&mut self, v: &tg::value::Array) {
		self.buf.push('[');
		if self.pretty && !v.is_empty() {
			self.buf.push('\n');
		}
		self.indent += 1;
		for value in v {
			self.indent();
			self.value(value);
			self.buf.push(',');
			if self.pretty {
				self.buf.push('\n');
			}
		}
		self.indent -= 1;
		self.indent();
		self.buf.push(']');
	}

	fn map(&mut self, v: &tg::value::Map) {
		self.buf.push('{');
		if self.pretty && !v.is_empty() {
			self.buf.push('\n');
		}
		self.indent += 1;
		for (key, value) in v {
			self.indent();
			self.buf += &format!("{key:#?}");
			self.buf.push(':');
			if self.pretty {
				self.buf.push(' ');
			}
			self.value(value);
			self.buf.push(',');
			if self.pretty {
				self.buf.push('\n');
			}
		}
		self.indent -= 1;
		self.indent();
		self.buf.push('}');
	}

	fn object(&mut self, v: &tg::Object) {
		match v {
			tg::Object::Leaf(v) => self.leaf(v),
			tg::Object::Branch(v) => self.branch(v),
			tg::Object::Directory(v) => self.directory(v),
			tg::Object::File(v) => self.file(v),
			tg::Object::Symlink(v) => self.symlink(v),
			tg::Object::Lock(v) => self.lock(v),
			tg::Object::Target(v) => self.target(v),
		}
	}

	fn leaf(&mut self, v: &tg::Leaf) {
		let state = v.state().read().unwrap();
		if let Some(id) = state.id() {
			self.buf += &id.to_string();
			return;
		}
		let object = state.object().unwrap();
		self.buf += "tg.leaf(";
		self.string(&data_encoding::BASE64.encode(&object.bytes));
		self.buf.push(')');
	}

	fn branch(&mut self, v: &tg::Branch) {
		let state = v.state().read().unwrap();
		if let Some(id) = state.id() {
			self.buf += &id.to_string();
			return;
		}
		let object = state.object().unwrap();
		let children = object
			.children
			.iter()
			.map(|child| {
				let size = tg::Value::Number(child.size.to_f64().unwrap());
				let blob = tg::Value::Object(child.blob.clone().into());
				let object = [("size".to_owned(), size), ("blob".to_owned(), blob)]
					.into_iter()
					.collect();
				tg::Value::Map(object)
			})
			.collect();
		let object = [("children".to_owned(), tg::Value::Array(children))];
		self.buf += "tg.branch(";
		self.map(&object.into_iter().collect());
		self.buf.push(')');
	}

	fn directory(&mut self, v: &tg::Directory) {
		let state = v.state().read().unwrap();
		if let Some(id) = state.id() {
			self.buf += &id.to_string();
			return;
		}
		let object = state.object().unwrap();
		let entries = object
			.entries
			.iter()
			.map(|(name, artifact)| (name.clone(), tg::Value::Object(artifact.clone().into())))
			.collect();

		self.buf += "tg.directory(";
		self.map(&entries);
		self.buf.push(')');
	}

	fn file(&mut self, v: &tg::File) {
		let state = v.state().read().unwrap();
		if let Some(id) = state.id() {
			self.buf += &id.to_string();
			return;
		}
		let object = state.object().unwrap();
		let mut map = BTreeMap::new();
		map.insert(
			"contents".to_owned(),
			tg::Value::Object(object.contents.clone().into()),
		);
		map.insert("executable".to_owned(), tg::Value::Bool(object.executable));
		let references = tg::Value::Array(
			object
				.references
				.iter()
				.map(|artifact| tg::Value::Object(artifact.clone().into()))
				.collect(),
		);
		map.insert("references".to_owned(), references);

		self.buf += "tg.file(";
		self.map(&map);
		self.buf.push(')');
	}

	fn symlink(&mut self, v: &tg::Symlink) {
		let state = v.state().read().unwrap();
		if let Some(id) = state.id() {
			self.buf += &id.to_string();
			return;
		}
		let object = state.object().unwrap();
		let mut map = BTreeMap::new();

		if let Some(artifact) = &object.artifact {
			map.insert(
				"artifact".to_owned(),
				tg::Value::Object(artifact.clone().into()),
			);
		}
		if let Some(path) = &object.path {
			map.insert("path".to_owned(), tg::Value::Path(path.clone()));
		}

		self.buf += "tg.symlink(";
		self.map(&map);
		self.buf.push(')');
	}

	fn lock(&mut self, v: &tg::Lock) {
		let state = v.state().read().unwrap();
		if let Some(id) = state.id() {
			self.buf += &id.to_string();
			return;
		}

		// Create the object.
		let object = state.object().unwrap();

		// Create the object.
		let mut map = BTreeMap::new();
		map.insert(
			"root".to_owned(),
			tg::Value::Number(object.root.to_f64().unwrap()),
		);

		// Get the nodes.
		let nodes = object
			.nodes
			.iter()
			.map(|node| {
				let mut map = BTreeMap::new();
				let dependencies = node
					.dependencies
					.iter()
					.map(|(dependency, entry)| {
						let dependency = dependency.to_string();
						let mut map = BTreeMap::new();
						if let Some(artifact) = &entry.artifact {
							map.insert(
								"artifact".to_owned(),
								tg::Value::Object(artifact.clone().into()),
							);
						}
						let lock = match &entry.lock {
							Either::Left(number) => tg::Value::Number(number.to_f64().unwrap()),
							Either::Right(lock) => tg::Value::Object(lock.clone().into()),
						};
						map.insert("lock".to_owned(), lock);
						(dependency, tg::Value::Map(map))
					})
					.collect();
				map.insert("dependencies".to_owned(), tg::Value::Map(dependencies));
				tg::Value::Map(map)
			})
			.collect();
		map.insert("nodes".to_owned(), tg::Value::Array(nodes));

		self.buf += "tg.lock(";
		self.map(&map);
		self.buf.push(')');
	}

	fn target(&mut self, v: &tg::Target) {
		let state = v.state().read().unwrap();
		if let Some(id) = state.id() {
			self.buf += &id.to_string();
			return;
		}
		let object = state.object().unwrap();
		let mut map = BTreeMap::new();
		map.insert("host".to_owned(), object.host.clone().into());
		if let Some(executable) = &object.executable {
			map.insert(
				"executable".to_owned(),
				tg::Value::Object(executable.clone().into()),
			);
		}
		map.insert("args".to_owned(), object.args.clone().into());
		map.insert("env".to_owned(), object.env.clone().into());
		if let Some(lock) = &object.lock {
			map.insert("lock".to_owned(), lock.clone().into());
		}
		if let Some(checksum) = &object.checksum {
			map.insert("checksum".to_owned(), checksum.to_string().into());
		}

		self.buf += "tg.target(";
		self.map(&map);
		self.buf.push(')');
	}

	fn bytes(&mut self, v: &Bytes) {
		self.buf += "tg.bytes(\"";
		self.buf += &data_encoding::BASE64.encode(v);
		self.buf += "\")";
	}

	fn path(&mut self, v: &tg::Path) {
		self.buf += "tg.path(";
		self.string(v.as_ref());
		self.buf.push(')');
	}

	fn mutation(&mut self, v: &tg::Mutation) {
		let mut map = BTreeMap::new();
		match v {
			tg::Mutation::Unset => {
				map.insert("kind".to_owned(), tg::Value::String("unset".to_owned()));
			},
			tg::Mutation::Set { value } => {
				map.insert("kind".to_owned(), tg::Value::String("set".to_owned()));
				map.insert("value".to_owned(), value.as_ref().clone());
			},
			tg::Mutation::SetIfUnset { value } => {
				map.insert(
					"kind".to_owned(),
					tg::Value::String("set_if_unset".to_owned()),
				);
				map.insert("value".to_owned(), value.as_ref().clone());
			},
			tg::Mutation::Prepend { values } => {
				map.insert("kind".to_owned(), tg::Value::String("prepend".to_owned()));
				map.insert("values".to_owned(), tg::Value::Array(values.clone()));
			},
			tg::Mutation::Append { values } => {
				map.insert("kind".to_owned(), tg::Value::String("append".to_owned()));
				map.insert("values".to_owned(), tg::Value::Array(values.clone()));
			},
			tg::Mutation::Prefix {
				template,
				separator,
			} => {
				map.insert("kind".to_owned(), tg::Value::String("prefix".to_owned()));
				map.insert("template".to_owned(), tg::Value::Template(template.clone()));
				if let Some(separator) = separator {
					map.insert("separator".to_owned(), tg::Value::String(separator.clone()));
				}
			},
			tg::Mutation::Suffix {
				template,
				separator,
			} => {
				map.insert("kind".to_owned(), tg::Value::String("suffix".to_owned()));
				map.insert("template".to_owned(), tg::Value::Template(template.clone()));
				if let Some(separator) = separator {
					map.insert("separator".to_owned(), tg::Value::String(separator.clone()));
				}
			},
		}
		self.buf += "tg.mutation(";
		self.map(&map);
		self.buf.push(')');
	}

	fn template(&mut self, v: &tg::Template) {
		let components = v
			.components
			.iter()
			.map(|component| match component {
				tg::template::Component::Artifact(artifact) => {
					tg::Value::Object(artifact.clone().into())
				},
				tg::template::Component::String(string) => tg::Value::String(string.clone()),
			})
			.collect();
		self.buf += "tg.template(";
		self.array(&components);
		self.buf.push(')');
	}
}

fn value<'a>(input: &mut Stream<'a>) -> PResult<tg::Value, Error> {
	delimited(whitespace, value_inner, whitespace).parse_next(input)
}

fn value_inner<'a>(input: &mut Stream<'a>) -> PResult<tg::Value, Error> {
	alt((
		null.value(tg::Value::Null),
		bool_.map(tg::Value::Bool),
		number.map(tg::Value::Number),
		string.map(tg::Value::String),
		object.map(tg::Value::Object),
		bytes.map(tg::Value::Bytes),
		path.map(tg::Value::Path),
		mutation.map(tg::Value::Mutation),
		template.map(tg::Value::Template),
		array.map(tg::Value::Array),
		map.map(tg::Value::Map),
	))
	.context("failed to parse tgvn")
	.parse_next(input)
}

fn null<'a>(input: &mut Stream<'a>) -> PResult<(), Error> {
	"null".value(()).parse_next(input)
}

fn bool_<'a>(input: &mut Stream<'a>) -> PResult<bool, Error> {
	let true_ = "true".value(true);
	let false_ = "false".value(false);
	alt((true_, false_)).parse_next(input)
}

fn number<'a>(input: &mut Stream<'a>) -> PResult<f64, Error> {
	winnow::ascii::float.parse_next(input)
}

fn string<'a>(input: &mut Stream<'a>) -> PResult<String, Error> {
	preceded(
		'\"',
		cut_err(terminated(
			repeat(0.., char_).fold(String::new, |mut string, c| {
				string.push(c);
				string
			}),
			'\"',
		)),
	)
	.context("string")
	.parse_next(input)
}

fn char_<'a>(input: &mut Stream<'a>) -> PResult<char, Error> {
	let c = none_of('\"').parse_next(input)?;
	if c == '\\' {
		alt((
			any.verify_map(|c| {
				Some(match c {
					'"' | '\\' | '/' => c,
					'b' => '\x08',
					'f' => '\x0C',
					'n' => '\n',
					'r' => '\r',
					't' => '\t',
					_ => return None,
				})
			}),
			preceded('u', unicode_escape),
		))
		.parse_next(input)
	} else {
		Ok(c)
	}
}

fn unicode_escape<'a>(input: &mut Stream<'a>) -> PResult<char, Error> {
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

fn u16_hex<'a>(input: &mut Stream<'a>) -> PResult<u16, Error> {
	take(4usize)
		.verify_map(|s| u16::from_str_radix(s, 16).ok())
		.parse_next(input)
}

fn id<'a>(input: &mut Stream<'a>) -> PResult<tg::Object, Error> {
	id_v0
		.verify_map(|id| id.try_into().map(tg::Object::with_id).ok())
		.parse_next(input)
}

fn id_v0<'a>(input: &mut Stream<'a>) -> PResult<tg::id::Id, Error> {
	(id_kind, "_", "0", id_body)
		.verify_map(|(kind, _, version, body)| match version {
			"0" => Some(tg::Id::V0(tg::id::V0 { kind, body })),
			_ => None,
		})
		.parse_next(input)
}

fn id_kind<'a>(input: &mut Stream<'a>) -> PResult<tg::id::Kind, Error> {
	alt((
		alt(("lef", "leaf")).value(tg::id::Kind::Leaf),
		alt(("bch", "branch")).value(tg::id::Kind::Branch),
		alt(("dir", "directory")).value(tg::id::Kind::Directory),
		alt(("fil", "file")).value(tg::id::Kind::File),
		alt(("sym", "symlink")).value(tg::id::Kind::Symlink),
		alt(("lok", "lock")).value(tg::id::Kind::Lock),
		alt(("tgt", "target")).value(tg::id::Kind::Target),
		alt(("bld", "build")).value(tg::id::Kind::Build),
		alt(("usr", "user")).value(tg::id::Kind::User),
		alt(("tok", "token")).value(tg::id::Kind::Token),
		alt(("req", "request")).value(tg::id::Kind::Request),
	))
	.parse_next(input)
}

fn id_body<'a>(input: &mut Stream<'a>) -> PResult<tg::id::Body, Error> {
	alt((
		preceded("0", decode_body)
			.verify_map(|bytes| bytes.try_into().map(tg::id::Body::UuidV7).ok()),
		preceded("1", decode_body)
			.verify_map(|bytes| bytes.try_into().map(tg::id::Body::Blake3).ok()),
	))
	.parse_next(input)
}

fn decode_body<'a>(input: &mut Stream<'a>) -> PResult<Vec<u8>, Error> {
	const ENCODING: data_encoding::Encoding = data_encoding_macro::new_encoding! {
		symbols: "0123456789abcdefghjkmnpqrstvwxyz",
	};
	terminated(
		repeat(
			0..,
			one_of(|ch| "0123456789abcdefghjkmnpqrstvwxyz".contains(ch)),
		)
		.fold(String::new, |mut string, c| {
			string.push(c);
			string
		}),
		whitespace,
	)
	.verify_map(|string| ENCODING.decode(string.as_bytes()).ok())
	.context("ID body")
	.parse_next(input)
}

fn array<'a>(input: &mut Stream<'a>) -> PResult<tg::value::Array, Error> {
	preceded(
		('[', whitespace),
		cut_err(terminated(
			separated(0.., value, (whitespace, ',', whitespace)),
			(whitespace, opt(","), whitespace, ']'),
		)),
	)
	.context("array")
	.parse_next(input)
}

fn map<'a>(input: &mut Stream<'a>) -> PResult<tg::value::Map, Error> {
	preceded(
		('{', whitespace),
		cut_err(terminated(
			separated(0.., key_value, (whitespace, ',', whitespace)),
			(whitespace, opt(","), whitespace, '}'),
		)),
	)
	.context("array")
	.parse_next(input)
}

fn key_value<'a>(input: &mut Stream<'a>) -> PResult<(String, tg::Value), Error> {
	separated_pair(string, cut_err((whitespace, ':', whitespace)), value).parse_next(input)
}

fn object<'a>(input: &mut Stream<'a>) -> PResult<tg::Object, Error> {
	alt((id, object_data)).parse_next(input)
}

fn object_data<'a>(input: &mut Stream<'a>) -> PResult<tg::Object, Error> {
	alt((
		leaf.map(|object| tg::object::Object::Leaf(Arc::new(object))),
		branch.map(|object| tg::object::Object::Branch(Arc::new(object))),
		directory.map(|object| tg::object::Object::Directory(Arc::new(object))),
		file.map(|object| tg::object::Object::File(Arc::new(object))),
		symlink.map(|object| tg::object::Object::Symlink(Arc::new(object))),
		lock.map(|object| tg::object::Object::Lock(Arc::new(object))),
		target.map(|object| tg::object::Object::Target(Arc::new(object))),
	))
	.map(tg::Object::with_object)
	.parse_next(input)
}

fn leaf<'a>(input: &mut Stream<'a>) -> PResult<tg::leaf::Object, Error> {
	preceded(
		("tg.leaf", whitespace, "("),
		terminated(bytes_data, (whitespace, ")")),
	)
	.map(|bytes| tg::leaf::Object { bytes })
	.parse_next(input)
}

fn branch<'a>(input: &mut Stream<'a>) -> PResult<tg::branch::Object, Error> {
	preceded(
		("tg.branch", whitespace, "("),
		terminated(branch_data, (whitespace, ")")),
	)
	.parse_next(input)
}

fn branch_data<'a>(input: &mut Stream<'a>) -> PResult<tg::branch::Object, Error> {
	map.verify_map(|map| {
		let children = map.get("children")?.try_unwrap_array_ref().ok()?;
		let mut children_ = Vec::with_capacity(children.len());
		for child in children {
			let child = child.try_unwrap_map_ref().ok()?;
			let blob = match child.get("blob")?.try_unwrap_object_ref().ok()? {
				tg::Object::Leaf(leaf) => tg::Blob::Leaf(leaf.clone()),
				tg::Object::Branch(branch) => tg::Blob::Branch(branch.clone()),
				_ => return None,
			};
			let size = child.get("size")?.try_unwrap_number_ref().ok()?.to_u64()?;
			children_.push(tg::branch::Child { blob, size });
		}
		Some(tg::branch::Object {
			children: children_,
		})
	})
	.parse_next(input)
}

fn directory<'a>(input: &mut Stream<'a>) -> PResult<tg::directory::Object, Error> {
	preceded(
		("tg.directory", whitespace, "("),
		terminated(directory_data, (whitespace, ")")),
	)
	.parse_next(input)
}

fn directory_data<'a>(input: &mut Stream<'a>) -> PResult<tg::directory::Object, Error> {
	map.verify_map(|map| {
		let mut entries = BTreeMap::new();
		for (name, artifact) in map {
			entries.insert(name, artifact.try_into().ok()?);
		}
		Some(tg::directory::Object { entries })
	})
	.parse_next(input)
}

fn file<'a>(input: &mut Stream<'a>) -> PResult<tg::file::Object, Error> {
	preceded(
		("tg.file", whitespace, "("),
		terminated(file_data, (whitespace, ")")),
	)
	.parse_next(input)
}

fn file_data<'a>(input: &mut Stream<'a>) -> PResult<tg::file::Object, Error> {
	map.verify_map(|map| {
		let contents = map.get("contents")?.clone().try_into().ok()?;
		let executable = if let Some(executable) = map.get("executable") {
			*executable.try_unwrap_bool_ref().ok()?
		} else {
			false
		};
		let references = if let Some(references) = map.get("references") {
			let references = references.try_unwrap_array_ref().ok()?;
			let mut references_ = Vec::with_capacity(references.len());
			for reference in references {
				references_.push(reference.clone().try_into().ok()?);
			}
			references_
		} else {
			Vec::new()
		};
		Some(tg::file::Object {
			contents,
			executable,
			references,
		})
	})
	.parse_next(input)
}

fn symlink<'a>(input: &mut Stream<'a>) -> PResult<tg::symlink::Object, Error> {
	preceded(
		("tg.symlink", whitespace, "("),
		terminated(symlink_data, (whitespace, ")")),
	)
	.parse_next(input)
}

fn symlink_data<'a>(input: &mut Stream<'a>) -> PResult<tg::symlink::Object, Error> {
	map.verify_map(|map| {
		let artifact = if let Some(artifact) = map.get("artifact") {
			Some(artifact.clone().try_into().ok()?)
		} else {
			None
		};
		let path = if let Some(path) = map.get("path") {
			Some(path.try_unwrap_path_ref().ok()?.clone())
		} else {
			None
		};
		if path.is_none() && artifact.is_none() {
			return None;
		};
		Some(tg::symlink::Object { artifact, path })
	})
	.parse_next(input)
}

fn lock<'a>(input: &mut Stream<'a>) -> PResult<tg::lock::Object, Error> {
	preceded(
		("tg.lock", whitespace, "("),
		terminated(lock_data, (whitespace, ")")),
	)
	.parse_next(input)
}

fn lock_data<'a>(input: &mut Stream<'a>) -> PResult<tg::lock::Object, Error> {
	map.verify_map(|map| {
		let root = map.get("root")?.try_unwrap_number_ref().ok()?.to_usize()?;
		let nodes = map.get("nodes")?.try_unwrap_array_ref().ok()?;
		let mut nodes_ = Vec::new();
		for node in nodes {
			let node = node.try_unwrap_map_ref().ok()?;
			let dependencies = node.get("dependencies")?.try_unwrap_map_ref().ok()?;
			let mut dependencies_ = BTreeMap::new();
			for (dependency, entry) in dependencies {
				let dependency = dependency.parse().ok()?;
				let entry = entry.try_unwrap_map_ref().ok()?;
				let artifact = if let Some(artifact) = entry.get("artifact") {
					Some(artifact.clone().try_into().ok()?)
				} else {
					None
				};
				let lock = match entry.get("lock")? {
					tg::Value::Number(number) => Either::Left(number.to_usize()?),
					tg::Value::Object(tg::Object::Lock(lock)) => Either::Right(lock.clone()),
					_ => return None,
				};
				let entry = tg::lock::Entry { artifact, lock };
				dependencies_.insert(dependency, entry);
			}
			nodes_.push(tg::lock::Node {
				dependencies: dependencies_,
			});
		}
		Some(tg::lock::Object {
			root,
			nodes: nodes_,
		})
	})
	.parse_next(input)
}

fn target<'a>(input: &mut Stream<'a>) -> PResult<tg::target::Object, Error> {
	preceded(
		("tg.target", whitespace, "("),
		terminated(target_data, (whitespace, ")")),
	)
	.parse_next(input)
}

fn target_data<'a>(input: &mut Stream<'a>) -> PResult<tg::target::Object, Error> {
	map.verify_map(|map| {
		let host = map.get("host")?.try_unwrap_string_ref().ok()?.clone();
		let executable = if let Some(executable) = map.get("executable") {
			Some(
				executable
					.try_unwrap_object_ref()
					.ok()?
					.clone()
					.try_into()
					.ok()?,
			)
		} else {
			None
		};
		let args = map.get("args")?.try_unwrap_array_ref().ok()?.clone();
		let env = map.get("env")?.try_unwrap_map_ref().ok()?.clone();
		let lock = if let Some(lock) = map.get("lock") {
			Some(
				lock.try_unwrap_object_ref()
					.ok()?
					.try_unwrap_lock_ref()
					.ok()?
					.clone(),
			)
		} else {
			None
		};
		let checksum = if let Some(checksum) = map.get("checksum") {
			Some(checksum.try_unwrap_string_ref().ok()?.parse().ok()?)
		} else {
			None
		};
		Some(tg::target::Object {
			host,
			executable,
			args,
			env,
			lock,
			checksum,
		})
	})
	.parse_next(input)
}

fn bytes<'a>(input: &mut Stream<'a>) -> PResult<Bytes, Error> {
	preceded(
		("tg.bytes", whitespace, "("),
		terminated(bytes_data, (whitespace, ")")),
	)
	.parse_next(input)
}

fn bytes_data<'a>(input: &mut Stream<'a>) -> PResult<Bytes, Error> {
	string
		.verify_map(|string| {
			let bytes = data_encoding::BASE64.decode(string.as_bytes()).ok()?;
			Some(bytes.into())
		})
		.parse_next(input)
}

fn path<'a>(input: &mut Stream<'a>) -> PResult<tg::Path, Error> {
	preceded(
		("tg.path", whitespace, "("),
		terminated(
			string.verify_map(|string| string.parse().ok()),
			(whitespace, ")"),
		),
	)
	.parse_next(input)
}

fn mutation<'a>(input: &mut Stream<'a>) -> PResult<tg::Mutation, Error> {
	preceded(
		("tg.mutation", whitespace, "("),
		terminated(
			alt((unset, set, set_if_unset, prepend, append, prefix, suffix)),
			(whitespace, ")"),
		),
	)
	.parse_next(input)
}

fn unset<'a>(input: &mut Stream<'a>) -> PResult<tg::Mutation, Error> {
	map.verify_map(|map| {
		let kind = map.get("kind")?.try_unwrap_string_ref().ok()?;
		(kind == "unset").then_some(tg::Mutation::Unset)
	})
	.parse_next(input)
}

fn set<'a>(input: &mut Stream<'a>) -> PResult<tg::Mutation, Error> {
	map.verify_map(|map| {
		let kind = map.get("kind")?.try_unwrap_string_ref().ok()?;
		let value = map.get("value")?;
		(kind == "set").then_some(tg::Mutation::Set {
			value: Box::new(value.clone()),
		})
	})
	.parse_next(input)
}

fn set_if_unset<'a>(input: &mut Stream<'a>) -> PResult<tg::Mutation, Error> {
	map.verify_map(|map| {
		let kind = map.get("kind")?.try_unwrap_string_ref().ok()?;
		let value = map.get("value")?;
		(kind == "set_if_unset").then_some(tg::Mutation::SetIfUnset {
			value: Box::new(value.clone()),
		})
	})
	.parse_next(input)
}

fn prepend<'a>(input: &mut Stream<'a>) -> PResult<tg::Mutation, Error> {
	map.verify_map(|map| {
		let kind = map.get("kind")?.try_unwrap_string_ref().ok()?;
		let values = map.get("values")?.try_unwrap_array_ref().ok()?.clone();
		(kind == "prepend").then_some(tg::Mutation::Prepend { values })
	})
	.parse_next(input)
}

fn append<'a>(input: &mut Stream<'a>) -> PResult<tg::Mutation, Error> {
	map.verify_map(|map| {
		let kind = map.get("kind")?.try_unwrap_string_ref().ok()?.clone();
		let values = map.get("values")?.try_unwrap_array_ref().ok()?.clone();
		(kind == "append").then_some(tg::Mutation::Append { values })
	})
	.parse_next(input)
}

fn prefix<'a>(input: &mut Stream<'a>) -> PResult<tg::Mutation, Error> {
	map.verify_map(|map| {
		let kind = map.get("kind")?.try_unwrap_string_ref().ok()?;
		let template = map.get("template")?.try_unwrap_template_ref().ok()?.clone();
		let separator = if let Some(seperator) = map.get("separator") {
			Some(seperator.try_unwrap_string_ref().ok()?.clone())
		} else {
			None
		};
		(kind == "prefix").then_some(tg::Mutation::Prefix {
			template,
			separator,
		})
	})
	.parse_next(input)
}

fn suffix<'a>(input: &mut Stream<'a>) -> PResult<tg::Mutation, Error> {
	map.verify_map(|map| {
		let kind = map.get("kind")?.try_unwrap_string_ref().ok()?;
		let template = map.get("template")?.try_unwrap_template_ref().ok()?.clone();
		let separator = if let Some(seperator) = map.get("separator") {
			Some(seperator.try_unwrap_string_ref().ok()?.clone())
		} else {
			None
		};
		(kind == "suffix").then_some(tg::Mutation::Suffix {
			template,
			separator,
		})
	})
	.parse_next(input)
}

fn template<'a>(input: &mut Stream<'a>) -> PResult<tg::Template, Error> {
	preceded(
		("tg.template", whitespace, "("),
		terminated(array, (whitespace, ")")),
	)
	.verify_map(|components_| {
		let mut components = Vec::with_capacity(components_.len());
		for component in components_ {
			let component = match component {
				tg::Value::String(string) => tg::template::Component::String(string),
				tg::Value::Object(tg::Object::Directory(artifact)) => {
					tg::template::Component::Artifact(artifact.into())
				},
				tg::Value::Object(tg::Object::File(artifact)) => {
					tg::template::Component::Artifact(artifact.into())
				},
				tg::Value::Object(tg::Object::Symlink(artifact)) => {
					tg::template::Component::Artifact(artifact.into())
				},
				_ => return None,
			};
			components.push(component);
		}
		Some(tg::Template { components })
	})
	.parse_next(input)
}

fn whitespace<'a>(input: &mut Stream<'a>) -> PResult<(), Error> {
	const WHITE_SPACE: &[char] = &[' ', '\t', '\r', '\n'];
	take_while(0.., WHITE_SPACE).map(|_| ()).parse_next(input)
}

#[cfg(test)]
mod tests {
	use crate as tg;
	use std::collections::BTreeMap;

	#[test]
	fn atoms() {
		assert!("null"
			.parse::<tg::Value>()
			.unwrap()
			.try_unwrap_null_ref()
			.is_ok());
		assert_eq!("true".parse::<tg::Value>().unwrap().unwrap_bool(), true);
		assert_eq!("false".parse::<tg::Value>().unwrap().unwrap_bool(), false);
		assert_eq!(
			"1.2345".parse::<tg::Value>().unwrap().unwrap_number(),
			1.2345
		);
		assert_eq!(
			"\"hello!\"".parse::<tg::Value>().unwrap().unwrap_string(),
			"hello!"
		);
		assert!("tgt_019jdv3vdj2pz90ajb6r74haf9bchcx3sz5dkadr30em2h8jdx0bqg"
			.parse::<tg::Value>()
			.unwrap()
			.try_unwrap_object_ref()
			.is_ok());
	}

	#[test]
	fn array() {
		assert!("[]"
			.parse::<tg::Value>()
			.unwrap()
			.try_unwrap_array_ref()
			.is_ok());
		assert!("[[], [null]]"
			.parse::<tg::Value>()
			.unwrap()
			.try_unwrap_array_ref()
			.is_ok());
		assert!("[1]"
			.parse::<tg::Value>()
			.unwrap()
			.try_unwrap_array_ref()
			.is_ok());
		assert!("[ 1, 2, 3 ]"
			.parse::<tg::Value>()
			.unwrap()
			.try_unwrap_array_ref()
			.is_ok());
		assert!("[true,]".parse::<tg::Value>().is_ok());
	}

	#[test]
	fn map() {
		assert!("{}"
			.parse::<tg::Value>()
			.unwrap()
			.try_unwrap_map_ref()
			.is_ok());
		assert!(r#"{ "key": "value" }"#.parse::<tg::Value>().unwrap().try_unwrap_map_ref().is_ok());
		assert!(
			r#"{ "key": { "key": {} }}"#.parse::<tg::Value>().unwrap().try_unwrap_map_ref().is_ok()
		);
		assert!(r#"{ "key": true, }"#.parse::<tg::Value>().is_ok());
	}

	#[test]
	fn example() {
		let text = r#"
		{
			"bool": true,
			"string": "value",
			"array": [
				"this",
				"that"
			]
		}
		"#;
		let _value: tg::Value = text.parse().unwrap();
	}

	#[test]
	fn to_tgvn_string() {
		let bytes = b"1234";
		let value = tg::Value::Map(
			[
				("bool".to_owned(), tg::Value::Bool(true)),
				("number".to_owned(), tg::Value::Number(1.2345)),
				("bytes".to_owned(), tg::Value::Bytes(bytes.to_vec().into())),
				(
					"string".to_owned(),
					tg::Value::String("a string".to_owned()),
				),
				(
					"array".to_owned(),
					tg::Value::Array(vec![tg::Value::String("array member".into())]),
				),
				(
					"target".to_owned(),
					tg::Value::Object(
						tg::Target::with_id(
							"tgt_019jdv3vdj2pz90ajb6r74haf9bchcx3sz5dkadr30em2h8jdx0bqg"
								.parse()
								.unwrap(),
						)
						.into(),
					),
				),
			]
			.into_iter()
			.collect(),
		);
		let tgvn = value.to_tgvn_string();
		eprintln!("{tgvn}");
		let value: tg::Value = tgvn.parse().unwrap();
		let tgvn = value.to_tgvn_string_pretty();
		eprintln!("{tgvn}");
	}

	#[test]
	fn leaf() {
		let object = tg::leaf::Object {
			bytes: b"12345".to_vec().into(),
		};
		let value = tg::Value::Object(tg::Leaf::with_object(object).into());
		let string = value.to_tgvn_string();
		eprintln!("parsing {string:#?}");
		let value = string.parse::<tg::Value>().unwrap();
		assert!(matches!(value, tg::Value::Object(tg::Object::Leaf(_))));
	}

	#[test]
	fn branch() {
		let object = tg::branch::Object {
			children: Vec::new(),
		};
		let value = tg::Value::Object(tg::Branch::with_object(object).into());
		let string = value.to_tgvn_string();
		let value = string.parse::<tg::Value>().unwrap();
		assert!(matches!(value, tg::Value::Object(tg::Object::Branch(_))));
	}

	#[test]
	fn directory() {
		let object = tg::directory::Object {
			entries: BTreeMap::new(),
		};
		let value = tg::Value::Object(tg::Directory::with_object(object).into());
		let string = value.to_tgvn_string();
		let value = string.parse::<tg::Value>().unwrap();
		assert!(matches!(value, tg::Value::Object(tg::Object::Directory(_))));
	}
	#[test]
	fn file() {
		let object = tg::file::Object {
			contents: tg::Leaf::with_object(tg::leaf::Object {
				bytes: b"hello, world!\n".to_vec().into(),
			})
			.into(),
			executable: false,
			references: Vec::new(),
		};
		let value = tg::Value::Object(tg::File::with_object(object).into());
		let string = value.to_tgvn_string();
		let value = string.parse::<tg::Value>().unwrap();
		assert!(matches!(value, tg::Value::Object(tg::Object::File(_))));
	}

	#[test]
	fn symlink() {
		let object = tg::symlink::Object {
			artifact: Some(
				tg::Directory::with_object(tg::directory::Object {
					entries: BTreeMap::new(),
				})
				.into(),
			),
			path: Some("sub/path".into()),
		};
		let value = tg::Value::Object(tg::Symlink::with_object(object).into());
		let string = value.to_tgvn_string();
		let value = string.parse::<tg::Value>().unwrap();
		assert!(matches!(value, tg::Value::Object(tg::Object::Symlink(_))));
	}

	#[test]
	fn lock() {
		let object = tg::lock::Object {
			root: 0,
			nodes: vec![tg::lock::Node {
				dependencies: BTreeMap::new(),
			}],
		};
		let value = tg::Value::Object(tg::Lock::with_object(object).into());
		let string = value.to_tgvn_string();
		let value = string.parse::<tg::Value>().unwrap();
		assert!(matches!(value, tg::Value::Object(tg::Object::Lock(_))));
	}

	#[test]
	fn target() {
		let object = tg::target::Object {
			host: "js".into(),
			env: BTreeMap::new(),
			executable: None,
			args: Vec::new(),
			lock: None,
			checksum: None,
		};
		let value = tg::Value::Object(tg::Target::with_object(object).into());
		let string = value.to_tgvn_string();
		let value = string.parse::<tg::Value>().unwrap();
		assert!(matches!(value, tg::Value::Object(tg::Object::Target(_))));
	}

	#[test]
	fn path() {
		let value = tg::Value::Path("path/to/thing".into());
		let string = value.to_tgvn_string();
		let value = string.parse::<tg::Value>().unwrap();
		assert!(matches!(value, tg::Value::Path(_)));
	}

	#[test]
	fn bytes() {
		let value = tg::Value::Bytes(b"1234".to_vec().into());
		let string = value.to_tgvn_string();
		let value = string.parse::<tg::Value>().unwrap();
		assert!(matches!(value, tg::Value::Bytes(_)));
	}

	#[test]
	fn mutation_set() {
		let mutation = tg::Mutation::Set {
			value: Box::new("value".to_owned().into()),
		};
		let value = tg::Value::Mutation(mutation);
		let string = value.to_tgvn_string();
		let value = string.parse::<tg::Value>().unwrap();
		assert!(matches!(
			value,
			tg::Value::Mutation(tg::Mutation::Set { .. })
		))
	}

	#[test]
	fn mutation_unset() {
		let mutation = tg::Mutation::Unset;
		let value = tg::Value::Mutation(mutation);
		let string = value.to_tgvn_string();
		let value = string.parse::<tg::Value>().unwrap();
		assert!(matches!(value, tg::Value::Mutation(tg::Mutation::Unset)));
	}

	#[test]
	fn mutation_set_if_unset() {
		let mutation = tg::Mutation::SetIfUnset {
			value: Box::new("hello".to_owned().into()),
		};
		let value = tg::Value::Mutation(mutation);
		let string = value.to_tgvn_string();
		let value = string.parse::<tg::Value>().unwrap();
		assert!(matches!(
			value,
			tg::Value::Mutation(tg::Mutation::SetIfUnset { .. })
		))
	}

	#[test]
	fn mutation_prepend() {
		let mutation = tg::Mutation::Prepend { values: Vec::new() };
		let value = tg::Value::Mutation(mutation);
		let string = value.to_tgvn_string();
		let value = string.parse::<tg::Value>().unwrap();
		assert!(matches!(
			value,
			tg::Value::Mutation(tg::Mutation::Prepend { .. })
		))
	}

	#[test]
	fn mutation_append() {
		let mutation = tg::Mutation::Append { values: Vec::new() };
		let value = tg::Value::Mutation(mutation);
		let string = value.to_tgvn_string();
		let value = string.parse::<tg::Value>().unwrap();
		assert!(matches!(
			value,
			tg::Value::Mutation(tg::Mutation::Append { .. })
		))
	}

	#[test]
	fn mutation_prefix() {
		let mutation = tg::Mutation::Prefix {
			template: tg::Template {
				components: Vec::new(),
			},
			separator: Some(",".to_owned()),
		};
		let value = tg::Value::Mutation(mutation);
		let string = value.to_tgvn_string();
		eprintln!("string: {string:#?}");
		let value = string.parse::<tg::Value>().unwrap();
		assert!(matches!(
			value,
			tg::Value::Mutation(tg::Mutation::Prefix { .. })
		))
	}

	#[test]
	fn mutation_suffix() {
		let mutation = tg::Mutation::Suffix {
			template: tg::Template {
				components: Vec::new(),
			},
			separator: Some(",".to_owned()),
		};
		let value = tg::Value::Mutation(mutation);
		let string = value.to_tgvn_string();
		let value = string.parse::<tg::Value>().unwrap();
		assert!(matches!(
			value,
			tg::Value::Mutation(tg::Mutation::Suffix { .. })
		))
	}

	#[test]
	fn template() {
		let value = tg::Value::Template(tg::template::Template { components: vec![] });
		let string = value.to_tgvn_string();
		let value = string.parse::<tg::Value>().unwrap();
		assert!(matches!(value, tg::Value::Template(_)));
	}
}
