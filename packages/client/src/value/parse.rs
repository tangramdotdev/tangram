use crate as tg;
use bytes::Bytes;
use either::Either;
use num::ToPrimitive;
use std::{collections::BTreeMap, sync::Arc};
use winnow::{
	combinator::{alt, cut_err, delimited, opt, preceded, repeat, separated, separated_pair},
	error::{AddContext, ErrorKind, ParserError},
	stream::Location as _,
	token::{any, none_of, one_of, take, take_while},
	PResult, Parser,
};

#[derive(Clone, Debug, derive_more::Error)]
pub struct Error {
	location: usize,
	message: String,
}

type Input<'a> = winnow::stream::Located<&'a str>;

pub fn parse(input: &str) -> tg::Result<tg::Value> {
	let mut input = Input::new(input);
	let value = value(&mut input).map_err(|error| {
		let source = error.into_inner().unwrap();
		tg::error!(!source, "failed to parse the value")
	})?;
	Ok(value)
}

fn value(input: &mut Input) -> PResult<tg::Value, Error> {
	delimited(whitespace, value_inner, whitespace).parse_next(input)
}

fn value_inner(input: &mut Input) -> PResult<tg::Value, Error> {
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
	.parse_next(input)
}

fn null(input: &mut Input) -> PResult<(), Error> {
	"null".value(()).parse_next(input)
}

fn bool_(input: &mut Input) -> PResult<bool, Error> {
	let true_ = "true".value(true);
	let false_ = "false".value(false);
	alt((true_, false_)).parse_next(input)
}

fn number(input: &mut Input) -> PResult<f64, Error> {
	winnow::ascii::float.parse_next(input)
}

fn string(input: &mut Input) -> PResult<String, Error> {
	delimited(
		'\"',
		cut_err(repeat(0.., char_).fold(String::new, |mut string, c| {
			string.push(c);
			string
		})),
		'\"',
	)
	.parse_next(input)
}

fn char_(input: &mut Input) -> PResult<char, Error> {
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

fn unicode_escape(input: &mut Input) -> PResult<char, Error> {
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

fn u16_hex(input: &mut Input) -> PResult<u16, Error> {
	take(4usize)
		.verify_map(|s| u16::from_str_radix(s, 16).ok())
		.parse_next(input)
}

fn object_id(input: &mut Input) -> PResult<tg::Object, Error> {
	id_v0
		.verify_map(|id| id.try_into().map(tg::Object::with_id).ok())
		.parse_next(input)
}

fn id_v0(input: &mut Input) -> PResult<tg::id::Id, Error> {
	(id_kind, "_", "0", id_body)
		.verify_map(|(kind, _, version, body)| match version {
			"0" => Some(tg::Id::V0(tg::id::V0 { kind, body })),
			_ => None,
		})
		.parse_next(input)
}

fn id_kind(input: &mut Input) -> PResult<tg::id::Kind, Error> {
	alt((
		alt(("lef", "leaf")).value(tg::id::Kind::Leaf),
		alt(("bch", "branch")).value(tg::id::Kind::Branch),
		alt(("dir", "directory")).value(tg::id::Kind::Directory),
		alt(("fil", "file")).value(tg::id::Kind::File),
		alt(("sym", "symlink")).value(tg::id::Kind::Symlink),
		alt(("pkg", "package")).value(tg::id::Kind::Package),
		alt(("tgt", "target")).value(tg::id::Kind::Target),
		alt(("bld", "build")).value(tg::id::Kind::Build),
		alt(("usr", "user")).value(tg::id::Kind::User),
		alt(("tok", "token")).value(tg::id::Kind::Token),
		alt(("req", "request")).value(tg::id::Kind::Request),
	))
	.parse_next(input)
}

fn id_body(input: &mut Input) -> PResult<tg::id::Body, Error> {
	alt((
		preceded("0", decode_body)
			.verify_map(|bytes| bytes.try_into().map(tg::id::Body::UuidV7).ok()),
		preceded("1", decode_body)
			.verify_map(|bytes| bytes.try_into().map(tg::id::Body::Blake3).ok()),
	))
	.parse_next(input)
}

fn decode_body(input: &mut Input) -> PResult<Vec<u8>, Error> {
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

fn array(input: &mut Input) -> PResult<tg::value::Array, Error> {
	delimited(
		(whitespace, '[', whitespace),
		cut_err(separated(0.., value, (whitespace, ',', whitespace))),
		(whitespace, opt(","), whitespace, ']', whitespace),
	)
	.parse_next(input)
}

fn map(input: &mut Input) -> PResult<tg::value::Map, Error> {
	delimited(
		(whitespace, '[', whitespace),
		cut_err(separated(0.., key_value, (whitespace, ',', whitespace))),
		(whitespace, opt(","), whitespace, ']', whitespace),
	)
	.parse_next(input)
}

fn key_value(input: &mut Input) -> PResult<(String, tg::Value), Error> {
	separated_pair(string, cut_err((whitespace, ':', whitespace)), value).parse_next(input)
}

fn object(input: &mut Input) -> PResult<tg::Object, Error> {
	alt((object_id, object_data)).parse_next(input)
}

fn object_data(input: &mut Input) -> PResult<tg::Object, Error> {
	alt((
		leaf.map(|object| tg::object::Object::Leaf(Arc::new(object))),
		branch.map(|object| tg::object::Object::Branch(Arc::new(object))),
		directory.map(|object| tg::object::Object::Directory(Arc::new(object))),
		file.map(|object| tg::object::Object::File(Arc::new(object))),
		symlink.map(|object| tg::object::Object::Symlink(Arc::new(object))),
		package.map(|object| tg::object::Object::Package(Arc::new(object))),
		target.map(|object| tg::object::Object::Target(Arc::new(object))),
	))
	.map(tg::Object::with_object)
	.parse_next(input)
}

fn leaf(input: &mut Input) -> PResult<tg::leaf::Object, Error> {
	delimited(
		("tg.leaf", whitespace, "(", whitespace),
		bytes_data,
		(whitespace, ")", whitespace),
	)
	.map(|bytes| tg::leaf::Object { bytes })
	.parse_next(input)
}

fn branch(input: &mut Input) -> PResult<tg::branch::Object, Error> {
	delimited(
		("tg.branch", whitespace, "(", whitespace),
		branch_data,
		(whitespace, ")", whitespace),
	)
	.parse_next(input)
}

fn branch_data(input: &mut Input) -> PResult<tg::branch::Object, Error> {
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

fn directory(input: &mut Input) -> PResult<tg::directory::Object, Error> {
	delimited(
		("tg.directory", whitespace, "(", whitespace),
		directory_data,
		(whitespace, ")", whitespace),
	)
	.parse_next(input)
}

fn directory_data(input: &mut Input) -> PResult<tg::directory::Object, Error> {
	map.verify_map(|map| {
		let mut entries = BTreeMap::new();
		for (name, artifact) in map {
			entries.insert(name, artifact.try_into().ok()?);
		}
		Some(tg::directory::Object { entries })
	})
	.parse_next(input)
}

fn file(input: &mut Input) -> PResult<tg::file::Object, Error> {
	delimited(
		("tg.file", whitespace, "(", whitespace),
		file_data,
		(whitespace, ")", whitespace),
	)
	.parse_next(input)
}

fn file_data(input: &mut Input) -> PResult<tg::file::Object, Error> {
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
			dependencies: references,
		})
	})
	.parse_next(input)
}

fn symlink(input: &mut Input) -> PResult<tg::symlink::Object, Error> {
	delimited(
		("tg.symlink", whitespace, "(", whitespace),
		symlink_data,
		(whitespace, ")", whitespace),
	)
	.parse_next(input)
}

fn symlink_data(input: &mut Input) -> PResult<tg::symlink::Object, Error> {
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

fn package(input: &mut Input) -> PResult<tg::package::Object, Error> {
	delimited(
		("tg.package", whitespace, "(", whitespace),
		package_data,
		(whitespace, ")", whitespace),
	)
	.parse_next(input)
}

fn package_data(input: &mut Input) -> PResult<tg::package::Object, Error> {
	map.verify_map(|map| {
		let mut nodes = Vec::new();
		let nodes_ = map.get("nodes")?.try_unwrap_array_ref().ok()?;
		for node in nodes_ {
			let node = node.try_unwrap_map_ref().ok()?;
			let mut dependencies = BTreeMap::new();
			let dependencies_ = node.get("dependencies")?.try_unwrap_map_ref().ok()?;
			for (dependency, object) in dependencies_ {
				let dependency = dependency.parse().ok()?;
				let object = match object {
					tg::Value::Null => None,
					tg::Value::Number(number) => Some(Either::Left(number.to_usize()?)),
					tg::Value::Object(object) => Some(Either::Right(object.clone())),
					_ => return None,
				};
				dependencies.insert(dependency, object);
			}
			let object = if let Some(object) = node.get("object") {
				Some(object.clone().try_into().ok()?)
			} else {
				None
			};
			let metadata = if let Some(metadata) = node.get("metadata") {
				metadata.clone().try_into().ok()?
			} else {
				BTreeMap::new()
			};
			nodes.push(tg::package::Node {
				dependencies,
				metadata,
				object,
			});
		}
		let root = map.get("root")?.try_unwrap_number_ref().ok()?.to_usize()?;
		Some(tg::package::Object { nodes, root })
	})
	.parse_next(input)
}

fn target(input: &mut Input) -> PResult<tg::target::Object, Error> {
	delimited(
		("tg.target", whitespace, "(", whitespace),
		target_data,
		(whitespace, ")", whitespace),
	)
	.parse_next(input)
}

fn target_data(input: &mut Input) -> PResult<tg::target::Object, Error> {
	map.verify_map(|map| {
		let args = if let Some(args) = map.get("args") {
			args.try_unwrap_array_ref().ok()?.clone()
		} else {
			Vec::new()
		};
		let checksum = if let Some(checksum) = map.get("checksum") {
			Some(checksum.try_unwrap_string_ref().ok()?.parse().ok()?)
		} else {
			None
		};
		let env = if let Some(env) = map.get("env") {
			env.try_unwrap_map_ref().ok()?.clone()
		} else {
			BTreeMap::new()
		};
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
		let host = map.get("host")?.try_unwrap_string_ref().ok()?.clone();
		Some(tg::target::Object {
			args,
			checksum,
			env,
			executable,
			host,
		})
	})
	.parse_next(input)
}

fn bytes(input: &mut Input) -> PResult<Bytes, Error> {
	delimited(
		("tg.bytes", whitespace, "(", whitespace),
		bytes_data,
		(whitespace, ")", whitespace),
	)
	.parse_next(input)
}

fn bytes_data(input: &mut Input) -> PResult<Bytes, Error> {
	string
		.verify_map(|string| {
			let bytes = data_encoding::BASE64.decode(string.as_bytes()).ok()?;
			Some(bytes.into())
		})
		.parse_next(input)
}

fn path(input: &mut Input) -> PResult<tg::Path, Error> {
	delimited(
		("tg.path", whitespace, "(", whitespace),
		string.verify_map(|string| string.parse().ok()),
		(whitespace, ")", whitespace),
	)
	.parse_next(input)
}

fn mutation(input: &mut Input) -> PResult<tg::Mutation, Error> {
	delimited(
		("tg.mutation", whitespace, "(", whitespace),
		alt((unset, set, set_if_unset, prepend, append, prefix, suffix)),
		(whitespace, ")", whitespace),
	)
	.parse_next(input)
}

fn unset(input: &mut Input) -> PResult<tg::Mutation, Error> {
	map.verify_map(|map| {
		let kind = map.get("kind")?.try_unwrap_string_ref().ok()?;
		(kind == "unset").then_some(tg::Mutation::Unset)
	})
	.parse_next(input)
}

fn set(input: &mut Input) -> PResult<tg::Mutation, Error> {
	map.verify_map(|map| {
		let kind = map.get("kind")?.try_unwrap_string_ref().ok()?;
		let value = map.get("value")?;
		(kind == "set").then_some(tg::Mutation::Set {
			value: Box::new(value.clone()),
		})
	})
	.parse_next(input)
}

fn set_if_unset(input: &mut Input) -> PResult<tg::Mutation, Error> {
	map.verify_map(|map| {
		let kind = map.get("kind")?.try_unwrap_string_ref().ok()?;
		let value = map.get("value")?;
		(kind == "set_if_unset").then_some(tg::Mutation::SetIfUnset {
			value: Box::new(value.clone()),
		})
	})
	.parse_next(input)
}

fn prepend(input: &mut Input) -> PResult<tg::Mutation, Error> {
	map.verify_map(|map| {
		let kind = map.get("kind")?.try_unwrap_string_ref().ok()?;
		let values = map.get("values")?.try_unwrap_array_ref().ok()?.clone();
		(kind == "prepend").then_some(tg::Mutation::Prepend { values })
	})
	.parse_next(input)
}

fn append(input: &mut Input) -> PResult<tg::Mutation, Error> {
	map.verify_map(|map| {
		let kind = map.get("kind")?.try_unwrap_string_ref().ok()?.clone();
		let values = map.get("values")?.try_unwrap_array_ref().ok()?.clone();
		(kind == "append").then_some(tg::Mutation::Append { values })
	})
	.parse_next(input)
}

fn prefix(input: &mut Input) -> PResult<tg::Mutation, Error> {
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

fn suffix(input: &mut Input) -> PResult<tg::Mutation, Error> {
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

fn template(input: &mut Input) -> PResult<tg::Template, Error> {
	delimited(
		("tg.template", whitespace, "(", whitespace),
		array,
		(whitespace, ")"),
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

fn whitespace(input: &mut Input) -> PResult<(), Error> {
	const WHITE_SPACE: &[char] = &[' ', '\t', '\r', '\n'];
	take_while(0.., WHITE_SPACE).map(|_| ()).parse_next(input)
}

impl std::fmt::Display for Error {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(
			f,
			"parse error at character {}: {}",
			self.location, self.message
		)
	}
}

impl<'a, C> AddContext<Input<'a>, C> for Error
where
	C: std::fmt::Display,
{
	fn add_context(
		mut self,
		input: &Input<'a>,
		_token_start: &<Input<'a> as winnow::stream::Stream>::Checkpoint,
		context: C,
	) -> Self {
		self.location = input.location();
		self.message = context.to_string();
		self
	}
}

impl<'a> ParserError<Input<'a>> for Error {
	fn from_error_kind(input: &Input<'a>, _kind: winnow::error::ErrorKind) -> Self {
		Error {
			location: input.location(),
			message: String::new(),
		}
	}

	fn append(
		self,
		_input: &Input<'a>,
		_token_start: &<Input<'a> as winnow::stream::Stream>::Checkpoint,
		_kind: ErrorKind,
	) -> Self {
		self
	}
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
		assert!("true".parse::<tg::Value>().unwrap().unwrap_bool());
		assert!(!"false".parse::<tg::Value>().unwrap().unwrap_bool());
		assert!(
			("1.2345".parse::<tg::Value>().unwrap().unwrap_number() - 1.2345).abs() < f64::EPSILON
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
		text.parse::<tg::Value>().unwrap();
	}

	#[test]
	fn to_string() {
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
		let string = value.to_string();
		eprintln!("{string}");
		let value: tg::Value = string.parse().unwrap();
		let string = value.to_string_pretty();
		eprintln!("{string}");
	}

	#[test]
	fn leaf() {
		let object = tg::leaf::Object {
			bytes: b"12345".to_vec().into(),
		};
		let value = tg::Value::Object(tg::Leaf::with_object(object).into());
		let string = value.to_string();
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
		let string = value.to_string();
		let value = string.parse::<tg::Value>().unwrap();
		assert!(matches!(value, tg::Value::Object(tg::Object::Branch(_))));
	}

	#[test]
	fn directory() {
		let object = tg::directory::Object {
			entries: BTreeMap::new(),
		};
		let value = tg::Value::Object(tg::Directory::with_object(object).into());
		let string = value.to_string();
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
			dependencies: Vec::new(),
		};
		let value = tg::Value::Object(tg::File::with_object(object).into());
		let string = value.to_string();
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
		let string = value.to_string();
		let value = string.parse::<tg::Value>().unwrap();
		assert!(matches!(value, tg::Value::Object(tg::Object::Symlink(_))));
	}

	#[test]
	fn package() {
		let object = tg::package::Object {
			root: 0,
			nodes: vec![tg::package::Node {
				dependencies: BTreeMap::new(),
				metadata: BTreeMap::new(),
				object: None,
			}],
		};
		let value = tg::Value::Object(tg::Package::with_object(object).into());
		let string = value.to_string();
		let value = string.parse::<tg::Value>().unwrap();
		assert!(matches!(value, tg::Value::Object(tg::Object::Package(_))));
	}

	#[test]
	fn target() {
		let object = tg::target::Object {
			args: Vec::new(),
			checksum: None,
			env: BTreeMap::new(),
			executable: None,
			host: "js".into(),
		};
		let value = tg::Value::Object(tg::Target::with_object(object).into());
		let string = value.to_string();
		let value = string.parse::<tg::Value>().unwrap();
		assert!(matches!(value, tg::Value::Object(tg::Object::Target(_))));
	}

	#[test]
	fn path() {
		let value = tg::Value::Path("path/to/thing".into());
		let string = value.to_string();
		let value = string.parse::<tg::Value>().unwrap();
		assert!(matches!(value, tg::Value::Path(_)));
	}

	#[test]
	fn bytes() {
		let value = tg::Value::Bytes(b"1234".to_vec().into());
		let string = value.to_string();
		let value = string.parse::<tg::Value>().unwrap();
		assert!(matches!(value, tg::Value::Bytes(_)));
	}

	#[test]
	fn mutation_set() {
		let mutation = tg::Mutation::Set {
			value: Box::new("value".to_owned().into()),
		};
		let value = tg::Value::Mutation(mutation);
		let string = value.to_string();
		let value = string.parse::<tg::Value>().unwrap();
		assert!(matches!(
			value,
			tg::Value::Mutation(tg::Mutation::Set { .. })
		));
	}

	#[test]
	fn mutation_unset() {
		let mutation = tg::Mutation::Unset;
		let value = tg::Value::Mutation(mutation);
		let string = value.to_string();
		let value = string.parse::<tg::Value>().unwrap();
		assert!(matches!(value, tg::Value::Mutation(tg::Mutation::Unset)));
	}

	#[test]
	fn mutation_set_if_unset() {
		let mutation = tg::Mutation::SetIfUnset {
			value: Box::new("hello".to_owned().into()),
		};
		let value = tg::Value::Mutation(mutation);
		let string = value.to_string();
		let value = string.parse::<tg::Value>().unwrap();
		assert!(matches!(
			value,
			tg::Value::Mutation(tg::Mutation::SetIfUnset { .. })
		));
	}

	#[test]
	fn mutation_prepend() {
		let mutation = tg::Mutation::Prepend { values: Vec::new() };
		let value = tg::Value::Mutation(mutation);
		let string = value.to_string();
		let value = string.parse::<tg::Value>().unwrap();
		assert!(matches!(
			value,
			tg::Value::Mutation(tg::Mutation::Prepend { .. })
		));
	}

	#[test]
	fn mutation_append() {
		let mutation = tg::Mutation::Append { values: Vec::new() };
		let value = tg::Value::Mutation(mutation);
		let string = value.to_string();
		let value = string.parse::<tg::Value>().unwrap();
		assert!(matches!(
			value,
			tg::Value::Mutation(tg::Mutation::Append { .. })
		));
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
		let string = value.to_string();
		eprintln!("string: {string:#?}");
		let value = string.parse::<tg::Value>().unwrap();
		assert!(matches!(
			value,
			tg::Value::Mutation(tg::Mutation::Prefix { .. })
		));
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
		let string = value.to_string();
		let value = string.parse::<tg::Value>().unwrap();
		assert!(matches!(
			value,
			tg::Value::Mutation(tg::Mutation::Suffix { .. })
		));
	}

	#[test]
	fn template() {
		let value = tg::Value::Template(tg::template::Template { components: vec![] });
		let string = value.to_string();
		let value = string.parse::<tg::Value>().unwrap();
		assert!(matches!(value, tg::Value::Template(_)));
	}
}
