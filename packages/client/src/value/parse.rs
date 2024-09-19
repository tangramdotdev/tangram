use crate as tg;
use bytes::Bytes;
use num::ToPrimitive;
use std::{collections::BTreeMap, sync::Arc};
use tangram_either::Either;
use winnow::{
	ascii::float,
	combinator::{alt, cut_err, delimited, opt, preceded, repeat, separated, separated_pair},
	prelude::*,
	token::{any, none_of, one_of, take, take_while},
};

type Input<'a> = winnow::stream::Located<&'a str>;

pub fn parse(input: &str) -> tg::Result<tg::Value> {
	value(&mut Input::new(input))
		.map_err(|error| tg::error!("{}", error.into_inner().unwrap().to_string()))
}

fn value(input: &mut Input) -> PResult<tg::Value> {
	delimited(whitespace, value_inner, whitespace).parse_next(input)
}

fn value_inner(input: &mut Input) -> PResult<tg::Value> {
	alt((
		null.value(tg::Value::Null),
		bool_.map(tg::Value::Bool),
		number.map(tg::Value::Number),
		string.map(tg::Value::String),
		array.map(tg::Value::Array),
		map.map(tg::Value::Map),
		object.map(tg::Value::Object),
		bytes.map(tg::Value::Bytes),
		mutation.map(tg::Value::Mutation),
		template.map(tg::Value::Template),
	))
	.parse_next(input)
}

fn null(input: &mut Input) -> PResult<()> {
	"null".value(()).parse_next(input)
}

fn bool_(input: &mut Input) -> PResult<bool> {
	alt(("true".value(true), "false".value(false))).parse_next(input)
}

fn number(input: &mut Input) -> PResult<f64> {
	float.parse_next(input)
}

fn string(input: &mut Input) -> PResult<String> {
	delimited(
		'"',
		cut_err(repeat(0.., char_).fold(String::new, |mut s, c| {
			s.push(c);
			s
		})),
		'"',
	)
	.parse_next(input)
}

fn char_(input: &mut Input) -> PResult<char> {
	let c = none_of('\"').parse_next(input)?;
	if c == '\\' {
		alt((
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
		.parse_next(input)
	} else {
		Ok(c)
	}
}

fn unicode_escape(input: &mut Input) -> PResult<char> {
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

fn u16_hex(input: &mut Input) -> PResult<u16> {
	take(4usize)
		.verify_map(|s| u16::from_str_radix(s, 16).ok())
		.parse_next(input)
}

fn id(input: &mut Input) -> PResult<tg::id::Id> {
	(id_kind, "_", "0", id_body)
		.verify_map(|(kind, _, version, body)| match version {
			"0" => Some(tg::Id::V0(tg::id::V0 { kind, body })),
			_ => None,
		})
		.parse_next(input)
}

fn id_kind(input: &mut Input) -> PResult<tg::id::Kind> {
	alt((
		alt(("lef", "leaf")).value(tg::id::Kind::Leaf),
		alt(("bch", "branch")).value(tg::id::Kind::Branch),
		alt(("dir", "directory")).value(tg::id::Kind::Directory),
		alt(("fil", "file")).value(tg::id::Kind::File),
		alt(("sym", "symlink")).value(tg::id::Kind::Symlink),
		alt(("gph", "graph")).value(tg::id::Kind::Graph),
		alt(("tgt", "target")).value(tg::id::Kind::Target),
		alt(("bld", "build")).value(tg::id::Kind::Build),
		alt(("usr", "user")).value(tg::id::Kind::User),
		alt(("tok", "token")).value(tg::id::Kind::Token),
		alt(("req", "request")).value(tg::id::Kind::Request),
	))
	.parse_next(input)
}

fn id_body(input: &mut Input) -> PResult<tg::id::Body> {
	alt((
		preceded("0", id_body_inner)
			.verify_map(|bytes| bytes.try_into().map(tg::id::Body::UuidV7).ok()),
		preceded("1", id_body_inner)
			.verify_map(|bytes| bytes.try_into().map(tg::id::Body::Blake3).ok()),
	))
	.parse_next(input)
}

fn id_body_inner(input: &mut Input) -> PResult<Vec<u8>> {
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

fn array(input: &mut Input) -> PResult<tg::value::Array> {
	delimited(
		('[', whitespace),
		cut_err(separated(0.., value, (whitespace, ',', whitespace))),
		(whitespace, opt(","), whitespace, ']'),
	)
	.parse_next(input)
}

fn map(input: &mut Input) -> PResult<tg::value::Map> {
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

fn object(input: &mut Input) -> PResult<tg::Object> {
	alt((
		id.verify_map(|id| id.try_into().map(tg::Object::with_id).ok()),
		alt((
			leaf.map(|object| tg::object::Object::Leaf(Arc::new(object))),
			branch.map(|object| tg::object::Object::Branch(Arc::new(object))),
			directory.map(|object| tg::object::Object::Directory(Arc::new(object))),
			file.map(|object| tg::object::Object::File(Arc::new(object))),
			symlink.map(|object| tg::object::Object::Symlink(Arc::new(object))),
			graph.map(|object| tg::object::Object::Graph(Arc::new(object))),
			target.map(|object| tg::object::Object::Target(Arc::new(object))),
		))
		.map(tg::Object::with_object),
	))
	.parse_next(input)
}

fn leaf(input: &mut Input) -> PResult<tg::leaf::Object> {
	delimited(
		("tg.leaf", whitespace, "(", whitespace),
		bytes_arg,
		(whitespace, ")"),
	)
	.map(|bytes| tg::leaf::Object { bytes })
	.parse_next(input)
}

fn branch(input: &mut Input) -> PResult<tg::branch::Object> {
	delimited(
		("tg.branch", whitespace, "(", whitespace),
		branch_arg,
		(whitespace, ")"),
	)
	.parse_next(input)
}

fn branch_arg(input: &mut Input) -> PResult<tg::branch::Object> {
	map.verify_map(|map| {
		let children = map.get("children")?.try_unwrap_array_ref().ok()?;
		let children = children
			.iter()
			.map(|child| {
				let child = child.try_unwrap_map_ref().ok()?;
				let blob = match child.get("blob")?.try_unwrap_object_ref().ok()? {
					tg::Object::Leaf(leaf) => tg::Blob::Leaf(leaf.clone()),
					tg::Object::Branch(branch) => tg::Blob::Branch(branch.clone()),
					_ => return None,
				};
				let size = child.get("size")?.try_unwrap_number_ref().ok()?.to_u64()?;
				Some(tg::branch::Child { blob, size })
			})
			.collect::<Option<_>>()?;
		Some(tg::branch::Object { children })
	})
	.parse_next(input)
}

fn directory(input: &mut Input) -> PResult<tg::directory::Object> {
	delimited(
		("tg.directory", whitespace, "(", whitespace),
		directory_arg,
		(whitespace, ")"),
	)
	.parse_next(input)
}

fn directory_arg(input: &mut Input) -> PResult<tg::directory::Object> {
	map.verify_map(|map| {
		let entries = map
			.into_iter()
			.map(|(key, value)| Some((key, value.try_into().ok()?)))
			.collect::<Option<_>>()?;
		Some(tg::directory::Object::Normal { entries })
	})
	.parse_next(input)
}

fn file(input: &mut Input) -> PResult<tg::file::Object> {
	delimited(
		("tg.file", whitespace, "(", whitespace),
		file_arg,
		(whitespace, ")"),
	)
	.parse_next(input)
}

fn file_arg(input: &mut Input) -> PResult<tg::file::Object> {
	map.verify_map(|map| {
		let contents = map.get("contents")?.clone().try_into().ok()?;
		let dependencies = if let Some(dependencies) = map.get("dependencies") {
			dependencies
				.try_unwrap_map_ref()
				.ok()?
				.iter()
				.map(|(key, value)| {
					let reference: tg::Reference = key.parse().ok()?;
					let value = value.try_unwrap_map_ref().ok()?;
					let object = value.get("object")?.try_unwrap_object_ref().ok()?.clone();
					let tag = if let Some(tag) = value.get("tag") {
						Some(tag.try_unwrap_string_ref().ok()?.parse().ok()?)
					} else {
						None
					};
					let dependency = tg::file::Dependency { object, tag };
					Some((reference, dependency))
				})
				.collect::<Option<_>>()?
		} else {
			BTreeMap::new()
		};
		let executable = if let Some(executable) = map.get("executable") {
			*executable.try_unwrap_bool_ref().ok()?
		} else {
			false
		};
		Some(tg::file::Object::Normal {
			contents,
			dependencies,
			executable,
		})
	})
	.parse_next(input)
}

fn symlink(input: &mut Input) -> PResult<tg::symlink::Object> {
	delimited(
		("tg.symlink", whitespace, "(", whitespace),
		symlink_arg,
		(whitespace, ")"),
	)
	.parse_next(input)
}

fn symlink_arg(input: &mut Input) -> PResult<tg::symlink::Object> {
	map.verify_map(|map| {
		let artifact = if let Some(artifact) = map.get("artifact") {
			Some(artifact.clone().try_into().ok()?)
		} else {
			None
		};
		let path = if let Some(path) = map.get("path") {
			Some(path.try_unwrap_string_ref().ok()?.clone())
		} else {
			None
		};
		if path.is_none() && artifact.is_none() {
			return None;
		};
		Some(tg::symlink::Object::Normal { artifact, path })
	})
	.parse_next(input)
}

fn graph(input: &mut Input) -> PResult<tg::graph::Object> {
	delimited(
		("tg.graph", whitespace, "(", whitespace),
		graph_arg,
		(whitespace, ")"),
	)
	.parse_next(input)
}

fn graph_arg(input: &mut Input) -> PResult<tg::graph::Object> {
	map.verify_map(|map| {
		let nodes = map
			.get("nodes")?
			.try_unwrap_array_ref()
			.ok()?
			.iter()
			.map(|value| {
				let map = value.try_unwrap_map_ref().ok()?;
				let kind = map
					.get("kind")?
					.try_unwrap_string_ref()
					.ok()?
					.parse()
					.ok()?;
				match kind {
					tg::graph::node::Kind::Directory => {
						let entries = map
							.get("entries")?
							.try_unwrap_map_ref()
							.ok()?
							.iter()
							.map(|(key, value)| {
								let name = key.clone();
								let either = if let tg::Value::Number(number) = value {
									Either::Left(number.to_usize()?)
								} else if let Ok(artifact) = value.clone().try_into() {
									Either::Right(artifact)
								} else {
									return None;
								};
								Some((name, either))
							})
							.collect::<Option<_>>()?;
						Some(tg::graph::Node::Directory(tg::graph::node::Directory {
							entries,
						}))
					},

					tg::graph::node::Kind::File => {
						let contents = map.get("contents")?.clone().try_into().ok()?;
						let dependencies = if let Some(value) = map.get("dependencies") {
							value
								.try_unwrap_map_ref()
								.ok()?
								.iter()
								.map(|(key, value)| {
									let reference: tg::Reference = key.parse().ok()?;
									let dependency = value.try_unwrap_map_ref().ok()?;
									let object = match dependency.get("object")? {
										tg::Value::Number(number) => {
											Either::Left(number.to_usize().unwrap())
										},
										tg::Value::Object(object) => Either::Right(object.clone()),
										_ => return None,
									};
									let tag = match dependency.get("tag") {
										Some(tag) => Some(
											tag.try_unwrap_string_ref()
												.ok()?
												.as_str()
												.parse()
												.ok()?,
										),
										None => None,
									};
									let dependency = tg::graph::node::Dependency { object, tag };
									Some((reference, dependency))
								})
								.collect::<Option<_>>()?
						} else {
							BTreeMap::new()
						};
						let executable = if let Some(value) = map.get("executable") {
							*value.try_unwrap_bool_ref().ok()?
						} else {
							false
						};
						Some(tg::graph::Node::File(tg::graph::node::File {
							contents,
							dependencies,
							executable,
						}))
					},

					tg::graph::node::Kind::Symlink => {
						let artifact = if let Some(value) = map.get("artifact") {
							let either = if let tg::Value::Number(number) = value {
								Either::Left(number.to_usize()?)
							} else if let Ok(artifact) = value.clone().try_into() {
								Either::Right(artifact)
							} else {
								return None;
							};
							Some(either)
						} else {
							None
						};
						let path = if let Some(path) = map.get("path") {
							Some(path.try_unwrap_string_ref().ok()?.clone())
						} else {
							None
						};
						if path.is_none() && artifact.is_none() {
							return None;
						};
						Some(tg::graph::Node::Symlink(tg::graph::node::Symlink {
							artifact,
							path,
						}))
					},
				}
			})
			.collect::<Option<_>>()?;
		Some(tg::graph::Object { nodes })
	})
	.parse_next(input)
}

fn target(input: &mut Input) -> PResult<tg::target::Object> {
	delimited(
		("tg.target", whitespace, "(", whitespace),
		target_arg,
		(whitespace, ")"),
	)
	.parse_next(input)
}

fn target_arg(input: &mut Input) -> PResult<tg::target::Object> {
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

fn bytes(input: &mut Input) -> PResult<Bytes> {
	delimited(
		("tg.bytes", whitespace, "(", whitespace),
		bytes_arg,
		(whitespace, ")"),
	)
	.parse_next(input)
}

fn bytes_arg(input: &mut Input) -> PResult<Bytes> {
	string
		.verify_map(|string| {
			let bytes = data_encoding::BASE64.decode(string.as_bytes()).ok()?;
			Some(bytes.into())
		})
		.parse_next(input)
}

fn mutation(input: &mut Input) -> PResult<tg::Mutation> {
	delimited(
		("tg.mutation", whitespace, "(", whitespace),
		alt((
			mutation_unset,
			mutation_set,
			mutation_set_if_unset,
			mutation_prepend,
			mutation_append,
			mutation_prefix,
			mutation_suffix,
		)),
		(whitespace, ")"),
	)
	.parse_next(input)
}

fn mutation_unset(input: &mut Input) -> PResult<tg::Mutation> {
	map.verify_map(|map| {
		let kind = map.get("kind")?.try_unwrap_string_ref().ok()?;
		(kind == "unset").then_some(tg::Mutation::Unset)
	})
	.parse_next(input)
}

fn mutation_set(input: &mut Input) -> PResult<tg::Mutation> {
	map.verify_map(|map| {
		let kind = map.get("kind")?.try_unwrap_string_ref().ok()?;
		let value = map.get("value")?;
		(kind == "set").then_some(tg::Mutation::Set {
			value: Box::new(value.clone()),
		})
	})
	.parse_next(input)
}

fn mutation_set_if_unset(input: &mut Input) -> PResult<tg::Mutation> {
	map.verify_map(|map| {
		let kind = map.get("kind")?.try_unwrap_string_ref().ok()?;
		let value = map.get("value")?;
		(kind == "set_if_unset").then_some(tg::Mutation::SetIfUnset {
			value: Box::new(value.clone()),
		})
	})
	.parse_next(input)
}

fn mutation_prepend(input: &mut Input) -> PResult<tg::Mutation> {
	map.verify_map(|map| {
		let kind = map.get("kind")?.try_unwrap_string_ref().ok()?;
		let values = map.get("values")?.try_unwrap_array_ref().ok()?.clone();
		(kind == "prepend").then_some(tg::Mutation::Prepend { values })
	})
	.parse_next(input)
}

fn mutation_append(input: &mut Input) -> PResult<tg::Mutation> {
	map.verify_map(|map| {
		let kind = map.get("kind")?.try_unwrap_string_ref().ok()?.clone();
		let values = map.get("values")?.try_unwrap_array_ref().ok()?.clone();
		(kind == "append").then_some(tg::Mutation::Append { values })
	})
	.parse_next(input)
}

fn mutation_prefix(input: &mut Input) -> PResult<tg::Mutation> {
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

fn mutation_suffix(input: &mut Input) -> PResult<tg::Mutation> {
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

fn template(input: &mut Input) -> PResult<tg::Template> {
	delimited(
		("tg.template", whitespace, "(", whitespace),
		array,
		(whitespace, ")"),
	)
	.verify_map(|components| {
		let components = components
			.into_iter()
			.map(|component| match component {
				tg::Value::String(string) => Some(tg::template::Component::String(string)),
				tg::Value::Object(tg::Object::Directory(artifact)) => {
					Some(tg::template::Component::Artifact(artifact.into()))
				},
				tg::Value::Object(tg::Object::File(artifact)) => {
					Some(tg::template::Component::Artifact(artifact.into()))
				},
				tg::Value::Object(tg::Object::Symlink(artifact)) => {
					Some(tg::template::Component::Artifact(artifact.into()))
				},
				_ => None,
			})
			.collect::<Option<_>>()?;
		Some(tg::Template { components })
	})
	.parse_next(input)
}

fn whitespace(input: &mut Input) -> PResult<()> {
	take_while(0.., [' ', '\t', '\r', '\n']).parse_next(input)?;
	Ok(())
}
