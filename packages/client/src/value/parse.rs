use {
	crate::prelude::*,
	bytes::Bytes,
	num::ToPrimitive,
	std::{collections::BTreeMap, path::PathBuf},
	winnow::{
		ascii::float,
		combinator::{alt, cut_err, delimited, opt, preceded, repeat, separated, separated_pair},
		prelude::*,
		token::{any, none_of, one_of, take, take_while},
	},
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
		bytes.map(tg::Value::Bytes),
		mutation.map(tg::Value::Mutation),
		template.map(tg::Value::Template),
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
					_ => {
						return Err(tg::error!("invalid char"));
					},
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
			"0" => Some(tg::Id::new(kind, body)),
			_ => None,
		})
		.parse_next(input)
}

fn id_kind(input: &mut Input) -> ModalResult<tg::id::Kind> {
	alt((
		alt(("blb", "blob")).value(tg::id::Kind::Blob),
		alt(("dir", "directory")).value(tg::id::Kind::Directory),
		alt(("fil", "file")).value(tg::id::Kind::File),
		alt(("sym", "symlink")).value(tg::id::Kind::Symlink),
		alt(("gph", "graph")).value(tg::id::Kind::Graph),
		alt(("cmd", "command")).value(tg::id::Kind::Command),
		alt(("pcs", "process")).value(tg::id::Kind::Process),
		alt(("usr", "user")).value(tg::id::Kind::User),
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
	alt((
		preceded(
			("tg", whitespace, ".", whitespace),
			alt((blob, directory, file, symlink, graph, command)),
		),
		id.verify_map(|id| id.try_into().map(tg::Object::with_id).ok()),
	))
	.parse_next(input)
}

fn blob(input: &mut Input) -> ModalResult<tg::Object> {
	preceded(
		("blob", whitespace, "(", whitespace),
		cut_err(delimited(
			whitespace,
			alt((blob_leaf, blob_branch)),
			(whitespace, ")"),
		)),
	)
	.parse_next(input)
}

fn blob_leaf(input: &mut Input) -> ModalResult<tg::Object> {
	string
		.map(|s| {
			let bytes = Bytes::from(s.into_bytes());
			let leaf = tg::blob::object::Leaf { bytes };
			let object = tg::blob::Object::Leaf(leaf);
			tg::Blob::with_object(object).into()
		})
		.parse_next(input)
}

fn blob_branch(input: &mut Input) -> ModalResult<tg::Object> {
	delimited(
		("{", whitespace),
		cut_err(separated(
			0..,
			separated_pair(
				string,
				(whitespace, ":", whitespace),
				|input: &mut Input| {
					alt((
						preceded(
							("length", whitespace, ":", whitespace),
							number.map(|value| ("length", tg::Value::Number(value))),
						),
						preceded(
							("blob", whitespace, ":", whitespace),
							value.map(|value| ("blob", value)),
						),
					))
					.parse_next(input)
				},
			),
			(whitespace, ",", whitespace),
		)),
		(whitespace, opt(","), whitespace, "}"),
	)
	.try_map(|entries: Vec<(String, (&str, tg::Value))>| {
		let mut length = None;
		let mut blob = None;
		for (key, (field, value)) in entries {
			match (key.as_str(), field) {
				("length", "length") => {
					let n = value
						.try_unwrap_number_ref()
						.map_err(|_| tg::error!("expected number for length"))?;
					length = Some(
						n.to_u64()
							.ok_or_else(|| tg::error!("length must be a positive integer"))?,
					);
				},
				("blob", "blob") => {
					let value = value
						.try_unwrap_object_ref()
						.map_err(|_| tg::error!("expected object for blob"))?;
					let value = value
						.try_unwrap_blob_ref()
						.map_err(|_| tg::error!("expected blob object for blob field"))?;
					blob = Some(value.clone());
				},
				_ => {
					return Err(tg::error!("unexpected field in blob branch: {}", key));
				},
			}
		}
		let length = length.ok_or_else(|| tg::error!("missing length field"))?;
		let blob = blob.ok_or_else(|| tg::error!("missing blob field"))?;
		let child = tg::blob::object::Child { blob, length };
		let branch = tg::blob::object::Branch {
			children: vec![child],
		};
		let object = tg::blob::Object::Branch(branch);
		Ok(tg::Blob::with_object(object).into())
	})
	.parse_next(input)
}

fn directory(input: &mut Input) -> ModalResult<tg::Object> {
	preceded(
		("directory", whitespace, "(", whitespace),
		cut_err(delimited(
			whitespace,
			alt((directory_reference, directory_node)),
			(whitespace, ")"),
		)),
	)
	.parse_next(input)
}

fn directory_reference(input: &mut Input) -> ModalResult<tg::Object> {
	graph_reference
		.map(|reference| {
			let object = tg::directory::Object::Reference(reference);
			tg::Directory::with_object(object).into()
		})
		.parse_next(input)
}

fn directory_node(input: &mut Input) -> ModalResult<tg::Object> {
	alt((
		delimited(
			("{", whitespace),
			cut_err(separated(
				0..,
				separated_pair(string, (whitespace, ":", whitespace), directory_node_entry),
				(whitespace, ",", whitespace),
			)),
			(whitespace, opt(","), whitespace, "}"),
		)
		.map(|entries: Vec<(String, tg::graph::Edge<tg::Artifact>)>| {
			let entries = entries.into_iter().collect::<BTreeMap<_, _>>();
			let node = tg::graph::Directory { entries };
			let object = tg::directory::Object::Node(node);
			tg::Directory::with_object(object).into()
		}),
		whitespace.map(|()| {
			let node = tg::graph::Directory {
				entries: BTreeMap::new(),
			};
			let object = tg::directory::Object::Node(node);
			tg::Directory::with_object(object).into()
		}),
	))
	.parse_next(input)
}

fn directory_node_entry(input: &mut Input) -> ModalResult<tg::graph::Edge<tg::Artifact>> {
	alt((
		string.map(|s| {
			let bytes = Bytes::from(s.into_bytes());
			let leaf = tg::blob::object::Leaf { bytes };
			let blob_object = tg::blob::Object::Leaf(leaf);
			let blob = tg::Blob::with_object(blob_object);
			let node = tg::graph::File {
				contents: blob,
				dependencies: BTreeMap::new(),
				executable: false,
			};
			let object = tg::file::Object::Node(node);
			let file = tg::File::with_object(object);
			tg::graph::Edge::Object(tg::Artifact::File(file))
		}),
		graph_edge_artifact,
	))
	.parse_next(input)
}

fn file(input: &mut Input) -> ModalResult<tg::Object> {
	preceded(
		("file", whitespace, "(", whitespace),
		cut_err(delimited(
			whitespace,
			alt((file_reference, file_node, file_string)),
			(whitespace, ")"),
		)),
	)
	.parse_next(input)
}

fn file_string(input: &mut Input) -> ModalResult<tg::Object> {
	string
		.map(|s| {
			let bytes = Bytes::from(s.into_bytes());
			let leaf = tg::blob::object::Leaf { bytes };
			let blob_object = tg::blob::Object::Leaf(leaf);
			let blob = tg::Blob::with_object(blob_object);
			let node = tg::graph::File {
				contents: blob,
				dependencies: BTreeMap::new(),
				executable: false,
			};
			let object = tg::file::Object::Node(node);
			tg::File::with_object(object).into()
		})
		.parse_next(input)
}

fn file_reference(input: &mut Input) -> ModalResult<tg::Object> {
	graph_reference
		.map(|reference| {
			let object = tg::file::Object::Reference(reference);
			tg::File::with_object(object).into()
		})
		.parse_next(input)
}

fn file_node(input: &mut Input) -> ModalResult<tg::Object> {
	delimited(
		("{", whitespace),
		cut_err(separated(
			0..,
			separated_pair(string, (whitespace, ":", whitespace), value),
			(whitespace, ",", whitespace),
		)),
		(whitespace, opt(","), whitespace, "}"),
	)
	.try_map(|entries: Vec<(String, tg::Value)>| {
		let mut contents = None;
		let mut dependencies = BTreeMap::new();
		let mut executable = false;
		for (key, value) in entries {
			match key.as_str() {
				"contents" => {
					let blob = if let Ok(s) = value.try_unwrap_string_ref() {
						let bytes = Bytes::from(s.as_bytes().to_vec());
						let leaf = tg::blob::object::Leaf { bytes };
						let blob_object = tg::blob::Object::Leaf(leaf);
						tg::Blob::with_object(blob_object)
					} else {
						let object = value
							.try_unwrap_object_ref()
							.map_err(|_| tg::error!("expected object or string for contents"))?;
						let blob = object
							.try_unwrap_blob_ref()
							.map_err(|_| tg::error!("expected blob object for contents"))?;
						blob.clone()
					};
					contents = Some(blob);
				},
				"dependencies" => {
					let map = value
						.try_unwrap_map_ref()
						.map_err(|_| tg::error!("expected map for dependencies"))?;
					for (key, value) in map {
						let reference = key
							.parse::<tg::Reference>()
							.map_err(|error| tg::error!(!error, "invalid reference key"))?;
						let referent = if value.is_null() {
							None
						} else {
							let value = value
								.try_unwrap_map_ref()
								.map_err(|_| tg::error!("expected map for referent"))?;
							Some(parse_referent(value)?)
						};
						dependencies.insert(reference, referent);
					}
				},
				"executable" => {
					let value = value
						.try_unwrap_bool_ref()
						.map_err(|_| tg::error!("expected boolean for executable"))?;
					executable = *value;
				},
				_ => {
					return Err(tg::error!("unexpected field in file: {}", key));
				},
			}
		}
		let contents = contents.ok_or_else(|| tg::error!("missing contents field"))?;
		let node = tg::graph::File {
			contents,
			dependencies,
			executable,
		};
		let object = tg::file::Object::Node(node);
		Ok(tg::File::with_object(object).into())
	})
	.parse_next(input)
}

fn symlink(input: &mut Input) -> ModalResult<tg::Object> {
	preceded(
		("symlink", whitespace, "(", whitespace),
		cut_err(delimited(
			whitespace,
			alt((symlink_reference, symlink_node)),
			(whitespace, ")"),
		)),
	)
	.parse_next(input)
}

fn symlink_reference(input: &mut Input) -> ModalResult<tg::Object> {
	graph_reference
		.map(|reference| {
			let object = tg::symlink::Object::Reference(reference);
			tg::Symlink::with_object(object).into()
		})
		.parse_next(input)
}

fn symlink_node(input: &mut Input) -> ModalResult<tg::Object> {
	delimited(
		("{", whitespace),
		cut_err(separated(
			0..,
			separated_pair(string, (whitespace, ":", whitespace), value),
			(whitespace, ",", whitespace),
		)),
		(whitespace, opt(","), whitespace, "}"),
	)
	.try_map(|entries: Vec<(String, tg::Value)>| {
		let mut artifact = None;
		let mut path = None;
		for (key, value) in entries {
			match key.as_str() {
				"artifact" => {
					let value = value
						.try_unwrap_object_ref()
						.map_err(|_| tg::error!("expected object for artifact"))?;
					let value = tg::Artifact::try_from(value.clone())
						.map_err(|error| tg::error!(!error, "expected artifact object"))?;
					artifact = Some(tg::graph::Edge::Object(value));
				},
				"path" => {
					let value = value
						.try_unwrap_string_ref()
						.map_err(|_| tg::error!("expected string for path"))?;
					path = Some(PathBuf::from(value));
				},
				_ => {
					return Err(tg::error!("unexpected field in symlink: {}", key));
				},
			}
		}
		let node = tg::graph::Symlink { artifact, path };
		let object = tg::symlink::Object::Node(node);
		Ok(tg::Symlink::with_object(object).into())
	})
	.parse_next(input)
}

fn graph(input: &mut Input) -> ModalResult<tg::Object> {
	preceded(
		("graph", whitespace, "(", whitespace),
		cut_err(delimited(whitespace, graph_inner, (whitespace, ")"))),
	)
	.parse_next(input)
}

fn graph_inner(input: &mut Input) -> ModalResult<tg::Object> {
	delimited(
		("{", whitespace),
		cut_err(separated(
			0..,
			separated_pair(string, (whitespace, ":", whitespace), value),
			(whitespace, ",", whitespace),
		)),
		(whitespace, opt(","), whitespace, "}"),
	)
	.try_map(|entries: Vec<(String, tg::Value)>| {
		let mut nodes = Vec::new();
		for (key, value) in entries {
			match key.as_str() {
				"nodes" => {
					let value = value
						.try_unwrap_array_ref()
						.map_err(|_| tg::error!("expected array for nodes"))?;
					for item in value {
						let value = item
							.try_unwrap_map_ref()
							.map_err(|_| tg::error!("expected object for node in nodes array"))?;
						let node = parse_graph_node(value)?;
						nodes.push(node);
					}
				},
				_ => {
					return Err(tg::error!("unexpected field in graph: {}", key));
				},
			}
		}
		let object = tg::graph::Object { nodes };
		Ok(tg::Graph::with_object(object).into())
	})
	.parse_next(input)
}

fn command(input: &mut Input) -> ModalResult<tg::Object> {
	preceded(
		("command", whitespace, "(", whitespace),
		cut_err(delimited(whitespace, command_inner, (whitespace, ")"))),
	)
	.parse_next(input)
}

fn command_inner(input: &mut Input) -> ModalResult<tg::Object> {
	delimited(
		("{", whitespace),
		cut_err(separated(
			0..,
			separated_pair(string, (whitespace, ":", whitespace), value),
			(whitespace, ",", whitespace),
		)),
		(whitespace, opt(","), whitespace, "}"),
	)
	.try_map(|entries: Vec<(String, tg::Value)>| {
		let mut args = Vec::new();
		let mut cwd = None;
		let mut env = BTreeMap::new();
		let mut executable = None;
		let mut host = String::from("builtin");
		let mut mounts = Vec::new();
		for (key, value) in entries {
			match key.as_str() {
				"args" => {
					let value = value
						.try_unwrap_array_ref()
						.map_err(|_| tg::error!("expected array for args"))?;
					args = value.clone();
				},
				"cwd" => {
					let value = value
						.try_unwrap_string_ref()
						.map_err(|_| tg::error!("expected string for cwd"))?;
					cwd = Some(PathBuf::from(value));
				},
				"env" => {
					let value = value
						.try_unwrap_map_ref()
						.map_err(|_| tg::error!("expected map for env"))?;
					env = value.clone();
				},
				"executable" => {
					executable = Some(parse_executable(&value)?);
				},
				"host" => {
					let value = value
						.try_unwrap_string_ref()
						.map_err(|_| tg::error!("expected string for host"))?;
					host = value.clone();
				},
				"mounts" => {
					let value = value
						.try_unwrap_array_ref()
						.map_err(|_| tg::error!("expected array for mounts"))?;
					for item in value {
						let value = item
							.try_unwrap_map_ref()
							.map_err(|_| tg::error!("expected object for mount in mounts array"))?;
						let mount = parse_mount(value)?;
						mounts.push(mount);
					}
				},
				_ => {
					return Err(tg::error!("unexpected field in command: {}", key));
				},
			}
		}
		let executable = executable.ok_or_else(|| tg::error!("missing executable field"))?;
		let object = tg::command::Object {
			args,
			cwd,
			env,
			executable,
			host,
			mounts,
			stdin: None,
			user: None,
		};
		Ok(tg::Command::with_object(object).into())
	})
	.parse_next(input)
}

fn bytes(input: &mut Input) -> ModalResult<Bytes> {
	preceded(
		(
			"tg", whitespace, ".", whitespace, "bytes", whitespace, "(", whitespace,
		),
		cut_err(delimited(whitespace, string, (whitespace, ")"))),
	)
	.verify_map(|s| {
		data_encoding::BASE64
			.decode(s.as_bytes())
			.ok()
			.map(Bytes::from)
	})
	.parse_next(input)
}

fn mutation(input: &mut Input) -> ModalResult<tg::Mutation> {
	preceded(
		(
			"tg", whitespace, ".", whitespace, "mutation", whitespace, "(", whitespace,
		),
		cut_err(delimited(whitespace, mutation_inner, (whitespace, ")"))),
	)
	.parse_next(input)
}

fn mutation_inner(input: &mut Input) -> ModalResult<tg::Mutation> {
	delimited(
		("{", whitespace),
		cut_err(separated(
			0..,
			separated_pair(string, (whitespace, ":", whitespace), value),
			(whitespace, ",", whitespace),
		)),
		(whitespace, opt(","), whitespace, "}"),
	)
	.try_map(|entries: Vec<(String, tg::Value)>| {
		let mut kind = None;
		let mut value_field = None;
		let mut values_field = None;
		let mut separator = None;
		let mut template = None;
		for (key, value) in entries {
			match key.as_str() {
				"kind" => {
					let valu = value
						.try_unwrap_string_ref()
						.map_err(|_| tg::error!("expected string for kind"))?;
					kind = Some(valu.clone());
				},
				"value" => {
					value_field = Some(value);
				},
				"values" => {
					let value = value
						.try_unwrap_array_ref()
						.map_err(|_| tg::error!("expected array for values"))?;
					values_field = Some(value.clone());
				},
				"separator" => {
					let value = value
						.try_unwrap_string_ref()
						.map_err(|_| tg::error!("expected string for separator"))?;
					separator = Some(value.clone());
				},
				"template" => {
					let value = value
						.try_unwrap_template_ref()
						.map_err(|_| tg::error!("expected template for template"))?;
					template = Some(value.clone());
				},
				_ => {
					return Err(tg::error!("unexpected field in mutation: {}", key));
				},
			}
		}
		let kind = kind.ok_or_else(|| tg::error!("missing kind field"))?;
		match kind.as_str() {
			"unset" => Ok(tg::Mutation::Unset),
			"set" => {
				let value = value_field
					.ok_or_else(|| tg::error!("missing value field for set mutation"))?;
				Ok(tg::Mutation::Set {
					value: Box::new(value),
				})
			},
			"set_if_unset" => {
				let value = value_field
					.ok_or_else(|| tg::error!("missing value field for set_if_unset mutation"))?;
				Ok(tg::Mutation::SetIfUnset {
					value: Box::new(value),
				})
			},
			"prepend" => {
				let values = values_field
					.ok_or_else(|| tg::error!("missing values field for prepend mutation"))?;
				Ok(tg::Mutation::Prepend { values })
			},
			"append" => {
				let values = values_field
					.ok_or_else(|| tg::error!("missing values field for append mutation"))?;
				Ok(tg::Mutation::Append { values })
			},
			"prefix" => {
				let template = template
					.ok_or_else(|| tg::error!("missing template field for prefix mutation"))?;
				Ok(tg::Mutation::Prefix {
					separator,
					template,
				})
			},
			"suffix" => {
				let template = template
					.ok_or_else(|| tg::error!("missing template field for suffix mutation"))?;
				Ok(tg::Mutation::Suffix {
					separator,
					template,
				})
			},
			"merge" => {
				let value_field = value_field
					.ok_or_else(|| tg::error!("missing value field for merge mutation"))?;
				let value = value_field
					.try_unwrap_map_ref()
					.map_err(|_| tg::error!("expected map for value field in merge mutation"))?;
				Ok(tg::Mutation::Merge {
					value: value.clone(),
				})
			},
			_ => Err(tg::error!("unknown mutation kind: {}", kind)),
		}
	})
	.parse_next(input)
}

fn template(input: &mut Input) -> ModalResult<tg::Template> {
	preceded(
		(
			"tg", whitespace, ".", whitespace, "template", whitespace, "(", whitespace,
		),
		cut_err(delimited(whitespace, template_inner, (whitespace, ")"))),
	)
	.parse_next(input)
}

fn template_inner(input: &mut Input) -> ModalResult<tg::Template> {
	delimited(
		("[", whitespace),
		cut_err(separated(0.., value, (whitespace, ",", whitespace))),
		(whitespace, opt(","), whitespace, "]"),
	)
	.try_map(|values: Vec<tg::Value>| {
		let mut components = Vec::new();
		for value in values {
			if value.is_string() {
				let value = value
					.try_unwrap_string()
					.map_err(|_| tg::error!("expected string"))?;
				components.push(tg::template::Component::String(value));
			} else if value.is_object() {
				let value = value
					.try_unwrap_object()
					.map_err(|_| tg::error!("expected object"))?;
				let artifact = tg::Artifact::try_from(value)
					.map_err(|error| tg::error!(!error, "expected artifact object in template"))?;
				components.push(tg::template::Component::Artifact(artifact));
			} else if value.is_placeholder() {
				let value = value
					.try_unwrap_placeholder()
					.map_err(|_| tg::error!("expected placeholder"))?;
				components.push(tg::template::Component::Placeholder(value));
			} else {
				return Err(tg::error!(
					"template components must be strings, artifacts, or placeholders"
				));
			}
		}
		Ok(tg::Template { components })
	})
	.parse_next(input)
}

fn graph_reference(input: &mut Input) -> ModalResult<tg::graph::Reference> {
	delimited(
		("{", whitespace),
		cut_err(separated(
			0..,
			separated_pair(string, (whitespace, ":", whitespace), value),
			(whitespace, ",", whitespace),
		)),
		(whitespace, opt(","), whitespace, "}"),
	)
	.try_map(|entries: Vec<(String, tg::Value)>| {
		let mut graph = None;
		let mut index = None;
		let mut kind = None;
		for (key, value) in entries {
			match key.as_str() {
				"graph" => {
					let value = value
						.try_unwrap_object_ref()
						.map_err(|_| tg::error!("expected object for graph"))?;
					let value = value
						.try_unwrap_graph_ref()
						.map_err(|_| tg::error!("expected graph object for graph field"))?;
					graph = Some(value.clone());
				},
				"index" => {
					let value = value
						.try_unwrap_number_ref()
						.map_err(|_| tg::error!("expected number for index"))?;
					let n = value
						.to_usize()
						.ok_or_else(|| tg::error!("index must be a non-negative integer"))?;
					index = Some(n);
				},
				"kind" => {
					let value = value
						.try_unwrap_string_ref()
						.map_err(|_| tg::error!("expected string for kind"))?
						.parse()
						.map_err(|_| tg::error!("invalid kind"))?;
					kind = Some(value);
				},
				_ => {
					return Err(tg::error!("unexpected field in graph reference: {}", key));
				},
			}
		}
		let index = index.ok_or_else(|| tg::error!("missing index field"))?;
		let kind = kind.ok_or_else(|| tg::error!("missing kind field"))?;
		Ok(tg::graph::Reference { graph, index, kind })
	})
	.parse_next(input)
}

fn graph_edge_artifact(input: &mut Input) -> ModalResult<tg::graph::Edge<tg::Artifact>> {
	alt((
		graph_reference.map(tg::graph::Edge::Reference),
		value.verify_map(|value| {
			if let tg::Value::Object(value) = value {
				tg::Artifact::try_from(value)
					.ok()
					.map(tg::graph::Edge::Object)
			} else {
				None
			}
		}),
	))
	.parse_next(input)
}

fn parse_referent(map: &tg::value::Map) -> tg::Result<tg::Referent<tg::graph::Edge<tg::Object>>> {
	let mut item = None;
	let mut artifact = None;
	let mut id = None;
	let mut name = None;
	let mut path = None;
	let mut tag = None;
	for (key, value) in map {
		match key.as_str() {
			"item" => {
				let value = value
					.try_unwrap_object_ref()
					.map_err(|_| tg::error!("expected object for item"))?;
				item = Some(tg::graph::Edge::Object(value.clone()));
			},
			"artifact" => {
				let value = value
					.try_unwrap_string_ref()
					.map_err(|_| tg::error!("expected string for artifact"))?;
				artifact = Some(
					value
						.parse()
						.map_err(|error| tg::error!(!error, "failed to parse artifact"))?,
				);
			},
			"id" => {
				let value = value
					.try_unwrap_string_ref()
					.map_err(|_| tg::error!("expected string for id"))?;
				id = Some(
					value
						.parse()
						.map_err(|error| tg::error!(!error, "failed to parse id"))?,
				);
			},
			"name" => {
				let value = value
					.try_unwrap_string_ref()
					.map_err(|_| tg::error!("expected string for name"))?;
				name = Some(value.clone());
			},
			"path" => {
				let value = value
					.try_unwrap_string_ref()
					.map_err(|_| tg::error!("expected string for path"))?;
				path = Some(PathBuf::from(value));
			},
			"tag" => {
				let value = value
					.try_unwrap_string_ref()
					.map_err(|_| tg::error!("expected string for tag"))?;
				tag = Some(
					value
						.parse()
						.map_err(|error| tg::error!(!error, "failed to parse tag"))?,
				);
			},
			_ => {
				return Err(tg::error!("unexpected field in referent: {}", key));
			},
		}
	}
	let item = item.ok_or_else(|| tg::error!("missing item field"))?;
	let options = tg::referent::Options {
		artifact,
		id,
		name,
		path,
		tag,
	};
	Ok(tg::Referent { item, options })
}

fn parse_graph_node(map: &tg::value::Map) -> tg::Result<tg::graph::Node> {
	let mut kind = None;
	let mut entries = BTreeMap::new();
	let mut contents = None;
	let mut dependencies = BTreeMap::new();
	let mut executable = false;
	let mut artifact = None;
	let mut path = None;
	for (key, value) in map {
		match key.as_str() {
			"kind" => {
				let s = value
					.try_unwrap_string_ref()
					.map_err(|_| tg::error!("expected string for kind"))?;
				kind = Some(s.clone());
			},
			"entries" => {
				let value = value
					.try_unwrap_map_ref()
					.map_err(|_| tg::error!("expected map for entries"))?;
				for (key, value) in value {
					let value = value
						.try_unwrap_object_ref()
						.map_err(|_| tg::error!("expected object for entry value"))?;
					let artifact = tg::Artifact::try_from(value.clone()).map_err(|error| {
						tg::error!(!error, "expected artifact object for entry")
					})?;
					entries.insert(key.clone(), tg::graph::Edge::Object(artifact));
				}
			},
			"contents" => {
				let obj = value
					.try_unwrap_object_ref()
					.map_err(|_| tg::error!("expected object for contents"))?;
				let blob = obj
					.try_unwrap_blob_ref()
					.map_err(|_| tg::error!("expected blob object for contents"))?;
				contents = Some(blob.clone());
			},
			"dependencies" => {
				let value = value
					.try_unwrap_map_ref()
					.map_err(|_| tg::error!("expected map for dependencies"))?;
				for (key, value) in value {
					let reference = key
						.parse::<tg::Reference>()
						.map_err(|error| tg::error!(!error, "failed to parse reference"))?;
					let referent = if value.is_null() {
						None
					} else {
						let value = value
							.try_unwrap_map_ref()
							.map_err(|_| tg::error!("expected map or null for dependency value"))?;
						Some(parse_referent(value)?)
					};
					dependencies.insert(reference, referent);
				}
			},
			"executable" => {
				let value = value
					.try_unwrap_bool_ref()
					.map_err(|_| tg::error!("expected bool for executable"))?;
				executable = *value;
			},
			"artifact" => {
				let value = value
					.try_unwrap_object_ref()
					.map_err(|_| tg::error!("expected object for artifact"))?;
				let artifact_value = tg::Artifact::try_from(value.clone()).map_err(|error| {
					tg::error!(!error, "expected artifact object for artifact field")
				})?;
				artifact = Some(tg::graph::Edge::Object(artifact_value));
			},
			"path" => {
				let value = value
					.try_unwrap_string_ref()
					.map_err(|_| tg::error!("expected string for path"))?;
				path = Some(PathBuf::from(value));
			},
			_ => {
				return Err(tg::error!("unexpected field in graph node: {}", key));
			},
		}
	}
	let kind = kind.ok_or_else(|| tg::error!("missing kind field"))?;
	match kind.as_str() {
		"directory" => Ok(tg::graph::Node::Directory(tg::graph::Directory { entries })),
		"file" => {
			let contents =
				contents.ok_or_else(|| tg::error!("missing contents field for file node"))?;
			Ok(tg::graph::Node::File(tg::graph::File {
				contents,
				dependencies,
				executable,
			}))
		},
		"symlink" => Ok(tg::graph::Node::Symlink(tg::graph::Symlink {
			artifact,
			path,
		})),
		_ => Err(tg::error!("unknown graph node kind: {}", kind)),
	}
}

fn parse_executable(value: &tg::Value) -> tg::Result<tg::command::Executable> {
	match value {
		tg::Value::String(value) => {
			let path = PathBuf::from(value);
			Ok(tg::command::Executable::Path(tg::command::PathExecutable {
				path,
			}))
		},
		tg::Value::Map(value) => {
			if value.contains_key("artifact") {
				let mut artifact = None;
				let mut path = None;
				for (key, value) in value {
					match key.as_str() {
						"artifact" => {
							let value = value
								.try_unwrap_object_ref()
								.map_err(|_| tg::error!("expected object for artifact"))?;
							let value = tg::Artifact::try_from(value.clone()).map_err(|error| {
								tg::error!(!error, "expected artifact object for artifact field")
							})?;
							artifact = Some(value);
						},
						"path" => {
							let value = value
								.try_unwrap_string_ref()
								.map_err(|_| tg::error!("expected string for path"))?;
							path = Some(PathBuf::from(value));
						},
						_ => {
							return Err(tg::error!(
								"unexpected field in artifact executable: {}",
								key
							));
						},
					}
				}
				let artifact = artifact.ok_or_else(|| tg::error!("missing artifact field"))?;
				Ok(tg::command::Executable::Artifact(
					tg::command::ArtifactExecutable { artifact, path },
				))
			} else if value.contains_key("module") {
				let mut module = None;
				let mut export = None;
				for (key, value) in value {
					match key.as_str() {
						"module" => {
							module = Some(parse_module(value)?);
						},
						"export" => {
							let value = value
								.try_unwrap_string_ref()
								.map_err(|_| tg::error!("expected string for export"))?;
							export = Some(value.clone());
						},
						_ => {
							return Err(tg::error!(
								"unexpected field in module executable: {}",
								key
							));
						},
					}
				}
				let module = module.ok_or_else(|| tg::error!("missing module field"))?;
				Ok(tg::command::Executable::Module(
					tg::command::ModuleExecutable { module, export },
				))
			} else {
				Err(tg::error!(
					"executable map must contain either artifact or module field"
				))
			}
		},
		_ => Err(tg::error!("executable must be a string or map")),
	}
}

fn parse_module(value: &tg::Value) -> tg::Result<tg::Module> {
	let map = value
		.try_unwrap_map_ref()
		.map_err(|_| tg::error!("expected map for module"))?;
	let mut kind = None;
	let mut referent = None;
	for (key, value) in map {
		match key.as_str() {
			"kind" => {
				let value = value
					.try_unwrap_string_ref()
					.map_err(|_| tg::error!("expected string for kind"))?;
				kind = Some(
					value
						.parse()
						.map_err(|error| tg::error!(!error, "failed to parse module kind"))?,
				);
			},
			"referent" => {
				let value = value
					.try_unwrap_map_ref()
					.map_err(|_| tg::error!("expected map for referent"))?;
				referent = Some(parse_module_referent(value)?);
			},
			_ => {
				return Err(tg::error!("unexpected field in module: {}", key));
			},
		}
	}
	let kind = kind.ok_or_else(|| tg::error!("missing kind field"))?;
	let referent = referent.ok_or_else(|| tg::error!("missing referent field"))?;
	Ok(tg::Module { kind, referent })
}

fn parse_module_referent(map: &tg::value::Map) -> tg::Result<tg::Referent<tg::module::Item>> {
	let mut item = None;
	let mut artifact = None;
	let mut id = None;
	let mut name = None;
	let mut path = None;
	let mut tag = None;
	for (key, value) in map {
		match key.as_str() {
			"item" => {
				if value.is_string() {
					let value = value
						.try_unwrap_string_ref()
						.map_err(|_| tg::error!("expected string"))?;
					item = Some(tg::module::Item::Path(PathBuf::from(value)));
				} else if value.is_object() {
					let object = value
						.try_unwrap_object_ref()
						.map_err(|_| tg::error!("expected object"))?;
					let edge = tg::graph::Edge::Object(object.clone());
					item = Some(tg::module::Item::Edge(edge));
				} else {
					return Err(tg::error!("expected string or object for item"));
				}
			},
			"artifact" => {
				let value = value
					.try_unwrap_string_ref()
					.map_err(|_| tg::error!("expected string for artifact"))?;
				artifact = Some(
					value
						.parse()
						.map_err(|error| tg::error!(!error, "failed to parse artifact"))?,
				);
			},
			"id" => {
				let value = value
					.try_unwrap_string_ref()
					.map_err(|_| tg::error!("expected string for id"))?;
				id = Some(
					value
						.parse()
						.map_err(|error| tg::error!(!error, "failed to parse id"))?,
				);
			},
			"name" => {
				let value = value
					.try_unwrap_string_ref()
					.map_err(|_| tg::error!("expected string for name"))?;
				name = Some(value.clone());
			},
			"path" => {
				let value = value
					.try_unwrap_string_ref()
					.map_err(|_| tg::error!("expected string for path"))?;
				path = Some(PathBuf::from(value));
			},
			"tag" => {
				let value = value
					.try_unwrap_string_ref()
					.map_err(|_| tg::error!("expected string for tag"))?;
				tag = Some(
					value
						.parse()
						.map_err(|error| tg::error!(!error, "failed to parse tag"))?,
				);
			},
			_ => {
				return Err(tg::error!("unexpected field in module referent: {}", key));
			},
		}
	}
	let item = item.ok_or_else(|| tg::error!("missing item field"))?;
	let options = tg::referent::Options {
		artifact,
		id,
		name,
		path,
		tag,
	};
	Ok(tg::Referent { item, options })
}

fn parse_mount(map: &tg::value::Map) -> tg::Result<tg::command::Mount> {
	let mut source = None;
	let mut target = None;
	for (key, value) in map {
		match key.as_str() {
			"source" => {
				let value = value
					.try_unwrap_object_ref()
					.map_err(|_| tg::error!("expected object for source"))?;
				let artifact = tg::Artifact::try_from(value.clone())
					.map_err(|error| tg::error!(!error, "expected artifact object for source"))?;
				source = Some(artifact);
			},
			"target" => {
				let value = value
					.try_unwrap_string_ref()
					.map_err(|_| tg::error!("expected string for target"))?;
				target = Some(PathBuf::from(value));
			},
			_ => {
				return Err(tg::error!("unexpected field in mount: {}", key));
			},
		}
	}
	let source = source.ok_or_else(|| tg::error!("missing source field"))?;
	let target = target.ok_or_else(|| tg::error!("missing target field"))?;
	Ok(tg::command::Mount { source, target })
}

fn whitespace(input: &mut Input) -> ModalResult<()> {
	take_while(0.., [' ', '\t', '\r', '\n'])
		.parse_next(input)
		.map(|_| ())
}
