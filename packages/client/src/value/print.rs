use crate as tg;
use bytes::Bytes;
use either::Either;
use num::ToPrimitive as _;
use std::collections::BTreeMap;

pub fn print(value: &tg::Value, pretty: bool) -> String {
	Printer::print(value, pretty)
}

struct Printer {
	indent: u32,
	pretty: bool,
	string: String,
}

impl Printer {
	fn print(value: &tg::Value, pretty: bool) -> String {
		let mut printer = Self {
			indent: 0,
			pretty,
			string: String::new(),
		};
		printer.value(value);
		printer.string
	}

	fn indent(&mut self) {
		if self.pretty {
			let indent = (0..self.indent).map(|_| '\t');
			self.string.extend(indent);
		}
	}

	fn value(&mut self, value: &tg::Value) {
		match value {
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

	fn null(&mut self) {
		self.string += "null";
	}

	fn bool(&mut self, value: bool) {
		self.string += if value { "true" } else { "false" };
	}

	fn number(&mut self, value: f64) {
		self.string += &value.to_string();
	}

	fn string(&mut self, value: &str) {
		self.string += &format!("{value:#?}");
	}

	fn array(&mut self, value: &tg::value::Array) {
		self.string.push('[');
		if self.pretty && !value.is_empty() {
			self.string.push('\n');
		}
		self.indent += 1;
		for value in value {
			self.indent();
			self.value(value);
			self.string.push(',');
			if self.pretty {
				self.string.push('\n');
			}
		}
		self.indent -= 1;
		self.indent();
		self.string.push(']');
	}

	fn map(&mut self, value: &tg::value::Map) {
		self.string.push('{');
		if self.pretty && !value.is_empty() {
			self.string.push('\n');
		}
		self.indent += 1;
		for (key, value) in value {
			self.indent();
			self.string += &format!("{key:#?}");
			self.string.push(':');
			if self.pretty {
				self.string.push(' ');
			}
			self.value(value);
			self.string.push(',');
			if self.pretty {
				self.string.push('\n');
			}
		}
		self.indent -= 1;
		self.indent();
		self.string.push('}');
	}

	fn object(&mut self, value: &tg::Object) {
		match value {
			tg::Object::Leaf(v) => self.leaf(v),
			tg::Object::Branch(v) => self.branch(v),
			tg::Object::Directory(v) => self.directory(v),
			tg::Object::File(v) => self.file(v),
			tg::Object::Symlink(v) => self.symlink(v),
			tg::Object::Lock(v) => self.lock(v),
			tg::Object::Target(v) => self.target(v),
		}
	}

	fn leaf(&mut self, value: &tg::Leaf) {
		let state = value.state().read().unwrap();
		if let Some(id) = state.id() {
			self.string += &id.to_string();
			return;
		}
		let object = state.object().unwrap();
		let bytes = data_encoding::BASE64.encode(&object.bytes);
		self.string += "tg.leaf(";
		self.string(&bytes);
		self.string.push(')');
	}

	fn branch(&mut self, value: &tg::Branch) {
		let state = value.state().read().unwrap();
		if let Some(id) = state.id() {
			self.string += &id.to_string();
			return;
		}
		let object = state.object().unwrap();
		let children = object
			.children
			.iter()
			.map(|child| {
				let size = child.size.to_f64().unwrap().into();
				let blob = child.blob.clone().into();
				[("size".to_owned(), size), ("blob".to_owned(), blob)]
					.into_iter()
					.collect::<tg::value::Map>()
					.into()
			})
			.collect::<tg::value::Array>()
			.into();
		let map = [("children".to_owned(), children)].into_iter().collect();
		self.string += "tg.branch(";
		self.map(&map);
		self.string.push(')');
	}

	fn directory(&mut self, value: &tg::Directory) {
		let state = value.state().read().unwrap();
		if let Some(id) = state.id() {
			self.string += &id.to_string();
			return;
		}
		let object = state.object().unwrap();
		let entries = object
			.entries
			.iter()
			.map(|(name, artifact)| (name.clone(), artifact.clone().into()))
			.collect();
		self.string += "tg.directory(";
		self.map(&entries);
		self.string.push(')');
	}

	fn file(&mut self, value: &tg::File) {
		let state = value.state().read().unwrap();
		if let Some(id) = state.id() {
			self.string += &id.to_string();
			return;
		}
		let object = state.object().unwrap();
		let mut map = BTreeMap::new();
		let contents = object.contents.clone().into();
		map.insert("contents".to_owned(), contents);
		if let Some(dependencies) = &object.dependencies {
			let dependencies = match dependencies {
				tg::file::Dependencies::Set(set) => set
					.iter()
					.cloned()
					.map(tg::Value::from)
					.collect::<Vec<_>>()
					.into(),
				tg::file::Dependencies::Map(map) => map
					.iter()
					.map(|(reference, object)| (reference.to_string(), object.clone().into()))
					.collect::<tg::value::Map>()
					.into(),
				tg::file::Dependencies::Lock(lock, index) => {
					if *index == 0 {
						lock.clone().into()
					} else {
						vec![lock.clone().into(), index.to_f64().into()].into()
					}
				},
			};
			map.insert("dependencies".to_owned(), dependencies);
		}
		let executable = object.executable.into();
		map.insert("executable".to_owned(), executable);
		self.string += "tg.file(";
		self.map(&map);
		self.string.push(')');
	}

	fn symlink(&mut self, value: &tg::Symlink) {
		let state = value.state().read().unwrap();
		if let Some(id) = state.id() {
			self.string += &id.to_string();
			return;
		}
		let object = state.object().unwrap();
		let mut map = BTreeMap::new();
		if let Some(artifact) = &object.artifact {
			map.insert("artifact".to_owned(), artifact.clone().into());
		}
		if let Some(path) = &object.path {
			map.insert("path".to_owned(), path.clone().into());
		}
		self.string += "tg.symlink(";
		self.map(&map);
		self.string.push(')');
	}

	fn lock(&mut self, value: &tg::Lock) {
		let state = value.state().read().unwrap();
		if let Some(id) = state.id() {
			self.string += &id.to_string();
			return;
		}
		let object = state.object().unwrap();
		let mut map = BTreeMap::new();
		let nodes = object
			.nodes
			.iter()
			.map(|node| {
				let mut map = BTreeMap::new();
				if let Some(dependencies) = &node.dependencies {
					let dependencies = dependencies
						.iter()
						.map(|(dependency, either)| {
							let either = match either {
								Either::Left(index) => index.to_f64().unwrap().into(),
								Either::Right(object) => object.clone().into(),
							};
							(dependency.to_string(), either)
						})
						.collect::<BTreeMap<String, tg::Value>>();
					map.insert("dependencies".to_owned(), dependencies.into());
				}
				if let Some(object) = &node.object {
					map.insert("object".to_owned(), object.clone().into());
				}
				map.into()
			})
			.collect::<Vec<_>>();
		map.insert("nodes".to_owned(), nodes.into());
		self.string += "tg.lock(";
		self.map(&map);
		self.string.push(')');
	}

	fn target(&mut self, value: &tg::Target) {
		let state = value.state().read().unwrap();
		if let Some(id) = state.id() {
			self.string += &id.to_string();
			return;
		}
		let object = state.object().unwrap();
		let mut map = BTreeMap::new();
		if !object.args.is_empty() {
			map.insert("args".to_owned(), object.args.clone().into());
		}
		if let Some(checksum) = &object.checksum {
			map.insert("checksum".to_owned(), checksum.to_string().into());
		}
		if !object.env.is_empty() {
			map.insert("env".to_owned(), object.env.clone().into());
		}
		if let Some(executable) = &object.executable {
			let key = "executable".to_owned();
			let value = executable.clone().into();
			map.insert(key, value);
		}
		map.insert("host".to_owned(), object.host.clone().into());
		self.string += "tg.target(";
		self.map(&map);
		self.string.push(')');
	}

	fn bytes(&mut self, value: &Bytes) {
		self.string += "tg.bytes(\"";
		self.string += &data_encoding::BASE64.encode(value);
		self.string += "\")";
	}

	fn path(&mut self, value: &tg::Path) {
		self.string += "tg.path(";
		self.string(value.as_ref());
		self.string.push(')');
	}

	fn mutation(&mut self, value: &tg::Mutation) {
		let mut map = BTreeMap::new();
		match value {
			tg::Mutation::Unset => {
				map.insert("kind".to_owned(), "unset".to_owned().into());
			},
			tg::Mutation::Set { value } => {
				map.insert("kind".to_owned(), "set".to_owned().into());
				map.insert("value".to_owned(), value.as_ref().clone());
			},
			tg::Mutation::SetIfUnset { value } => {
				map.insert("kind".to_owned(), "set_if_unset".to_owned().into());
				map.insert("value".to_owned(), value.as_ref().clone());
			},
			tg::Mutation::Prepend { values } => {
				map.insert("kind".to_owned(), "prepend".to_owned().into());
				map.insert("values".to_owned(), values.clone().into());
			},
			tg::Mutation::Append { values } => {
				map.insert("kind".to_owned(), "append".to_owned().into());
				map.insert("values".to_owned(), values.clone().into());
			},
			tg::Mutation::Prefix {
				template,
				separator,
			} => {
				map.insert("kind".to_owned(), "prefix".to_owned().into());
				if let Some(separator) = separator {
					map.insert("separator".to_owned(), separator.clone().into());
				}
				map.insert("template".to_owned(), template.clone().into());
			},
			tg::Mutation::Suffix {
				template,
				separator,
			} => {
				map.insert("kind".to_owned(), "suffix".to_owned().into());
				if let Some(separator) = separator {
					map.insert("separator".to_owned(), separator.clone().into());
				}
				map.insert("template".to_owned(), template.clone().into());
			},
		}
		self.string += "tg.mutation(";
		self.map(&map);
		self.string.push(')');
	}

	fn template(&mut self, value: &tg::Template) {
		let components = value
			.components
			.iter()
			.map(|component| match component {
				tg::template::Component::Artifact(artifact) => artifact.clone().into(),
				tg::template::Component::String(string) => string.clone().into(),
			})
			.collect();
		self.string += "tg.template(";
		self.array(&components);
		self.string.push(')');
	}
}
