use crate as tg;
use bytes::Bytes;
use num::ToPrimitive as _;
use std::fmt::Result;

pub struct Printer<W> {
	depth: u64,
	first: bool,
	indent: u32,
	options: Options,
	writer: W,
}

#[derive(Clone, Debug, Default)]
pub struct Options {
	pub depth: Option<u64>,
	pub style: Style,
	pub blobs: bool,
}

#[derive(Clone, Debug, Default, derive_more::IsVariant)]
pub enum Style {
	#[default]
	Compact,
	Pretty {
		indentation: &'static str,
	},
}

impl<W> Printer<W>
where
	W: std::fmt::Write,
{
	pub fn new(writer: W, options: Options) -> Self {
		Self {
			depth: 0,
			first: true,
			indent: 0,
			options,
			writer,
		}
	}

	fn indent(&mut self) -> Result {
		if let Style::Pretty { indentation } = &self.options.style {
			for _ in 0..self.indent {
				write!(self.writer, "{indentation}")?;
			}
		}
		Ok(())
	}

	fn start_map(&mut self) -> Result {
		self.first = true;
		write!(self.writer, "{{")?;
		self.indent += 1;
		Ok(())
	}

	fn map_entry(&mut self, key: &str, f: impl FnOnce(&mut Self) -> Result) -> Result {
		if !self.first {
			write!(self.writer, ",")?;
		}
		if self.options.style.is_pretty() {
			writeln!(self.writer)?;
			self.indent()?;
		}
		write!(self.writer, "\"{key}\":")?;
		if self.options.style.is_pretty() {
			write!(self.writer, " ")?;
		}
		f(self)?;
		self.first = false;
		Ok(())
	}

	fn finish_map(&mut self) -> Result {
		self.indent -= 1;
		if !self.first && self.options.style.is_pretty() {
			write!(self.writer, ",")?;
			writeln!(self.writer)?;
			self.indent()?;
		}
		write!(self.writer, "}}")?;
		self.first = false;
		Ok(())
	}

	fn start_array(&mut self) -> Result {
		self.first = true;
		write!(self.writer, "[")?;
		self.indent += 1;
		Ok(())
	}

	fn array_value(&mut self, f: impl FnOnce(&mut Self) -> Result) -> Result {
		if !self.first {
			write!(self.writer, ",")?;
		}
		if self.options.style.is_pretty() {
			writeln!(self.writer)?;
			self.indent()?;
		}
		f(self)?;
		self.first = false;
		Ok(())
	}

	fn finish_array(&mut self) -> Result {
		self.indent -= 1;
		if !self.first && self.options.style.is_pretty() {
			write!(self.writer, ",")?;
			writeln!(self.writer)?;
			self.indent()?;
		}
		write!(self.writer, "]")?;
		self.first = false;
		Ok(())
	}

	pub fn value(&mut self, value: &tg::Value) -> Result {
		match value {
			tg::Value::Null => self.null(),
			tg::Value::Bool(v) => self.bool(*v),
			tg::Value::Number(v) => self.number(*v),
			tg::Value::String(v) => self.string(v),
			tg::Value::Array(v) => self.array(v),
			tg::Value::Map(v) => self.map(v),
			tg::Value::Object(v) => self.object(v),
			tg::Value::Bytes(v) => self.bytes(v),
			tg::Value::Mutation(v) => self.mutation(v),
			tg::Value::Template(v) => self.template(v),
		}
	}

	pub fn null(&mut self) -> Result {
		write!(self.writer, "null")
	}

	pub fn bool(&mut self, value: bool) -> Result {
		write!(self.writer, "{}", if value { "true" } else { "false" })
	}

	pub fn number(&mut self, value: f64) -> Result {
		write!(self.writer, "{value}")
	}

	pub fn string(&mut self, value: &str) -> Result {
		let value = serde_json::to_string(value).unwrap();
		write!(self.writer, "{value}")
	}

	pub fn array(&mut self, value: &tg::value::Array) -> Result {
		self.depth += 1;
		self.start_array()?;
		for value in value {
			self.array_value(|s| s.value(value))?;
		}
		self.finish_array()?;
		Ok(())
	}

	pub fn map(&mut self, value: &tg::value::Map) -> Result {
		self.depth += 1;
		self.start_map()?;
		for (key, value) in value {
			self.map_entry(key, |s| s.value(value))?;
		}
		self.finish_map()?;
		Ok(())
	}

	pub fn object(&mut self, value: &tg::Object) -> Result {
		match value {
			tg::Object::Blob(v) => self.blob(v),
			tg::Object::Directory(v) => self.directory(v),
			tg::Object::File(v) => self.file(v),
			tg::Object::Symlink(v) => self.symlink(v),
			tg::Object::Graph(v) => self.graph(v),
			tg::Object::Command(v) => self.command(v),
		}
	}

	pub fn artifact(&mut self, value: &tg::Artifact) -> Result {
		match value {
			tg::Artifact::Directory(directory) => self.directory(directory),
			tg::Artifact::File(file) => self.file(file),
			tg::Artifact::Symlink(symlink) => self.symlink(symlink),
		}
	}

	pub fn blob(&mut self, value: &tg::Blob) -> Result {
		let state = value.state().read().unwrap();
		let recurse = self.options.blobs && self.depth < self.options.depth.unwrap_or(u64::MAX);
		self.depth += 1;
		match (&state.id, &state.object, recurse) {
			(Some(id), None, _) | (Some(id), Some(_), false) => {
				write!(self.writer, "{id}")?;
			},
			(None, Some(object), _) | (Some(_), Some(object), true) => {
				self.blob_object(object)?;
			},
			(None, None, _) => unreachable!(),
		}
		Ok(())
	}

	fn blob_object(&mut self, object: &tg::blob::Object) -> Result {
		write!(self.writer, "tg.blob(")?;
		match object {
			tg::blob::Object::Leaf(object) => {
				if let Ok(string) = String::from_utf8(object.bytes.to_vec()) {
					self.string(&string)?;
				}
			},
			tg::blob::Object::Branch(object) => {
				self.start_map()?;
				for child in &object.children {
					self.map_entry("length", |s| s.number(child.length.to_f64().unwrap()))?;
					self.map_entry("blob", |s| s.blob(&child.blob))?;
				}
				self.finish_map()?;
			},
		}
		write!(self.writer, ")")?;
		Ok(())
	}

	pub fn directory(&mut self, value: &tg::Directory) -> Result {
		let state = value.state().read().unwrap();
		let recurse = self.depth < self.options.depth.unwrap_or(u64::MAX);
		self.depth += 1;
		match (&state.id, &state.object, recurse) {
			(Some(id), None, _) | (Some(id), Some(_), false) => {
				write!(self.writer, "{id}")?;
			},
			(None, Some(object), _) | (Some(_), Some(object), true) => {
				self.directory_object(object)?;
			},
			(None, None, _) => unreachable!(),
		}
		Ok(())
	}

	fn directory_object(&mut self, object: &tg::directory::Object) -> Result {
		write!(self.writer, "tg.directory(")?;
		match object {
			tg::directory::Object::Reference(reference) => {
				self.graph_reference(reference)?;
			},
			tg::directory::Object::Node(node) => {
				if !node.entries.is_empty() {
					self.start_map()?;
					for (name, edge) in &node.entries {
						self.map_entry(name, |s| s.graph_edge_artifact(edge))?;
					}
					self.finish_map()?;
				}
			},
		}
		write!(self.writer, ")")?;
		Ok(())
	}

	fn directory_node(&mut self, directory: &tg::directory::object::Node, tag: bool) -> Result {
		self.start_map()?;
		if tag {
			self.map_entry("kind", |s| s.string("directory"))?;
		}
		if !directory.entries.is_empty() {
			self.map_entry("entries", |s| {
				s.start_map()?;
				for (name, edge) in &directory.entries {
					s.map_entry(name, |s| s.graph_edge_artifact(edge))?;
				}
				s.finish_map()?;
				Ok(())
			})?;
		}
		self.finish_map()
	}

	pub fn file(&mut self, value: &tg::File) -> Result {
		let state = value.state().read().unwrap();
		let recurse = self.depth < self.options.depth.unwrap_or(u64::MAX);
		self.depth += 1;
		match (&state.id, &state.object, recurse) {
			(Some(id), None, _) | (Some(id), Some(_), false) => {
				write!(self.writer, "{id}")?;
			},
			(None, Some(object), _) | (Some(_), Some(object), true) => {
				self.file_object(object)?;
			},
			(None, None, _) => unreachable!(),
		}
		Ok(())
	}

	fn file_object(&mut self, object: &tg::file::Object) -> Result {
		write!(self.writer, "tg.file(")?;
		match object {
			tg::file::Object::Reference(reference) => {
				self.graph_reference(reference)?;
			},
			tg::file::Object::Node(node) => {
				self.file_node(node, false)?;
			},
		}
		write!(self.writer, ")")?;
		Ok(())
	}

	fn file_node(&mut self, file: &tg::file::object::Node, tag: bool) -> Result {
		self.start_map()?;
		if tag {
			self.map_entry("kind", |s| s.string("file"))?;
		}
		self.map_entry("contents", |s| s.blob(&file.contents))?;
		if !file.dependencies.is_empty() {
			self.map_entry("dependencies", |s| {
				s.start_map()?;
				for (reference, referent) in &file.dependencies {
					s.map_entry(reference.as_str(), |s| {
						s.start_map()?;
						s.map_entry("item", |s| s.graph_edge_object(referent.item()))?;
						if let Some(path) = referent.path() {
							s.map_entry("path", |s| s.string(path.to_string_lossy().as_ref()))?;
						}
						if let Some(tag) = referent.tag() {
							s.map_entry("tag", |s| s.string(tag.as_str()))?;
						}
						s.finish_map()?;
						Ok(())
					})?;
				}
				s.finish_map()?;
				Ok(())
			})?;
		}
		if file.executable {
			self.map_entry("executable", |s| s.bool(file.executable))?;
		}
		self.finish_map()
	}

	pub fn symlink(&mut self, value: &tg::Symlink) -> Result {
		let state = value.state().read().unwrap();
		let recurse = self.depth < self.options.depth.unwrap_or(u64::MAX);
		self.depth += 1;
		match (&state.id, &state.object, recurse) {
			(Some(id), None, _) | (Some(id), Some(_), false) => {
				write!(self.writer, "{id}")?;
			},
			(None, Some(object), _) | (Some(_), Some(object), true) => {
				self.symlink_object(object)?;
			},
			(None, None, _) => unreachable!(),
		}
		Ok(())
	}

	fn symlink_object(&mut self, object: &tg::symlink::Object) -> Result {
		write!(self.writer, "tg.symlink(")?;
		match object {
			tg::symlink::Object::Reference(reference) => {
				self.graph_reference(reference)?;
			},
			tg::symlink::Object::Node(node) => {
				self.symlink_node(node, false)?;
			},
		}
		write!(self.writer, ")")?;
		Ok(())
	}

	fn symlink_node(&mut self, symlink: &tg::symlink::object::Node, tag: bool) -> Result {
		self.start_map()?;
		if tag {
			self.map_entry("kind", |s| s.string("symlink"))?;
		}
		if let Some(artifact) = &symlink.artifact {
			self.map_entry("artifact", |s| s.graph_edge_artifact(artifact))?;
		}
		if let Some(path) = &symlink.path {
			self.map_entry("path", |s| s.string(path.to_string_lossy().as_ref()))?;
		}
		self.finish_map()
	}

	pub fn graph(&mut self, value: &tg::Graph) -> Result {
		let state = value.state().read().unwrap();
		let recurse = self.depth < self.options.depth.unwrap_or(u64::MAX);
		self.depth += 1;
		match (&state.id, &state.object, recurse) {
			(Some(id), None, _) | (Some(id), Some(_), false) => {
				write!(self.writer, "{id}")?;
			},
			(None, Some(object), _) | (Some(_), Some(object), true) => {
				self.graph_object(object)?;
			},
			(None, None, _) => unreachable!(),
		}
		Ok(())
	}

	fn graph_object(&mut self, object: &tg::graph::Object) -> Result {
		write!(self.writer, "tg.graph(")?;
		self.start_map()?;
		if !object.nodes.is_empty() {
			self.map_entry("nodes", |s| {
				s.start_array()?;
				for node in &object.nodes {
					s.array_value(|s| match node {
						tg::graph::Node::Directory(directory) => s.directory_node(directory, true),
						tg::graph::Node::File(file) => s.file_node(file, true),
						tg::graph::Node::Symlink(symlink) => s.symlink_node(symlink, true),
					})?;
				}
				s.finish_array()?;
				Ok(())
			})?;
		}
		self.finish_map()?;
		write!(self.writer, ")")?;
		Ok(())
	}

	fn graph_edge_object(&mut self, edge: &tg::graph::object::Edge<tg::Object>) -> Result {
		match edge {
			tg::graph::object::Edge::Reference(reference) => {
				self.graph_reference(reference)?;
			},
			tg::graph::object::Edge::Object(object) => {
				self.object(object)?;
			},
		}
		Ok(())
	}

	fn graph_edge_artifact(&mut self, edge: &tg::graph::object::Edge<tg::Artifact>) -> Result {
		match edge {
			tg::graph::object::Edge::Reference(reference) => {
				self.graph_reference(reference)?;
			},
			tg::graph::object::Edge::Object(object) => {
				self.artifact(object)?;
			},
		}
		Ok(())
	}

	fn graph_reference(&mut self, reference: &tg::graph::object::Reference) -> Result {
		self.start_map()?;
		if let Some(graph) = &reference.graph {
			self.map_entry("graph", |s| s.graph(graph))?;
		}
		self.map_entry("node", |s| s.number(reference.node.to_f64().unwrap()))?;
		self.finish_map()?;
		Ok(())
	}

	pub fn command(&mut self, value: &tg::Command) -> Result {
		let state = value.state().read().unwrap();
		let recurse = self.depth < self.options.depth.unwrap_or(u64::MAX);
		self.depth += 1;
		match (&state.id, &state.object, recurse) {
			(Some(id), None, _) | (Some(id), Some(_), false) => {
				write!(self.writer, "{id}")?;
			},
			(None, Some(object), _) | (Some(_), Some(object), true) => {
				self.command_object(object)?;
			},
			(None, None, _) => unreachable!(),
		}
		Ok(())
	}

	fn command_object(&mut self, object: &tg::command::Object) -> Result {
		write!(self.writer, "tg.command(")?;
		self.start_map()?;
		if !object.args.is_empty() {
			self.map_entry("args", |s| s.array(&object.args))?;
		}
		if let Some(cwd) = &object.cwd {
			self.map_entry("cwd", |s| s.string(cwd.to_string_lossy().as_ref()))?;
		}
		if !object.env.is_empty() {
			self.map_entry("env", |s| s.map(&object.env))?;
		}
		self.map_entry("executable", |s| match &object.executable {
			tg::command::Executable::Artifact(executable) => {
				s.command_executable_artifact(executable)
			},
			tg::command::Executable::Module(executable) => s.command_executable_module(executable),
			tg::command::Executable::Path(executable) => {
				s.string(executable.path.to_string_lossy().as_ref())
			},
		})?;
		self.map_entry("host", |s| s.string(&object.host))?;
		if !object.mounts.is_empty() {
			self.map_entry("mounts", |s| {
				s.start_array()?;
				for mount in &object.mounts {
					s.start_map()?;
					s.map_entry("source", |s| s.artifact(&mount.source))?;
					s.map_entry("target", |s| {
						s.string(mount.target.to_string_lossy().as_ref())
					})?;
					s.finish_map()?;
				}
				s.finish_array()?;
				Ok(())
			})?;
		}
		self.finish_map()?;
		write!(self.writer, ")")?;
		Ok(())
	}

	pub fn command_executable_artifact(
		&mut self,
		value: &tg::command::ArtifactExecutable,
	) -> Result {
		self.start_map()?;
		self.map_entry("artifact", |s| s.artifact(&value.artifact))?;
		if let Some(path) = &value.path {
			self.map_entry("path", |s| s.string(path.to_string_lossy().as_ref()))?;
		}
		self.finish_map()?;
		Ok(())
	}

	pub fn command_executable_module(&mut self, value: &tg::command::ModuleExecutable) -> Result {
		self.start_map()?;
		self.map_entry("module", |s| s.module(&value.module))?;
		if let Some(export) = &value.export {
			self.map_entry("export", |s| s.string(export))?;
		}
		self.finish_map()?;
		Ok(())
	}

	pub fn module(&mut self, value: &tg::Module) -> Result {
		self.start_map()?;
		self.map_entry("kind", |s| s.string(&value.kind.to_string()))?;
		self.map_entry("referent", |s| {
			s.start_map()?;
			s.map_entry("item", |s| match &value.referent.item {
				tg::module::Item::Path(path) => s.string(path.to_string_lossy().as_ref()),
				tg::module::Item::Object(object) => s.object(object),
			})?;
			if let Some(path) = &value.referent.path() {
				s.map_entry("path", |s| s.string(path.to_string_lossy().as_ref()))?;
			}
			if let Some(tag) = &value.referent.tag() {
				s.map_entry("tag", |s| s.string(tag.as_str()))?;
			}
			s.finish_map()?;
			Ok(())
		})?;
		self.finish_map()?;
		Ok(())
	}

	pub fn bytes(&mut self, value: &Bytes) -> Result {
		write!(self.writer, "tg.bytes(")?;
		write!(self.writer, "\"{}\"", data_encoding::BASE64.encode(value))?;
		write!(self.writer, ")")?;
		Ok(())
	}

	pub fn mutation(&mut self, value: &tg::Mutation) -> Result {
		self.depth += 1;
		write!(self.writer, "tg.mutation(")?;
		self.start_map()?;
		match value {
			tg::Mutation::Unset => {
				self.map_entry("kind", |s| s.string("unset"))?;
			},
			tg::Mutation::Set { value } => {
				self.map_entry("kind", |s| s.string("set"))?;
				self.map_entry("value", |s| s.value(value.as_ref()))?;
			},
			tg::Mutation::SetIfUnset { value } => {
				self.map_entry("kind", |s| s.string("set_if_unset"))?;
				self.map_entry("value", |s| s.value(&value.as_ref().clone()))?;
			},
			tg::Mutation::Prepend { values } => {
				self.map_entry("kind", |s| s.string("prepend"))?;
				self.map_entry("values", |s| s.array(values))?;
			},
			tg::Mutation::Append { values } => {
				self.map_entry("kind", |s| s.string("append"))?;
				self.map_entry("values", |s| s.array(values))?;
			},
			tg::Mutation::Prefix {
				template,
				separator,
			} => {
				self.map_entry("kind", |s| s.string("prefix"))?;
				if let Some(separator) = separator {
					self.map_entry("separator", |s| s.string(separator))?;
				}
				self.map_entry("template", |s| s.template(template))?;
			},
			tg::Mutation::Suffix {
				template,
				separator,
			} => {
				self.map_entry("kind", |s| s.string("suffix"))?;
				if let Some(separator) = separator {
					self.map_entry("separator", |s| s.string(separator))?;
				}
				self.map_entry("template", |s| s.template(template))?;
			},
			tg::Mutation::Merge { value } => {
				self.map_entry("kind", |s| s.string("merge"))?;
				self.map_entry("value", |s| s.map(value))?;
			},
		}
		self.finish_map()?;
		write!(self.writer, ")")?;
		Ok(())
	}

	pub fn template(&mut self, value: &tg::Template) -> Result {
		self.depth += 1;
		write!(self.writer, "tg.template(")?;
		self.start_array()?;
		for component in &value.components {
			self.array_value(|s| {
				match component {
					tg::template::Component::Artifact(artifact) => {
						s.artifact(artifact)?;
					},
					tg::template::Component::String(string) => {
						s.string(string)?;
					},
				}
				Ok(())
			})?;
		}
		self.finish_array()?;
		write!(self.writer, ")")?;
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use indoc::indoc;

	#[test]
	fn compact_map() {
		let mut left = String::new();
		let options = Options::default();
		let mut printer = Printer::new(&mut left, options);
		printer.start_map().unwrap();
		printer.finish_map().unwrap();
		let right = "{}";
		assert_eq!(left, right);

		let mut left = String::new();
		let options = Options::default();
		let mut printer = Printer::new(&mut left, options);
		printer.start_map().unwrap();
		printer.map_entry("foo", |s| s.string("bar")).unwrap();
		printer.finish_map().unwrap();
		let right = r#"{"foo":"bar"}"#;
		assert_eq!(left, right);

		let mut left = String::new();
		let options = Options::default();
		let mut printer = Printer::new(&mut left, options);
		printer.start_map().unwrap();
		printer.map_entry("foo", |s| s.string("bar")).unwrap();
		printer.map_entry("baz", |s| s.string("qux")).unwrap();
		printer.finish_map().unwrap();
		let right = r#"{"foo":"bar","baz":"qux"}"#;
		assert_eq!(left, right);
	}

	#[test]
	fn pretty_map() {
		let mut left = String::new();
		let options = Options {
			style: Style::Pretty { indentation: "\t" },
			..Default::default()
		};
		let mut printer = Printer::new(&mut left, options);
		printer.start_map().unwrap();
		printer.finish_map().unwrap();
		let right = "{}";
		assert_eq!(left, right);

		let mut left = String::new();
		let options = Options {
			style: Style::Pretty { indentation: "\t" },
			..Default::default()
		};
		let mut printer = Printer::new(&mut left, options);
		printer.start_map().unwrap();
		printer.map_entry("foo", |s| s.string("bar")).unwrap();
		printer.finish_map().unwrap();
		let right = indoc!(
			r#"
				{
					"foo": "bar",
				}
			"#
		)
		.trim();
		assert_eq!(left, right);

		let mut left = String::new();
		let options = Options {
			style: Style::Pretty { indentation: "\t" },
			..Default::default()
		};
		let mut printer = Printer::new(&mut left, options);
		printer.start_map().unwrap();
		printer.map_entry("foo", |s| s.string("bar")).unwrap();
		printer.map_entry("baz", |s| s.string("qux")).unwrap();
		printer.finish_map().unwrap();
		let right = indoc!(
			r#"
				{
					"foo": "bar",
					"baz": "qux",
				}
			"#
		)
		.trim();
		assert_eq!(left, right);

		let mut left = String::new();
		let options = Options {
			style: Style::Pretty { indentation: "\t" },
			..Default::default()
		};
		let mut printer = Printer::new(&mut left, options);
		printer.start_map().unwrap();
		printer
			.map_entry("foo", |s| {
				s.start_map()?;
				s.map_entry("foo", |s| s.string("foo"))?;
				s.finish_map()?;
				Ok(())
			})
			.unwrap();
		printer
			.map_entry("bar", |s| {
				s.start_map()?;
				s.map_entry("bar", |s| s.string("bar"))?;
				s.finish_map()?;
				Ok(())
			})
			.unwrap();
		printer.finish_map().unwrap();
		let right = indoc!(
			r#"
				{
					"foo": {
						"foo": "foo",
					},
					"bar": {
						"bar": "bar",
					},
				}
			"#
		)
		.trim();
		assert_eq!(left, right);
	}
}
