use crate as tg;
use bytes::Bytes;
use num::ToPrimitive;
use std::fmt::Result;
use tangram_either::Either;

pub struct Printer<W> {
	first: bool,
	indent: u32,
	style: Style,
	writer: W,
}

#[derive(derive_more::IsVariant)]
pub enum Style {
	Compact,
	Pretty { indentation: &'static str },
}

impl<W> Printer<W>
where
	W: std::fmt::Write,
{
	pub fn new(writer: W, style: Style) -> Self {
		Self {
			first: true,
			indent: 0,
			style,
			writer,
		}
	}

	fn indent(&mut self) -> Result {
		if let Style::Pretty { indentation } = &self.style {
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
		if self.style.is_pretty() {
			writeln!(self.writer)?;
			self.indent()?;
		}
		write!(self.writer, "\"{key}\":")?;
		if self.style.is_pretty() {
			write!(self.writer, " ")?;
		}
		f(self)?;
		self.first = false;
		Ok(())
	}

	fn finish_map(&mut self) -> Result {
		self.indent -= 1;
		if !self.first && self.style.is_pretty() {
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
		if self.style.is_pretty() {
			writeln!(self.writer)?;
			self.indent()?;
		}
		f(self)?;
		self.first = false;
		Ok(())
	}

	fn finish_array(&mut self) -> Result {
		self.indent -= 1;
		if !self.first && self.style.is_pretty() {
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

	fn null(&mut self) -> Result {
		write!(self.writer, "null")
	}

	fn bool(&mut self, value: bool) -> Result {
		write!(self.writer, "{}", if value { "true" } else { "false" })
	}

	fn number(&mut self, value: f64) -> Result {
		write!(self.writer, "{value}")
	}

	fn string(&mut self, value: &str) -> Result {
		write!(self.writer, "\"{value}\"")
	}

	fn array(&mut self, value: &tg::value::Array) -> Result {
		self.start_array()?;
		for value in value {
			self.array_value(|s| s.value(value))?;
		}
		self.finish_array()?;
		Ok(())
	}

	fn map(&mut self, value: &tg::value::Map) -> Result {
		self.start_map()?;
		for (key, value) in value {
			self.map_entry(key, |s| s.value(value))?;
		}
		self.finish_map()?;
		Ok(())
	}

	fn object(&mut self, value: &tg::Object) -> Result {
		match value {
			tg::Object::Leaf(v) => self.leaf(v),
			tg::Object::Branch(v) => self.branch(v),
			tg::Object::Directory(v) => self.directory(v),
			tg::Object::File(v) => self.file(v),
			tg::Object::Symlink(v) => self.symlink(v),
			tg::Object::Graph(v) => self.graph(v),
			tg::Object::Target(v) => self.target(v),
		}
	}

	fn blob(&mut self, value: &tg::Blob) -> Result {
		match value {
			tg::Blob::Leaf(leaf) => self.leaf(leaf),
			tg::Blob::Branch(branch) => self.branch(branch),
		}
	}

	fn leaf(&mut self, value: &tg::Leaf) -> Result {
		let state = value.state().read().unwrap();
		if let Some(id) = state.id() {
			write!(self.writer, "{id}")?;
			return Ok(());
		}
		let object = state.object().unwrap();
		let bytes = data_encoding::BASE64.encode(&object.bytes);
		write!(self.writer, "tg.leaf(")?;
		self.string(&bytes)?;
		write!(self.writer, ")")?;
		Ok(())
	}

	fn branch(&mut self, value: &tg::Branch) -> Result {
		let state = value.state().read().unwrap();
		if let Some(id) = state.id() {
			write!(self.writer, "{id}")?;
			return Ok(());
		}
		let object = state.object().unwrap();
		write!(self.writer, "tg.branch(")?;
		self.start_map()?;
		for child in &object.children {
			self.map_entry("size", |s| s.number(child.size.to_f64().unwrap()))?;
			self.map_entry("blob", |s| s.blob(&child.blob))?;
		}
		self.finish_map()?;
		write!(self.writer, ")")?;
		Ok(())
	}

	fn artifact(&mut self, value: &tg::Artifact) -> Result {
		match value {
			tg::Artifact::Directory(directory) => self.directory(directory),
			tg::Artifact::File(file) => self.file(file),
			tg::Artifact::Symlink(symlink) => self.symlink(symlink),
		}
	}

	fn directory(&mut self, value: &tg::Directory) -> Result {
		let state = value.state().read().unwrap();
		if let Some(id) = state.id() {
			write!(self.writer, "{id}")?;
			return Ok(());
		}
		let object = state.object().unwrap();
		write!(self.writer, "tg.directory(")?;
		self.start_map()?;
		match object.as_ref() {
			tg::directory::Object::Normal { entries } => {
				for (name, artifact) in entries {
					self.map_entry(name, |s| s.artifact(artifact))?;
				}
			},
			tg::directory::Object::Graph { graph, node } => {
				self.map_entry("graph", |s| s.graph(graph))?;
				self.map_entry("node", |s| s.number(node.to_f64().unwrap()))?;
			},
		}
		self.finish_map()?;
		write!(self.writer, ")")?;
		Ok(())
	}

	fn file(&mut self, value: &tg::File) -> Result {
		let state = value.state().read().unwrap();
		if let Some(id) = state.id() {
			write!(self.writer, "{id}")?;
			return Ok(());
		}
		let object = state.object().unwrap();
		write!(self.writer, "tg.file(")?;
		self.start_map()?;
		match object.as_ref() {
			tg::file::Object::Normal {
				contents,
				dependencies,
				executable,
			} => {
				self.map_entry("contents", |s| s.blob(contents))?;
				if !dependencies.is_empty() {
					self.map_entry("dependencies", |s| {
						s.start_map()?;
						for (reference, referent) in dependencies {
							s.map_entry(reference.as_str(), |s| {
								s.start_map()?;
								s.map_entry("item", |s| s.object(&referent.item))?;
								if let Some(tag) = &referent.tag {
									s.map_entry("tag", |s| s.string(tag.as_str()))?;
								}
								if let Some(subpath) = &referent.subpath {
									s.map_entry("subpath", |s| {
										s.string(subpath.to_string_lossy().as_ref())
									})?;
								}
								s.finish_map()?;
								Ok(())
							})?;
						}
						s.finish_map()?;
						Ok(())
					})?;
				}
				if *executable {
					self.map_entry("executable", |s| s.bool(true))?;
				}
			},
			tg::file::Object::Graph { graph, node } => {
				self.map_entry("graph", |s| s.graph(graph))?;
				self.map_entry("node", |s| s.number(node.to_f64().unwrap()))?;
			},
		}
		self.finish_map()?;
		write!(self.writer, ")")?;
		Ok(())
	}

	fn symlink(&mut self, value: &tg::Symlink) -> Result {
		let state = value.state().read().unwrap();
		if let Some(id) = state.id() {
			write!(self.writer, "{id}")?;
			return Ok(());
		}
		let object = state.object().unwrap();
		write!(self.writer, "tg.symlink(")?;
		self.start_map()?;
		match object.as_ref() {
			tg::symlink::Object::Normal { artifact, subpath } => {
				if let Some(artifact) = &artifact {
					self.map_entry("artifact", |s| s.artifact(artifact))?;
				}
				if let Some(subpath) = &subpath {
					self.map_entry("subpath", |s| s.string(subpath.to_string_lossy().as_ref()))?;
				}
			},
			tg::symlink::Object::Graph { graph, node } => {
				self.map_entry("graph", |s| s.graph(graph))?;
				self.map_entry("node", |s| s.number(node.to_f64().unwrap()))?;
			},
		}
		self.finish_map()?;
		write!(self.writer, ")")?;
		Ok(())
	}

	fn graph(&mut self, value: &tg::Graph) -> Result {
		let state = value.state().read().unwrap();
		if let Some(id) = state.id() {
			write!(self.writer, "{id}")?;
			return Ok(());
		}
		let object = state.object().unwrap();
		write!(self.writer, "tg.graph(")?;
		self.start_map()?;
		if !object.nodes.is_empty() {
			self.map_entry("nodes", |s| {
				s.start_array()?;
				for node in &object.nodes {
					match node {
						tg::graph::Node::Directory(directory) => {
							s.start_map()?;
							s.map_entry("kind", |s| s.string("directory"))?;
							s.map_entry("entries", |s| {
								s.start_map()?;
								for (name, either) in &directory.entries {
									s.map_entry(name, |s| {
										match either {
											Either::Left(index) => {
												s.number(index.to_f64().unwrap())?;
											},
											Either::Right(artifact) => {
												s.artifact(artifact)?;
											},
										}
										Ok(())
									})?;
								}
								s.finish_map()?;
								Ok(())
							})?;
							s.finish_map()?;
						},
						tg::graph::Node::File(file) => {
							s.map_entry("kind", |s| s.string("file"))?;
							s.map_entry("contents", |s| s.blob(&file.contents))?;
							if !file.dependencies.is_empty() {
								s.map_entry("dependencies", |s| {
									s.start_map()?;
									for (reference, referent) in &file.dependencies {
										s.map_entry(reference.as_str(), |s| {
											s.start_map()?;
											s.map_entry("item", |s| {
												match &referent.item {
													Either::Left(index) => {
														s.number(index.to_f64().unwrap())?;
													},
													Either::Right(object) => {
														s.object(object)?;
													},
												}
												Ok(())
											})?;
											if let Some(tag) = &referent.tag {
												s.map_entry("tag", |s| s.string(tag.as_str()))?;
											}
											if let Some(subpath) = &referent.subpath {
												s.map_entry("subpath", |s| {
													s.string(subpath.to_string_lossy().as_ref())
												})?;
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
								s.map_entry("executable", |s| s.bool(file.executable))?;
							}
						},
						tg::graph::Node::Symlink(symlink) => {
							s.map_entry("kind", |s| s.string("symlink"))?;
							if let Some(artifact) = &symlink.artifact {
								s.map_entry("artifact", |s| {
									match &artifact {
										Either::Left(index) => {
											s.number(index.to_f64().unwrap())?;
										},
										Either::Right(artifact) => {
											s.artifact(artifact)?;
										},
									}
									Ok(())
								})?;
							}
							if let Some(subpath) = &symlink.subpath {
								s.map_entry("subpath", |s| {
									s.string(subpath.to_string_lossy().as_ref())
								})?;
							}
						},
					}
				}
				s.finish_array()?;
				Ok(())
			})?;
		}
		self.finish_map()?;
		write!(self.writer, ")")?;
		Ok(())
	}

	fn target(&mut self, value: &tg::Target) -> Result {
		let state = value.state().read().unwrap();
		if let Some(id) = state.id() {
			write!(self.writer, "{id}")?;
			return Ok(());
		}
		let object = state.object().unwrap();
		write!(self.writer, "tg.target(")?;
		self.start_map()?;
		if !object.args.is_empty() {
			self.map_entry("args", |s| s.array(&object.args))?;
		}
		if let Some(checksum) = &object.checksum {
			self.map_entry("checksum", |s| write!(s.writer, "\"{checksum}\""))?;
		}
		if !object.env.is_empty() {
			self.map_entry("env", |s| s.map(&object.env))?;
		}
		if let Some(executable) = &object.executable {
			self.map_entry("executable", |s| s.artifact(executable))?;
		}
		self.map_entry("host", |s| s.string(&object.host))?;
		self.finish_map()?;
		write!(self.writer, ")")?;
		Ok(())
	}

	fn bytes(&mut self, value: &Bytes) -> Result {
		write!(self.writer, "tg.bytes(")?;
		write!(self.writer, "\"{}\"", data_encoding::BASE64.encode(value))?;
		write!(self.writer, ")")?;
		Ok(())
	}

	fn mutation(&mut self, value: &tg::Mutation) -> Result {
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
		}
		self.finish_map()?;
		write!(self.writer, ")")?;
		Ok(())
	}

	fn template(&mut self, value: &tg::Template) -> Result {
		write!(self.writer, "tg.template(")?;
		self.start_array()?;
		for component in &value.components {
			match component {
				tg::template::Component::Artifact(artifact) => {
					self.artifact(artifact)?;
				},
				tg::template::Component::String(string) => {
					self.string(string)?;
				},
			}
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
		let style = Style::Compact;
		let mut printer = Printer::new(&mut left, style);
		printer.start_map().unwrap();
		printer.finish_map().unwrap();
		let right = "{}";
		assert_eq!(left, right);

		let mut left = String::new();
		let style = Style::Compact;
		let mut printer = Printer::new(&mut left, style);
		printer.start_map().unwrap();
		printer.map_entry("foo", |s| s.string("bar")).unwrap();
		printer.finish_map().unwrap();
		let right = r#"{"foo":"bar"}"#;
		assert_eq!(left, right);

		let mut left = String::new();
		let style = Style::Compact;
		let mut printer = Printer::new(&mut left, style);
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
		let style = Style::Pretty { indentation: "\t" };
		let mut printer = Printer::new(&mut left, style);
		printer.start_map().unwrap();
		printer.finish_map().unwrap();
		let right = "{}";
		assert_eq!(left, right);

		let mut left = String::new();
		let style = Style::Pretty { indentation: "\t" };
		let mut printer = Printer::new(&mut left, style);
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
		let style = Style::Pretty { indentation: "\t" };
		let mut printer = Printer::new(&mut left, style);
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
		let style = Style::Pretty { indentation: "\t" };
		let mut printer = Printer::new(&mut left, style);
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
