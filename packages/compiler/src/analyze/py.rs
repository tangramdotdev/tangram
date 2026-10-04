use {
	super::Analysis,
	ruff_python_ast::{
		Stmt,
		statement_visitor::{self, StatementVisitor as _},
	},
	std::{collections::BTreeSet, path::Path},
	tangram_client::prelude::*,
};

pub mod metadata;

struct Visitor {
	references: BTreeSet<String>,
}

pub fn analyze(path: &Path, text: &str) -> tg::Result<Analysis> {
	let parsed = ruff_python_parser::parse_module(text).map_err(|error| {
		let byte_range = usize::from(error.location.start())..usize::from(error.location.end());
		let range = tg::Range::try_from_byte_range_in_string(
			text,
			byte_range,
			tg::position::Encoding::Utf8,
		)
		.expect("the parser error range must be within the source text");
		let module = tg::Module {
			kind: tg::module::Kind::Py,
			referent: tg::Referent::with_node(tg::module::Source::Path(path.to_owned())),
		};
		let location = tg::error::Location {
			file: tg::error::File::Module(module),
			range,
			symbol: None,
		};
		let mut object = tg::error::Object {
			location: Some(location),
			..Default::default()
		};
		tg::error!({ object }, !error, "failed to parse the Python module");
		tg::Error::with_object(object)
	})?;
	let mut visitor = Visitor {
		references: BTreeSet::new(),
	};
	visitor.visit_body(&parsed.syntax().body);
	let directory = path
		.parent()
		.ok_or_else(|| tg::error!("the Python module has no parent directory"))?;
	let mut imports = std::collections::HashSet::default();
	let metadata = metadata::parse(path, text)?;
	imports.extend(metadata.imports.into_values());
	// Capture explicit package initializers, including ancestors of namespace directories.
	for (level, ancestor) in directory.ancestors().enumerate() {
		if level == 0 && path.file_name().is_some_and(|name| name == "tangram.py") {
			continue;
		}
		if !ancestor.join("tangram.py").is_file() {
			continue;
		}
		let prefix = if level == 0 {
			".".to_owned()
		} else {
			vec![".."; level].join("/")
		};
		let reference = format!("{prefix}/tangram.py").parse()?;
		let import = tg::module::Import {
			kind: Some(tg::module::Kind::Py),
			reference,
		};
		imports.insert(import);
	}
	for reference in visitor.references {
		let file = format!("{reference}.tg.py");
		let package = format!("{reference}/tangram.py");
		let file_exists = directory.join(&file).is_file();
		let package_exists = directory.join(&package).is_file();
		if file_exists && package_exists {
			return Err(tg::error!("ambiguous Python module: {file} and {package}"));
		}
		let reference = if file_exists {
			file
		} else if package_exists {
			package
		} else {
			continue;
		};
		let import = tg::module::Import {
			kind: Some(tg::module::Kind::Py),
			reference: reference.parse()?,
		};
		imports.insert(import);
	}
	let analysis = Analysis {
		diagnostics: Vec::new(),
		imports,
	};
	Ok(analysis)
}

impl<'a> statement_visitor::StatementVisitor<'a> for Visitor {
	fn visit_stmt(&mut self, statement: &'a Stmt) {
		if let Stmt::ImportFrom(import) = statement
			&& import.level > 0
		{
			let mut reference = if import.level == 1 {
				".".to_owned()
			} else {
				vec![".."; (import.level - 1) as usize].join("/")
			};
			if let Some(module) = &import.module {
				for part in module.as_str().split('.') {
					reference.push('/');
					reference.push_str(part);
					self.references.insert(reference.clone());
				}
			}
			for alias in &import.names {
				if alias.name.as_str() != "*" {
					self.references
						.insert(format!("{reference}/{}", alias.name));
				}
			}
		}
		statement_visitor::walk_stmt(self, statement);
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn python_parse_errors_have_source_locations() {
		let path = Path::new("/test/main.tg.py");
		for source in ["pass\nx = )", "pass\nx = '🙂'; )", "pass\nx = ("] {
			let parsed = ruff_python_parser::parse_module(source).unwrap_err();
			let error = analyze(path, source)
				.err()
				.unwrap()
				.to_data_or_id()
				.unwrap_left();
			let location = error.location.unwrap();
			let tg::error::data::File::Module(module) = location.file else {
				panic!("expected a module location");
			};
			assert_eq!(module.kind, tg::module::Kind::Py);
			assert_eq!(module.referent.node.unwrap_path(), path);
			assert_eq!(location.range.start.line, 1);
			let range = location
				.range
				.try_to_byte_range_in_string(source, tg::position::Encoding::Utf8)
				.unwrap();
			assert_eq!(range.start, usize::from(parsed.location.start()));
			assert_eq!(range.end, usize::from(parsed.location.end()));
			assert!(error.source.is_some());
		}
	}

	#[test]
	fn python_relative_imports_use_the_ruff_ast() {
		let source = r#"
import absolute
from absolute import value
text = "from .fake import value"
from . import helper as alias
from .sub.child import value
def default():
    from ..other import value
    from ... import parent
"#;
		let parsed = ruff_python_parser::parse_module(source).unwrap();
		let mut visitor = Visitor {
			references: BTreeSet::new(),
		};
		visitor.visit_body(&parsed.syntax().body);
		let expected = [
			"../../parent",
			"../other",
			"../other/value",
			"./helper",
			"./sub",
			"./sub/child",
			"./sub/child/value",
		]
		.map(str::to_owned)
		.into_iter()
		.collect();
		assert_eq!(visitor.references, expected);
	}
}
