use {
	super::Analysis,
	ruff_python_ast::{
		Expr, Stmt,
		visitor::{self, Visitor as _},
	},
	std::{collections::BTreeSet, path::Path},
	tangram_client::prelude::*,
};

pub mod metadata;

struct Visitor {
	references: BTreeSet<String>,
}

struct ImportValidator<'a> {
	star: Option<&'a ruff_python_ast::StmtImportFrom>,
}

pub fn validate_imports(module: &tg::module::Data, text: &str, body: &[Stmt]) -> tg::Result<()> {
	let mut visitor = ImportValidator { star: None };
	visitor.visit_body(body);
	let Some(import) = visitor.star else {
		return Ok(());
	};
	let bytes = usize::from(import.range.start())..usize::from(import.range.end());
	let range = tg::Range::try_from_byte_range_in_string(text, bytes, tg::position::Encoding::Utf8)
		.unwrap();
	let location = tg::error::Location {
		file: tg::error::File::Module(tg::Module::try_from_data(module.without_token())?),
		range,
		symbol: None,
	};
	let mut object = tg::error::Object {
		location: Some(location),
		..Default::default()
	};
	tg::error!(
		{ object },
		"star imports are not supported in python modules; use explicit imports"
	);
	Err(tg::Error::with_object(object))
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
		tg::error!({ object }, !error, "failed to parse the python module");
		tg::Error::with_object(object)
	})?;
	let mut visitor = Visitor {
		references: BTreeSet::new(),
	};
	visitor.visit_body(&parsed.syntax().body);
	let directory = path
		.parent()
		.ok_or_else(|| tg::error!("the python module has no parent directory"))?;
	let root = directory
		.ancestors()
		.filter(|ancestor| ancestor.join("tangram.py").is_file())
		.last();
	// Record the module's own package-relative path as a file dependency, without capturing a directory.
	let root = root.unwrap_or(directory);
	let depth = directory.strip_prefix(root).unwrap().components().count();
	let prefix = if depth == 0 {
		".".to_owned()
	} else {
		vec![".."; depth].join("/")
	};
	let reference = Path::new(&prefix).join(path.strip_prefix(root).unwrap());
	let reference = tg::Reference::with_path(tangram_util::path::normalize(reference));
	let mut imports = std::collections::HashSet::default();
	let import = tg::module::Import {
		kind: Some(tg::module::Kind::Py),
		reference,
	};
	imports.insert(import);
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
	for reference in &visitor.references {
		if !tangram_util::path::normalize(directory.join(reference)).starts_with(root) {
			continue;
		}
		let file = format!("{reference}.tg.py");
		let package = format!("{reference}/tangram.py");
		let file_exists = directory.join(&file).is_file();
		let package_exists = directory.join(&package).is_file();
		if file_exists && package_exists {
			return Err(tg::error!("ambiguous python module: {file} and {package}"));
		}
		let reference = if file_exists {
			file
		} else if package_exists {
			package
		} else if directory.join(reference).is_dir()
			&& !visitor
				.references
				.iter()
				.any(|other| other.starts_with(&format!("{reference}/")))
		{
			let import = tg::module::Import {
				kind: Some(tg::module::Kind::Directory),
				reference: reference.parse()?,
			};
			imports.insert(import);
			continue;
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

impl Visitor {
	fn reference(&mut self, level: usize, name: &str) -> String {
		let mut reference = if level == 1 {
			".".to_owned()
		} else {
			vec![".."; level - 1].join("/")
		};
		for part in name.split('.').filter(|part| !part.is_empty()) {
			reference.push('/');
			reference.push_str(part);
			self.references.insert(reference.clone());
		}
		reference
	}
}

impl<'a> visitor::Visitor<'a> for Visitor {
	fn visit_stmt(&mut self, statement: &'a Stmt) {
		if let Stmt::ImportFrom(import) = statement
			&& import.level > 0
		{
			let reference = self.reference(
				import.level as usize,
				import.module.as_deref().unwrap_or_default(),
			);
			for alias in &import.names {
				if alias.name.as_str() != "*" {
					self.references
						.insert(format!("{reference}/{}", alias.name));
				}
			}
		}
		visitor::walk_stmt(self, statement);
	}
	fn visit_expr(&mut self, expression: &'a Expr) {
		if let Expr::Call(call) = expression
			&& let Expr::Attribute(function) = call.func.as_ref()
			&& matches!(function.attr.as_str(), "import_module" | "find_spec")
			&& (matches!(function.value.as_ref(), Expr::Name(name) if name.id.as_str() == "importlib")
				|| matches!(function.value.as_ref(), Expr::Attribute(attribute) if attribute.attr.as_str() == "util" && matches!(attribute.value.as_ref(), Expr::Name(name) if name.id.as_str() == "importlib")))
			&& matches!(call.arguments.find_argument_value("package", 1), Some(Expr::Name(name)) if name.id.as_str() == "__package__")
			&& let Some(Expr::StringLiteral(name)) = call.arguments.find_argument_value("name", 0)
		{
			let name = name.value.to_str();
			let level = name.len() - name.trim_start_matches('.').len();
			if level > 0 {
				self.reference(level, &name[level..]);
			}
		}
		visitor::walk_expr(self, expression);
	}
}

impl<'a> visitor::Visitor<'a> for ImportValidator<'a> {
	fn visit_stmt(&mut self, statement: &'a Stmt) {
		if self.star.is_some() {
			return;
		}
		if let Stmt::ImportFrom(import) = statement
			&& import.names.iter().any(|name| name.name.as_str() == "*")
		{
			self.star = Some(import);
			return;
		}
		visitor::walk_stmt(self, statement);
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn python_self_reference_preserves_the_filename() {
		let path = Path::new("/python-test/task?value#1.tg.py");
		let module = tg::module::Data {
			kind: tg::module::Kind::Py,
			referent: tg::Referent::with_node(tg::module::data::Source::Path(path.to_owned())),
		};
		let analysis = crate::Compiler::analyze(&module, "pass").unwrap();
		let expected = tg::Reference::with_path(Path::new("task?value#1.tg.py").to_owned());
		assert!(
			analysis
				.imports
				.iter()
				.any(|import| import.reference == expected)
		);
	}

	#[test]
	fn python_self_reference_round_trips_through_a_lock() {
		let module = tg::module::Data {
			kind: tg::module::Kind::Py,
			referent: tg::Referent::with_node(tg::module::data::Source::Path(
				"/python-test/tangram.py".into(),
			)),
		};
		let analysis = crate::Compiler::analyze(&module, "pass").unwrap();
		for import in analysis.imports {
			let encoded = serde_json::to_string(&import.reference).unwrap();
			let decoded: tg::Reference = serde_json::from_str(&encoded).unwrap();
			assert_eq!(decoded, import.reference);
		}
	}

	#[test]
	fn python_parse_errors_have_source_locations() {
		let path = Path::new("/test/main.tg.py");
		let module = tg::module::Data {
			kind: tg::module::Kind::Py,
			referent: tg::Referent::with_node(tg::module::data::Source::Path(path.to_owned())),
		};
		for source in ["pass\nx = )", "pass\nx = '🙂'; )", "pass\nx = ("] {
			let parsed = ruff_python_parser::parse_module(source).unwrap_err();
			let error = crate::Compiler::analyze(&module, source)
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
importlib.import_module(".late.child", __package__)
importlib.util.find_spec("..sibling", __package__)
importlib.import_module(name=".keyword", package=__package__)
importlib.util.find_spec(name=".spec", package=__package__)
other.import_module(".not_an_import")
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
			"./keyword",
			"./late",
			"./late/child",
			"../sibling",
			"./spec",
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
