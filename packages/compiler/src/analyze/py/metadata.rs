use {
	pep440_rs::{Version, VersionSpecifiers},
	ruff_python_ast::{Stmt, token::TokenKind},
	std::{
		collections::{BTreeMap, BTreeSet},
		ops::Range,
		path::Path,
	},
	tangram_client::prelude::*,
};

#[derive(Default, serde::Serialize)]
pub struct Metadata {
	pub imports: BTreeMap<String, tg::module::Import>,
}

#[derive(Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Script {
	dependencies: Option<toml::Spanned<Vec<String>>>,
	#[serde(rename = "requires-python")]
	requires_python: Option<toml::Spanned<String>>,
	#[serde(default)]
	tool: Tool,
}

#[derive(Default, serde::Deserialize)]
struct Tool {
	#[serde(default)]
	tangram: Config,
}

#[derive(Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
	#[serde(default)]
	imports: BTreeMap<String, toml::Spanned<Declaration>>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Declaration {
	#[serde(default)]
	attributes: BTreeMap<String, String>,
	specifier: String,
}

pub fn parse(path: &Path, text: &str) -> tg::Result<Metadata> {
	// Identify real, unindented comments with Ruff rather than matching strings.
	let parsed = ruff_python_parser::parse_module(text).map_err(|source| {
		let range = usize::from(source.location.start())..usize::from(source.location.end());
		located_error(
			path,
			text,
			range,
			&tg::error!(!source, "failed to parse the python module"),
		)
	})?;
	parse_module(path, text, &parsed)
}

pub(crate) fn parse_unchecked(path: &Path, text: &str) -> tg::Result<Metadata> {
	let parsed =
		ruff_python_parser::parse_unchecked_source(text, ruff_python_ast::PySourceType::Python);
	parse_module(path, text, &parsed)
}

fn parse_module(
	path: &Path,
	text: &str,
	parsed: &ruff_python_parser::Parsed<ruff_python_ast::ModModule>,
) -> tg::Result<Metadata> {
	let module = tg::module::Data {
		kind: tg::module::Kind::Py,
		referent: tg::Referent::with_node(tg::module::data::Source::Path(path.to_owned())),
	};
	super::validate_imports(&module, text, &parsed.syntax().body)?;
	let comments: BTreeSet<_> = parsed
		.tokens()
		.iter()
		.filter_map(|token| {
			let (kind, range) = token.as_tuple();
			(kind == TokenKind::Comment).then_some(usize::from(range.start()))
		})
		.collect();
	let mut offset = 0;
	let lines: Vec<_> = text
		.split_inclusive('\n')
		.map(|line| {
			let start = offset;
			offset += line.len();
			(start, line.trim_end_matches(['\r', '\n']))
		})
		.collect();
	let mut block: Option<(String, usize, String, Vec<(usize, usize)>)> = None;
	let mut types = BTreeSet::new();
	let mut output = Metadata::default();
	for (index, &(start, line)) in lines.iter().enumerate() {
		let comment = comments.contains(&start);
		let marker = line.strip_prefix("# /// ").filter(|name| {
			!name.is_empty()
				&& name
					.bytes()
					.all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
		});
		if comment && let Some(name) = marker {
			if block.is_some() {
				return Err(located_error(
					path,
					text,
					start..start + line.len(),
					&tg::error!("a metadata block cannot contain another block"),
				));
			}
			block = Some((name.to_owned(), start, String::new(), Vec::new()));
			continue;
		}
		let next_content = lines.get(index + 1).is_some_and(|(start, line)| {
			comments.contains(start) && (line == &"#" || line.starts_with("# "))
		});
		if comment && line == "# ///" && !next_content {
			if let Some((name, beginning, contents, positions)) = block.take() {
				if !types.insert(name.clone()) {
					return Err(located_error(
						path,
						text,
						beginning..start + line.len(),
						&tg::error!("duplicate metadata block: {name}"),
					));
				}
				if name == "script" {
					output = parse_script(path, text, &contents, &positions)?;
				}
			}
			continue;
		}
		if let Some((_, _, contents, positions)) = &mut block {
			let content = if comment {
				line.strip_prefix("# ")
					.or_else(|| (line == "#").then_some(""))
			} else {
				None
			};
			if let Some(content) = content {
				positions.push((contents.len(), start + line.len() - content.len()));
				contents.push_str(content);
				contents.push('\n');
			} else {
				// PEP 723 requires tools to ignore unclosed blocks.
				block = None;
			}
		}
	}
	Ok(output)
}

fn parse_script(
	path: &Path,
	text: &str,
	contents: &str,
	positions: &[(usize, usize)],
) -> tg::Result<Metadata> {
	let locate = |range: Range<usize>, error| {
		located_error(path, text, source_range(range, positions), &error)
	};
	let script: Script = toml::from_str(contents).map_err(|source: toml::de::Error| {
		let range = source.span().unwrap_or(0..0);
		locate(range, tg::error!(!source, "invalid script metadata"))
	})?;
	if let Some(dependencies) = script.dependencies
		&& !dependencies.get_ref().is_empty()
	{
		return Err(locate(
			dependencies.span(),
			tg::error!(
				"PEP 508 dependencies are not supported; declare Tangram imports under tool.tangram.imports"
			),
		));
	}
	if let Some(requirement) = script.requires_python {
		let specifiers = requirement
			.get_ref()
			.parse::<VersionSpecifiers>()
			.map_err(|source| {
				locate(
					requirement.span(),
					tg::error!(!source, "invalid requires-python requirement"),
				)
			})?;
		let distribution: serde_json::Value =
			serde_json::from_str(include_str!("../../../../py/distributions.json")).unwrap();
		let version = distribution["python"]
			.as_str()
			.unwrap()
			.parse::<Version>()
			.unwrap();
		if !specifiers.contains(&version) {
			return Err(locate(
				requirement.span(),
				tg::error!(
					"the embedded python {version} does not satisfy requires-python {}",
					requirement.get_ref()
				),
			));
		}
	}
	let mut imports = BTreeMap::new();
	for (name, declaration) in script.tool.tangram.imports {
		let valid = !name.contains('.') && ruff_python_parser::parse_module(&format!("import {name}")).is_ok_and(|parsed| {
			matches!(parsed.syntax().body.as_slice(), [Stmt::Import(import)] if import.names.len() == 1 && import.names[0].name.as_str() == name && import.names[0].asname.is_none())
		});
		if !valid {
			return Err(locate(
				declaration.span(),
				tg::error!("invalid python import name: {name}"),
			));
		}
		let value = declaration.get_ref();
		if value.specifier.is_empty() {
			return Err(locate(
				declaration.span(),
				tg::error!("the import specifier cannot be empty"),
			));
		}
		let import = tg::module::Import::with_specifier_and_attributes(
			&value.specifier,
			Some(value.attributes.clone()),
		)
		.map_err(|source| {
			locate(
				declaration.span(),
				tg::error!(!source, "invalid import declaration: {name}"),
			)
		})?;
		imports.insert(name, import);
	}
	Ok(Metadata { imports })
}

fn source_range(range: Range<usize>, positions: &[(usize, usize)]) -> Range<usize> {
	let position = |offset| {
		positions
			.iter()
			.rev()
			.find(|(start, _)| *start <= offset)
			.map_or(0, |(start, source)| source + offset - start)
	};
	position(range.start)..position(range.end)
}

fn located_error(path: &Path, text: &str, range: Range<usize>, error: &tg::Error) -> tg::Error {
	let range = tg::Range::try_from_byte_range_in_string(text, range, tg::position::Encoding::Utf8)
		.unwrap();
	let module = tg::Module {
		kind: tg::module::Kind::Py,
		referent: tg::Referent::with_node(tg::module::Source::Path(path.to_owned())),
	};
	let location = tg::error::Location {
		file: tg::error::File::Module(module),
		range,
		symbol: None,
	};
	let mut object = (**error.state().object().unwrap().unwrap_error_ref()).clone();
	object.location = Some(location);
	tg::Error::with_object(object)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn star_imports_have_source_locations() {
		for text in [
			"pass\nfrom . import *",
			"if False:\n    from math import *",
			"__all__ = ['default']\nfrom .other import *",
		] {
			let error = parse(Path::new("/test/main.tg.py"), text)
				.err()
				.unwrap()
				.to_data_or_id()
				.unwrap_left();
			let location = error.location.unwrap();
			assert_eq!(location.range.start.line, 1);
			let range = location
				.range
				.try_to_byte_range_in_string(text, tg::position::Encoding::Utf8)
				.unwrap();
			assert!(text[range].starts_with("from "));
		}
		assert!(parse(Path::new("/test/main.tg.py"), "text = 'from . import *'").is_ok());
	}

	#[test]
	fn declarations_preserve_attributed_imports() {
		let text = "# /// script\n# [tool.tangram.imports.debug]\n# specifier = 'tools/^1'\n# attributes = { get = 'debug/tangram.py' }\n# [tool.tangram.imports.release]\n# specifier = 'tools/^1'\n# attributes = { get = 'release/tangram.py' }\n# ///\n\nimport debug\n";
		let metadata = parse(Path::new("/test/main.tg.py"), text).unwrap();
		assert_eq!(metadata.imports.len(), 2);
		assert_ne!(metadata.imports["debug"], metadata.imports["release"]);
		assert_eq!(
			metadata.imports["debug"].reference.options().get.as_deref(),
			Some(Path::new("debug/tangram.py"))
		);
	}

	#[test]
	fn metadata_uses_real_top_level_comments() {
		for text in [
			"text = '''\n# /// script\n# dependencies = ['ignored']\n# ///\n'''\n",
			"# /// script\n# dependencies = ['ignored']\n",
			"if True:\n    # /// script\n    # dependencies = ['ignored']\n    # ///\n    pass\n",
		] {
			assert!(
				parse(Path::new("/test/main.tg.py"), text)
					.unwrap()
					.imports
					.is_empty()
			);
		}
	}

	#[test]
	fn invalid_metadata_has_source_locations() {
		for contents in [
			"dependencies = ['requests']",
			"requires-python = '<3'",
			"requires-python = 'invalid'",
			"[tool.tangram.imports.bad]\nspecifier = 1",
			"[tool.tangram.imports.'bad.name']\nspecifier = 'tools'",
			"[tool.tangram.imports.bad]\nspecifier = ''",
			"[tool.tangram.imports.bad]\nspecifier = 'tools'\nattributes = { get = true }",
		] {
			let contents = format!("# {}\n", contents.replace('\n', "\n# "));
			let text = format!("# /// script\n{contents}# ///\n\npass\n");
			let error = parse(Path::new("/test/main.tg.py"), &text)
				.err()
				.unwrap()
				.to_data_or_id()
				.unwrap_left();
			assert!(error.location.is_some());
		}
	}

	#[test]
	fn duplicate_blocks_are_rejected_and_crlf_is_supported() {
		let duplicate = "# /// script\n# ///\n\n# /// script\n# ///\n";
		assert!(parse(Path::new("/test/main.tg.py"), duplicate).is_err());
		let text = "# /// script\r\n# [tool.tangram.imports.tools]\r\n# specifier = 'tools/^1'\r\n# ///\r\n\r\npass\r\n";
		assert_eq!(
			parse(Path::new("/test/main.tg.py"), text)
				.unwrap()
				.imports
				.len(),
			1
		);
	}
}
