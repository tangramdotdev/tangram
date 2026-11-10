#[cfg(not(all(feature = "javascript", feature = "python")))]
use tangram_client::prelude::*;
#[cfg(all(feature = "javascript", feature = "python"))]
use {
	std::{
		collections::{BTreeMap, BTreeSet},
		fmt::Write as _,
	},
	tangram_client::prelude::*,
};

#[cfg(all(feature = "javascript", feature = "python"))]
mod python;
#[cfg(all(feature = "javascript", feature = "python"))]
mod typescript;

/// The globals that Python bindings define before they bind their re-exports, which are the future import of annotations and the client that the runtime injects.
#[cfg(all(feature = "javascript", feature = "python"))]
const GLOBALS: &[&str] = &["annotations", "tg"];

/// The kinds of modules that Python loads as bindings, whose exports are the commands that a module re-exporting them exposes.
#[cfg(all(feature = "javascript", feature = "python"))]
const KINDS: [tg::module::Kind; 5] = [
	tg::module::Kind::JavaScript,
	tg::module::Kind::Json,
	tg::module::Kind::Toml,
	tg::module::Kind::TypeScript,
	tg::module::Kind::Yaml,
];

/// The prefix of the comment that records the source of each export in bindings.
const MARKER: &str = "tangram: ";

/// The source of an export in bindings, recorded so that positions in the bindings can be mapped to the original module.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub(crate) struct Source {
	/// The name of the export.
	pub(crate) name: String,

	/// The range of the export's name in the original module, with UTF-8 character offsets.
	utf8: [u32; 4],

	/// The range of the export's name in the original module, with UTF-16 character offsets.
	utf16: [u32; 4],
}

/// Load a module in a consuming language, generating its bindings from its interface without types if the module is in another language.
#[cfg(all(feature = "javascript", feature = "python"))]
pub fn module(
	module: &tg::module::Data,
	text: &str,
	language: Option<tg::module::load::Language>,
) -> tg::Result<String> {
	let Some(language) = bindings_language(module, language)? else {
		return Ok(text.to_owned());
	};
	let interface = crate::interface::module(module, text)
		.map_err(|error| tg::error!(!error, "failed to discover the cross-language exports"))?;
	let output = bindings(&interface, text, language);
	Ok(output)
}

/// Get the language of the bindings that a consuming language loads in place of a module, or `None` when the consuming language loads the module as written.
#[cfg(all(feature = "javascript", feature = "python"))]
pub(crate) fn bindings_language(
	module: &tg::module::Data,
	language: Option<tg::module::load::Language>,
) -> tg::Result<Option<tg::module::load::Language>> {
	match (language, module.kind) {
		(Some(_), tg::module::Kind::Json | tg::module::Kind::Toml | tg::module::Kind::Yaml)
		| (Some(tg::module::load::Language::JavaScript), tg::module::Kind::Python)
		| (
			Some(tg::module::load::Language::Python),
			tg::module::Kind::JavaScript | tg::module::Kind::TypeScript,
		) => Ok(language),
		(Some(tg::module::load::Language::Python), tg::module::Kind::TypeScriptDeclaration) => {
			Err(tg::error!("cannot execute a declaration module in python"))
		},
		_ => Ok(None),
	}
}

/// Determine whether Python bindings bind a re-export to its module, given the module's kind if it is known. A namespace is the module, if Python imports the module. A re-exported export is the module's binding, if Python can name the export and loads the module as bindings, so that Python reaches the same command that the re-exporting module exposes.
#[cfg(all(feature = "javascript", feature = "python"))]
pub(crate) fn forwards(reexport: &tg::interface::Reexport, kind: Option<tg::module::Kind>) -> bool {
	let kinds = self::kinds(reexport);
	let nameable = reexport.export.as_ref().is_none_or(|member| {
		python::binding(member, &BTreeSet::new()).is_ok_and(|binding| binding == *member)
	});
	nameable && kind.is_none_or(|kind| kinds.contains(&kind))
}

/// Get the module that an interface imports which the interface's bindings import with a specifier.
#[cfg(all(feature = "javascript", feature = "python"))]
pub(crate) fn try_get_import<'a>(
	interface: &'a tg::Interface,
	specifier: &str,
) -> Option<&'a tg::module::Data> {
	let name = import_specifiers(interface)
		.into_iter()
		.find_map(|(name, other)| (other == specifier).then_some(name))?;
	interface.imports.get(&name)
}

/// Generate the bindings of an interface in a language, which export a command for each export of the interface.
#[cfg(all(feature = "javascript", feature = "python"))]
#[must_use]
pub fn bindings(
	interface: &tg::Interface,
	text: &str,
	language: tg::module::load::Language,
) -> String {
	match language {
		tg::module::load::Language::JavaScript => typescript_bindings(interface, text),
		tg::module::load::Language::Python => python_bindings(interface, text),
	}
}

#[cfg(not(all(feature = "javascript", feature = "python")))]
pub fn module(
	module: &tg::module::Data,
	text: &str,
	language: Option<tg::module::load::Language>,
) -> tg::Result<String> {
	if !cfg!(feature = "python")
		&& (module.kind == tg::module::Kind::Python
			|| language == Some(tg::module::load::Language::Python))
	{
		return Err(tg::error!("the python feature is not enabled"));
	}
	if !cfg!(feature = "javascript")
		&& (matches!(
			module.kind,
			tg::module::Kind::JavaScript
				| tg::module::Kind::TypeScript
				| tg::module::Kind::TypeScriptDeclaration
		) || language == Some(tg::module::load::Language::JavaScript))
	{
		return Err(tg::error!("the javascript feature is not enabled"));
	}
	Ok(text.to_owned())
}

/// Replace a diagnostic's message with the reason that an export is not a value, if the message refers to the type that carries the reason.
#[cfg(any(feature = "javascript", feature = "python"))]
pub(crate) fn non_value_message(message: String) -> String {
	let reason = message.split_once("NonValue").and_then(|(_, rest)| {
		if let Some(rest) = rest.strip_prefix("<\"") {
			let (reason, _) = rest.split_once("\">")?;
			serde_json::from_str::<String>(&format!("\"{reason}\"")).ok()
		} else {
			let rest = rest.strip_prefix("[Literal[\"")?;
			let (reason, _) = rest.split_once("\"]]")?;
			Some(reason.to_owned())
		}
	});
	match reason {
		Some(reason) => format!("cannot use the export: {reason}"),
		None => message,
	}
}

/// Get the source of the export that contains an offset in bindings.
pub(crate) fn source(text: &str, offset: usize) -> Option<Source> {
	let text = text.get(..offset)?;
	text.lines().rev().find_map(|line| {
		let json = line
			.strip_prefix("// ")
			.or_else(|| line.strip_prefix("# "))?
			.strip_prefix(MARKER)?;
		serde_json::from_str(json).ok()
	})
}

impl Source {
	#[cfg(all(feature = "javascript", feature = "python"))]
	fn new(name: &str, export: &tg::interface::Export, text: &str) -> Option<Self> {
		let range = export
			.range?
			.try_to_byte_range_in_string(text, tg::position::Encoding::Utf8)?;
		let convert = |encoding| {
			let range = tg::Range::try_from_byte_range_in_string(text, range.clone(), encoding)?;
			Some([
				range.start.line,
				range.start.character,
				range.end.line,
				range.end.character,
			])
		};
		let source = Self {
			name: name.to_owned(),
			utf8: convert(tg::position::Encoding::Utf8)?,
			utf16: convert(tg::position::Encoding::Utf16)?,
		};
		Some(source)
	}

	/// Get the range of the export's name in the original module.
	#[must_use]
	pub(crate) fn range(&self, encoding: tg::position::Encoding) -> tg::Range {
		let [start_line, start_character, end_line, end_character] = match encoding {
			tg::position::Encoding::Utf8 => self.utf8,
			tg::position::Encoding::Utf16 => self.utf16,
		};
		tg::Range {
			end: tg::Position {
				character: end_character,
				line: end_line,
			},
			start: tg::Position {
				character: start_character,
				line: start_line,
			},
		}
	}
}

/// Generate the TypeScript bindings of an interface, which export a command for each export of the interface.
#[cfg(all(feature = "javascript", feature = "python"))]
fn typescript_bindings(interface: &tg::Interface, text: &str) -> String {
	let mut output = String::new();

	// Import the modules whose definitions the types refer to, as namespaces that the compiler resolves from the interface's imports.
	let specifiers = import_specifiers(interface);
	let imports = interface
		.imports
		.keys()
		.enumerate()
		.map(|(index, name)| (name.clone(), format!("$i{index}")))
		.collect::<BTreeMap<_, _>>();
	for (name, namespace) in &imports {
		let quoted = serde_json::to_string(&specifiers[name]).unwrap();
		writeln!(output, "import type * as {namespace} from {quoted};").unwrap();
	}

	// Export the interface's named types by their names, so that other bindings can import them.
	let definitions = interface
		.definitions
		.keys()
		.enumerate()
		.map(|(index, name)| {
			let alias = if typescript::is_identifier(name) {
				name.clone()
			} else {
				format!("$t{index}")
			};
			(name.clone(), alias)
		})
		.collect::<BTreeMap<_, _>>();
	let names = typescript::Names {
		definitions,
		imports,
	};
	for (name, type_) in &interface.definitions {
		let alias = &names.definitions[name];
		let type_ = typescript::type_(type_, &names);
		writeln!(output, "export type {alias} = {type_};").unwrap();
	}

	for (index, (name, export)) in interface.exports.iter().enumerate() {
		// Re-export a namespace or an export from its module, which resolves relative to the module, recording the source of the re-export.
		let quoted = serde_json::to_string(name).unwrap();
		if let Some(reexport) = &export.reexport {
			if let Some(source) = Source::new(name, export, text) {
				writeln!(
					output,
					"// {MARKER}{}",
					serde_json::to_string(&source).unwrap()
				)
				.unwrap();
			}
			let import = &reexport.import;
			let specifier = serde_json::to_string(&import.reference.to_string()).unwrap();
			match &reexport.export {
				None => write!(output, "export * as {quoted} from {specifier}"),
				Some(member) => {
					let member = serde_json::to_string(member).unwrap();
					write!(output, "export {{ {member} as {quoted} }} from {specifier}")
				},
			}
			.unwrap();
			if let Some(kind) = import.kind {
				let kind = serde_json::to_string(&kind.to_string()).unwrap();
				write!(output, " with {{ kind: {kind} }}").unwrap();
			}
			output.push_str(";\n");
			continue;
		}

		// Define a function whose name is the export's name, so that a command built from it targets the export.
		writeln!(
			output,
			"const f{index} = {{ async {quoted}() {{}} }}[{quoted}];"
		)
		.unwrap();

		// Record the source of the export and its documentation.
		if let Some(source) = Source::new(name, export, text) {
			writeln!(
				output,
				"// {MARKER}{}",
				serde_json::to_string(&source).unwrap()
			)
			.unwrap();
		}
		if let Some(documentation) = &export.documentation {
			output.push_str("/**\n");
			for line in documentation.replace("*/", "*\\/").lines() {
				if line.is_empty() {
					output.push_str(" *\n");
				} else {
					writeln!(output, " * {line}").unwrap();
				}
			}
			output.push_str(" */\n");
		}

		// Export the command.
		let type_ = typescript::export(export, &names);
		writeln!(
			output,
			"const c{index}: {type_} = tg.command(f{index}) as unknown as {type_};\nexport {{ c{index} as {quoted} }};"
		)
		.unwrap();
	}
	output.push_str("export {};\n");

	output
}

/// Generate the Python bindings of an interface, which bind a command to each export of the interface that Python can name, and bind each re-export to the bindings of its module.
#[cfg(all(feature = "javascript", feature = "python"))]
fn python_bindings(interface: &tg::Interface, text: &str) -> String {
	let mut output = String::new();

	// Bind each export that Python can name.
	let names = interface.exports.keys().cloned().collect();
	let bindings = interface
		.exports
		.iter()
		.filter_map(|(name, export)| {
			let binding = python::binding(name, &names).ok()?;
			Some((binding, (name, export)))
		})
		.collect::<BTreeMap<_, _>>();

	// Choose helper names that cannot shadow a binding or a definition, preferring the client's global name.
	let mut prefix = "_tg".to_owned();
	while bindings
		.keys()
		.chain(interface.definitions.keys())
		.any(|name| name.starts_with(&prefix))
	{
		prefix.push('_');
	}

	// Bind each re-export to the bindings of its module if Python can, checking at runtime that Python loads the module as bindings if its import does not determine its kind, and declaring each module as a dependency once.
	let mut modules = Vec::<&tg::module::Import>::new();
	let mut reexports = BTreeMap::new();
	for (binding, (_, export)) in &bindings {
		let Some(reexport) = &export.reexport else {
			continue;
		};
		let kind = self::kind(&reexport.import);
		if !forwards(reexport, kind) {
			continue;
		}
		let index = if let Some(index) = modules
			.iter()
			.position(|import| **import == reexport.import)
		{
			index
		} else {
			modules.push(&reexport.import);
			modules.len() - 1
		};
		let dependency = format!("{prefix}_reexport_{index}");
		let fallback = kind
			.is_none()
			.then(|| format!("{prefix}_fallback_{}", reexports.len()));
		reexports.insert(binding.clone(), (dependency, reexport, fallback));
	}

	// Declare the modules whose definitions the types refer to as dependencies, which the compiler resolves from the interface's imports, and the modules of the re-exports as dependencies, which resolve relative to the module.
	let specifiers = import_specifiers(interface);
	let imports = interface
		.imports
		.keys()
		.enumerate()
		.map(|(index, name)| (name.clone(), format!("{prefix}_import_{index}")))
		.collect::<BTreeMap<_, _>>();
	if !imports.is_empty() || !modules.is_empty() {
		output.push_str("# /// script\n");
		for (name, module) in &imports {
			let quoted = serde_json::to_string(&specifiers[name]).unwrap();
			writeln!(output, "# [tool.tangram.dependencies.{module}]").unwrap();
			writeln!(output, "# specifier = {quoted}").unwrap();
		}
		for (index, import) in modules.iter().enumerate() {
			let quoted = serde_json::to_string(&import.reference.to_string()).unwrap();
			writeln!(
				output,
				"# [tool.tangram.dependencies.{prefix}_reexport_{index}]"
			)
			.unwrap();
			writeln!(output, "# specifier = {quoted}").unwrap();
			if let Some(kind) = import.kind {
				let quoted = serde_json::to_string(&kind.to_string()).unwrap();
				writeln!(output, "# kind = {quoted}").unwrap();
			}
		}
		output.push_str("# ///\n");
	}
	output.push_str("from __future__ import annotations\n");
	let typing = format!("{prefix}_typing");
	writeln!(output, "import typing as {typing}").unwrap();
	let mut references = BTreeSet::new();
	for type_ in interface.definitions.values().chain(
		interface
			.exports
			.values()
			.filter_map(|export| export.type_.as_ref()),
	) {
		python::references(type_, &mut references);
	}
	if !references.is_empty() {
		writeln!(output, "if {typing}.TYPE_CHECKING:").unwrap();
		for (import, name) in &references {
			let Some(module) = imports.get(import) else {
				continue;
			};
			writeln!(output, "    from {module} import {name} as {module}_{name}").unwrap();
		}
	}
	let client = if bindings.contains_key("tg") {
		writeln!(output, "import tangram as {prefix}_client").unwrap();
		format!("{prefix}_client")
	} else {
		"tg".to_owned()
	};

	// Name the interface's definitions by their names, so that other bindings can import them, unless a name is not a Python identifier or a binding uses it.
	let definitions = interface
		.definitions
		.keys()
		.enumerate()
		.map(|(index, name)| {
			let alias = if python::is_identifier(name) && !bindings.contains_key(name) {
				name.clone()
			} else {
				format!("{prefix}_type_{index}")
			};
			(name.clone(), alias)
		})
		.collect::<BTreeMap<_, _>>();
	let names = python::Names {
		client: client.clone(),
		definitions,
		imports,
		literals: std::cell::RefCell::default(),
		prefix: prefix.clone(),
		typing: typing.clone(),
	};

	// Print the definitions and the types of the exports, typing a re-export that Python binds as a command, which has no type in an interface without types, as a command whose args and output are any values.
	let definitions = interface
		.definitions
		.iter()
		.map(|(name, type_)| python::definition(&names.definitions[name], type_, &names))
		.collect::<Vec<_>>();
	let commands = bindings
		.iter()
		.filter(|(binding, _)| !reexports.contains_key(*binding))
		.collect::<Vec<_>>();
	let types = commands
		.iter()
		.map(|(_, (_, export))| {
			if export.reexport.is_some() {
				python::type_(&crate::interface::untyped_command(), &names)
			} else {
				python::export(export, &names)
			}
		})
		.collect::<Vec<_>>();
	for literal in names.literals.borrow().iter() {
		writeln!(output, "{literal}").unwrap();
	}
	for definition in &definitions {
		writeln!(output, "{definition}").unwrap();
	}

	// Define a function for each re-export whose module's kind is known only at runtime, so that the re-export can be a command for this module's export if Python cannot bind it to the module.
	for (binding, (_, _, fallback)) in &reexports {
		let Some(function) = fallback else {
			continue;
		};
		let (name, _) = bindings[binding];
		writeln!(output, "async def {function}(): pass").unwrap();
		let quoted = serde_json::to_string(name).unwrap();
		writeln!(output, "{function}.__tangram_export__ = {quoted}").unwrap();
	}

	for (index, ((binding, (name, export)), type_)) in commands.into_iter().zip(types).enumerate() {
		// Define a function that records the export's name, so that a command built from it targets the export.
		let function = format!("{prefix}_function_{index}");
		writeln!(output, "async def {function}(): pass").unwrap();
		let quoted = serde_json::to_string(name).unwrap();
		writeln!(output, "{function}.__tangram_export__ = {quoted}").unwrap();

		// Record the source of the export.
		if let Some(source) = Source::new(name, export, text) {
			writeln!(
				output,
				"# {MARKER}{}",
				serde_json::to_string(&source).unwrap()
			)
			.unwrap();
		}

		// Bind the command, followed by the export's documentation.
		let quoted_type = serde_json::to_string(&type_).unwrap();
		writeln!(
			output,
			"{binding} = {typing}.cast({quoted_type}, {client}.command({function}))"
		)
		.unwrap();
		if let Some(documentation) = &export.documentation {
			writeln!(output, "{}", serde_json::to_string(documentation).unwrap()).unwrap();
		}
	}

	// Import the bindings of each re-export's module for the checker, recording the source of the re-export after the sources of the commands, so that no other binding maps to it.
	let import = |binding: &str, dependency: &str, reexport: &tg::interface::Reexport| {
		match &reexport.export {
			None => format!("import {dependency} as {binding}"),
			Some(member) => format!("from {dependency} import {member} as {binding}"),
		}
	};
	if !reexports.is_empty() {
		writeln!(output, "if {typing}.TYPE_CHECKING:").unwrap();
		for (binding, (dependency, reexport, _)) in &reexports {
			let (name, export) = bindings[binding];
			if let Some(source) = Source::new(name, export, text) {
				writeln!(
					output,
					"# {MARKER}{}",
					serde_json::to_string(&source).unwrap()
				)
				.unwrap();
			}
			writeln!(output, "    {}", import(binding, dependency, reexport)).unwrap();
		}

		// Remove the globals that would hide the re-exports from the module's attribute lookup at runtime.
		writeln!(output, "else:").unwrap();
		for binding in reexports.keys() {
			if GLOBALS.contains(&binding.as_str()) {
				writeln!(output, "    del {binding}").unwrap();
			}
		}

		// Import the built-in names that the helpers use with a name that no export can shadow.
		let builtins = format!("{prefix}_builtins");
		writeln!(output, "    import builtins as {builtins}").unwrap();

		// Define a function that gets the kind of a dependency's module from its resolution, without executing the module.
		let kind = format!("{prefix}_kind");
		if reexports
			.values()
			.any(|(_, _, fallback)| fallback.is_some())
		{
			writeln!(output, "    def {kind}({prefix}_dependency):").unwrap();
			writeln!(output, "        import importlib.util").unwrap();
			writeln!(
				output,
				"        {prefix}_spec = importlib.util.find_spec({prefix}_dependency)"
			)
			.unwrap();
			writeln!(
				output,
				"        return {builtins}.getattr({builtins}.getattr({prefix}_spec, \"loader_state\", None), \"kind\", None)"
			)
			.unwrap();
		}

		// Import each re-export from its module when it is first used at runtime, binding a command for this module's export instead if the module's kind is known only at runtime and Python cannot bind the re-export to it, and keep the re-export as a global.
		writeln!(output, "    def __getattr__({prefix}_name):").unwrap();
		for (binding, (dependency, reexport, fallback)) in &reexports {
			let quoted = serde_json::to_string(binding).unwrap();
			writeln!(output, "        if {prefix}_name == {quoted}:").unwrap();
			writeln!(output, "            global {binding}").unwrap();
			if let Some(fallback) = fallback {
				let kinds = self::kinds(reexport)
					.iter()
					.map(|kind| serde_json::to_string(&kind.to_string()).unwrap())
					.collect::<Vec<_>>()
					.join(", ");
				let quoted = serde_json::to_string(dependency).unwrap();
				writeln!(output, "            if {kind}({quoted}) in ({kinds}):").unwrap();
				writeln!(
					output,
					"                {}",
					import(binding, dependency, reexport)
				)
				.unwrap();
				writeln!(output, "            else:").unwrap();
				writeln!(
					output,
					"                {binding} = {client}.command({fallback})"
				)
				.unwrap();
			} else {
				writeln!(
					output,
					"            {}",
					import(binding, dependency, reexport)
				)
				.unwrap();
			}
			writeln!(output, "            return {binding}").unwrap();
		}
		writeln!(
			output,
			"        raise {builtins}.AttributeError(f\"module {{__name__!r}} has no attribute {{{prefix}_name!r}}\")"
		)
		.unwrap();
	}

	// List the bindings and the definitions that keep their names as the module's public names.
	let names = bindings
		.keys()
		.chain(
			names
				.definitions
				.iter()
				.filter(|(name, alias)| name == alias)
				.map(|(name, _)| name),
		)
		.collect::<BTreeSet<_>>();
	let names = serde_json::to_string(&names).unwrap();
	writeln!(output, "__all__ = {names}").unwrap();

	output
}

/// Get the kinds of the modules that Python bindings bind a re-export to, which include Python modules for a namespace.
#[cfg(all(feature = "javascript", feature = "python"))]
fn kinds(reexport: &tg::interface::Reexport) -> Vec<tg::module::Kind> {
	let mut kinds = KINDS.to_vec();
	if reexport.export.is_none() {
		kinds.push(tg::module::Kind::Python);
	}
	kinds
}

/// Get the kind of a re-exported module if its import determines it, with an attribute or the extension of its path.
#[cfg(all(feature = "javascript", feature = "python"))]
fn kind(import: &tg::module::Import) -> Option<tg::module::Kind> {
	import.kind.or_else(|| match import.reference.node() {
		tg::reference::Node::Path(path) => tg::module::module_kind_for_path(path).ok(),
		_ => None,
	})
}

/// Get the specifier with which bindings import each module that an interface imports, by the import's name, which is the name unless a re-export or another import uses it as a specifier, so that the compiler resolves each specifier in bindings to one module.
#[cfg(all(feature = "javascript", feature = "python"))]
fn import_specifiers(interface: &tg::Interface) -> BTreeMap<String, String> {
	let mut taken = interface
		.exports
		.values()
		.filter_map(|export| export.reexport.as_ref())
		.map(|reexport| reexport.import.reference.to_string())
		.collect::<BTreeSet<_>>();
	let mut specifiers = BTreeMap::new();
	for name in interface.imports.keys() {
		let mut specifier = name.clone();
		while taken.contains(&specifier)
			|| (specifier != *name && interface.imports.contains_key(&specifier))
		{
			specifier.push('_');
		}
		taken.insert(specifier.clone());
		specifiers.insert(name.clone(), specifier);
	}
	specifiers
}

#[cfg(all(test, feature = "javascript", feature = "python"))]
mod tests {
	use super::*;

	pub(super) fn source(kind: tg::module::Kind) -> tg::module::Data {
		tg::module::Data {
			kind,
			referent: tg::Referent::with_node(tg::module::data::Source::Path(
				"/test/module".into(),
			)),
		}
	}

	#[test]
	fn preserves_original_source_without_translation() {
		let source = source(tg::module::Kind::Python);
		let text = "def default(): return 42\n";
		assert_eq!(module(&source, text, None).unwrap(), text);
		assert_eq!(
			module(&source, text, Some(tg::module::load::Language::Python)).unwrap(),
			text
		);
	}

	#[test]
	fn generates_parseable_bindings_in_both_languages() {
		let source = source(tg::module::Kind::Python);
		let text = module(
			&source,
			"def default(): return 42\ndef f0(): pass\n",
			Some(tg::module::load::Language::JavaScript),
		)
		.unwrap();
		let result = crate::Compiler::transpile(&text, &source);
		assert!(result.diagnostics.is_empty(), "{:?}", result.diagnostics);
		let mut source = source;
		source.kind = tg::module::Kind::TypeScript;
		let text = module(
			&source,
			"export default () => 42; export const _tg_client = () => 1;",
			Some(tg::module::load::Language::Python),
		)
		.unwrap();
		assert!(ruff_python_parser::parse_module(&text).is_ok());
		assert!(!text.contains("import tangram as _tg_client\n"));
		assert!(text.contains("\n_tg_client = "));
		assert!(text.contains("\ndefault = "));
	}

	#[test]
	fn records_the_sources_of_exports() {
		// A JavaScript representation of a Python module records each export's source and docstring.
		let source_ = source(tg::module::Kind::Python);
		let python = "x = 1\n\ndef greet(name):\n    \"\"\"Greet someone.\n\n    Args:\n        name: The name.\n    \"\"\"\n";
		let text = module(
			&source_,
			python,
			Some(tg::module::load::Language::JavaScript),
		)
		.unwrap();
		assert!(
			text.contains("/**\n * Greet someone.\n *\n * Args:\n *     name: The name.\n */\n"),
			"{text}"
		);
		let offset = text.find("export { c0 as \"greet\" }").unwrap();
		let export = super::source(&text, offset).unwrap();
		assert_eq!(export.name, "greet");
		let range = export.range(tg::position::Encoding::Utf8);
		assert_eq!((range.start.line, range.start.character), (2, 4));
		assert_eq!((range.end.line, range.end.character), (2, 9));

		// A Python representation of a TypeScript module records each export's source and documentation.
		let mut source_ = source_;
		source_.kind = tg::module::Kind::TypeScript;
		let typescript =
			"/** Say hello. */\nexport const hello = () => \"hi\";\nexport const other = 1;";
		let text = module(
			&source_,
			typescript,
			Some(tg::module::load::Language::Python),
		)
		.unwrap();
		assert!(ruff_python_parser::parse_module(&text).is_ok());
		assert!(
			text.contains(
				"hello = _tg_typing.cast(\"tg.Command[tuple[tg.Value.Type, ...], tg.Value.Type]\", tg.command(_tg_function_0))\n\"Say hello.\"\n"
			),
			"{text}"
		);
		let offset = text.find("\nhello = ").unwrap() + 1;
		let export = super::source(&text, offset).unwrap();
		assert_eq!(export.name, "hello");
		let range = export.range(tg::position::Encoding::Utf16);
		assert_eq!((range.start.line, range.start.character), (1, 13));
		assert!(super::source(&text, 0).is_none());
	}

	#[test]
	fn renames_python_keyword_exports() {
		let source = source(tg::module::Kind::TypeScript);
		let text = module(
			&source,
			"export const lambda = () => 42; export const match = () => 42; export const type = () => 42; export const _ = () => 42; export default () => 42;",
			Some(tg::module::load::Language::Python),
		)
		.unwrap();
		assert!(ruff_python_parser::parse_module(&text).is_ok());
		assert!(text.contains("\nlambda_ = "), "{text}");
		// Python writes a soft keyword and the default export's name as attributes, so they keep their names.
		for name in ["match", "type", "_", "default"] {
			assert!(text.contains(&format!("\n{name} = ")), "{text}");
		}
		assert!(text.contains("__all__ = [\"_\",\"default\",\"lambda_\",\"match\",\"type\"]\n"));
	}

	#[test]
	fn renames_colliding_python_exports() {
		let source = source(tg::module::Kind::TypeScript);
		let text = module(
			&source,
			"export const lambda = () => 42; export const lambda_ = () => 42; export const lambda__ = () => 42;",
			Some(tg::module::load::Language::Python),
		)
		.unwrap();
		assert!(ruff_python_parser::parse_module(&text).is_ok());
		// The export's own name takes precedence over the escaped name of another export, and each binding's command targets its export.
		assert!(
			text.contains("_tg_function_0.__tangram_export__ = \"lambda_\"\n"),
			"{text}"
		);
		assert!(text.contains("\nlambda_ = _tg_typing.cast("), "{text}");
		assert!(
			text.contains("_tg_function_2.__tangram_export__ = \"lambda\"\n"),
			"{text}"
		);
		assert!(text.contains("\nlambda___ = _tg_typing.cast(\"tg.Command[tuple[tg.Value.Type, ...], tg.Value.Type]\", tg.command(_tg_function_2))"), "{text}");
		assert!(text.contains("__all__ = [\"lambda_\",\"lambda__\",\"lambda___\"]\n"));
	}

	#[test]
	fn omits_unbindable_python_exports() {
		let source = source(tg::module::Kind::TypeScript);
		let text = module(
			&source,
			"export const $ = () => 42; export const __all__ = () => 42; export const __name__ = () => 42; export const value = () => 42;",
			Some(tg::module::load::Language::Python),
		)
		.unwrap();
		assert!(ruff_python_parser::parse_module(&text).is_ok());
		assert!(!text.contains('$'), "{text}");
		assert!(!text.contains("\n__all__ = _tg"), "{text}");
		assert!(!text.contains("\n__name__ = "), "{text}");
		assert!(text.contains("__all__ = [\"value\"]\n"), "{text}");
	}

	#[test]
	fn generates_typed_commands() {
		let args = tg::interface::Type::Tuple {
			elements: vec![
				tg::interface::Element {
					optional: false,
					type_: tg::interface::Type::String,
				},
				tg::interface::Element {
					optional: true,
					type_: tg::interface::Type::Reference {
						import: None,
						name: "Options".to_owned(),
					},
				},
			],
			rest: None,
		};
		let greeting = tg::interface::Export {
			documentation: None,
			error: None,
			range: None,
			reexport: None,
			type_: Some(tg::interface::Type::Command {
				args: Box::new(args),
				output: Box::new(tg::interface::Type::File),
			}),
		};
		let field = tg::interface::Field {
			optional: true,
			type_: tg::interface::Type::Boolean,
		};
		let options = tg::interface::Type::Record {
			fields: [("loud".to_owned(), field)].into(),
		};
		let interface = tg::Interface {
			definitions: [("Options".to_owned(), options)].into(),
			exports: [("greeting".to_owned(), greeting)].into(),
			imports: BTreeMap::new(),
		};

		// The TypeScript module declares the command's type.
		let text = typescript_bindings(&interface, "");
		assert!(
			text.contains("export type Options = { \"loud\"?: boolean };\n"),
			"{text}"
		);
		assert!(
			text.contains(
				"const c0: tg.Command<([string] | [string, Options]), tg.File> = tg.command(f0)"
			),
			"{text}"
		);
		let source = source(tg::module::Kind::Python);
		let result = crate::Compiler::transpile(&text, &source);
		assert!(result.diagnostics.is_empty(), "{:?}", result.diagnostics);

		// The Python module declares the command's type.
		let text = python_bindings(&interface, "");
		assert!(ruff_python_parser::parse_module(&text).is_ok(), "{text}");
		assert!(
			text.contains("Options = _tg_typing.TypedDict(\"Options\", {\"loud\": \"_tg_typing.NotRequired[bool]\"}, closed=True)\n"),
			"{text}"
		);
		assert!(
			text.contains("greeting = _tg_typing.cast(\"tg.Command[tuple[str, *tuple[Options, ...]], tg.File]\", tg.command(_tg_function_0))\n"),
			"{text}"
		);
	}

	#[test]
	fn types_an_export_that_is_not_a_value_with_its_reason() {
		let curry = tg::interface::Export {
			documentation: None,
			error: Some("a function is not a value".to_owned()),
			range: None,
			reexport: None,
			type_: None,
		};
		let interface = tg::Interface {
			definitions: BTreeMap::new(),
			exports: [("curry".to_owned(), curry)].into(),
			imports: BTreeMap::new(),
		};

		// The TypeScript module types the export with the reason.
		let text = typescript_bindings(&interface, "");
		assert!(
			text.contains("const c0: tg.NonValue<\"a function is not a value\"> = tg.command(f0)"),
			"{text}"
		);

		// The Python module types the export with the reason.
		let text = python_bindings(&interface, "");
		assert!(ruff_python_parser::parse_module(&text).is_ok(), "{text}");
		assert!(
			text.contains("curry = _tg_typing.cast(\"tg.NonValue[_tg_typing.Literal['a function is not a value']]\", tg.command(_tg_function_0))\n"),
			"{text}"
		);
	}

	#[test]
	fn binds_reexports_to_the_bindings_of_their_modules() {
		let reexport = |specifier: &str, kind: Option<&str>, export: Option<&str>| {
			let attributes =
				kind.map(|kind| BTreeMap::from([("kind".to_owned(), kind.to_owned())]));
			let import =
				tg::module::Import::with_specifier_and_attributes(specifier, attributes).unwrap();
			let reexport = tg::interface::Reexport {
				export: export.map(ToOwned::to_owned),
				import,
			};
			tg::interface::Export {
				documentation: None,
				error: None,
				range: None,
				reexport: Some(reexport),
				type_: None,
			}
		};
		let module = source(tg::module::Kind::TypeScript);
		let source = "export * as tools from \"./tools.tg.ts\";\n";
		let mut tools = reexport("./tools.tg.ts", None, None);
		tools.range =
			tg::Range::try_from_byte_range_in_string(source, 12..17, tg::position::Encoding::Utf8);
		let options = tg::interface::Type::Record {
			fields: BTreeMap::new(),
		};
		let interface = tg::Interface {
			definitions: [("Options".to_owned(), options)].into(),
			exports: [
				(
					"forwarded".to_owned(),
					reexport("./barrel.tg.ts", None, Some("tools")),
				),
				(
					"keyword".to_owned(),
					reexport("./barrel.tg.ts", None, Some("lambda")),
				),
				(
					"script".to_owned(),
					reexport("./scripts.tg.py", None, Some("script")),
				),
				(
					"scripts".to_owned(),
					reexport("./scripts", Some("python"), None),
				),
				("tg".to_owned(), reexport("tools", None, None)),
				(
					"tool".to_owned(),
					reexport("./tools.tg.ts", None, Some("tool")),
				),
				("tools".to_owned(), tools),
			]
			.into(),
			imports: [("tools".to_owned(), module)].into(),
		};

		// Generate valid Python bindings and resolve the type import separately from the re-export.
		let text = python_bindings(&interface, source);
		assert!(ruff_python_parser::parse_module(&text).is_ok(), "{text}");
		assert_eq!(
			try_get_import(&interface, "tools_"),
			interface.imports.get("tools")
		);
		assert!(try_get_import(&interface, "tools").is_none());

		// Only the re-export's import maps to the re-export's source.
		let offset = text.find("import _tg_reexport_3 as tools").unwrap();
		assert_eq!(
			super::source(&text, offset).map(|source| source.name),
			Some("tools".to_owned())
		);
		let offset = text.find("Options = ").unwrap();
		assert!(super::source(&text, offset).is_none(), "{text}");
		assert!(
			text.ends_with(
				"__all__ = [\"Options\",\"forwarded\",\"keyword\",\"script\",\"scripts\",\"tg\",\"tool\",\"tools\"]\n"
			),
			"{text}"
		);

		// The TypeScript module re-exports each namespace and export from its module.
		let text = typescript_bindings(&interface, source);
		for statement in [
			"import type * as $i0 from \"tools_\";\n",
			"export { \"tools\" as \"forwarded\" } from \"./barrel.tg.ts\";\n",
			"export { \"lambda\" as \"keyword\" } from \"./barrel.tg.ts\";\n",
			"export { \"script\" as \"script\" } from \"./scripts.tg.py\";\n",
			"export * as \"scripts\" from \"./scripts\" with { kind: \"python\" };\n",
			"export * as \"tg\" from \"tools\";\n",
			"export { \"tool\" as \"tool\" } from \"./tools.tg.ts\";\n",
			"export * as \"tools\" from \"./tools.tg.ts\";\n",
		] {
			assert!(text.contains(statement), "{text}");
		}
	}

	#[test]
	fn imports_the_modules_that_references_name() {
		let module = source(tg::module::Kind::TypeScript);
		let greeting = tg::interface::Export {
			documentation: None,
			error: None,
			range: None,
			reexport: None,
			type_: Some(tg::interface::Type::Command {
				args: Box::new(tg::interface::Type::Tuple {
					elements: Vec::new(),
					rest: None,
				}),
				output: Box::new(tg::interface::Type::Reference {
					import: Some("greeting".to_owned()),
					name: "Greeting".to_owned(),
				}),
			}),
		};
		let interface = tg::Interface {
			definitions: BTreeMap::new(),
			exports: [("greet".to_owned(), greeting)].into(),
			imports: [("greeting".to_owned(), module)].into(),
		};

		// The TypeScript module imports the referenced module as a namespace.
		let text = typescript_bindings(&interface, "");
		assert!(
			text.contains("import type * as $i0 from \"greeting\";\n"),
			"{text}"
		);
		assert!(
			text.contains("const c0: tg.Command<[], $i0.Greeting> = tg.command(f0)"),
			"{text}"
		);

		// The Python module declares the referenced module as a dependency and imports it while type checking.
		let text = python_bindings(&interface, "");
		assert!(ruff_python_parser::parse_module(&text).is_ok(), "{text}");
		assert!(
			text.starts_with(
				"# /// script\n# [tool.tangram.dependencies._tg_import_0]\n# specifier = \"greeting\"\n# ///\n"
			),
			"{text}"
		);
		assert!(
			text.contains(
				"if _tg_typing.TYPE_CHECKING:\n    from _tg_import_0 import Greeting as _tg_import_0_Greeting\n"
			),
			"{text}"
		);
		assert!(
			text.contains(
				"greet = _tg_typing.cast(\"tg.Command[tuple[()], _tg_import_0_Greeting]\""
			),
			"{text}"
		);
	}
}
