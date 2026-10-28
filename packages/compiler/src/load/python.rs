use {
	ruff_python_ast::{Expr, Stmt},
	std::collections::BTreeSet,
	tangram_client::prelude::*,
};

/// Escape keywords without displacing an export that already has a valid Python name.
pub(super) fn binding(name: &str, exports: &BTreeSet<String>) -> tg::Result<String> {
	if name == "__all__" {
		return Err(tg::error!(
			"the export name __all__ is reserved by the python loader"
		));
	}
	if is_identifier(name) {
		return Ok(name.to_owned());
	}
	let mut binding = format!("{name}_");
	if !is_identifier(&binding) {
		return Err(tg::error!(export = %name, "the export name is not a python identifier"));
	}
	while exports.contains(&binding) {
		binding.push('_');
	}
	Ok(binding)
}

fn is_identifier(name: &str) -> bool {
	let assignment = format!("{name} = None");
	let Ok(parsed) = ruff_python_parser::parse_module(&assignment) else {
		return false;
	};
	matches!(parsed.syntax().body.as_slice(), [Stmt::Assign(assign)] if matches!(assign.targets.as_slice(), [Expr::Name(target)] if target.id.as_str() == name))
}
