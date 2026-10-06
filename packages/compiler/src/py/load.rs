use {std::fmt::Write as _, tangram_client::prelude::*};

pub fn object_module(module: &tg::module::Data) -> tg::Result<String> {
	let class = match module.kind {
		tg::module::Kind::Artifact => "Artifact",
		tg::module::Kind::Blob => "Blob",
		tg::module::Kind::Command => "Command",
		tg::module::Kind::Directory => "Directory",
		tg::module::Kind::Error => "Error",
		tg::module::Kind::File => "File",
		tg::module::Kind::Graph => "Graph",
		tg::module::Kind::Object => "Object",
		tg::module::Kind::Symlink => "Symlink",
		tg::module::Kind::Dts
		| tg::module::Kind::Js
		| tg::module::Kind::Py
		| tg::module::Kind::Ts => return Err(tg::error!("expected an object module")),
	};
	let mut prefix = String::new();
	let expression = match &module.referent.node {
		tg::module::data::Source::Edge(edge) => match edge {
			tg::graph::data::Edge::Index(_) => return Err(tg::error!("missing graph")),
			tg::graph::data::Edge::Object(id) => {
				let id = serde_json::to_string(&id.to_string()).unwrap();
				format!("tg.{class}.with_id({id})")
			},
			tg::graph::data::Edge::Pointer(pointer) => {
				let pointer = serde_json::to_string(&pointer.to_string()).unwrap();
				prefix = format!("pointer = tg.Graph.Pointer.from_data_string({pointer})\n");
				let class = if class == "Object" { "Artifact" } else { class };
				format!("tg.{class}.with_pointer(pointer)")
			},
		},
		tg::module::data::Source::Path(_) => "None".to_owned(),
	};
	let tokens = serde_json::to_string(&module.referent.options.tokens).unwrap();
	let tokens = serde_json::to_string(&tokens).unwrap();
	// Python's Artifact is a factory namespace, rather than a base class.
	let annotation = if module.kind == tg::module::Kind::Artifact {
		"tg.Directory | tg.File | tg.Symlink".to_owned()
	} else {
		format!("tg.{class}")
	};
	let mut text = format!(
		"from typing import cast as _cast\n{prefix}default: {annotation} = _cast({annotation}, {expression})\n"
	);
	if !matches!(module.referent.node, tg::module::data::Source::Path(_)) {
		writeln!(
			text,
			"tg.Object.inherit_tokens(default, __import__('json').loads({tokens}))"
		)
		.unwrap();
		text.push_str(
			"tg.Object.inherit_location(default, __tangram_module__.referent.options.get(\"location\"))\n",
		);
		if !prefix.is_empty() {
			text.push_str("tg.Object.inherit_location(pointer.graph, default.state.location)\n");
			text.push_str("tg.Object.inherit_tokens(pointer.graph, default.state.tokens)\n");
		}
	}
	Ok(text)
}
