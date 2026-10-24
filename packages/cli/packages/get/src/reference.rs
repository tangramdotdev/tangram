use {futures::FutureExt as _, tangram_cli_core::Cli, tangram_client::prelude::*};

pub async fn get_with_follow(
	cli: &mut Cli,
	reference: &tg::Reference,
) -> tg::Result<tg::Referent<tg::get::Node>> {
	let mut reference = reference.clone();
	let mut options = reference.options().clone();
	options.follow = true;
	reference.set_options(options);
	let output = get(cli, &reference).await?;

	Ok(output.referent)
}

pub async fn get_artifact(
	cli: &mut Cli,
	reference: &tg::Reference,
) -> tg::Result<tg::Referent<tg::artifact::Id>> {
	let referent = get_with_follow(cli, reference).await?;
	let referent = referent.try_map(|node| match node {
		tg::get::Node::Id(id) => id
			.try_into()
			.map_err(|_| tg::error!("expected an artifact")),
		tg::get::Node::Pointer(_) => Err(tg::error!("expected an artifact")),
	})?;
	Ok(referent)
}

pub async fn get_object(
	cli: &mut Cli,
	reference: &tg::Reference,
) -> tg::Result<tg::Referent<tg::object::Id>> {
	let referent = get_with_follow(cli, reference).await?;
	let referent = referent.try_map(|node| match node {
		tg::get::Node::Id(id) => id.try_into().map_err(|_| tg::error!("expected an object")),
		tg::get::Node::Pointer(_) => Err(tg::error!("expected an object")),
	})?;
	Ok(referent)
}

pub async fn get_object_with_locations(
	cli: &mut Cli,
	reference: &tg::Reference,
	locations: &tangram_cli_core::location::Args,
) -> tg::Result<tg::Referent<tg::object::Id>> {
	let reference = locations.apply_to_reference(reference);
	get_object(cli, &reference).await
}

pub async fn get_process(
	cli: &mut Cli,
	reference: &tg::Reference,
) -> tg::Result<tg::Referent<tg::process::Id>> {
	let referent = get_with_follow(cli, reference).await?;
	let referent = referent.try_map(|node| match node {
		tg::get::Node::Id(id) => id.try_into().map_err(|_| tg::error!("expected a process")),
		tg::get::Node::Pointer(_) => Err(tg::error!("expected a process")),
	})?;
	Ok(referent)
}

pub async fn get_process_with_locations(
	cli: &mut Cli,
	reference: &tg::Reference,
	locations: &tangram_cli_core::location::Args,
) -> tg::Result<tg::Referent<tg::process::Id>> {
	let reference = locations.apply_to_reference(reference);
	get_process(cli, &reference).await
}

pub async fn get(cli: &mut Cli, reference: &tg::Reference) -> tg::Result<tg::get::Output> {
	get_with_arg(cli, reference, tg::get::Arg::default())
		.boxed()
		.await
}

pub async fn get_with_arg(
	cli: &mut Cli,
	reference: &tg::Reference,
	arg: tg::get::Arg,
) -> tg::Result<tg::get::Output> {
	let tokens = reference.options().tokens.clone();
	let direct_reference =
		tg::Reference::with_node_and_tokens(reference.node().clone(), tokens.clone());
	if reference == &direct_reference {
		match reference.node() {
			tg::reference::Node::Id(id)
				if matches!(
					id.kind(),
					tg::id::Kind::Group | tg::id::Kind::Organization | tg::id::Kind::User
				) => {},
			tg::reference::Node::Id(id)
				if !tokens.is_empty()
					|| !matches!(
						id.kind(),
						tg::id::Kind::Group
							| tg::id::Kind::Organization
							| tg::id::Kind::Sandbox
							| tg::id::Kind::Tag | tg::id::Kind::User
					) =>
			{
				let referent = tg::Referent::with_node_and_tokens(
					tg::get::Node::Id(id.clone()),
					tokens.clone(),
				);
				let output = tg::get::Output { referent };

				return Ok(output);
			},
			tg::reference::Node::Pointer(pointer) => {
				let referent = tg::Referent::with_node_and_tokens(
					tg::get::Node::Pointer(pointer.clone()),
					tokens,
				);
				let output = tg::get::Output { referent };

				return Ok(output);
			},
			_ => (),
		}
	}

	let client = cli.client().await?;

	// Determine if the path is relative.
	let relative = reference
		.node()
		.try_unwrap_path_ref()
		.is_ok_and(|path| path.is_relative());

	// Make the path absolute.
	let mut reference = reference.clone();
	let mut node = reference.node().clone();
	if let tg::reference::Node::Path(path) = &mut node {
		*path = tangram_util::fs::canonicalize_parent(&path)
			.await
			.map_err(|error| tg::error!(!error, "failed to canonicalize the path"))?;
	}
	reference.set_node(node);

	// Get the reference.
	let stream = client
		.try_get(&reference, arg)
		.await
		.map_err(|error| tg::error!(!error, %reference, "failed to get the reference"))?;
	let mut output = cli
		.render_progress_stream(stream)
		.await
		.map_err(|error| tg::error!(!error, %reference, "failed to get the reference"))?
		.ok_or_else(|| tg::error!(%reference, "failed to get the reference"))?;

	// If the reference is a local relative path, then make the referent's path relative to the current working directory.
	if relative && let Some(path) = output.referent.path() {
		let current_dir = std::env::current_dir()
			.map_err(|error| tg::error!(!error, "failed to get the working directory"))?;
		let path = tangram_util::path::diff(&current_dir, path)
			.map_err(|error| tg::error!(!error, "failed to diff the paths"))?
			.unwrap_or_default();
		output.referent.options.path = Some(path);
	}

	Ok(output)
}

pub async fn get_references(
	cli: &mut Cli,
	references: &[tg::Reference],
) -> tg::Result<Vec<tg::Referent<tg::get::Node>>> {
	let mut referents = Vec::with_capacity(references.len());
	for reference in references {
		let output = get(cli, reference).await?;
		referents.push(output.referent);
	}
	Ok(referents)
}

pub async fn get_modules(
	cli: &mut Cli,
	references: &[tg::Reference],
) -> tg::Result<Vec<tg::Module>> {
	let mut modules = Vec::with_capacity(references.len());
	for reference in references {
		let module = get_module(cli, reference).await?;
		modules.push(module);
	}
	Ok(modules)
}

pub async fn get_module(cli: &mut Cli, reference: &tg::Reference) -> tg::Result<tg::Module> {
	let client = cli.client().await?;

	// Get the reference.
	let referent = get_with_follow(cli, reference).await?;
	let mut referent = referent.into_graph_edge()?;
	let module = match referent.node.clone() {
		tg::graph::Edge::Index(_) => return Err(tg::error!("missing graph")),
		tg::graph::Edge::Object(tg::Object::Directory(directory)) => {
			let root_module_name = tg::module::try_get_root_module_file_name_with_instance(
				&client,
				tg::Either::Left(&directory),
			)
			.await?
			.ok_or_else(
				|| tg::error!(directory = %directory.id(), "failed to find a root module"),
			)?;
			let kind = tg::module::module_kind_for_path(root_module_name).unwrap();
			if let Some(path) = &mut referent.options.path {
				*path = path.join(root_module_name);
			} else {
				referent.options.path.replace(root_module_name.into());
			}
			let edge = directory
				.get_entry_edge_with_instance(&client, root_module_name)
				.await
				.map_err(|error| tg::error!(!error, "failed to get the root module"))?;
			let source = tg::module::Source::Edge(edge.into());
			let referent = referent.map(|_| source);
			tg::Module { kind, referent }
		},

		tg::graph::Edge::Object(tg::Object::File(file)) => {
			let path = referent
				.path()
				.ok_or_else(|| tg::error!("expected a path"))?;
			if !tg::module::is_module_path(path) {
				return Err(tg::error!("expected a module path"));
			}
			let kind = tg::module::module_kind_for_path(path).unwrap();
			let object = file.clone().into();
			let edge = tg::graph::Edge::Object(object);
			let source = tg::module::Source::Edge(edge);
			let referent = referent.map(|_| source);
			tg::Module { kind, referent }
		},

		tg::graph::Edge::Object(tg::Object::Symlink(_)) => {
			return Err(tg::error!("unimplemented"));
		},

		tg::graph::Edge::Pointer(pointer) if pointer.kind == tg::artifact::Kind::Directory => {
			let directory = tg::Directory::with_object(tg::directory::Object::Pointer(pointer));
			let root_module_name = tg::module::try_get_root_module_file_name_with_instance(
				&client,
				tg::Either::Left(&directory),
			)
			.await?
			.ok_or_else(
				|| tg::error!(directory = %directory.id(), "failed to find a root module"),
			)?;
			let kind = tg::module::module_kind_for_path(root_module_name).unwrap();
			if let Some(path) = &mut referent.options.path {
				*path = path.join(root_module_name);
			} else {
				referent.options.path.replace(root_module_name.into());
			}
			let edge = directory
				.get_entry_edge_with_instance(&client, root_module_name)
				.await
				.map_err(|error| tg::error!(!error, "failed to get the root module"))?;
			let source = tg::module::Source::Edge(edge.into());
			let referent = referent.map(|_| source);
			tg::Module { kind, referent }
		},

		tg::graph::Edge::Pointer(pointer) if pointer.kind == tg::artifact::Kind::File => {
			let path = referent
				.path()
				.ok_or_else(|| tg::error!("expected a path"))?;
			if !tg::module::is_module_path(path) {
				return Err(tg::error!("expected a module path"));
			}
			let kind = tg::module::module_kind_for_path(path).unwrap();
			let source = tg::module::Source::Edge(tg::graph::Edge::Pointer(pointer.clone()));
			let referent = referent.map(|_| source);
			tg::Module { kind, referent }
		},

		tg::graph::Edge::Pointer(pointer) if pointer.kind == tg::artifact::Kind::Symlink => {
			return Err(tg::error!("unimplemented"));
		},

		_ => {
			return Err(tg::error!("expected an artifact"));
		},
	};

	Ok(module)
}
