use {
	super::Database,
	ruff_db::{files::File, source::source_text},
	std::collections::BTreeSet,
	tangram_client::prelude::*,
	ty_python_semantic::Db as _,
	ty_text_size::{TextRange, TextSize},
};

impl Database {
	/// Run a query on a module's file, synchronizing the project first when the query reads the modules that depend on the file, and report an error from module resolution.
	pub(in crate::python) fn query<T>(
		&mut self,
		module: &tg::module::Data,
		sync: bool,
		f: impl FnOnce(&Self, File) -> tg::Result<T>,
	) -> tg::Result<T> {
		let file = self.query_file(module)?;
		if sync {
			self.sync_project(Some(file))?;
		}
		let output = f(self, file)?;
		if let Some(error) = self.error.lock().unwrap().take() {
			return Err(error);
		}
		Ok(output)
	}

	pub(in crate::python) fn call_hierarchy_incoming(
		&self,
		file: File,
		arg: &crate::call_hierarchy::IncomingArg,
		encoding: tg::position::Encoding,
	) -> tg::Result<crate::call_hierarchy::IncomingOutput> {
		let program_file = self.program_file(file);
		let offset = self.offset(file, arg.position, encoding)?;
		let calls = ty_ide::incoming_calls(self, program_file, offset)
			.into_iter()
			.map(|call| {
				let from_ranges = call
					.from_ranges
					.into_iter()
					.map(|range| self.range(call.from.file, range, encoding))
					.collect::<tg::Result<Vec<_>>>()?;
				let from = self.call_item(&call.from, encoding)?;
				Ok(crate::call_hierarchy::IncomingCall { from, from_ranges })
			})
			.collect::<tg::Result<Vec<_>>>()?;
		Ok(crate::call_hierarchy::IncomingOutput { calls: Some(calls) })
	}

	pub(in crate::python) fn call_hierarchy_outgoing(
		&self,
		file: File,
		arg: &crate::call_hierarchy::OutgoingArg,
		encoding: tg::position::Encoding,
	) -> tg::Result<crate::call_hierarchy::OutgoingOutput> {
		let program_file = self.program_file(file);
		let offset = self.offset(file, arg.position, encoding)?;
		let calls = ty_ide::outgoing_calls(self, program_file, offset)
			.into_iter()
			.map(|call| {
				let from_ranges = call
					.from_ranges
					.into_iter()
					.map(|range| self.range(file, range, encoding))
					.collect::<tg::Result<Vec<_>>>()?;
				let to = self.call_item(&call.to, encoding)?;
				Ok(crate::call_hierarchy::OutgoingCall { from_ranges, to })
			})
			.collect::<tg::Result<Vec<_>>>()?;
		Ok(crate::call_hierarchy::OutgoingOutput { calls: Some(calls) })
	}

	pub(in crate::python) fn call_hierarchy_prepare(
		&self,
		file: File,
		arg: &crate::call_hierarchy::PrepareArg,
		encoding: tg::position::Encoding,
	) -> tg::Result<crate::call_hierarchy::PrepareOutput> {
		let program_file = self.program_file(file);
		let offset = self.offset(file, arg.position, encoding)?;
		let items = ty_ide::prepare_call_hierarchy(self, program_file, offset)
			.map(|items| {
				items
					.into_iter()
					.map(|item| self.call_item(&item, encoding))
					.collect::<tg::Result<Vec<_>>>()
			})
			.transpose()?;
		Ok(crate::call_hierarchy::PrepareOutput { items })
	}

	pub(in crate::python) fn declaration(
		&self,
		file: File,
		arg: &crate::definition::Arg,
		encoding: tg::position::Encoding,
	) -> tg::Result<crate::definition::Output> {
		let program_file = self.program_file(file);
		let offset = self.offset(file, arg.position, encoding)?;
		let locations = self.navigation_locations(
			ty_ide::goto_declaration(self, program_file, offset),
			encoding,
		)?;
		Ok(crate::definition::Output { locations })
	}

	pub(in crate::python) fn definition(
		&self,
		file: File,
		arg: &crate::definition::Arg,
		encoding: tg::position::Encoding,
	) -> tg::Result<crate::definition::Output> {
		let program_file = self.program_file(file);
		let offset = self.offset(file, arg.position, encoding)?;
		let locations = self.navigation_locations(
			ty_ide::goto_definition(self, program_file, offset),
			encoding,
		)?;
		Ok(crate::definition::Output { locations })
	}

	pub(in crate::python) fn document_highlight(
		&self,
		file: File,
		arg: &crate::document_highlight::Arg,
		encoding: tg::position::Encoding,
	) -> tg::Result<crate::document_highlight::Output> {
		let program_file = self.program_file(file);
		let offset = self.offset(file, arg.position, encoding)?;
		let highlights = ty_ide::document_highlights(self, program_file, offset)
			.map(|references| {
				references
					.into_iter()
					.map(|reference| {
						let kind = Some(match reference.kind() {
							ty_ide::ReferenceKind::Other => crate::document_highlight::Kind::Text,
							ty_ide::ReferenceKind::Read => crate::document_highlight::Kind::Read,
							ty_ide::ReferenceKind::Write => crate::document_highlight::Kind::Write,
						});
						let range = self.range(file, reference.range(), encoding)?;
						Ok(crate::document_highlight::Highlight { kind, range })
					})
					.collect::<tg::Result<Vec<_>>>()
			})
			.transpose()?;
		Ok(crate::document_highlight::Output { highlights })
	}

	pub(in crate::python) fn folding_range(
		&self,
		file: File,
		_arg: &crate::folding_range::Arg,
		encoding: tg::position::Encoding,
	) -> tg::Result<crate::folding_range::Output> {
		let program_file = self.program_file(file);
		let ranges = ty_ide::folding_ranges(self, program_file.python_file(self), None)
			.into_iter()
			.map(|fold| {
				let range = self.range(file, fold.range, encoding)?;
				let kind = fold.kind.map(|kind| match kind {
					ty_ide::FoldingRangeKind::Comment => crate::folding_range::Kind::Comment,
					ty_ide::FoldingRangeKind::Imports => crate::folding_range::Kind::Imports,
					ty_ide::FoldingRangeKind::Region => crate::folding_range::Kind::Region,
				});
				Ok(crate::folding_range::Range {
					end_character: Some(range.end.character),
					end_line: range.end.line,
					kind,
					start_character: Some(range.start.character),
					start_line: range.start.line,
				})
			})
			.collect::<tg::Result<Vec<_>>>()?;
		Ok(crate::folding_range::Output {
			ranges: Some(ranges),
		})
	}

	pub(in crate::python) fn hover(
		&self,
		file: File,
		arg: &crate::hover::Arg,
		encoding: tg::position::Encoding,
	) -> tg::Result<crate::hover::Output> {
		let program_file = self.program_file(file);
		let offset = self.offset(file, arg.position, encoding)?;
		let text = ty_ide::hover(self, program_file, offset).map(|hover| {
			hover
				.value
				.display(self, ty_ide::MarkupKind::Markdown)
				.to_string()
		});
		Ok(crate::hover::Output { text })
	}

	pub(in crate::python) fn implementation(
		&self,
		file: File,
		arg: &crate::implementation::Arg,
		encoding: tg::position::Encoding,
	) -> tg::Result<crate::implementation::Output> {
		let program_file = self.program_file(file);
		let offset = self.offset(file, arg.position, encoding)?;
		let locations = self.navigation_locations(
			ty_ide::goto_implementation(self, program_file, offset),
			encoding,
		)?;
		Ok(crate::implementation::Output { locations })
	}

	pub(in crate::python) fn inlay_hint(
		&self,
		file: File,
		arg: &crate::inlay_hint::Arg,
		encoding: tg::position::Encoding,
	) -> tg::Result<crate::inlay_hint::Output> {
		let program_file = self.program_file(file);
		let range = TextRange::new(
			self.offset(file, arg.range.start, encoding)?,
			self.offset(file, arg.range.end, encoding)?,
		);
		let settings = ty_ide::InlayHintSettings {
			call_argument_names: true,
			return_types: false,
			variable_types: true,
		};
		let hints = ty_ide::inlay_hints(self, program_file, range, &settings)
			.into_iter()
			.map(|hint| {
				let kind = match hint.kind {
					ty_ide::InlayHintKind::CallArgumentName => crate::inlay_hint::Kind::Parameter,
					ty_ide::InlayHintKind::Type => crate::inlay_hint::Kind::Type,
				};
				let label = hint.display().to_string();
				let position = self
					.range(file, TextRange::empty(hint.position), encoding)?
					.start;
				Ok(crate::inlay_hint::Hint {
					kind: Some(kind),
					label,
					padding_left: Some(false),
					padding_right: Some(true),
					position,
				})
			})
			.collect::<tg::Result<Vec<_>>>()?;
		Ok(crate::inlay_hint::Output { hints: Some(hints) })
	}

	pub(in crate::python) fn prepare_rename(
		&self,
		file: File,
		arg: &crate::prepare_rename::Arg,
		encoding: tg::position::Encoding,
	) -> tg::Result<crate::prepare_rename::Output> {
		let program_file = self.program_file(file);
		let offset = self.offset(file, arg.position, encoding)?;
		let prepare = ty_ide::can_rename(self, program_file, offset)
			.map(|range| {
				let text = source_text(self, file);
				let placeholder =
					text[usize::from(range.start())..usize::from(range.end())].to_owned();
				let range = self.range(file, range, encoding)?;
				Ok::<_, tg::Error>(crate::prepare_rename::Prepare { placeholder, range })
			})
			.transpose()?;
		Ok(crate::prepare_rename::Output { prepare })
	}

	pub(in crate::python) fn references(
		&self,
		file: File,
		arg: &crate::references::Arg,
		encoding: tg::position::Encoding,
	) -> tg::Result<crate::references::Output> {
		let program_file = self.program_file(file);
		let offset = self.offset(file, arg.position, encoding)?;
		let locations = self.reference_locations(
			ty_ide::find_references(self, program_file, offset, arg.include_declaration),
			encoding,
		)?;
		Ok(crate::references::Output { locations })
	}

	pub(in crate::python) fn rename(
		&self,
		file: File,
		arg: &crate::rename::Arg,
		encoding: tg::position::Encoding,
	) -> tg::Result<crate::rename::Output> {
		let program_file = self.program_file(file);
		let offset = self.offset(file, arg.position, encoding)?;
		let references = ty_ide::can_rename(self, program_file, offset)
			.and_then(|_| ty_ide::rename(self, program_file, offset, &arg.new_name));
		let locations = self.reference_locations(references, encoding)?;
		let edits = locations.map(|locations| {
			locations
				.into_iter()
				.map(|location| crate::rename::Data {
					location,
					new_text: arg.new_name.clone(),
				})
				.collect()
		});
		Ok(crate::rename::Output { edits })
	}

	pub(in crate::python) fn selection_range(
		&self,
		file: File,
		arg: &crate::selection_range::Arg,
		encoding: tg::position::Encoding,
	) -> tg::Result<crate::selection_range::Output> {
		let program_file = self.program_file(file);
		let ranges = arg
			.positions
			.iter()
			.copied()
			.map(|position| {
				let offset = self.offset(file, position, encoding)?;
				let mut parent = None;
				for range in ty_ide::selection_range(self, program_file.python_file(self), offset) {
					let range = self.range(file, range, encoding)?;
					parent = Some(Box::new(crate::selection_range::SelectionRange {
						parent,
						range,
					}));
				}
				Ok(parent
					.map(|range| *range)
					.unwrap_or(crate::selection_range::SelectionRange {
						parent: None,
						range: tg::Range {
							start: position,
							end: position,
						},
					}))
			})
			.collect::<tg::Result<Vec<_>>>()?;
		Ok(crate::selection_range::Output {
			ranges: Some(ranges),
		})
	}

	pub(in crate::python) fn signature_help(
		&self,
		file: File,
		arg: &crate::signature_help::Arg,
		encoding: tg::position::Encoding,
	) -> tg::Result<crate::signature_help::Output> {
		let program_file = self.program_file(file);
		let offset = self.offset(file, arg.position, encoding)?;
		let help = ty_ide::signature_help(self, program_file, offset).map(|help| {
			crate::signature_help::Help {
				active_parameter: None,
				active_signature: help
					.active_signature
					.map(|index| u32::try_from(index).unwrap()),
				signatures: help
					.signatures
					.into_iter()
					.map(|signature| crate::signature_help::Signature {
						active_parameter: signature
							.active_parameter
							.map(|index| u32::try_from(index).unwrap()),
						documentation: signature
							.documentation
							.map(|doc| doc.render(ty_ide::MarkupKind::PlainText)),
						label: signature.label,
						parameters: signature
							.parameters
							.into_iter()
							.map(|parameter| crate::signature_help::Parameter {
								documentation: parameter.documentation,
								label: parameter.label,
							})
							.collect(),
					})
					.collect(),
			}
		});
		Ok(crate::signature_help::Output { help })
	}

	pub(in crate::python) fn type_definition(
		&self,
		file: File,
		arg: &crate::definition::Arg,
		encoding: tg::position::Encoding,
	) -> tg::Result<crate::definition::Output> {
		let program_file = self.program_file(file);
		let offset = self.offset(file, arg.position, encoding)?;
		let locations = self.navigation_locations(
			ty_ide::goto_type_definition(self, program_file, offset),
			encoding,
		)?;
		Ok(crate::definition::Output { locations })
	}

	pub(super) fn query_file(&self, module: &tg::module::Data) -> tg::Result<File> {
		if let Some(file) = self.library_file(module)? {
			return Ok(file);
		}
		Ok(self.register(module.clone())?.file)
	}

	pub(super) fn offset(
		&self,
		file: File,
		position: tg::Position,
		encoding: tg::position::Encoding,
	) -> tg::Result<TextSize> {
		let text = source_text(self, file);
		let offset = position
			.try_to_byte_index_in_string(&text, encoding)
			.ok_or_else(|| tg::error!("invalid python source position"))?;
		let offset = u32::try_from(offset)
			.map_err(|error| tg::error!(!error, "the python source is too large"))?;
		Ok(offset.into())
	}

	pub(super) fn range(
		&self,
		file: File,
		range: TextRange,
		encoding: tg::position::Encoding,
	) -> tg::Result<tg::Range> {
		let text = source_text(self, file);
		tg::Range::try_from_byte_range_in_string(
			&text,
			usize::from(range.start())..usize::from(range.end()),
			encoding,
		)
		.ok_or_else(|| tg::error!("invalid python source range"))
	}

	fn navigation_locations(
		&self,
		targets: Option<ty_ide::RangedValue<ty_ide::NavigationTargets>>,
		encoding: tg::position::Encoding,
	) -> tg::Result<Option<Vec<tg::module::data::Location>>> {
		targets
			.map(|targets| {
				targets
					.value
					.into_iter()
					.map(|target| self.location(target.file(), target.focus_range(), encoding))
					.collect()
			})
			.transpose()
	}

	fn reference_locations(
		&self,
		references: Option<Vec<ty_ide::ReferenceTarget>>,
		encoding: tg::position::Encoding,
	) -> tg::Result<Option<Vec<tg::module::data::Location>>> {
		references
			.map(|references| {
				references
					.into_iter()
					.map(|reference| self.location(reference.file(), reference.range(), encoding))
					.collect()
			})
			.transpose()
	}

	pub(in crate::python) fn sync_project(&mut self, file: Option<File>) -> tg::Result<()> {
		// Match the JavaScript program scope: editor documents and their dependencies.
		let mut modules = Vec::new();
		for document in self
			.documents
			.values()
			.filter(|document| document.module.kind == tg::module::Kind::Python)
		{
			if self.library_file(&document.module)?.is_none() {
				modules.push(document.module.clone());
			}
		}
		if let Some(entry) = file.and_then(|file| self.entry(file)) {
			modules.push(entry.module.clone());
		}
		self.check(modules.clone())?;
		let mut pending = modules
			.into_iter()
			.map(|module| self.register(module))
			.collect::<tg::Result<Vec<_>>>()?;
		let mut visited = BTreeSet::new();
		let mut paths = BTreeSet::new();
		while let Some(entry) = pending.pop() {
			if !visited.insert(entry.module.without_token().to_string()) {
				continue;
			}
			if entry.module.kind == tg::module::Kind::Python
				&& entry.error.is_none()
				&& matches!(
					entry.module.referent.node,
					tg::module::data::Source::Path(_)
				) {
				paths.insert(entry.file.path(self).as_system_path().unwrap().to_owned());
			}
			pending.extend(self.dependencies(entry.file));
		}
		let paths = paths.into_iter().collect::<Vec<_>>();
		if paths != self.project_paths {
			self.project_paths.clone_from(&paths);
			self.project.unwrap().set_included_paths(self, paths);
		}
		Ok(())
	}
}
