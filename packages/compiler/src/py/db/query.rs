use {
	super::Database,
	crate::{Request, Response},
	ruff_db::{files::File, source::source_text},
	std::collections::BTreeSet,
	tangram_client::prelude::*,
	ty_python_semantic::Db as _,
	ty_text_size::{TextRange, TextSize},
};

impl Database {
	pub(in crate::py) fn request(
		&mut self,
		request: Request,
		encoding: tg::position::Encoding,
	) -> tg::Result<Response> {
		if let Request::CompletionResolve(request) = request {
			let entry = request
				.data
				.map(serde_json::from_value)
				.transpose()
				.map_err(|error| tg::error!(!error, "invalid Python completion data"))?;
			return Ok(Response::CompletionResolve(
				crate::completion::ResolveResponse { entry },
			));
		}
		if let Request::WorkspaceSymbol(request) = request {
			self.sync_project(None)?;
			return self
				.workspace_symbols(&request.query, encoding)
				.map(Response::WorkspaceSymbol);
		}
		if let Request::DocumentDiagnostics(request) = request {
			return self
				.document_diagnostics(request.modules, encoding)
				.map(Response::DocumentDiagnostics);
		}
		let module = request
			.module()
			.ok_or_else(|| tg::error!("unsupported Python request"))?;
		let file = self.query_file(module)?;
		if matches!(
			request,
			Request::References(_)
				| Request::Rename(_)
				| Request::PrepareRename(_)
				| Request::CallHierarchyIncoming(_)
				| Request::Implementation(_)
		) {
			self.sync_project(Some(file))?;
		}
		let program_file = self.program_file(file);
		let response = match request {
			Request::CallHierarchyIncoming(request) => {
				let offset = self.offset(file, request.position, encoding)?;
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
				Response::CallHierarchyIncoming(crate::call_hierarchy::IncomingResponse {
					calls: Some(calls),
				})
			},
			Request::CallHierarchyOutgoing(request) => {
				let offset = self.offset(file, request.position, encoding)?;
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
				Response::CallHierarchyOutgoing(crate::call_hierarchy::OutgoingResponse {
					calls: Some(calls),
				})
			},
			Request::CallHierarchyPrepare(request) => {
				let offset = self.offset(file, request.position, encoding)?;
				let items = ty_ide::prepare_call_hierarchy(self, program_file, offset)
					.map(|items| {
						items
							.into_iter()
							.map(|item| self.call_item(&item, encoding))
							.collect::<tg::Result<Vec<_>>>()
					})
					.transpose()?;
				Response::CallHierarchyPrepare(crate::call_hierarchy::PrepareResponse { items })
			},
			Request::CodeAction(request) => {
				Response::CodeAction(self.code_actions(file, &request, encoding)?)
			},
			Request::Completion(request) => {
				Response::Completion(self.completion(file, request.position, encoding)?)
			},
			Request::Declaration(request) => {
				let offset = self.offset(file, request.position, encoding)?;
				let locations = self.navigation_locations(
					ty_ide::goto_declaration(self, program_file, offset),
					encoding,
				)?;
				Response::Declaration(crate::definition::Response { locations })
			},
			Request::Definition(request) => {
				let offset = self.offset(file, request.position, encoding)?;
				let locations = self.navigation_locations(
					ty_ide::goto_definition(self, program_file, offset),
					encoding,
				)?;
				Response::Definition(crate::definition::Response { locations })
			},
			Request::Document(_) => {
				return Err(tg::error!(
					"Python documentation generation is not supported"
				));
			},
			Request::DocumentHighlight(request) => {
				let offset = self.offset(file, request.position, encoding)?;
				let highlights = ty_ide::document_highlights(self, program_file, offset)
					.map(|references| {
						references
							.into_iter()
							.map(|reference| {
								let kind = Some(match reference.kind() {
									ty_ide::ReferenceKind::Other => {
										crate::document_highlight::Kind::Text
									},
									ty_ide::ReferenceKind::Read => {
										crate::document_highlight::Kind::Read
									},
									ty_ide::ReferenceKind::Write => {
										crate::document_highlight::Kind::Write
									},
								});
								let range = self.range(file, reference.range(), encoding)?;
								Ok(crate::document_highlight::Highlight { kind, range })
							})
							.collect::<tg::Result<Vec<_>>>()
					})
					.transpose()?;
				Response::DocumentHighlight(crate::document_highlight::Response { highlights })
			},
			Request::DocumentLink(_) => {
				Response::DocumentLink(crate::document_link::Response { links: None })
			},
			Request::FoldingRange(_) => {
				let ranges = ty_ide::folding_ranges(self, program_file.python_file(self), None)
					.into_iter()
					.map(|fold| {
						let range = self.range(file, fold.range, encoding)?;
						let kind = fold.kind.map(|kind| match kind {
							ty_ide::FoldingRangeKind::Comment => {
								crate::folding_range::Kind::Comment
							},
							ty_ide::FoldingRangeKind::Imports => {
								crate::folding_range::Kind::Imports
							},
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
				Response::FoldingRange(crate::folding_range::Response {
					ranges: Some(ranges),
				})
			},
			Request::Hover(request) => {
				let offset = self.offset(file, request.position, encoding)?;
				let text = ty_ide::hover(self, program_file, offset).map(|hover| {
					hover
						.value
						.display(self, ty_ide::MarkupKind::Markdown)
						.to_string()
				});
				Response::Hover(crate::hover::Response { text })
			},
			Request::Implementation(request) => {
				let offset = self.offset(file, request.position, encoding)?;
				let locations = self.navigation_locations(
					ty_ide::goto_implementation(self, program_file, offset),
					encoding,
				)?;
				Response::Implementation(crate::implementation::Response { locations })
			},
			Request::InlayHint(request) => {
				let range = TextRange::new(
					self.offset(file, request.range.start, encoding)?,
					self.offset(file, request.range.end, encoding)?,
				);
				let settings = ty_ide::InlayHintSettings {
					call_argument_names: true,
					variable_types: true,
				};
				let hints = ty_ide::inlay_hints(self, program_file, range, &settings)
					.into_iter()
					.map(|hint| {
						let kind = match hint.kind {
							ty_ide::InlayHintKind::CallArgumentName => {
								crate::inlay_hint::Kind::Parameter
							},
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
				Response::InlayHint(crate::inlay_hint::Response { hints: Some(hints) })
			},
			Request::PrepareRename(request) => {
				let offset = self.offset(file, request.position, encoding)?;
				let prepare = ty_ide::can_rename(self, program_file, offset)
					.map(|range| {
						let text = source_text(self, file);
						let placeholder =
							text[usize::from(range.start())..usize::from(range.end())].to_owned();
						let range = self.range(file, range, encoding)?;
						Ok::<_, tg::Error>(crate::prepare_rename::Prepare { placeholder, range })
					})
					.transpose()?;
				Response::PrepareRename(crate::prepare_rename::Response { prepare })
			},
			Request::References(request) => {
				let offset = self.offset(file, request.position, encoding)?;
				let locations = self.reference_locations(
					ty_ide::find_references(
						self,
						program_file,
						offset,
						request.include_declaration,
					),
					encoding,
				)?;
				Response::References(crate::references::Response { locations })
			},
			Request::Rename(request) => {
				let offset = self.offset(file, request.position, encoding)?;
				let references = ty_ide::can_rename(self, program_file, offset)
					.and_then(|_| ty_ide::rename(self, program_file, offset, &request.new_name));
				let locations = self.reference_locations(references, encoding)?;
				Response::Rename(crate::rename::Response { locations })
			},
			Request::SelectionRange(request) => {
				let ranges = request
					.positions
					.into_iter()
					.map(|position| {
						let offset = self.offset(file, position, encoding)?;
						let mut parent = None;
						for range in
							ty_ide::selection_range(self, program_file.python_file(self), offset)
						{
							let range = self.range(file, range, encoding)?;
							parent = Some(Box::new(crate::selection_range::SelectionRange {
								parent,
								range,
							}));
						}
						Ok(parent.map(|range| *range).unwrap_or(
							crate::selection_range::SelectionRange {
								parent: None,
								range: tg::Range {
									start: position,
									end: position,
								},
							},
						))
					})
					.collect::<tg::Result<Vec<_>>>()?;
				Response::SelectionRange(crate::selection_range::Response {
					ranges: Some(ranges),
				})
			},
			Request::SemanticTokens(_) => {
				Response::SemanticTokens(self.semantic_tokens(file, encoding)?)
			},
			Request::SignatureHelp(request) => {
				let offset = self.offset(file, request.position, encoding)?;
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
									.map(|doc| doc.render_plaintext()),
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
				Response::SignatureHelp(crate::signature_help::Response { help })
			},
			Request::Symbols(_) => Response::Symbols(self.symbols(file, encoding)?),
			Request::TypeDefinition(request) => {
				let offset = self.offset(file, request.position, encoding)?;
				let locations = self.navigation_locations(
					ty_ide::goto_type_definition(self, program_file, offset),
					encoding,
				)?;
				Response::TypeDefinition(crate::definition::Response { locations })
			},
			Request::Check(_)
			| Request::CompletionResolve(_)
			| Request::DocumentDiagnostics(_)
			| Request::WorkspaceSymbol(_) => unreachable!(),
		};
		if let Some(error) = self.error.lock().unwrap().take() {
			return Err(error);
		}
		Ok(response)
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
			.ok_or_else(|| tg::error!("invalid Python source position"))?;
		let offset = u32::try_from(offset)
			.map_err(|error| tg::error!(!error, "the Python source is too large"))?;
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
		.ok_or_else(|| tg::error!("invalid Python source range"))
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

	fn sync_project(&mut self, file: Option<File>) -> tg::Result<()> {
		// Match the JavaScript program scope: editor documents and their dependencies.
		let mut modules = Vec::new();
		for document in self
			.documents
			.values()
			.filter(|document| document.module.kind == tg::module::Kind::Py)
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
			if entry.module.kind == tg::module::Kind::Py
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
