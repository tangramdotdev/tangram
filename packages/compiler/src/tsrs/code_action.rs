use {
	super::{Project, position},
	crate::code_action,
	tangram_client::prelude::*,
	tsrs_diagnostics as diagnostics, tsrs_lsproto as native,
};

impl Project {
	pub(super) fn code_action(&self, arg: &code_action::Arg) -> tg::Result<code_action::Output> {
		self.with_language_service_with_auto_imports(&arg.module, |context, service| {
			// Collect the current diagnostics overlapping the requested range.
			let uri = self.uri(&arg.module);
			let diagnostics = service
				.provide_diagnostics(context, &uri)
				.map_err(|error| tg::error!(!error, "failed to get the tsrs diagnostics"))?
				.full_document_diagnostic_report
				.map(|report| report.items)
				.unwrap_or_default()
				.into_iter()
				.filter(|diagnostic| {
					let Some(code) = diagnostic.code.as_ref().and_then(|code| code.integer) else {
						return false;
					};
					if !supports_diagnostic(code) {
						return false;
					}
					let start = diagnostic.range.start;
					let end = diagnostic.range.end;
					(start.line, start.character) <= (arg.range.end.line, arg.range.end.character)
						&& (end.line, end.character)
							>= (arg.range.start.line, arg.range.start.character)
				})
				.collect();

			// Request the supported quick fixes and import organization.
			let mut only = Vec::new();
			if includes(arg.only.as_deref(), "quickfix") {
				only.push(native::CodeActionKind::QuickFix);
			}
			if includes(arg.only.as_deref(), "source.organizeImports") {
				only.push(native::CodeActionKind::SourceOrganizeImportsTs);
			}
			if only.is_empty() {
				return Ok(code_action::Output { actions: None });
			}
			let params = native::CodeActionParams {
				context: native::CodeActionContext {
					diagnostics,
					only: Some(only),
					..Default::default()
				},
				range: native::Range {
					start: position(arg.range.start),
					end: position(arg.range.end),
				},
				text_document: native::TextDocumentIdentifier { uri },
				..Default::default()
			};
			let output = service
				.provide_code_actions(context, &params)
				.map_err(|error| tg::error!(!error, "failed to get the tsrs code actions"))?;

			// Convert the eager edits into Tangram actions.
			let mut actions = Vec::new();
			for action in output.command_or_code_action_array.unwrap_or_default() {
				let Some(action) = action.code_action else {
					continue;
				};
				if action.disabled.is_some() {
					continue;
				}
				let Some(edit) = action.edit else { continue };
				if edit.document_changes.is_some() {
					return Err(tg::error!(
						"unsupported document changes in a tsrs code action"
					));
				}
				let mut edits = Vec::new();
				for (uri, changes) in edit.changes.unwrap_or_default() {
					for change in changes {
						let location = native::Location {
							range: change.range,
							uri: uri.clone(),
						};
						let location = self.location(&location)?;
						let edit = code_action::Edit {
							module: location.module,
							new_text: change.new_text,
							range: location.range,
						};
						edits.push(edit);
					}
				}
				if edits.is_empty() {
					continue;
				}

				// Reject edits that reference a synthetic module identity.
				let has_synthetic_specifier = edits
					.iter()
					.any(|edit| self.host.contains_module_identity(&edit.new_text));
				if has_synthetic_specifier {
					continue;
				}

				let kind = action.kind.map(|kind| {
					if kind == native::CodeActionKind::SourceOrganizeImportsTs {
						"source.organizeImports".to_owned()
					} else {
						kind.0.to_owned()
					}
				});
				let action = code_action::Action {
					edits: Some(edits),
					kind,
					title: action.title,
				};
				actions.push(action);
			}
			let actions = (!actions.is_empty()).then_some(actions);
			let output = code_action::Output { actions };

			Ok(output)
		})
	}
}

fn includes(only: Option<&[String]>, kind: &str) -> bool {
	only.is_none_or(|only| {
		only.is_empty()
			|| only
				.iter()
				.any(|filter| kind == filter || kind.starts_with(&format!("{filter}.")))
	})
}

fn supports_diagnostic(code: i32) -> bool {
	// Limit the fixes to declarations and interface implementations until generated imports use Tangram specifiers.
	let codes = [
        diagnostics::Add_satisfies_and_a_type_assertion_to_this_expression_satisfies_T_as_T_to_make_the_type_explicit.code(),
        diagnostics::Arrays_with_spread_elements_can_t_inferred_with_isolatedDeclarations.code(),
        diagnostics::Assigning_properties_to_functions_without_declaring_them_is_not_supported_with_isolatedDeclarations_Add_an_explicit_declaration_for_the_properties_assigned_to_this_function.code(),
        diagnostics::At_least_one_accessor_must_have_an_explicit_type_annotation_with_isolatedDeclarations.code(),
        diagnostics::Binding_elements_with_initializers_can_t_be_exported_directly_with_isolatedDeclarations.code(),
        diagnostics::Class_0_incorrectly_implements_class_1_Did_you_mean_to_extend_1_and_inherit_its_members_as_a_subclass.code(),
        diagnostics::Class_0_incorrectly_implements_interface_1.code(),
        diagnostics::Computed_properties_must_be_number_or_string_literals_variables_or_dotted_expressions_with_isolatedDeclarations.code(),
        diagnostics::Computed_property_names_on_class_or_object_literals_cannot_be_inferred_with_isolatedDeclarations.code(),
        diagnostics::Declaration_emit_for_this_parameter_requires_implicitly_adding_undefined_to_its_type_This_is_not_supported_with_isolatedDeclarations.code(),
        diagnostics::Default_exports_can_t_be_inferred_with_isolatedDeclarations.code(),
        diagnostics::Enum_member_initializers_must_be_computable_without_references_to_external_symbols_with_isolatedDeclarations.code(),
        diagnostics::Expression_type_can_t_be_inferred_with_isolatedDeclarations.code(),
        diagnostics::Extends_clause_can_t_contain_an_expression_with_isolatedDeclarations.code(),
        diagnostics::Function_must_have_an_explicit_return_type_annotation_with_isolatedDeclarations.code(),
        diagnostics::Method_must_have_an_explicit_return_type_annotation_with_isolatedDeclarations.code(),
        diagnostics::Objects_that_contain_shorthand_properties_can_t_be_inferred_with_isolatedDeclarations.code(),
        diagnostics::Objects_that_contain_spread_assignments_can_t_be_inferred_with_isolatedDeclarations.code(),
        diagnostics::Only_const_arrays_can_be_inferred_with_isolatedDeclarations.code(),
        diagnostics::Parameter_must_have_an_explicit_type_annotation_with_isolatedDeclarations.code(),
        diagnostics::Property_must_have_an_explicit_type_annotation_with_isolatedDeclarations.code(),
        diagnostics::Type_containing_private_name_0_can_t_be_used_with_isolatedDeclarations.code(),
        diagnostics::Variable_must_have_an_explicit_type_annotation_with_isolatedDeclarations.code(),
    ];
	codes.contains(&code)
}
