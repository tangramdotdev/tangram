use {
	super::Project, crate::code_action, tangram_client::prelude::*,
	tsrs_diagnostics as diagnostics, tsrs_lsproto as native,
};

impl Project {
	pub(super) fn code_action(&self, arg: &code_action::Arg) -> tg::Result<code_action::Output> {
		self.with_language_service_with_auto_imports(&arg.module, |context, service| {
			// Collect the current diagnostics overlapping the requested range.
			let uri = self.uri(&arg.module);
			let diagnostics = service
				.provide_diagnostics(context, &uri)
				.map_err(|error| tg::error!(!error, "failed to get the javascript diagnostics"))?
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
				only.push(native::CodeActionKind::QUICK_FIX);
			}
			if includes(arg.only.as_deref(), "source.organizeImports") {
				only.push(native::CodeActionKind::SOURCE_ORGANIZE_IMPORTS_TS);
			}
			if only.is_empty() {
				return Ok(code_action::Output { actions: None });
			}
			let mut code_action_context = native::CodeActionContext::default();
			code_action_context.diagnostics = diagnostics;
			code_action_context.only = Some(only);
			let mut params = native::CodeActionParams::default();
			params.context = code_action_context;
			params.text_document = native::TextDocumentIdentifier { uri };
			let output = service
				.provide_code_actions(context, &params)
				.map_err(|error| tg::error!(!error, "failed to get the javascript code actions"))?;

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
						"unsupported document changes in a javascript code action"
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
					if kind == native::CodeActionKind::SOURCE_ORGANIZE_IMPORTS_TS {
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
        diagnostics::ADD_SATISFIES_AND_A_TYPE_ASSERTION_TO_THIS_EXPRESSION_SATISFIES_T_AS_T_TO_MAKE_THE_TYPE_EXPLICIT.code(),
        diagnostics::ARRAYS_WITH_SPREAD_ELEMENTS_CAN_T_INFERRED_WITH_ISOLATED_DECLARATIONS.code(),
        diagnostics::ASSIGNING_PROPERTIES_TO_FUNCTIONS_WITHOUT_DECLARING_THEM_IS_NOT_SUPPORTED_WITH_ISOLATED_DECLARATIONS_ADD_AN_EXPLICIT_DECLARATION_FOR_THE_PROPERTIES_ASSIGNED_TO_THIS_FUNCTION.code(),
        diagnostics::AT_LEAST_ONE_ACCESSOR_MUST_HAVE_AN_EXPLICIT_TYPE_ANNOTATION_WITH_ISOLATED_DECLARATIONS.code(),
        diagnostics::BINDING_ELEMENTS_WITH_INITIALIZERS_CAN_T_BE_EXPORTED_DIRECTLY_WITH_ISOLATED_DECLARATIONS.code(),
        diagnostics::CLASS_0_INCORRECTLY_IMPLEMENTS_CLASS_1_DID_YOU_MEAN_TO_EXTEND_1_AND_INHERIT_ITS_MEMBERS_AS_A_SUBCLASS.code(),
        diagnostics::CLASS_0_INCORRECTLY_IMPLEMENTS_INTERFACE_1.code(),
        diagnostics::COMPUTED_PROPERTIES_MUST_BE_NUMBER_OR_STRING_LITERALS_VARIABLES_OR_DOTTED_EXPRESSIONS_WITH_ISOLATED_DECLARATIONS.code(),
        diagnostics::COMPUTED_PROPERTY_NAMES_ON_CLASS_OR_OBJECT_LITERALS_CANNOT_BE_INFERRED_WITH_ISOLATED_DECLARATIONS.code(),
        diagnostics::DECLARATION_EMIT_FOR_THIS_PARAMETER_REQUIRES_IMPLICITLY_ADDING_UNDEFINED_TO_ITS_TYPE_THIS_IS_NOT_SUPPORTED_WITH_ISOLATED_DECLARATIONS.code(),
        diagnostics::DEFAULT_EXPORTS_CAN_T_BE_INFERRED_WITH_ISOLATED_DECLARATIONS.code(),
        diagnostics::ENUM_MEMBER_INITIALIZERS_MUST_BE_COMPUTABLE_WITHOUT_REFERENCES_TO_EXTERNAL_SYMBOLS_WITH_ISOLATED_DECLARATIONS.code(),
        diagnostics::EXPRESSION_TYPE_CAN_T_BE_INFERRED_WITH_ISOLATED_DECLARATIONS.code(),
        diagnostics::EXTENDS_CLAUSE_CAN_T_CONTAIN_AN_EXPRESSION_WITH_ISOLATED_DECLARATIONS.code(),
        diagnostics::FUNCTION_MUST_HAVE_AN_EXPLICIT_RETURN_TYPE_ANNOTATION_WITH_ISOLATED_DECLARATIONS.code(),
        diagnostics::METHOD_MUST_HAVE_AN_EXPLICIT_RETURN_TYPE_ANNOTATION_WITH_ISOLATED_DECLARATIONS.code(),
        diagnostics::OBJECTS_THAT_CONTAIN_SHORTHAND_PROPERTIES_CAN_T_BE_INFERRED_WITH_ISOLATED_DECLARATIONS.code(),
        diagnostics::OBJECTS_THAT_CONTAIN_SPREAD_ASSIGNMENTS_CAN_T_BE_INFERRED_WITH_ISOLATED_DECLARATIONS.code(),
        diagnostics::ONLY_CONST_ARRAYS_CAN_BE_INFERRED_WITH_ISOLATED_DECLARATIONS.code(),
        diagnostics::PARAMETER_MUST_HAVE_AN_EXPLICIT_TYPE_ANNOTATION_WITH_ISOLATED_DECLARATIONS.code(),
        diagnostics::PROPERTY_MUST_HAVE_AN_EXPLICIT_TYPE_ANNOTATION_WITH_ISOLATED_DECLARATIONS.code(),
        diagnostics::TYPE_CONTAINING_PRIVATE_NAME_0_CAN_T_BE_USED_WITH_ISOLATED_DECLARATIONS.code(),
        diagnostics::VARIABLE_MUST_HAVE_AN_EXPLICIT_TYPE_ANNOTATION_WITH_ISOLATED_DECLARATIONS.code(),
    ];
	codes.contains(&code)
}
