use {
	super::host::Host,
	std::{collections::BTreeMap, path::Path, sync::Arc},
	tsrs_ast::{Node, SourceFile},
	tsrs_core::{P, ResolutionMode, context::Context},
	tsrs_module::{
		DefaultResolver, DiagAndArgs, ResolutionData, ResolvedModule, ResolvedProjectReference,
		ResolvedTypeReferenceDirective, Resolver, ResolverOptions,
	},
	tsrs_project::ModuleResolverFactory,
};

pub(super) struct Factory(pub(super) Arc<Host>);

struct ModuleResolver {
	default: DefaultResolver,
	host: Arc<Host>,
}

impl ModuleResolver {
	fn resolve(
		&self,
		specifier: &str,
		containing_file: &str,
		attributes: Option<BTreeMap<String, String>>,
	) -> Result<(P<ResolvedModule>, Vec<DiagAndArgs>), String> {
		let path = self
			.host
			.resolve(specifier, containing_file, attributes)
			.map_err(|error| error.to_string())?;
		let module = if let Some(path) = path {
			let extension = if path.ends_with(".d.ts") {
				".d.ts"
			} else if Path::new(&path)
				.extension()
				.is_some_and(|extension| extension == "js")
			{
				".js"
			} else {
				".ts"
			};
			ResolvedModule {
				extension,
				resolved_file_name: tsrs_core::alloc_str(&path),
				..Default::default()
			}
		} else {
			ResolvedModule::default()
		};
		Ok((P::new(module), Vec::new()))
	}
}

impl ModuleResolverFactory for Factory {
	fn new_resolver(
		&self,
		_context: &Context,
		options: ResolverOptions,
	) -> (Box<dyn Resolver>, Box<dyn FnOnce() + Send>) {
		let resolver = ModuleResolver {
			default: tsrs_module::new_resolver(options),
			host: self.0.clone(),
		};
		(Box::new(resolver), Box::new(|| {}))
	}
}

impl Resolver for ModuleResolver {
	fn uses_import_attributes(&self) -> bool {
		true
	}

	fn resolve_module_name_literal(
		&self,
		module_specifier: P<Node>,
		_containing_source_file: P<SourceFile>,
		containing_file: &str,
		_resolution_mode: ResolutionMode,
		_redirected_reference: Option<&dyn ResolvedProjectReference>,
	) -> Result<(P<ResolvedModule>, Vec<DiagAndArgs>), String> {
		let attributes =
			tsrs_module::get_module_resolution_attributes(module_specifier).map(|_| {
				tsrs_module::module_resolution_attributes(module_specifier)
					.into_iter()
					.map(|(key, value)| (key.to_owned(), value.to_owned()))
					.collect::<BTreeMap<_, _>>()
			});
		self.resolve(module_specifier.text(), containing_file, attributes)
	}

	fn resolve_module_name(
		&self,
		module_name: &str,
		containing_file: &str,
		_resolution_mode: ResolutionMode,
		_redirected_reference: Option<&dyn ResolvedProjectReference>,
	) -> Result<(P<ResolvedModule>, Vec<DiagAndArgs>), String> {
		self.resolve(module_name, containing_file, None)
	}

	fn resolve_module_name_from_directory(
		&self,
		module_name: &str,
		containing_directory: &str,
		resolution_mode: ResolutionMode,
	) -> Result<(P<ResolvedModule>, Vec<DiagAndArgs>), String> {
		self.default.resolve_module_name_from_directory(
			module_name,
			containing_directory,
			resolution_mode,
		)
	}

	fn resolve_type_reference_directive(
		&self,
		name: &str,
		containing_file: &str,
		resolution_mode: ResolutionMode,
		redirected_reference: Option<&dyn ResolvedProjectReference>,
	) -> (P<ResolvedTypeReferenceDirective>, Vec<DiagAndArgs>) {
		self.default.resolve_type_reference_directive(
			name,
			containing_file,
			resolution_mode,
			redirected_reference,
		)
	}

	fn get_resolution_data(&self) -> P<ResolutionData> {
		self.default.get_resolution_data()
	}
}
