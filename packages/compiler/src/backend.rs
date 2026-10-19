use {
	crate::{
		Compiler, call_hierarchy, check, code_action, completion, definition, diagnostics,
		document, document_highlight, document_link, folding_range, hover, implementation,
		inlay_hint, prepare_rename, references, rename, selection_range, semantic_tokens,
		signature_help, symbols, workspace_symbols,
	},
	futures::{FutureExt as _, future::BoxFuture},
	tangram_client::prelude::*,
};

/// A language backend, which answers queries about the modules of its language on behalf of the compiler. A backend whose feature is not enabled uses the default methods, which report the missing feature.
pub(crate) trait Backend: Send + Sync {
	/// The Cargo feature that enables the backend.
	fn feature(&self) -> &'static str;

	fn call_hierarchy_incoming(
		&self,
		_compiler: &Compiler,
		_arg: call_hierarchy::IncomingArg,
	) -> BoxFuture<'static, tg::Result<call_hierarchy::IncomingOutput>> {
		unavailable(self.feature())
	}

	fn call_hierarchy_outgoing(
		&self,
		_compiler: &Compiler,
		_arg: call_hierarchy::OutgoingArg,
	) -> BoxFuture<'static, tg::Result<call_hierarchy::OutgoingOutput>> {
		unavailable(self.feature())
	}

	fn call_hierarchy_prepare(
		&self,
		_compiler: &Compiler,
		_arg: call_hierarchy::PrepareArg,
	) -> BoxFuture<'static, tg::Result<call_hierarchy::PrepareOutput>> {
		unavailable(self.feature())
	}

	fn check(
		&self,
		_compiler: &Compiler,
		_arg: check::Arg,
	) -> BoxFuture<'static, tg::Result<check::Output>> {
		unavailable(self.feature())
	}

	fn code_action(
		&self,
		_compiler: &Compiler,
		_arg: code_action::Arg,
	) -> BoxFuture<'static, tg::Result<code_action::Output>> {
		unavailable(self.feature())
	}

	fn completion(
		&self,
		_compiler: &Compiler,
		_arg: completion::Arg,
	) -> BoxFuture<'static, tg::Result<completion::Output>> {
		unavailable(self.feature())
	}

	fn completion_resolve(
		&self,
		_compiler: &Compiler,
		_arg: completion::ResolveArg,
	) -> BoxFuture<'static, tg::Result<completion::ResolveOutput>> {
		unavailable(self.feature())
	}

	fn declaration(
		&self,
		_compiler: &Compiler,
		_arg: definition::Arg,
	) -> BoxFuture<'static, tg::Result<definition::Output>> {
		unavailable(self.feature())
	}

	fn definition(
		&self,
		_compiler: &Compiler,
		_arg: definition::Arg,
	) -> BoxFuture<'static, tg::Result<definition::Output>> {
		unavailable(self.feature())
	}

	fn document(
		&self,
		_compiler: &Compiler,
		_arg: document::Arg,
	) -> BoxFuture<'static, tg::Result<document::Output>> {
		unavailable(self.feature())
	}

	fn document_diagnostics(
		&self,
		_compiler: &Compiler,
		_arg: diagnostics::DocumentArg,
	) -> BoxFuture<'static, tg::Result<diagnostics::DocumentOutput>> {
		unavailable(self.feature())
	}

	fn document_highlight(
		&self,
		_compiler: &Compiler,
		_arg: document_highlight::Arg,
	) -> BoxFuture<'static, tg::Result<document_highlight::Output>> {
		unavailable(self.feature())
	}

	fn document_link(
		&self,
		_compiler: &Compiler,
		_arg: document_link::Arg,
	) -> BoxFuture<'static, tg::Result<document_link::Output>> {
		unavailable(self.feature())
	}

	fn folding_range(
		&self,
		_compiler: &Compiler,
		_arg: folding_range::Arg,
	) -> BoxFuture<'static, tg::Result<folding_range::Output>> {
		unavailable(self.feature())
	}

	fn hover(
		&self,
		_compiler: &Compiler,
		_arg: hover::Arg,
	) -> BoxFuture<'static, tg::Result<hover::Output>> {
		unavailable(self.feature())
	}

	fn implementation(
		&self,
		_compiler: &Compiler,
		_arg: implementation::Arg,
	) -> BoxFuture<'static, tg::Result<implementation::Output>> {
		unavailable(self.feature())
	}

	fn inlay_hint(
		&self,
		_compiler: &Compiler,
		_arg: inlay_hint::Arg,
	) -> BoxFuture<'static, tg::Result<inlay_hint::Output>> {
		unavailable(self.feature())
	}

	fn prepare_rename(
		&self,
		_compiler: &Compiler,
		_arg: prepare_rename::Arg,
	) -> BoxFuture<'static, tg::Result<prepare_rename::Output>> {
		unavailable(self.feature())
	}

	fn references(
		&self,
		_compiler: &Compiler,
		_arg: references::Arg,
	) -> BoxFuture<'static, tg::Result<references::Output>> {
		unavailable(self.feature())
	}

	fn rename(
		&self,
		_compiler: &Compiler,
		_arg: rename::Arg,
	) -> BoxFuture<'static, tg::Result<rename::Output>> {
		unavailable(self.feature())
	}

	fn selection_range(
		&self,
		_compiler: &Compiler,
		_arg: selection_range::Arg,
	) -> BoxFuture<'static, tg::Result<selection_range::Output>> {
		unavailable(self.feature())
	}

	fn semantic_tokens(
		&self,
		_compiler: &Compiler,
		_arg: semantic_tokens::Arg,
	) -> BoxFuture<'static, tg::Result<semantic_tokens::Output>> {
		unavailable(self.feature())
	}

	fn signature_help(
		&self,
		_compiler: &Compiler,
		_arg: signature_help::Arg,
	) -> BoxFuture<'static, tg::Result<signature_help::Output>> {
		unavailable(self.feature())
	}

	fn symbols(
		&self,
		_compiler: &Compiler,
		_arg: symbols::Arg,
	) -> BoxFuture<'static, tg::Result<symbols::Output>> {
		unavailable(self.feature())
	}

	fn type_definition(
		&self,
		_compiler: &Compiler,
		_arg: definition::Arg,
	) -> BoxFuture<'static, tg::Result<definition::Output>> {
		unavailable(self.feature())
	}

	fn workspace_symbol(
		&self,
		_compiler: &Compiler,
		_arg: workspace_symbols::Arg,
	) -> BoxFuture<'static, tg::Result<workspace_symbols::Output>> {
		unavailable(self.feature())
	}
}

/// A backend for a language whose backend feature is not enabled.
#[cfg(not(all(feature = "tsrs", feature = "ty")))]
pub(crate) struct Unavailable {
	pub(crate) feature: &'static str,
}

#[cfg(not(all(feature = "tsrs", feature = "ty")))]
impl Backend for Unavailable {
	fn feature(&self) -> &'static str {
		self.feature
	}
}

fn unavailable<T: Send + 'static>(feature: &str) -> BoxFuture<'static, tg::Result<T>> {
	let error = tg::error!("the {feature} feature is not enabled");
	futures::future::ready(Err(error)).boxed()
}
