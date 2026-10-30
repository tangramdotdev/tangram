use {
	super::Compiler,
	futures::{TryStreamExt as _, stream::FuturesOrdered},
	lsp_types as lsp,
	tangram_client::prelude::*,
};

#[derive(Debug)]
pub struct Arg {
	pub module: tg::module::Data,
}

#[derive(Debug)]
pub struct Output {
	pub links: Option<Vec<Link>>,
}

#[derive(Debug)]
pub struct Link {
	pub range: tg::Range,
	pub module: tg::module::Data,
	pub tooltip: Option<String>,
}

impl Compiler {
	pub async fn document_links(&self, module: &tg::module::Data) -> tg::Result<Option<Vec<Link>>> {
		// Create the arg.
		let arg = Arg {
			module: module.clone(),
		};

		// Get the output.
		let output = self.backend(module.kind).document_link(self, arg).await?;

		Ok(output.links)
	}
}

impl Compiler {
	pub(crate) async fn handle_document_link_request(
		&self,
		params: lsp::DocumentLinkParams,
	) -> tg::Result<Option<Vec<lsp::DocumentLink>>> {
		// Get the module.
		let module = self.module_for_lsp_uri(&params.text_document.uri).await?;

		// Get the document links.
		let links = self.document_links(&module).await?;
		let Some(links) = links else {
			return Ok(None);
		};

		// Convert the links.
		let links = links
			.into_iter()
			.map(|link| {
				let compiler = self.clone();
				async move {
					let home = compiler.interface_home(&link.module).await?;
					let target = Some(compiler.lsp_uri_for_module(&home).await?);
					let link = lsp::DocumentLink {
						range: link.range.into(),
						target,
						tooltip: link.tooltip,
						data: None,
					};
					Ok::<_, tg::Error>(link)
				}
			})
			.collect::<FuturesOrdered<_>>()
			.try_collect()
			.await?;

		Ok(Some(links))
	}

	pub(crate) async fn handle_document_link_resolve_request(
		&self,
		link: lsp::DocumentLink,
	) -> tg::Result<lsp::DocumentLink> {
		let _ = self;
		Ok(link)
	}
}
