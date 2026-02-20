use {super::Compiler, lsp_types as lsp, tangram_client::prelude::*};

#[derive(Debug, serde::Serialize)]
pub struct Request {
	pub module: tg::module::Data,
}

#[derive(Debug, serde::Deserialize)]
pub struct Response {
	pub ranges: Option<Vec<Range>>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Range {
	pub start_line: u32,
	pub start_character: Option<u32>,
	pub end_line: u32,
	pub end_character: Option<u32>,
	pub kind: Option<Kind>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
	Comment,
	Imports,
	Region,
}

impl Compiler {
	pub async fn folding_ranges(
		&self,
		module: &tg::module::Data,
	) -> tg::Result<Option<Vec<Range>>> {
		// Create the request.
		let request = super::Request::FoldingRange(Request {
			module: module.clone(),
		});

		// Perform the request.
		let response = self.request(request).await?;

		// Get the response.
		let super::Response::FoldingRange(response) = response else {
			return Err(tg::error!("unexpected response type"));
		};

		Ok(response.ranges)
	}
}

impl Compiler {
	pub(crate) async fn handle_folding_range_request(
		&self,
		params: lsp::FoldingRangeParams,
	) -> tg::Result<Option<Vec<lsp::FoldingRange>>> {
		// Get the module.
		let module = self.module_for_lsp_uri(&params.text_document.uri).await?;

		// Get the folding ranges.
		let ranges = self.folding_ranges(&module).await?;
		let Some(ranges) = ranges else {
			return Ok(None);
		};

		// Convert the folding ranges.
		let ranges = ranges
			.into_iter()
			.map(|range| {
				let kind = range.kind.map(|kind| match kind {
					Kind::Comment => lsp::FoldingRangeKind::Comment,
					Kind::Imports => lsp::FoldingRangeKind::Imports,
					Kind::Region => lsp::FoldingRangeKind::Region,
				});
				lsp::FoldingRange {
					start_line: range.start_line,
					start_character: range.start_character,
					end_line: range.end_line,
					end_character: range.end_character,
					kind,
					collapsed_text: None,
				}
			})
			.collect();

		Ok(Some(ranges))
	}
}
