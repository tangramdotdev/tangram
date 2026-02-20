use {super::Compiler, lsp_types as lsp, tangram_client::prelude::*};

#[derive(Debug, serde::Serialize)]
pub struct Request {
	pub module: tg::module::Data,
	pub positions: Vec<tg::Position>,
}

#[derive(Debug, serde::Deserialize)]
pub struct Response {
	pub ranges: Option<Vec<SelectionRange>>,
}

#[derive(Debug, serde::Deserialize)]
pub struct SelectionRange {
	pub range: tg::Range,
	pub parent: Option<Box<SelectionRange>>,
}

impl Compiler {
	pub async fn selection_ranges(
		&self,
		module: &tg::module::Data,
		positions: Vec<tg::Position>,
	) -> tg::Result<Option<Vec<SelectionRange>>> {
		// Create the request.
		let request = super::Request::SelectionRange(Request {
			module: module.clone(),
			positions,
		});

		// Perform the request.
		let response = self.request(request).await?;

		// Get the response.
		let super::Response::SelectionRange(response) = response else {
			return Err(tg::error!("unexpected response type"));
		};

		Ok(response.ranges)
	}
}

impl Compiler {
	pub(crate) async fn handle_selection_range_request(
		&self,
		params: lsp::SelectionRangeParams,
	) -> tg::Result<Option<Vec<lsp::SelectionRange>>> {
		// Get the module.
		let module = self.module_for_lsp_uri(&params.text_document.uri).await?;

		// Get the positions for the request.
		let positions = params.positions.into_iter().map(Into::into).collect();

		// Get the selection ranges.
		let ranges = self.selection_ranges(&module, positions).await?;
		let Some(ranges) = ranges else {
			return Ok(None);
		};

		// Convert the selection ranges.
		let ranges = ranges.into_iter().map(convert_selection_range).collect();

		Ok(Some(ranges))
	}
}

fn convert_selection_range(range: SelectionRange) -> lsp::SelectionRange {
	lsp::SelectionRange {
		range: range.range.into(),
		parent: range
			.parent
			.map(|range| Box::new(convert_selection_range(*range))),
	}
}
