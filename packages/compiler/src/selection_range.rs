use {super::Compiler, lsp_types as lsp, tangram_client::prelude::*};

#[derive(Debug)]
pub struct Arg {
	pub module: tg::module::Data,
	pub positions: Vec<tg::Position>,
}

#[derive(Debug)]
pub struct Output {
	pub ranges: Option<Vec<SelectionRange>>,
}

#[derive(Debug)]
pub struct SelectionRange {
	pub parent: Option<Box<SelectionRange>>,
	pub range: tg::Range,
}

impl Compiler {
	pub async fn selection_ranges(
		&self,
		module: &tg::module::Data,
		positions: Vec<tg::Position>,
	) -> tg::Result<Option<Vec<SelectionRange>>> {
		// Create the arg.
		let arg = Arg {
			module: module.clone(),
			positions,
		};

		// Get the output.
		let output = self.backend(module.kind).selection_range(self, arg).await?;

		Ok(output.ranges)
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
