use {
	crossterm as ct,
	ratatui::{self as tui, style::Stylize as _, widgets::Widget as _},
};

pub struct Help;

impl Help {
	#[allow(clippy::unused_self, clippy::needless_pass_by_value)]
	pub fn handle(&mut self, _event: &ct::event::Event) {}

	#[allow(clippy::unused_self)]
	pub fn render(&mut self, rect: tui::layout::Rect, buffer: &mut tui::buffer::Buffer) {
		let commands = [
			("?", "help", "Show this help view."),
			("q", "exit", "Close the TUI."),
			("Shift+G", "bottom", "Go to bottom."),
			("g", "top", "Go to top."),
			("h", "collapse", "Collapse the current node."),
			("j", "down", "Navigate down."),
			("k", "up", "Navigate up."),
			("l", "expand", "Expand the current node."),
			("y", "yank", "Copy the current item to the clipboard."),
			("/", "rotate", "Rotate the view split"),
			("enter", "push", "Push the current to the top"),
			("backspace", "pop", "Pop the last node to the top."),
		];
		let rows = commands.into_iter().map(|(key, command, description)| {
			let cells = vec![
				tui::widgets::Cell::new(tui::text::Text::raw(key).left_aligned()),
				tui::widgets::Cell::new(tui::text::Text::raw(command).left_aligned()),
				tui::widgets::Cell::new(tui::text::Text::raw(description).left_aligned()),
			];
			tui::widgets::Row::new(cells)
		});
		let widths = [
			tui::layout::Constraint::Percentage(25),
			tui::layout::Constraint::Percentage(25),
			tui::layout::Constraint::Percentage(50),
		];
		let titles = tui::widgets::Row::new([
			tui::widgets::Cell::new("KEY".bold()).underlined(),
			tui::widgets::Cell::new("COMMAND").bold().underlined(),
			tui::widgets::Cell::new("DESCRIPTION").bold().underlined(),
		]);
		let table = tui::widgets::Table::new(std::iter::once(titles).chain(rows), widths);
		table.render(rect, buffer);
	}
}
