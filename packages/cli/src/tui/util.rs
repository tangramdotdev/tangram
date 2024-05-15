use ratatui::{prelude::*, widgets::Block};

pub fn render_block_and_get_area(title: &str, area: Rect, buf: &mut Buffer) -> Rect {
	let block = Block::bordered()
		.title(title)
		.border_style(Style::default().fg(Color::White));
	block.render(area, buf);
	Layout::default()
		.constraints([Constraint::Percentage(100)])
		.margin(1)
		.split(area)
		.first()
		.copied()
		.unwrap_or(area)
}
