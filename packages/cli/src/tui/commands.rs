use super::app::App;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use itertools::Itertools;
use ratatui::{self as tui, prelude::*};
use std::sync::Arc;
use tangram_client as tg;

pub struct Commands<H> {
	commands: Vec<Command<H>>,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
struct KeyBinding {
	keycode: KeyCode,
	modifiers: Option<KeyModifiers>,
}

type Callback<H> = Box<dyn Fn(&App<H>) + Send + Sync>;

struct Command<H> {
	name: String,
	description: String,
	keybindings: Vec<KeyBinding>,
	callback: Callback<H>,
}

impl<H> Commands<H>
where
	H: tg::Handle,
{
	pub fn help() -> Arc<Self> {
		let commands = vec![
			Command {
				name: "Quit".to_owned(),
				description: "Exit the application.".to_owned(),
				keybindings: vec![KeyBinding {
					keycode: KeyCode::Char('q'),
					modifiers: None,
				}],
				callback: Box::new(App::quit),
			},
			Command {
				name: "Exit".to_owned(),
				description: "Toggle help".to_owned(),
				keybindings: vec![KeyBinding {
					keycode: KeyCode::Esc,
					modifiers: None,
				}],
				callback: Box::new(App::toggle_help),
			},
		];
		Arc::new(Self { commands })
	}

	fn common() -> Vec<Command<H>> {
		vec![
			Command {
				name: "Split".to_owned(),
				description: "Toggle split view".to_owned(),
				keybindings: vec![KeyBinding {
					keycode: KeyCode::Char('v'),
					modifiers: None,
				}],
				callback: Box::new(App::toggle_split),
			},
			Command {
				name: "Rotate".to_owned(),
				description: "Rotate the split view".to_owned(),
				keybindings: vec![KeyBinding {
					keycode: KeyCode::Char('r'),
					modifiers: None,
				}],
				callback: Box::new(App::rotate),
			},
			Command {
				name: "Quit".to_owned(),
				description: "Exit the application.".to_owned(),
				keybindings: vec![KeyBinding {
					keycode: KeyCode::Char('q'),
					modifiers: None,
				}],
				callback: Box::new(App::quit),
			},
			Command {
				name: "Help".to_owned(),
				description: "Toggle help".to_owned(),
				keybindings: vec![KeyBinding {
					keycode: KeyCode::Char('?'),
					modifiers: None,
				}],
				callback: Box::new(App::toggle_help),
			},
			Command {
				name: "Down".to_owned(),
				description: String::new(),
				keybindings: vec![
					KeyBinding {
						keycode: KeyCode::Char('j'),
						modifiers: None,
					},
					KeyBinding {
						keycode: KeyCode::Down,
						modifiers: None,
					},
				],
				callback: Box::new(App::down),
			},
			Command {
				name: "Up".to_owned(),
				description: String::new(),
				keybindings: vec![
					KeyBinding {
						keycode: KeyCode::Char('k'),
						modifiers: None,
					},
					KeyBinding {
						keycode: KeyCode::Up,
						modifiers: None,
					},
				],
				callback: Box::new(App::up),
			},
			Command {
				name: "Bottom".to_owned(),
				description: String::new(),
				keybindings: vec![KeyBinding {
					keycode: KeyCode::Char('G'),
					modifiers: Some(KeyModifiers::SHIFT),
				}],
				callback: Box::new(App::bottom),
			},
			Command {
				name: "Top".to_owned(),
				description: String::new(),
				keybindings: vec![KeyBinding {
					keycode: KeyCode::Char('g'),
					modifiers: None,
				}],
				callback: Box::new(App::top),
			},
			Command {
				name: String::new(),
				description: String::new(),
				keybindings: vec![KeyBinding {
					keycode: KeyCode::Tab,
					modifiers: None,
				}],
				callback: Box::new(App::tab),
			},
		]
	}

	pub fn tree() -> Arc<Self> {
		let mut commands = Self::common();
		commands.extend([
			Command {
				name: "Copy".to_owned(),
				description: "Copy the selected item to the clipboard.".to_owned(),
				keybindings: vec![KeyBinding {
					keycode: KeyCode::Char('y'),
					modifiers: None,
				}],
				callback: Box::new(App::copy_selected_to_clipboard),
			},
			Command {
				name: "Cancel".to_owned(),
				description: "Cancel the selected build".to_owned(),
				keybindings: vec![KeyBinding {
					keycode: KeyCode::Char('c'),
					modifiers: None,
				}],
				callback: Box::new(App::cancel),
			},
			Command {
				name: "Expand Children".to_owned(),
				description: "View build children".to_owned(),
				keybindings: vec![
					KeyBinding {
						keycode: KeyCode::Char('l'),
						modifiers: None,
					},
					KeyBinding {
						keycode: KeyCode::Right,
						modifiers: None,
					},
				],
				callback: Box::new(App::expand_children),
			},
			Command {
				name: "Collapse".to_owned(),
				description: "Collapse children.".to_owned(),
				keybindings: vec![
					KeyBinding {
						keycode: KeyCode::Char('h'),
						modifiers: None,
					},
					KeyBinding {
						keycode: KeyCode::Left,
						modifiers: None,
					},
				],
				callback: Box::new(|app| {
					app.collapse_children();
				}),
			},
			Command {
				name: "Push".to_owned(),
				description: "Replace the root with the selected item.".to_owned(),
				keybindings: vec![
					KeyBinding {
						keycode: KeyCode::Char(']'),
						modifiers: None,
					},
					KeyBinding {
						keycode: KeyCode::Left,
						modifiers: None,
					},
				],
				callback: Box::new(App::push),
			},
			Command {
				name: "Pop".to_owned(),
				description: "Return to the previous root.".to_owned(),
				keybindings: vec![
					KeyBinding {
						keycode: KeyCode::Char('['),
						modifiers: None,
					},
					KeyBinding {
						keycode: KeyCode::Left,
						modifiers: None,
					},
				],
				callback: Box::new(App::pop),
			},
			Command {
				name: "Help".to_owned(),
				description: "Toggle help".to_owned(),
				keybindings: vec![KeyBinding {
					keycode: KeyCode::Char('?'),
					modifiers: None,
				}],
				callback: Box::new(App::toggle_help),
			},
			Command {
				name: "Enter".to_owned(),
				description: "Show details about the selected item.".to_owned(),
				keybindings: vec![KeyBinding {
					keycode: KeyCode::Enter,
					modifiers: None,
				}],
				callback: Box::new(App::enter),
			},
			Command {
				name: String::new(),
				description: String::new(),
				keybindings: vec![KeyBinding {
					keycode: KeyCode::Tab,
					modifiers: None,
				}],
				callback: Box::new(App::tab),
			},
		]);
		Arc::new(Self { commands })
	}

	pub fn detail() -> Arc<Self> {
		let mut commands = Self::common();
		commands.extend([
			Command {
				name: "Back".to_owned(),
				description: String::new(),
				keybindings: vec![KeyBinding {
					keycode: KeyCode::Backspace,
					modifiers: None,
				}],
				callback: Box::new(App::back),
			},
			Command {
				name: "1".to_owned(),
				description: "Select first tab".to_owned(),
				keybindings: vec![KeyBinding {
					keycode: KeyCode::Char('1'),
					modifiers: None,
				}],
				callback: Box::new(|app| app.set_tab(0)),
			},
			Command {
				name: "2".to_owned(),
				description: "Select second tab".to_owned(),
				keybindings: vec![KeyBinding {
					keycode: KeyCode::Char('2'),
					modifiers: None,
				}],
				callback: Box::new(|app| app.set_tab(1)),
			},
		]);
		Arc::new(Self { commands })
	}

	pub fn dispatch(&self, event: KeyEvent, app: &App<H>) {
		let binding = KeyBinding {
			keycode: event.code,
			modifiers: (!event.modifiers.is_empty()).then_some(event.modifiers),
		};
		for command in &self.commands {
			for keybinding in &command.keybindings {
				if keybinding == &binding {
					(command.callback)(app);
					return;
				}
			}
		}
	}

	pub fn render_full(&self, area: Rect, buf: &mut Buffer) {
		let rows = self.commands.iter().map(|command| {
			let keybindings = command
				.keybindings
				.iter()
				.map(KeyBinding::to_string)
				.join(", ");
			let cells = vec![
				tui::widgets::Cell::new(Text::raw(keybindings).left_aligned()),
				tui::widgets::Cell::new(Text::raw(command.name.clone()).left_aligned()),
				tui::widgets::Cell::new(Text::raw(command.description.clone()).left_aligned()),
			];
			tui::widgets::Row::new(cells)
		});
		let widths = [
			Constraint::Percentage(10),
			Constraint::Percentage(10),
			Constraint::Percentage(33),
		];

		let title_row = tui::widgets::Row::new([
			tui::widgets::Cell::new("KEY".bold()).underlined(),
			tui::widgets::Cell::new("COMMAND").bold().underlined(),
			tui::widgets::Cell::new("DESCRIPTION").bold().underlined(),
		]);
		let table = tui::widgets::Table::new(std::iter::once(title_row).chain(rows), widths);
		<tui::widgets::Table as Widget>::render(table, area, buf);
	}

	pub fn render_short(&self, area: Rect, buf: &mut Buffer) {
		let text = self
			.commands
			.iter()
			.map(|command| format!("{}:{}", command.keybindings.first().unwrap(), command.name))
			.join("  ");
		let paragraph = tui::widgets::Paragraph::new(text).wrap(tui::widgets::Wrap { trim: true });
		paragraph.render(area, buf);
	}
}

impl std::fmt::Display for KeyBinding {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		if let Some(modifiers) = self.modifiers {
			let names = modifiers.iter_names().map(|(name, _)| name).join("+");
			write!(f, "{names}+")?;
		}
		match self.keycode {
			KeyCode::F(n) => write!(f, "F{n}")?,
			KeyCode::Char(c) => write!(f, "{}", c.to_lowercase())?,
			code => write!(f, "{code:?}")?,
		}
		Ok(())
	}
}
