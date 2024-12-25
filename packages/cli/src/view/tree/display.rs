use super::{
	Tree,
	node::{Indicator, Node},
};
use num::ToPrimitive as _;
use std::fmt::Write as _;

pub struct Display {
	pub title: String,
	pub children: Vec<Self>,
}

impl<H> Tree<H> {
	pub fn display(&self) -> Display {
		let state = self.state.read().unwrap();
		let root = state.roots.last().unwrap().clone();
		let tree = root.read().unwrap().display();
		tree
	}
}

impl<H> Node<H> {
	pub fn display(&self) -> Display {
		use crossterm::style::Stylize as _;

		let mut title = String::new();
		match self.indicator {
			Some(Indicator::Created) => write!(title, "{} ", "⟳".yellow()).unwrap(),
			Some(Indicator::Dequeued) => write!(title, "{} ", "•".yellow()).unwrap(),
			Some(Indicator::Started) => {
				const SPINNER: [char; 10] = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];
				let now = std::time::SystemTime::now()
					.duration_since(std::time::UNIX_EPOCH)
					.unwrap()
					.as_millis();
				let position = (now / (1000 / 10)) % 10;
				let position = position.to_usize().unwrap();
				write!(title, "{} ", SPINNER[position].to_string().blue()).unwrap();
			},
			Some(Indicator::Canceled) => write!(title, "{} ", "⦻".yellow()).unwrap(),
			Some(Indicator::Failed) => write!(title, "{} ", "✗".red()).unwrap(),
			Some(Indicator::Succeeded) => write!(title, "{} ", "✓".green()).unwrap(),
			None => (),
		}

		if let Some(name) = &self.provider.name {
			write!(title, "{name}: ").unwrap();
		}

		write!(title, "{}", self.title.as_deref().unwrap_or("<unknown>")).unwrap();

		if let Some(log) = &self.log {
			write!(title, " {log}").unwrap();
		}

		if self.options.depth == Some(0) {
			return Display {
				title,
				children: Vec::new(),
			};
		};
		let mut children = Vec::new();
		if self.options.builds {
			let build_children = self
				.build_children
				.iter()
				.flatten()
				.map(|child| child.read().unwrap().display());
			children.extend(build_children);
		}
		if self.options.objects {
			let object_children = self
				.object_children
				.iter()
				.flatten()
				.map(|child| child.read().unwrap().display());
			children.extend(object_children);
		}

		Display { title, children }
	}
}

impl std::fmt::Display for Display {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		fn inner(
			self_: &Display,
			f: &mut std::fmt::Formatter<'_>,
			prefix: &str,
		) -> std::fmt::Result {
			write!(f, "{}", self_.title)?;
			for (n, child) in self_.children.iter().enumerate() {
				write!(f, "\n{prefix}")?;
				if n < self_.children.len() - 1 {
					write!(f, "├ ")?;
					inner(child, f, &format!("{prefix}│ "))?;
				} else {
					write!(f, "└ ")?;
					inner(child, f, &format!("{prefix}  "))?;
				}
			}
			Ok(())
		}
		inner(self, f, "")?;
		Ok(())
	}
}
