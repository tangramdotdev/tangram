use console::style;
use tangram_error::Error;

pub fn print_error_trace(mut error: &Error) {
	eprintln!("{}", style("error").red());
	loop {
		eprint!("{}", style("->").red());
		let Error {
			message,
			location,
			stack,
			source,
			values,
		} = error;
		eprint!(" {message}");
		if let Some(location) = &location {
			let location = style(location).dim().white();
			eprint!(" {location}");
		}
		for (name, value) in values {
			eprintln!();
			let name = style(name).blue();
			let value = style(value).green();
			eprint!("   {name} = {value}");
		}
		for location in stack.iter().flatten() {
			eprintln!();
			let location = style(location).dim().white();
			eprint!(" {location}");
		}
		if let Some(source) = &source {
			error = source;
		} else {
			break;
		}
		eprintln!();
	}
}

#[derive(Debug)]
pub struct Tree {
	pub title: String,
	pub children: Vec<Self>,
}

pub fn print_tree(tree: &Tree) {
	fn inner(tree: &Tree, prefix: &str) {
		print!("{}", tree.title);
		for (n, child) in tree.children.iter().enumerate() {
			print!("\n{prefix}");
			if n == tree.children.len() - 1 {
				print!("└── ");
				inner(child, &format!("{prefix}   "));
			} else {
				print!("├── ");
				inner(child, &format!("{prefix}│   "));
			}
		}
	}
	inner(tree, "");
}
