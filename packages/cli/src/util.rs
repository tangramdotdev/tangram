use console::style;
use either::Either;
use tangram_client as tg;
use tangram_error::Error;

pub fn build_or_object_id(s: &str) -> Result<Either<tg::build::Id, tg::object::Id>, String> {
	if let Ok(value) = s.parse() {
		return Ok(Either::Left(value));
	}
	if let Ok(value) = s.parse() {
		return Ok(Either::Right(value));
	}
	Err("failed to parse".to_string())
}

pub fn print_error(mut error: &tangram_error::Error) {
	eprintln!("{}:", style("Error").red());
	let mut first = true;
	loop {
		if !first {
			eprintln!();
		}
		first = false;
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
	}
	eprintln!();
}

#[derive(Debug)]
pub struct TreeDisplay {
	pub data: String,
	pub children: Vec<Self>,
}

impl std::fmt::Display for TreeDisplay {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		self.fmt_inner(f, "")
	}
}

impl TreeDisplay {
	fn fmt_inner(&self, f: &mut std::fmt::Formatter<'_>, prefix: &str) -> std::fmt::Result {
		write!(f, "{}", self.data)?;
		for (n, child) in self.children.iter().enumerate() {
			write!(f, "\n{prefix}")?;
			if n == self.children.len() - 1 {
				write!(f, "└── ")?;
				child.fmt_inner(f, &format!("{prefix}   "))?;
			} else {
				write!(f, "├── ")?;
				child.fmt_inner(f, &format!("{prefix}│   "))?;
			}
		}
		Ok(())
	}
}
