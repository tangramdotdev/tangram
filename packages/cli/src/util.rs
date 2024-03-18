use crate::config::Config;
use console::style;
use tangram_error::Error;

pub fn print_error_trace(error: &tangram_error::Error, config: Option<&Config>) {
	// Extract the options for constructing the trace.
	let stack_trace = config
		.and_then(|config| config.advanced.as_ref())
		.and_then(|advanced| advanced.stack_trace.as_ref());
	let reverse = stack_trace
		.and_then(|stack_trace| stack_trace.reverse)
		.unwrap_or(false);
	let exclude = stack_trace.and_then(|stack_trace| stack_trace.exclude.clone())
		.unwrap_or_else(tangram_error::default_exclude)
		.into_iter()
		.filter_map(|pat| {
			glob::Pattern::new(&pat)
				.inspect_err(|error| {
					eprintln!("{}: failed to parse pattern in in config.advanced.stack_trace.exclude ({pat:#?}", style("warning").yellow());
					eprintln!("{}", style(error).yellow());
				})
				.ok()
		})
		.collect::<Vec<_>>();

	let include = stack_trace.and_then(|config| config.include.clone())
		.unwrap_or_else(tangram_error::default_exclude)
		.into_iter()
		.filter_map(|pat| {
			glob::Pattern::new(&pat)
				.inspect_err(|error| {
					eprintln!("{}: failed to parse pattern in in config.advanced.stack_trace.include ({pat:#?}", style("warning").yellow());
					eprintln!("{}", style(error).yellow());
				})
				.ok()
		})
		.collect::<Vec<_>>();

	let options = tangram_error::TraceOptions {
		exclude: &exclude,
		include: &include,
		reverse,
	};

	// Collect the error trace.
	let mut errors = vec![error];
	while let Some(next) = errors.last().unwrap().source.as_ref() {
		errors.push(next);
	}
	if options.reverse {
		errors.reverse();
	}

	eprintln!("{}:", style("error").red());
	for error in &errors {
		eprint!("{}", style("->").red());
		let Error {
			message,
			location,
			stack,
			values,
			..
		} = error;

		eprint!(" {message}");

		if let Some(location) = &location {
			if options.location_included(location) {
				eprint!(" {location}");
			}
		}

		for (name, value) in values {
			eprintln!();
			let name = style(name).blue();
			let value = style(value).green();
			eprint!("   {name} = {value}");
		}

		let mut stack = stack
			.iter()
			.flatten()
			.filter(|location| options.location_included(location))
			.collect::<Vec<_>>();
		if options.reverse {
			stack.reverse();
		}
		for location in stack {
			eprintln!();
			eprint!("   {location}");
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
