use crossterm::style::Stylize;
use tangram_client as tg;

pub fn print(trace: tg::error::Trace) {
	let mut errors = vec![trace.error];
	while let Some(next) = errors.last().unwrap().source.as_ref() {
		errors.push(next);
	}
	if !trace.options.reverse {
		errors.reverse();
	}

	for error in errors {
		eprintln!("{} {}", "->".red(), error.message);
		if let Some(location) = &error.location {
			if !location.source.is_internal() || self.options.internal {
				let location = location.to_string().yellow();
				eprintln!("   {location}");
			}
		}

		for (name, value) in &error.values {
			let name = name.as_str().blue();
			let value = value.as_str().green();
			eprintln!("   {name} = {value}");
		}

		let mut stack = error.stack.iter().flatten().collect::<Vec<_>>();
		if !self.options.reverse {
			stack.reverse();
		}
		for location in stack {
			if !location.source.is_internal() || self.options.internal {
				let location = location.to_string().yellow();
				eprintln!("   {location}");
			}
		}
	}
}
