use {
	std::collections::BTreeMap,
	tangram_client::document::{Comment, Tag},
};

/// The sections that document parameters.
const PARAMETER_SECTIONS: [&str; 8] = [
	"args",
	"arguments",
	"keyword args",
	"keyword arguments",
	"other parameters",
	"parameters",
	"params",
	"kwargs",
];

/// The sections that document the return value.
const RETURN_SECTIONS: [&str; 4] = ["return", "returns", "yield", "yields"];

/// The documentation in a docstring.
#[derive(Debug, Default)]
pub(super) struct Docstring {
	pub(super) comment: Option<Comment>,
	pub(super) parameters: BTreeMap<String, String>,
	pub(super) returns: Option<String>,
}

/// A section of a docstring.
enum Section {
	Deprecated,
	Examples,
	Parameters,
	Raises,
	Returns,
}

impl Docstring {
	/// Parse a docstring in the Google, `NumPy`, or `reStructuredText` style.
	pub(super) fn parse(raw: &str) -> Self {
		// Clean the docstring.
		let text = clean(raw);
		let lines = text.lines().collect::<Vec<_>>();

		// Sort the lines into the body and the recognized sections.
		let mut docstring = Self::default();
		let mut comment = Comment::default();
		let mut body = Vec::new();
		let mut index = 0;
		while index < lines.len() {
			let line = lines[index];
			let trimmed = line.trim();

			// Parse a reStructuredText field.
			if indentation(line) == 0
				&& let Some((field, text)) = field(trimmed)
			{
				let (block, next) = block(&lines, index + 1, 0);
				let text = join(std::iter::once(text).chain(block.iter().map(|line| line.trim())));
				docstring.field(&mut comment, &field, text);
				index = next;
				continue;
			}

			// Parse a reStructuredText deprecation directive.
			if indentation(line) == 0
				&& let Some(rest) = trimmed.strip_prefix(".. deprecated::")
			{
				let (block, next) = block(&lines, index + 1, 0);
				let version = rest.trim();
				let text = join(block.iter().map(|line| line.trim()));
				let text = if text.is_empty() {
					version.to_owned()
				} else {
					text
				};
				comment.deprecated = Some(text);
				index = next;
				continue;
			}

			// Parse a NumPy section, whose header is underlined with dashes.
			if indentation(line) == 0
				&& let Some(section) = section(trimmed)
				&& lines.get(index + 1).is_some_and(|line| is_underline(line))
			{
				let (block, next) = numpy_block(&lines, index + 2);
				docstring.numpy_section(&mut comment, &section, &block);
				index = next;
				continue;
			}

			// Parse a Google section, whose header ends with a colon.
			if indentation(line) == 0
				&& let Some(header) = trimmed.strip_suffix(':')
				&& let Some(section) = section(header)
			{
				let (block, next) = block(&lines, index + 1, 0);
				docstring.google_section(&mut comment, &section, &block);
				index = next;
				continue;
			}

			body.push(line);
			index += 1;
		}

		// Create the comment.
		comment.text = body.join("\n").trim().to_owned();
		let empty = comment.deprecated.is_none()
			&& comment.examples.is_empty()
			&& comment.tags.is_empty()
			&& comment.text.is_empty();
		if !empty {
			docstring.comment = Some(comment);
		}

		docstring
	}

	fn field(&mut self, comment: &mut Comment, field: &[&str], text: String) {
		let Some(name) = field.first() else {
			return;
		};
		match *name {
			"arg" | "argument" | "key" | "keyword" | "param" | "parameter" => {
				if let Some(parameter) = field.last().filter(|_| field.len() > 1) {
					self.parameters.insert((*parameter).to_owned(), text);
				}
			},
			"except" | "exception" | "raise" | "raises" => {
				let exception = field[1..].join(" ");
				let text = join([exception.as_str(), text.as_str()]);
				let tag = Tag {
					name: "raises".to_owned(),
					text,
				};
				comment.tags.push(tag);
			},
			"return" | "returns" => {
				self.returns = Some(text);
			},
			"rtype" | "type" | "vartype" => {},
			_ => {
				let tag = Tag {
					name: field.join(" "),
					text,
				};
				comment.tags.push(tag);
			},
		}
	}

	fn google_section(&mut self, comment: &mut Comment, section: &Section, block: &[&str]) {
		match section {
			Section::Deprecated => {
				comment.deprecated = Some(join(block.iter().map(|line| line.trim())));
			},
			Section::Examples => {
				comment.examples.push(dedent(block));
			},
			Section::Parameters => {
				for (entry, description) in entries(block) {
					// Remove the type from an entry of the form `name (type)`.
					let name = entry
						.split_once(' ')
						.map_or(entry.as_str(), |(name, _)| name)
						.trim_start_matches('*');
					self.parameters.insert(name.to_owned(), description);
				}
			},
			Section::Raises => {
				for (exception, description) in entries(block) {
					let text = join([exception.as_str(), description.as_str()]);
					let tag = Tag {
						name: "raises".to_owned(),
						text,
					};
					comment.tags.push(tag);
				}
			},
			Section::Returns => {
				// Remove the type from text of the form `type: description`.
				let text = join(block.iter().map(|line| line.trim()));
				let text = match text.split_once(": ") {
					Some((type_, description)) if !type_.contains(' ') => description.to_owned(),
					_ => text,
				};
				self.returns = Some(text);
			},
		}
	}

	fn numpy_section(&mut self, comment: &mut Comment, section: &Section, block: &[&str]) {
		match section {
			Section::Deprecated => {
				comment.deprecated = Some(join(block.iter().map(|line| line.trim())));
			},
			Section::Examples => {
				comment.examples.push(dedent(block));
			},
			Section::Parameters => {
				for (entry, description) in entries(block) {
					// Remove the type from an entry of the form `name : type`, and split entries of the form `a, b : type`.
					let names = entry
						.split_once(':')
						.map_or(entry.as_str(), |(names, _)| names);
					for name in names.split(',') {
						let name = name.trim().trim_start_matches('*');
						self.parameters.insert(name.to_owned(), description.clone());
					}
				}
			},
			Section::Raises => {
				for (exception, description) in entries(block) {
					let text = join([exception.as_str(), description.as_str()]);
					let tag = Tag {
						name: "raises".to_owned(),
						text,
					};
					comment.tags.push(tag);
				}
			},
			Section::Returns => {
				// Use the descriptions, or the types when there are no descriptions.
				let entries = entries(block);
				let descriptions = entries
					.iter()
					.map(|(_, description)| description.as_str())
					.filter(|description| !description.is_empty())
					.collect::<Vec<_>>();
				let text = if descriptions.is_empty() {
					join(entries.iter().map(|(entry, _)| entry.as_str()))
				} else {
					join(descriptions)
				};
				self.returns = Some(text);
			},
		}
	}
}

/// Clean a docstring as `inspect.cleandoc` does, removing the common indentation of the lines after the first.
fn clean(raw: &str) -> String {
	let raw = raw.replace('\t', "        ");
	let mut lines = raw.lines();
	let first = lines.next().unwrap_or_default().trim();
	let rest = lines.collect::<Vec<_>>();
	let indentation = rest
		.iter()
		.filter(|line| !line.trim().is_empty())
		.map(|line| indentation(line))
		.min()
		.unwrap_or(0);
	let mut lines = std::iter::once(first.to_owned())
		.chain(rest.iter().map(|line| {
			line.get(indentation..)
				.unwrap_or_default()
				.trim_end()
				.to_owned()
		}))
		.collect::<Vec<_>>();
	while lines.first().is_some_and(String::is_empty) {
		lines.remove(0);
	}
	while lines.last().is_some_and(String::is_empty) {
		lines.pop();
	}
	lines.join("\n")
}

/// Get the lines after a header that are indented more than the header or blank, excluding trailing blank lines.
fn block<'a>(lines: &[&'a str], start: usize, header: usize) -> (Vec<&'a str>, usize) {
	let mut end = start;
	while end < lines.len() && (lines[end].trim().is_empty() || indentation(lines[end]) > header) {
		end += 1;
	}
	let mut block = lines[start..end].to_vec();
	while block.last().is_some_and(|line| line.trim().is_empty()) {
		block.pop();
	}
	(block, end)
}

/// Get the lines of a section underlined with dashes, which end at the next such section or at the end.
fn numpy_block<'a>(lines: &[&'a str], start: usize) -> (Vec<&'a str>, usize) {
	let mut end = start;
	while end < lines.len() {
		let header = lines[end].trim();
		if lines.get(end + 1).is_some_and(|line| is_underline(line)) && section(header).is_some() {
			break;
		}
		end += 1;
	}
	let mut block = lines[start..end].to_vec();
	while block.last().is_some_and(|line| line.trim().is_empty()) {
		block.pop();
	}
	(block, end)
}

/// Remove the common indentation of the lines.
fn dedent(lines: &[&str]) -> String {
	let indentation = lines
		.iter()
		.filter(|line| !line.trim().is_empty())
		.map(|line| indentation(line))
		.min()
		.unwrap_or(0);
	lines
		.iter()
		.map(|line| line.get(indentation..).unwrap_or_default())
		.collect::<Vec<_>>()
		.join("\n")
}

/// Parse the entries of a section, where each entry is a line at the section's indentation followed by more indented description lines.
fn entries(lines: &[&str]) -> Vec<(String, String)> {
	let indentation_ = lines
		.iter()
		.filter(|line| !line.trim().is_empty())
		.map(|line| indentation(line))
		.min()
		.unwrap_or(0);
	let mut entries = Vec::<(String, Vec<String>)>::new();
	for line in lines {
		if line.trim().is_empty() {
			continue;
		}
		if indentation(line) == indentation_ {
			// Split an entry of the form `name: description`, unless the colon is part of a NumPy `name : type` entry.
			let line = line.trim();
			let (entry, description) = match line.split_once(':') {
				Some((entry, description)) if !entry.ends_with(' ') => {
					(entry.trim().to_owned(), description.trim().to_owned())
				},
				_ => (line.to_owned(), String::new()),
			};
			let description = if description.is_empty() {
				Vec::new()
			} else {
				vec![description]
			};
			entries.push((entry, description));
		} else if let Some((_, description)) = entries.last_mut() {
			description.push(line.trim().to_owned());
		}
	}
	entries
		.into_iter()
		.map(|(entry, description)| (entry, description.join(" ")))
		.collect()
}

/// Parse a reStructuredText field of the form `:name arguments: text`.
fn field(line: &str) -> Option<(Vec<&str>, &str)> {
	let line = line.strip_prefix(':')?;
	let (field, text) = line.split_once(':')?;
	let field = field.split_whitespace().collect::<Vec<_>>();
	if field.is_empty() {
		return None;
	}
	Some((field, text.trim()))
}

fn indentation(line: &str) -> usize {
	line.len() - line.trim_start().len()
}

fn is_underline(line: &str) -> bool {
	let line = line.trim();
	!line.is_empty() && line.chars().all(|character| character == '-')
}

fn join<'a>(parts: impl IntoIterator<Item = &'a str>) -> String {
	parts
		.into_iter()
		.filter(|part| !part.is_empty())
		.collect::<Vec<_>>()
		.join(" ")
}

fn section(header: &str) -> Option<Section> {
	let header = header.to_lowercase();
	let section = if PARAMETER_SECTIONS.contains(&header.as_str()) {
		Section::Parameters
	} else if RETURN_SECTIONS.contains(&header.as_str()) {
		Section::Returns
	} else {
		match header.as_str() {
			"deprecated" => Section::Deprecated,
			"example" | "examples" => Section::Examples,
			"raises" => Section::Raises,
			_ => return None,
		}
	};
	Some(section)
}

#[cfg(test)]
mod tests {
	use super::Docstring;

	#[test]
	fn google() {
		let docstring = Docstring::parse(
			"Add two numbers.

			Args:
			    a (int): The first number.
			    b: The second number,
			        which may span lines.

			Returns:
			    int: The sum.

			Raises:
			    ValueError: If a number is negative.

			Examples:
			    >>> add(1, 2)
			    3
			",
		);
		let comment = docstring.comment.unwrap();
		assert_eq!(comment.text, "Add two numbers.");
		assert_eq!(docstring.parameters["a"], "The first number.");
		assert_eq!(
			docstring.parameters["b"],
			"The second number, which may span lines."
		);
		assert_eq!(docstring.returns.unwrap(), "The sum.");
		assert_eq!(comment.tags[0].text, "ValueError If a number is negative.");
		assert_eq!(comment.examples, [">>> add(1, 2)\n3"]);
	}

	#[test]
	fn numpy() {
		let docstring = Docstring::parse(
			"Add two numbers.

			Parameters
			----------
			a, b : int
			    The numbers.

			Returns
			-------
			int
			    The sum.
			",
		);
		assert_eq!(docstring.comment.unwrap().text, "Add two numbers.");
		assert_eq!(docstring.parameters["a"], "The numbers.");
		assert_eq!(docstring.parameters["b"], "The numbers.");
		assert_eq!(docstring.returns.unwrap(), "The sum.");
	}

	#[test]
	fn restructured_text() {
		let docstring = Docstring::parse(
			"Add two numbers.

			:param int a: The first number.
			:param b: The second number.
			:returns: The sum.
			:rtype: int

			.. deprecated:: 2.0
			    Use plus instead.
			",
		);
		let comment = docstring.comment.unwrap();
		assert_eq!(comment.text, "Add two numbers.");
		assert_eq!(comment.deprecated.unwrap(), "Use plus instead.");
		assert_eq!(docstring.parameters["a"], "The first number.");
		assert_eq!(docstring.parameters["b"], "The second number.");
		assert_eq!(docstring.returns.unwrap(), "The sum.");
	}

	#[test]
	fn text() {
		let docstring = Docstring::parse("  A summary.\n\n  More detail.\n  ");
		assert_eq!(
			docstring.comment.unwrap().text,
			"A summary.\n\nMore detail."
		);
		assert!(Docstring::parse("   ").comment.is_none());
	}
}
