use {
	crate::prelude::*,
	std::{collections::BTreeMap, ops::Range, path::PathBuf},
};

mod javascript;
mod python;

#[cfg(test)]
mod tests;

/// An error in an interface file, at a byte range of the text.
struct Error {
	message: String,
	range: Range<usize>,
}

/// A token of an interface file.
#[derive(Clone, Debug)]
struct Token {
	/// The column of the token's first byte on its line.
	column: usize,

	kind: TokenKind,

	/// Whether the token is the first token on its line.
	line_start: bool,

	range: Range<usize>,
}

#[derive(Clone, Debug, PartialEq)]
enum TokenKind {
	Documentation(String),
	Identifier(String),
	Number(f64),
	Punctuation(&'static str),
	String(String),
}

/// A parser over the tokens of an interface file.
struct Parser {
	end: usize,
	index: usize,

	/// The references to types, with their ranges, to check after the definitions and imports are known.
	references: Vec<(Option<String>, String, Range<usize>)>,

	tokens: Vec<Token>,
}

/// The punctuation of interface files, with longer punctuation before the punctuation that it begins with.
const PUNCTUATION: &[&str] = &[
	"...", "(", ")", "*", ",", "-", ".", ":", ";", "<", "=", ">", "?", "[", "]", "{", "|", "}",
];

/// Parse an interface file in the syntax that the magic comment on its first line names, or as JSON if it begins with an object, or else in the JavaScript syntax.
pub fn parse(text: &str) -> tg::Result<tg::Interface> {
	parse_with_range(text).map_err(|(error, _)| error)
}

/// Parse an interface file, returning the range of the error in the text if it has one.
pub fn parse_with_range(
	text: &str,
) -> std::result::Result<tg::Interface, (tg::Error, Option<tg::Range>)> {
	let result = match syntax(text) {
		Ok(tg::interface::Syntax::JavaScript) => javascript::parse(text),
		Ok(tg::interface::Syntax::Json) => json(text),
		Ok(tg::interface::Syntax::Python) => python::parse(text),
		Err(error) => Err(error),
	};
	result.map_err(|error| {
		let range = tg::Range::try_from_byte_range_in_string(
			text,
			error.range,
			tg::position::Encoding::Utf8,
		);
		let message = error.message;
		(tg::error!(argument, %message, "invalid interface"), range)
	})
}

/// Get the syntax of an interface file from the magic comment on its first line, which is `// @tangram interface javascript` or `# @tangram interface python`, or else JSON if the file begins with an object.
fn syntax(text: &str) -> Result<tg::interface::Syntax, Error> {
	if text.trim_start().starts_with('{') {
		return Ok(tg::interface::Syntax::Json);
	}
	let line = text.lines().next().unwrap_or_default();
	let range = 0..line.len();
	let comment = line
		.trim()
		.strip_prefix("//")
		.map(|rest| ("//", rest))
		.or_else(|| line.trim().strip_prefix('#').map(|rest| ("#", rest)));
	let Some((prefix, rest)) = comment else {
		return Ok(tg::interface::Syntax::JavaScript);
	};
	let Some(name) = rest.trim().strip_prefix("@tangram interface") else {
		return Ok(tg::interface::Syntax::JavaScript);
	};
	let syntax = match (prefix, name.trim()) {
		("//", "javascript") => tg::interface::Syntax::JavaScript,
		("#", "python") => tg::interface::Syntax::Python,
		(_, name) => {
			let message =
				format!("the interface syntax {name:?} is not known in a {prefix} comment");
			return Err(Error { message, range });
		},
	};
	Ok(syntax)
}

/// Parse an interface file in JSON.
fn json(text: &str) -> Result<tg::Interface, Error> {
	serde_json::from_str(text).map_err(|error| {
		let line = error.line().saturating_sub(1);
		let start = text
			.split_inclusive('\n')
			.take(line)
			.map(str::len)
			.sum::<usize>()
			+ error.column().saturating_sub(1);
		let start = start.min(text.len());
		let message = error.to_string();
		Error {
			message,
			range: start..start,
		}
	})
}

/// Split the text of an interface file into tokens, skipping comments other than JavaScript documentation comments.
fn lex(text: &str, syntax: tg::interface::Syntax) -> Result<Vec<Token>, Error> {
	let bytes = text.as_bytes();
	let mut tokens = Vec::new();
	let mut index = 0;
	let mut line_begin = 0;
	let mut line_start = true;
	while index < bytes.len() {
		let start = index;
		let byte = bytes[index];

		// Skip whitespace, and track the beginnings of lines.
		if byte == b'\n' {
			index += 1;
			line_begin = index;
			line_start = true;
			continue;
		}
		if byte.is_ascii_whitespace() {
			index += 1;
			continue;
		}

		// Skip comments, and keep JavaScript documentation comments.
		let rest = &text[index..];
		let line_comment = match syntax {
			tg::interface::Syntax::JavaScript => rest.starts_with("//"),
			tg::interface::Syntax::Json => false,
			tg::interface::Syntax::Python => byte == b'#',
		};
		let kind = if line_comment {
			index += rest.find('\n').unwrap_or(rest.len());
			continue;
		} else if syntax == tg::interface::Syntax::JavaScript && rest.starts_with("/*") {
			let Some(end) = rest[2..].find("*/") else {
				let message = "the comment is not closed".to_owned();
				return Err(Error {
					message,
					range: start..text.len(),
				});
			};
			let comment = &rest[..end + 4];
			index += comment.len();
			let documentation = comment.starts_with("/**") && comment != "/**/";
			if !documentation {
				line_begin = text[..index]
					.rfind('\n')
					.map_or(line_begin, |offset| offset + 1);
				continue;
			}
			TokenKind::Documentation(documentation_text(&comment[3..comment.len() - 2]))
		} else if byte == b'"' || byte == b'\'' {
			let (string, length) = string(rest, syntax).ok_or_else(|| Error {
				message: "the string is not closed".to_owned(),
				range: start..text.len(),
			})?;
			index += length;
			TokenKind::String(string)
		} else if byte.is_ascii_digit() {
			let length = rest
				.find(|c: char| !(c.is_ascii_alphanumeric() || c == '.' || c == '_'))
				.unwrap_or(rest.len());
			let number = rest[..length].replace('_', "");
			let number = number.parse::<f64>().map_err(|_| Error {
				message: "the number is not valid".to_owned(),
				range: start..start + length,
			})?;
			index += length;
			TokenKind::Number(number)
		} else if byte.is_ascii_alphabetic() || byte == b'_' || byte == b'$' {
			let length = rest
				.find(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == '$'))
				.unwrap_or(rest.len());
			index += length;
			TokenKind::Identifier(rest[..length].to_owned())
		} else if let Some(punctuation) = PUNCTUATION.iter().find(|p| rest.starts_with(**p)) {
			index += punctuation.len();
			TokenKind::Punctuation(punctuation)
		} else {
			let length = rest.chars().next().map_or(1, char::len_utf8);
			let message = "the character is not valid here".to_owned();
			return Err(Error {
				message,
				range: start..start + length,
			});
		};
		let token = Token {
			column: start - line_begin,
			kind,
			line_start,
			range: start..index,
		};
		tokens.push(token);
		line_start = false;
	}
	Ok(tokens)
}

/// Get the text of a documentation comment, without the leading asterisks of its lines.
fn documentation_text(comment: &str) -> String {
	comment
		.lines()
		.map(|line| {
			let line = line.trim();
			let line = line.strip_prefix('*').unwrap_or(line);
			line.strip_prefix(' ').unwrap_or(line)
		})
		.collect::<Vec<_>>()
		.join("\n")
		.trim()
		.to_owned()
}

/// Lex a string at the beginning of the text, returning its value and its length in the text.
fn string(text: &str, syntax: tg::interface::Syntax) -> Option<(String, usize)> {
	let quote = text.chars().next()?;
	let triple =
		syntax == tg::interface::Syntax::Python && text.starts_with(&quote.to_string().repeat(3));
	let delimiter = if triple {
		quote.to_string().repeat(3)
	} else {
		quote.to_string()
	};
	let mut value = String::new();
	let mut chars = text[delimiter.len()..].char_indices();
	while let Some((offset, c)) = chars.next() {
		let offset = offset + delimiter.len();
		if text[offset..].starts_with(&delimiter) {
			return Some((value, offset + delimiter.len()));
		}
		match c {
			'\\' => {
				let (_, escape) = chars.next()?;
				let c = match escape {
					'0' => '\0',
					'b' => '\x08',
					'f' => '\x0C',
					'n' => '\n',
					'r' => '\r',
					't' => '\t',
					'u' => {
						let digits = (0..4)
							.map(|_| chars.next().map(|(_, c)| c))
							.collect::<Option<String>>()?;
						char::from_u32(u32::from_str_radix(&digits, 16).ok()?)?
					},
					'x' => {
						let digits = (0..2)
							.map(|_| chars.next().map(|(_, c)| c))
							.collect::<Option<String>>()?;
						char::from_u32(u32::from_str_radix(&digits, 16).ok()?)?
					},
					c => c,
				};
				value.push(c);
			},
			'\n' if !triple => return None,
			c => value.push(c),
		}
	}
	None
}

/// Get the module of an import from its specifier, which is a path or an object ID, and its kind, which defaults to the kind that the path's file name implies.
fn import(specifier: &str, kind: Option<&str>) -> std::result::Result<tg::module::Data, String> {
	let kind = kind
		.map(|kind| {
			kind.parse::<tg::module::Kind>()
				.map_err(|_| format!("the module kind {kind:?} is not known"))
		})
		.transpose()?;
	let (source, kind) = if let Ok(id) = specifier.parse::<tg::object::Id>() {
		let source = tg::module::data::Source::Edge(tg::graph::data::Edge::Object(id));
		(source, kind)
	} else {
		let kind = kind.or_else(|| tg::module::module_kind_for_path(specifier).ok());
		let source = tg::module::data::Source::Path(PathBuf::from(specifier));
		(source, kind)
	};
	let kind = kind.ok_or_else(|| format!("the kind of the module {specifier:?} is not known"))?;
	let module = tg::module::Data {
		kind,
		referent: tg::Referent::with_node(source),
	};
	Ok(module)
}

impl Parser {
	fn new(tokens: Vec<Token>, end: usize) -> Self {
		Self {
			end,
			index: 0,
			references: Vec::new(),
			tokens,
		}
	}

	fn peek(&self) -> Option<&Token> {
		self.tokens.get(self.index)
	}

	fn peek_kind(&self, offset: usize) -> Option<&TokenKind> {
		self.tokens
			.get(self.index + offset)
			.map(|token| &token.kind)
	}

	fn next(&mut self) -> Option<Token> {
		let token = self.tokens.get(self.index).cloned();
		if token.is_some() {
			self.index += 1;
		}
		token
	}

	/// Get the range of the current token, or of the end of the text.
	fn range(&self) -> Range<usize> {
		self.peek()
			.map_or(self.end..self.end, |token| token.range.clone())
	}

	fn error<T>(&self, message: impl Into<String>) -> Result<T, Error> {
		let message = message.into();
		let range = self.range();
		Err(Error { message, range })
	}

	fn is_punctuation(&self, punctuation: &str) -> bool {
		matches!(self.peek_kind(0), Some(TokenKind::Punctuation(p)) if *p == punctuation)
	}

	fn is_keyword(&self, keyword: &str) -> bool {
		matches!(self.peek_kind(0), Some(TokenKind::Identifier(name)) if name == keyword)
	}

	fn eat_punctuation(&mut self, punctuation: &str) -> bool {
		let matches = self.is_punctuation(punctuation);
		if matches {
			self.index += 1;
		}
		matches
	}

	fn eat_keyword(&mut self, keyword: &str) -> bool {
		let matches = self.is_keyword(keyword);
		if matches {
			self.index += 1;
		}
		matches
	}

	fn expect_punctuation(&mut self, punctuation: &str) -> Result<(), Error> {
		if self.eat_punctuation(punctuation) {
			return Ok(());
		}
		self.error(format!("expected {punctuation:?}"))
	}

	fn expect_keyword(&mut self, keyword: &str) -> Result<(), Error> {
		if self.eat_keyword(keyword) {
			return Ok(());
		}
		self.error(format!("expected {keyword:?}"))
	}

	fn expect_identifier(&mut self) -> Result<String, Error> {
		if let Some(TokenKind::Identifier(name)) = self.peek_kind(0) {
			let name = name.clone();
			self.index += 1;
			return Ok(name);
		}
		self.error("expected a name")
	}

	fn expect_string(&mut self) -> Result<String, Error> {
		if let Some(TokenKind::String(string)) = self.peek_kind(0) {
			let string = string.clone();
			self.index += 1;
			return Ok(string);
		}
		self.error("expected a string")
	}

	/// Parse a literal, which is a string, a number, or a boolean, with the names of the booleans in the syntax.
	fn literal(&mut self, booleans: [&str; 2]) -> Result<tg::interface::Literal, Error> {
		let negative = self.eat_punctuation("-");
		let literal = match self.peek_kind(0) {
			Some(TokenKind::Number(number)) => {
				tg::interface::Literal::Number(if negative { -number } else { *number })
			},
			Some(TokenKind::String(string)) if !negative => {
				tg::interface::Literal::String(string.clone())
			},
			Some(TokenKind::Identifier(name)) if !negative && name == booleans[0] => {
				tg::interface::Literal::Boolean(true)
			},
			Some(TokenKind::Identifier(name)) if !negative && name == booleans[1] => {
				tg::interface::Literal::Boolean(false)
			},
			_ => return self.error("expected a literal"),
		};
		self.index += 1;
		Ok(literal)
	}

	/// Parse a reference to a definition, or to a definition of an import as `import.Name`.
	fn reference(&mut self, name: String) -> Result<tg::interface::Type, Error> {
		let start = self.tokens[self.index - 1].range.start;
		let (import, name) = if self.eat_punctuation(".") {
			let member = self.expect_identifier()?;
			(Some(name), member)
		} else {
			(None, name)
		};
		let end = self.tokens[self.index - 1].range.end;
		self.references
			.push((import.clone(), name.clone(), start..end));
		Ok(tg::interface::Type::Reference { import, name })
	}

	/// Check that the references refer to the interface's definitions and imports.
	fn check_references(&self, interface: &tg::interface::Interface) -> Result<(), Error> {
		for (import, name, range) in &self.references {
			let known = match import {
				Some(import) => interface.imports.contains_key(import),
				None => interface.definitions.contains_key(name),
			};
			if !known {
				let message = match import {
					Some(import) => format!("the import {import:?} is not declared"),
					None => format!("the type {name:?} is not defined"),
				};
				let range = range.clone();
				return Err(Error { message, range });
			}
		}
		Ok(())
	}
}

/// Insert an entry into a map, or fail if the map has an entry with the name.
fn insert<T>(
	map: &mut BTreeMap<String, T>,
	name: String,
	value: T,
	range: Range<usize>,
	what: &str,
) -> Result<(), Error> {
	if map.contains_key(&name) {
		let message = format!("the {what} {name:?} is declared more than once");
		return Err(Error { message, range });
	}
	map.insert(name, value);
	Ok(())
}
