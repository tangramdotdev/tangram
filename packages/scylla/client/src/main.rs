use {
	clap::Parser as _,
	scylla::{
		client::{session::Session, session_builder::SessionBuilder},
		statement::unprepared::Statement,
	},
	std::{error::Error, fs, path::PathBuf, time::Duration},
};

#[cfg(test)]
mod test;

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Debug, clap::Parser)]
#[command(group(clap::ArgGroup::new("input").required(true).args(["execute", "file"])))]
struct Args {
	#[arg(short, long)]
	execute: Option<String>,

	#[arg(short, long)]
	file: Option<PathBuf>,

	#[arg(default_value = "127.0.0.1", index = 1)]
	host: String,

	#[arg(short, long)]
	keyspace: Option<String>,

	#[arg(
		short = 'p',
		long,
		env = "SCYLLA_PASSWORD",
		hide_env_values = true,
		requires = "username"
	)]
	password: Option<String>,

	#[arg(default_value_t = 9042, index = 2)]
	port: u16,

	/// Timeout in seconds for connecting and executing each statement.
	#[arg(long, default_value_t = 30, value_parser = clap::value_parser!(u64).range(1..))]
	request_timeout: u64,

	#[arg(short = 'u', long, requires = "password")]
	username: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ParserState {
	BlockComment,
	DoubleQuote,
	LineComment,
	Normal,
	SingleQuote,
}

#[tokio::main]
async fn main() -> Result<()> {
	// Load the CQL input.
	let args = Args::parse();
	let source = if let Some(source) = args.execute {
		source
	} else if let Some(path) = args.file {
		fs::read_to_string(path)?
	} else {
		unreachable!()
	};

	// Connect to ScyllaDB.
	let address = format!("{}:{}", args.host, args.port);
	let timeout = Duration::from_secs(args.request_timeout);
	let mut builder = SessionBuilder::new()
		.known_node(address)
		.connection_timeout(timeout);
	if let (Some(username), Some(password)) = (args.username, args.password) {
		builder = builder.user(username, password);
	}
	let session = tokio::time::timeout(timeout, Box::pin(builder.build())).await??;
	if let Some(keyspace) = args.keyspace {
		tokio::time::timeout(timeout, session.use_keyspace(keyspace, true)).await??;
	}

	// Execute the statements.
	for statement in split_statements(&source)? {
		execute_statement(&session, &statement, timeout).await?;
	}

	Ok(())
}

async fn execute_statement(session: &Session, statement: &str, timeout: Duration) -> Result<()> {
	let select_json = statement
		.trim_start()
		.get(..12)
		.is_some_and(|prefix| prefix.eq_ignore_ascii_case("select json "));
	let mut statement = Statement::new(statement);
	statement.set_request_timeout(Some(timeout));
	let result = tokio::time::timeout(timeout, session.query_unpaged(statement, ())).await??;
	if select_json {
		let rows = result.into_rows_result()?;
		for row in rows.rows::<(String,)>()? {
			let (json,) = row?;
			println!("{json}");
		}
	}

	Ok(())
}

fn split_statements(source: &str) -> Result<Vec<String>> {
	// Parse the input.
	let mut chars = source.chars().peekable();
	let mut state = ParserState::Normal;
	let mut statement = String::new();
	let mut statements = Vec::new();
	while let Some(character) = chars.next() {
		match state {
			ParserState::BlockComment => {
				if character == '*' && chars.next_if_eq(&'/').is_some() {
					statement.push(' ');
					state = ParserState::Normal;
				} else if character == '\n' {
					statement.push(character);
				}
			},
			ParserState::DoubleQuote => {
				statement.push(character);
				if character == '"' {
					if chars.next_if_eq(&'"').is_some() {
						statement.push('"');
					} else {
						state = ParserState::Normal;
					}
				}
			},
			ParserState::LineComment => {
				if character == '\n' {
					statement.push(character);
					state = ParserState::Normal;
				}
			},
			ParserState::Normal => match character {
				'\'' => {
					statement.push(character);
					state = ParserState::SingleQuote;
				},
				'"' => {
					statement.push(character);
					state = ParserState::DoubleQuote;
				},
				'-' if chars.next_if_eq(&'-').is_some() => {
					state = ParserState::LineComment;
				},
				'/' if chars.next_if_eq(&'*').is_some() => {
					state = ParserState::BlockComment;
				},
				';' => {
					let value = statement.trim();
					if !value.is_empty() {
						statements.push(value.to_owned());
					}
					statement.clear();
				},
				_ => statement.push(character),
			},
			ParserState::SingleQuote => {
				statement.push(character);
				if character == '\'' {
					if chars.next_if_eq(&'\'').is_some() {
						statement.push('\'');
					} else {
						state = ParserState::Normal;
					}
				}
			},
		}
	}

	// Validate and collect the trailing statement.
	if state != ParserState::Normal && state != ParserState::LineComment {
		return Err("the CQL input ended inside a quote or block comment".into());
	}
	let value = statement.trim();
	if !value.is_empty() {
		statements.push(value.to_owned());
	}
	if statements.is_empty() {
		return Err("the CQL input did not contain any statements".into());
	}

	Ok(statements)
}
