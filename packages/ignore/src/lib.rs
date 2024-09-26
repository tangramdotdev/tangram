use itertools::Itertools;
use std::{
	ffi::OsString,
	path::{Path, PathBuf},
	str::FromStr,
	sync::Arc,
};
use tokio::sync::RwLock;

#[derive(Debug)]
pub enum Error {
	Io(std::io::Error),
	NonNormalizedPath(PathBuf),
	Parse(globset::Error),
	StripPrefix(std::path::StripPrefixError),
}

pub struct Ignore {
	root: Arc<RwLock<Node>>,
	ignore_files: Vec<OsString>,
}

struct Node {
	path: PathBuf,
	name: Option<OsString>,
	patterns: Option<PatternSet>,
	children: Vec<Arc<RwLock<Self>>>,
}

#[derive(Debug)]
struct PatternSet {
	allow: Vec<Pattern>,
	deny: Vec<Pattern>,
}

#[derive(Debug)]
struct Pattern {
	allowlist: bool,
	only_directories: bool,
	glob: globset::GlobMatcher,
}

impl Ignore {
	pub async fn new(
		ignore_files: impl IntoIterator<Item = impl AsRef<Path>>,
	) -> Result<Self, Error> {
		let ignore_files = ignore_files
			.into_iter()
			.map(|path| path.as_ref().as_os_str().to_owned())
			.collect::<Vec<_>>();
		let root = Arc::new(RwLock::new(
			Node::new("/".into(), None, &ignore_files).await?,
		));
		Ok(Self { root, ignore_files })
	}

	pub async fn should_ignore(
		&self,
		path: &Path,
		metadata: &std::fs::Metadata,
	) -> Result<bool, Error> {
		let mut components = path.strip_prefix("/")?.components().peekable();

		let mut current = self.root.clone();
		while let Some(component) = components.next() {
			// Check if this ignore has a pattern for the path.
			{
				let node = current.read().await;
				if let Some(patterns) = &node.patterns {
					if patterns.should_ignore(&node.path, path, metadata) {
						return Ok(true);
					}
				}
			}

			// Otherwise keep searching.
			let std::path::Component::Normal(name) = component else {
				return Err(Error::NonNormalizedPath(path.to_owned()));
			};

			// Find the child.
			let mut next = None;
			{
				let node = current.read().await;
				for child in &node.children {
					if child.read().await.name.as_deref() == Some(name) {
						next.replace(child.clone());
						break;
					}
				}
			}

			current = if let Some(child) = next {
				// If the child was found, recurse.
				child
			} else if components.peek().is_some() {
				// If this is not the last path component and there is no child yet, create one.
				let node = current.read().await;
				let path = node.path.join(name);
				let child = Arc::new(RwLock::new(
					Node::new(path, Some(name.to_owned()), &self.ignore_files).await?,
				));
				drop(node);

				// Add the child to the parent and then recurse.
				current.write().await.children.push(child.clone());
				child
			} else {
				break;
			};
		}
		Ok(false)
	}
}

impl Node {
	async fn new(
		path: PathBuf,
		name: Option<OsString>,
		ignore_files: &[OsString],
	) -> Result<Self, Error> {
		let patterns = Self::try_parse_ignore_files(&path, ignore_files).await?;
		let children = Vec::new();
		Ok(Self {
			path,
			name,
			patterns,
			children,
		})
	}

	async fn try_parse_ignore_files(
		path: &Path,
		ignore_files: &[OsString],
	) -> Result<Option<PatternSet>, Error> {
		for ignore_file in ignore_files {
			let path = path.join(ignore_file);
			let contents = match tokio::fs::read_to_string(&path).await {
				Ok(contents) => contents,
				Err(error) if error.raw_os_error() == Some(libc::ENOENT) => continue,
				Err(error) => {
					return Err(error.into());
				},
			};

			// Parse the contents.
			let mut allow = Vec::new();
			let mut deny = Vec::new();
			contents
				.lines()
				.filter_map(|mut line| {
					// A line starting with # is a comment.
					if line.starts_with('#') {
						return None;
					}

					// Trailing spaces are ignored unless they are quoted with a backslash.
					if line.ends_with("\\ ") {
						line = line.trim_end();
					}

					// Empty lines are ignored.
					if line.is_empty() {
						return None;
					}

					Some(line)
				})
				.map(|line| {
					let pattern = line.parse::<Pattern>()?;
					if pattern.allowlist {
						allow.push(pattern);
					} else {
						deny.push(pattern);
					}
					Ok::<_, Error>(())
				})
				.try_collect()?;
			let patterns = PatternSet { allow, deny };
			return Ok(Some(patterns));
		}

		Ok(None)
	}
}

impl PatternSet {
	fn should_ignore(&self, root: &Path, path: &Path, metadata: &std::fs::Metadata) -> bool {
		let allow = self
			.allow
			.iter()
			.any(|pattern| pattern.matches(root, path, metadata));
		let deny = self
			.deny
			.iter()
			.any(|pattern| pattern.matches(root, path, metadata));
		deny && !allow
	}
}

impl Pattern {
	fn matches(&self, root: &Path, path: &Path, metadata: &std::fs::Metadata) -> bool {
		let path = path.strip_prefix(root).unwrap_or(path);
		if self.only_directories && !metadata.is_dir() {
			return false;
		}
		self.glob.is_match(path)
	}
}

impl FromStr for Pattern {
	type Err = Error;
	fn from_str(mut line: &str) -> Result<Self, Error> {
		let mut absolute = false;
		let mut allowlist = false;
		let mut only_directories = false;
		if line.starts_with("\\!") || line.starts_with("\\#") {
			line = &line[1..];
			absolute = line.starts_with('/');
		} else {
			if line.starts_with('!') {
				allowlist = true;
				line = &line[1..];
			}
			if line.starts_with('/') {
				absolute = true;
				line = &line[1..];
			}
		}
		if line.ends_with('/') {
			only_directories = true;
			line = &line[..line.len() - 1];
		}
		let mut line = if !absolute && !line.chars().any(|c| c == '/') && !line.starts_with("**/") {
			format!("**/{line}")
		} else {
			line.to_owned()
		};
		if line.ends_with("/**") {
			line.push_str("/*");
		}

		let glob = globset::Glob::new(&line)?.compile_matcher();

		Ok(Self {
			allowlist,
			only_directories,
			glob,
		})
	}
}

impl std::fmt::Display for Error {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Io(e) => write!(f, "io error: {e}"),
			Self::NonNormalizedPath(path) => {
				write!(f, "expected a normalized path, got {}", path.display())
			},
			Self::Parse(e) => write!(f, "parse error: {e}"),
			Self::StripPrefix(e) => write!(f, "path error: {e}"),
		}
	}
}

impl std::error::Error for Error {
	fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
		match self {
			Self::Io(e) => Some(e),
			Self::NonNormalizedPath(_) => None,
			Self::Parse(e) => Some(e),
			Self::StripPrefix(e) => Some(e),
		}
	}
}

impl From<std::io::Error> for Error {
	fn from(value: std::io::Error) -> Self {
		Self::Io(value)
	}
}

impl From<globset::Error> for Error {
	fn from(value: globset::Error) -> Self {
		Self::Parse(value)
	}
}

impl From<std::path::StripPrefixError> for Error {
	fn from(value: std::path::StripPrefixError) -> Self {
		Self::StripPrefix(value)
	}
}

#[cfg(test)]
mod tests {
	use super::Pattern;

	#[test]
	fn pattern() {
		let metadata = std::fs::metadata(".").unwrap();

		let root = "/home/user";
		let path = "/home/user/thing";
		let pattern: Pattern = "thing".parse().unwrap();
		assert!(pattern.matches(root.as_ref(), path.as_ref(), &metadata));

		let root = "/home/user";
		let path = "/home/user/thing";
		let pattern: Pattern = "/thing".parse().unwrap();
		assert!(pattern.matches(root.as_ref(), path.as_ref(), &metadata));
	}
}
