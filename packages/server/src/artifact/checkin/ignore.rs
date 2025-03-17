use globset::{Candidate, GlobBuilder, GlobSet, GlobSetBuilder};
use std::{
	collections::BTreeMap,
	ffi::OsString,
	path::{Path, PathBuf},
	sync::{Arc, RwLock},
};

#[derive(Debug, derive_more::Display, derive_more::Error, derive_more::From)]
pub enum Error {
	Glob(globset::Error),
	Io(std::io::Error),
	Path,
}

#[derive(Debug)]
pub struct Matcher {
	file_names: Vec<OsString>,
	global: Option<File>,
	root: Arc<RwLock<Node>>,
}

#[derive(Debug)]
struct Node {
	children: BTreeMap<OsString, Arc<RwLock<Self>>>,
	files: Vec<File>,
}

#[derive(Debug)]
struct File {
	glob_set: GlobSet,
	patterns: Vec<Pattern>,
}

#[derive(Debug)]
struct Pattern {
	negated: bool,
	#[allow(dead_code)]
	string: String,
	trailing_slash: bool,
}

impl Matcher {
	pub async fn new(file_names: Vec<OsString>, global: Option<&str>) -> Result<Self, Error> {
		let root = Self::node_with_path_and_file_names(Path::new("/"), &file_names).await?;
		let global = if let Some(global) = global {
			Some(Self::file_with_contents(global)?)
		} else {
			None
		};
		Ok(Self {
			file_names,
			global,
			root,
		})
	}

	pub async fn matches(&self, path: &Path, is_directory: Option<bool>) -> Result<bool, Error> {
		// Check if the path is a directory if necessary.
		let is_directory = if let Some(is_directory) = is_directory {
			is_directory
		} else {
			tokio::fs::symlink_metadata(path).await?.is_dir()
		};

		// Split the path into components.
		let mut components = path
			.strip_prefix("/")
			.map_err(|_| Error::Path)?
			.components()
			.peekable();

		// Get or create the nodes.
		let mut path = PathBuf::from("/");
		let mut nodes = vec![self.root.clone()];
		while let Some(component) = components.next() {
			path.push(component);
			let std::path::Component::Normal(name) = component else {
				return Err(Error::Path);
			};
			let node = nodes.last().unwrap();
			let option = node.read().unwrap().children.get(name).cloned();
			let child = if let Some(child) = option {
				child
			} else if components.peek().is_some() {
				let child = Self::node_with_path_and_file_names(&path, &self.file_names).await?;
				let child = node
					.write()
					.unwrap()
					.children
					.entry(name.to_owned())
					.or_insert(child)
					.clone();
				child
			} else {
				break;
			};
			nodes.push(child);
		}

		// Match.
		let mut matches = Vec::new();
		for (node, node_path) in std::iter::zip(nodes.iter().rev(), path.ancestors().skip(1)) {
			let candidate = Candidate::new(path.strip_prefix(node_path).unwrap());
			let files = &node.read().unwrap().files;
			for file in files {
				file.glob_set
					.matches_candidate_into(&candidate, &mut matches);
				if let Some(index) = matches.last() {
					let pattern = file.patterns.get(*index).unwrap();
					if !pattern.trailing_slash || is_directory {
						return Ok(!pattern.negated);
					}
				}
			}
		}
		if let Some(global) = &self.global {
			let candidate = Candidate::new(path.strip_prefix("/").unwrap());
			global
				.glob_set
				.matches_candidate_into(&candidate, &mut matches);
			if let Some(index) = matches.last() {
				let pattern = global.patterns.get(*index).unwrap();
				if !pattern.trailing_slash || is_directory {
					return Ok(!pattern.negated);
				}
			}
		}

		Ok(false)
	}

	async fn node_with_path_and_file_names(
		path: &Path,
		file_names: &[OsString],
	) -> Result<Arc<RwLock<Node>>, Error> {
		let mut files = Vec::new();
		for name in file_names {
			let contents = match tokio::fs::read_to_string(path.join(name)).await {
				Ok(contents) => contents,
				Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
					continue;
				},
				Err(error) => return Err(error.into()),
			};
			let file = Self::file_with_contents(&contents)?;
			files.push(file);
		}
		let node = Arc::new(RwLock::new(Node {
			children: BTreeMap::new(),
			files,
		}));
		Ok(node)
	}

	fn file_with_contents(contents: &str) -> Result<File, Error> {
		// Create the patterns and glob set builder.
		let mut patterns = Vec::new();
		let mut glob_set = GlobSetBuilder::new();

		// Handle each line.
		for mut line in contents.lines() {
			// Skip commented lines.
			if line.starts_with('#') {
				continue;
			}

			// Trim the end.
			if !line.ends_with("\\ ") {
				line = line.trim_end();
			}

			// Skip empty lines.
			if line.is_empty() {
				continue;
			}

			// Create the glob. This code is derived from here: <https://github.com/BurntSushi/ripgrep/blob/79cbe89deb1151e703f4d91b19af9cdcc128b765/crates/ignore/src/gitignore.rs#L436>.
			let mut absolute = false;
			let mut negated = false;
			let mut trailing_slash = false;
			if line.starts_with("\\!") || line.starts_with("\\#") {
				line = &line[1..];
				absolute = line.chars().nth(0) == Some('/');
			} else {
				if line.starts_with('!') {
					negated = true;
					line = &line[1..];
				}
				if line.starts_with('/') {
					line = &line[1..];
					absolute = true;
				}
			}
			if line.as_bytes().last() == Some(&b'/') {
				trailing_slash = true;
				line = &line[..line.len() - 1];
				if line.as_bytes().last() == Some(&b'\\') {
					line = &line[..line.len() - 1];
				}
			}
			let mut string = line.to_owned();
			if !absolute && !line.chars().any(|c| c == '/') && !string.starts_with("**/") {
				string = format!("**/{string}");
			}
			if string.ends_with("/**") {
				string = format!("{string}/*");
			}
			let glob = GlobBuilder::new(&string)
				.literal_separator(true)
				.case_insensitive(false)
				.backslash_escape(true)
				.build()?;

			// Add the glob to the glob set.
			glob_set.add(glob);

			// Add the pattern.
			let pattern = Pattern {
				negated,
				string,
				trailing_slash,
			};
			patterns.push(pattern);
		}

		// Build the glob set.
		let glob_set = glob_set.build()?;

		Ok(File { glob_set, patterns })
	}
}
