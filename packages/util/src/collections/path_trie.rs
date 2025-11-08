use std::{ffi::OsString, path::Path};

#[derive(Clone, Debug)]
pub struct PathTrie<V> {
	root: Node<V>,
}

#[derive(Clone, Debug)]
struct Node<V> {
	value: Option<V>,
	children: im::HashMap<OsString, Node<V>, fnv::FnvBuildHasher>,
}

impl<V> PathTrie<V>
where
	V: Clone,
{
	pub fn insert(&mut self, path: &Path, value: V) {
		let mut node = &mut self.root;
		for component in path.components() {
			let component = component.as_os_str();
			if !node.children.contains_key(component) {
				node.children.insert(component.to_owned(), Node::default());
			}
			node = node.children.get_mut(component).unwrap();
		}
		node.value = Some(value);
	}

	pub fn get(&self, path: &Path) -> Option<&V> {
		let mut node = &self.root;
		for component in path.components() {
			node = node.children.get(component.as_os_str())?;
		}
		node.value.as_ref()
	}

	pub fn remove(&mut self, path: &Path) -> Option<V> {
		fn inner<V>(node: &mut Node<V>, path: &Path) -> Option<V>
		where
			V: Clone,
		{
			let mut components = path.components();
			let Some(component) = components.next() else {
				return node.value.take();
			};
			let remaining = components.as_path();
			let child = node.children.get_mut(component.as_os_str())?;
			let value = inner(child, remaining)?;
			if child.value.is_none() && child.children.is_empty() {
				node.children.remove(component.as_os_str());
			}
			Some(value)
		}
		inner(&mut self.root, path)
	}

	pub fn is_empty(&self) -> bool {
		self.root.value.is_none() && self.root.children.is_empty()
	}

	pub fn clear(&mut self) {
		self.root = Node::default();
	}
}

impl<V> Default for PathTrie<V> {
	fn default() -> Self {
		Self {
			root: Node::default(),
		}
	}
}

impl<V> Default for Node<V> {
	fn default() -> Self {
		Self {
			value: None,
			children: im::HashMap::default(),
		}
	}
}

#[cfg(test)]
mod tests {
	use {super::*, std::path::PathBuf};

	#[test]
	fn test_insert_and_get() {
		let mut trie = PathTrie::default();
		let path = PathBuf::from("a/b/c");

		trie.insert(&path, 42);
		assert_eq!(trie.get(&path), Some(&42));
	}

	#[test]
	fn test_get_nonexistent() {
		let trie: PathTrie<i32> = PathTrie::default();
		let path = PathBuf::from("a/b/c");

		assert_eq!(trie.get(&path), None);
	}

	#[test]
	fn test_multiple_paths() {
		let mut trie = PathTrie::default();
		let path1 = PathBuf::from("a/b");
		let path2 = PathBuf::from("a/c");
		let path3 = PathBuf::from("b/d");

		trie.insert(&path1, 1);
		trie.insert(&path2, 2);
		trie.insert(&path3, 3);

		assert_eq!(trie.get(&path1), Some(&1));
		assert_eq!(trie.get(&path2), Some(&2));
		assert_eq!(trie.get(&path3), Some(&3));
	}

	#[test]
	fn test_nested_paths() {
		let mut trie = PathTrie::default();
		let path1 = PathBuf::from("a");
		let path2 = PathBuf::from("a/b");
		let path3 = PathBuf::from("a/b/c");

		trie.insert(&path1, 1);
		trie.insert(&path2, 2);
		trie.insert(&path3, 3);

		assert_eq!(trie.get(&path1), Some(&1));
		assert_eq!(trie.get(&path2), Some(&2));
		assert_eq!(trie.get(&path3), Some(&3));
	}

	#[test]
	fn test_overwrite_value() {
		let mut trie = PathTrie::default();
		let path = PathBuf::from("a/b");

		trie.insert(&path, 1);
		assert_eq!(trie.get(&path), Some(&1));

		trie.insert(&path, 2);
		assert_eq!(trie.get(&path), Some(&2));
	}

	#[test]
	fn test_remove_leaf() {
		let mut trie = PathTrie::default();
		let path = PathBuf::from("a/b/c");

		trie.insert(&path, 42);
		assert_eq!(trie.remove(&path), Some(42));
		assert_eq!(trie.get(&path), None);
	}

	#[test]
	fn test_remove_nonexistent() {
		let mut trie: PathTrie<i32> = PathTrie::default();
		let path = PathBuf::from("a/b/c");

		assert_eq!(trie.remove(&path), None);
	}

	#[test]
	fn test_remove_cleans_up_empty_nodes() {
		let mut trie = PathTrie::default();
		let path1 = PathBuf::from("a/b/c");
		let path2 = PathBuf::from("a/b/d");

		trie.insert(&path1, 1);
		trie.insert(&path2, 2);

		assert_eq!(trie.remove(&path1), Some(1));
		assert_eq!(trie.get(&path2), Some(&2));

		assert_eq!(trie.remove(&path2), Some(2));
		assert!(trie.is_empty());
	}

	#[test]
	fn test_remove_intermediate_node_with_children() {
		let mut trie = PathTrie::default();
		let path1 = PathBuf::from("a/b");
		let path2 = PathBuf::from("a/b/c");

		trie.insert(&path1, 1);
		trie.insert(&path2, 2);

		assert_eq!(trie.remove(&path1), Some(1));
		assert_eq!(trie.get(&path1), None);
		assert_eq!(trie.get(&path2), Some(&2));
	}

	#[test]
	fn test_is_empty() {
		let mut trie: PathTrie<i32> = PathTrie::default();
		assert!(trie.is_empty());

		let path = PathBuf::from("a/b");
		trie.insert(&path, 42);
		assert!(!trie.is_empty());

		trie.remove(&path);
		assert!(trie.is_empty());
	}

	#[test]
	fn test_clear() {
		let mut trie = PathTrie::default();
		let path1 = PathBuf::from("a/b");
		let path2 = PathBuf::from("c/d");

		trie.insert(&path1, 1);
		trie.insert(&path2, 2);

		trie.clear();
		assert!(trie.is_empty());
		assert_eq!(trie.get(&path1), None);
		assert_eq!(trie.get(&path2), None);
	}

	#[test]
	fn test_empty_path() {
		let mut trie = PathTrie::default();
		let path = PathBuf::from("");

		trie.insert(&path, 42);
		assert_eq!(trie.get(&path), Some(&42));

		assert_eq!(trie.remove(&path), Some(42));
		assert!(trie.is_empty());
	}
}
