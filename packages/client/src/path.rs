use std::path::{Path, PathBuf};

pub trait Ext {
	fn diff(&self, src: impl AsRef<Path>) -> Option<PathBuf>;
	fn is_external(&self) -> bool;
	fn is_internal(&self) -> bool;
	fn normalize(&self) -> PathBuf;
}

impl<T> Ext for T
where
	T: AsRef<Path>,
{
	#[must_use]
	fn diff(&self, src: impl AsRef<Path>) -> Option<PathBuf> {
		let dst = self.as_ref();
		let src = src.as_ref();

		if dst.is_absolute() != src.is_absolute() {
			return if dst.is_absolute() {
				Some(dst.to_owned())
			} else {
				None
			};
		}
		let mut components = Vec::new();
		let mut dst = dst.components();
		let mut src = src.components();
		loop {
			match (dst.next(), src.next()) {
				(None, None) => break,
				(None, Some(_)) => components.push(std::path::Component::ParentDir),
				(Some(d), None) => {
					components.push(d);
					components.extend(dst);
					break;
				},
				(Some(d), Some(s)) if components.is_empty() && d == s => (),
				(Some(d), Some(std::path::Component::CurDir)) => components.push(d),
				(Some(_), Some(std::path::Component::ParentDir)) => return None,
				(Some(d), Some(_)) => {
					components.push(std::path::Component::ParentDir);
					for _ in src {
						components.push(std::path::Component::ParentDir);
					}
					components.push(d);
					components.extend(dst);
					break;
				},
			}
		}

		if matches!(
			components.first(),
			Some(std::path::Component::Normal(_)) | None
		) {
			components.insert(0, std::path::Component::CurDir);
		}

		let mut path = PathBuf::new();
		for component in components {
			path.push(component);
		}

		Some(path)
	}

	fn is_external(&self) -> bool {
		matches!(
			self.as_ref().components().next(),
			Some(std::path::Component::ParentDir)
		)
	}

	fn is_internal(&self) -> bool {
		matches!(
			self.as_ref().components().next(),
			Some(std::path::Component::CurDir | std::path::Component::Normal(_))
		)
	}

	fn normalize(&self) -> PathBuf {
		let mut components = Vec::new();
		for component in self.as_ref().components() {
			match (component, components.last()) {
				// Skip any current dir components.
				(std::path::Component::CurDir, _) => continue,

				// If the component is a parent component following a normal component, then remove the normal component.
				(std::path::Component::ParentDir, Some(std::path::Component::Normal(_))) => {
					components.pop();
				},

				// Otherwise, add the component.
				(component, _) => {
					components.push(component);
				},
			}
		}
		let mut path = PathBuf::new();
		for component in components {
			path.push(component);
		}
		path
	}
}
