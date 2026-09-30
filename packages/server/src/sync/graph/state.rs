use tangram_client::prelude::*;

#[derive(Clone, Debug, Default)]
pub(super) struct Object {
	pub dependencies: Dependencies,
	pub propagated: Facts,
	pub propagated_permissions: Option<tg::authorization::permission::Set>,
}

#[derive(Clone, Debug, Default)]
pub(super) struct Process {
	pub children: Dependencies,
	pub objects: Aspects<Dependencies>,
	pub propagated: Published,
	pub propagated_permissions: Option<tg::authorization::permission::Set>,
	pub subtree_objects: Aspects<Dependencies>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct Published {
	pub node: Facts,
	pub objects: Aspects<Facts>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[expect(
	clippy::struct_field_names,
	reason = "keep aspect names aligned with process metadata"
)]
pub(super) struct Aspects<T> {
	pub command_objects: T,
	pub error_objects: T,
	pub log_objects: T,
	pub output_objects: T,
}

/// The facts a dependency contributes to its parents.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct Facts {
	pub metadata: tg::object::metadata::Subtree,
	pub permissions: bool,
	pub storage: bool,
}

/// The dependencies contribute once per edge and newly known fact; known metadata is immutable, proofs only accumulate, and callers must know the complete dependency set before publishing derived metadata.
#[derive(Clone, Debug, Default)]
pub(super) struct Dependencies {
	count: Field,
	depth: Field,
	permitted: usize,
	size: Field,
	solvable: Field,
	solved: Field,
	stored: usize,
	total: usize,
}

#[derive(Clone, Debug, Default)]
struct Field {
	known: usize,
	max: u64,
	sum: u64,
}

impl<T> Aspects<T> {
	#[must_use]
	pub fn map<U>(&self, map: impl Fn(&T) -> U) -> Aspects<U> {
		let command_objects = map(&self.command_objects);
		let error_objects = map(&self.error_objects);
		let log_objects = map(&self.log_objects);
		let output_objects = map(&self.output_objects);
		Aspects {
			command_objects,
			error_objects,
			log_objects,
			output_objects,
		}
	}

	#[must_use]
	pub fn aspect_mut(&mut self, kind: crate::sync::queue::ObjectKind) -> &mut T {
		match kind {
			crate::sync::queue::ObjectKind::Command => &mut self.command_objects,
			crate::sync::queue::ObjectKind::Error => &mut self.error_objects,
			crate::sync::queue::ObjectKind::Log => &mut self.log_objects,
			crate::sync::queue::ObjectKind::Output => &mut self.output_objects,
		}
	}
}

impl Dependencies {
	pub fn insert(&mut self, facts: &Facts) {
		self.total += 1;
		self.update(&Facts::default(), facts);
	}

	pub fn update(&mut self, old: &Facts, new: &Facts) {
		debug_assert!(!old.storage || new.storage, "storage must not regress");
		debug_assert!(
			!old.permissions || new.permissions,
			"permissions must not regress"
		);
		self.permitted += usize::from(!old.permissions && new.permissions);
		self.stored += usize::from(!old.storage && new.storage);
		self.count.update(old.metadata.count, new.metadata.count);
		self.depth.update(old.metadata.depth, new.metadata.depth);
		self.size.update(old.metadata.size, new.metadata.size);
		self.solvable.update(
			old.metadata.solvable.map(u64::from),
			new.metadata.solvable.map(u64::from),
		);
		self.solved.update(
			old.metadata.solved.map(u64::from),
			new.metadata.solved.map(u64::from),
		);
		debug_assert!(self.permitted <= self.total);
		debug_assert!(self.stored <= self.total);
	}

	#[must_use]
	pub fn facts(&self) -> Facts {
		debug_assert!(self.count.known <= self.total);
		debug_assert!(self.depth.known <= self.total);
		debug_assert!(self.size.known <= self.total);
		debug_assert!(self.solvable.known <= self.total);
		debug_assert!(self.solved.known <= self.total);
		let metadata = tg::object::metadata::Subtree {
			count: (self.count.known == self.total).then_some(self.count.sum),
			depth: (self.depth.known == self.total).then_some(self.depth.max),
			size: (self.size.known == self.total).then_some(self.size.sum),
			solvable: (self.solvable.known == self.total).then_some(self.solvable.sum != 0),
			solved: (self.solved.known == self.total)
				.then_some(self.solved.sum == self.total as u64),
		};
		Facts {
			metadata,
			permissions: self.permitted == self.total,
			storage: self.stored == self.total,
		}
	}
}

impl Field {
	fn update(&mut self, old: Option<u64>, new: Option<u64>) {
		debug_assert!(
			old.is_none() || old == new,
			"known metadata must not change"
		);
		if old.is_none()
			&& let Some(value) = new
		{
			self.known += 1;
			self.max = self.max.max(value);
			self.sum += value;
		}
	}
}
