#[derive(
	Clone,
	Copy,
	Debug,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	derive_more::Display,
	derive_more::FromStr,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
)]
#[display(rename_all = "snake_case")]
#[from_str(rename_all = "snake_case")]
pub enum Permission {
	Read,
	Write,
	Admin,
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct Set(u8);

impl Set {
	pub const READ: Self = Self(1 << 0);
	pub const WRITE: Self = Self(1 << 1);
	pub const ADMIN: Self = Self(1 << 2);
}

impl Permission {
	#[must_use]
	pub fn implies(self, needed: Self) -> bool {
		matches!(
			(self, needed),
			(Self::Admin, Self::Admin | Self::Write | Self::Read)
				| (Self::Write, Self::Write | Self::Read)
				| (Self::Read, Self::Read)
		)
	}
}

impl Set {
	#[must_use]
	pub fn empty() -> Self {
		Self::default()
	}

	#[must_use]
	pub fn from_permission(permission: Permission) -> Self {
		match permission {
			Permission::Read => Self::READ,
			Permission::Write => Self::WRITE,
			Permission::Admin => Self::ADMIN,
		}
	}

	#[must_use]
	pub fn contains(self, other: Self) -> bool {
		self.0 & other.0 == other.0
	}

	#[must_use]
	pub fn is_empty(self) -> bool {
		self.0 == 0
	}

	pub fn insert(&mut self, other: Self) {
		self.0 |= other.0;
	}

	pub fn remove(&mut self, other: Self) {
		self.0 &= !other.0;
	}
}
