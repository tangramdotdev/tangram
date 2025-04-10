use super::content_encoding::ContentEncoding;

#[derive(Clone, Debug)]
pub struct AcceptEncoding {
	pub preferences: Vec<Preference>,
}

#[derive(Clone, Debug)]
pub struct Preference {
	pub encoding: ContentEncoding,
	pub weight: Option<f64>,
}

#[derive(Debug, derive_more::Display, derive_more::Error)]
pub enum FromStrError {
	MissingEncoding,
	InvalidWeight,
	ContentEncoding(super::content_encoding::FromStrError),
}

impl std::fmt::Display for AcceptEncoding {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		for (i, preference) in self.preferences.iter().enumerate() {
			if i > 0 {
				write!(f, ", ")?;
			}
			write!(f, "{preference}")?;
		}
		Ok(())
	}
}

impl std::str::FromStr for AcceptEncoding {
	type Err = FromStrError;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let preferences = s
			.split(',')
			.map(str::trim)
			.filter(|part| !part.is_empty())
			.map(str::parse)
			.collect::<Result<_, _>>()?;
		Ok(AcceptEncoding { preferences })
	}
}

impl std::fmt::Display for Preference {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.encoding)?;
		if let Some(weight) = self.weight {
			write!(f, "; q={weight}")?;
		}
		Ok(())
	}
}

impl std::str::FromStr for Preference {
	type Err = FromStrError;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let mut parts = s.split(';');
		let encoding = parts
			.next()
			.ok_or(FromStrError::MissingEncoding)?
			.parse()
			.map_err(FromStrError::ContentEncoding)?;
		let weight = parts
			.next()
			.map(str::parse)
			.transpose()
			.map_err(|_| FromStrError::InvalidWeight)?;
		Ok(Preference { encoding, weight })
	}
}
