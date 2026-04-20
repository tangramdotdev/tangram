use {
	super::{Local, Location, Remote},
	crate as tg,
	winnow::{
		combinator::{alt, cut_err, delimited, eof, opt, preceded, separated, terminated},
		prelude::*,
		token::take_while,
	},
};

#[derive(
	Clone,
	Debug,
	Default,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[tangram_serialize(display, from_str)]
pub struct Arg(pub Vec<Component>);

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Component {
	Local(LocalComponent),
	Remote(RemoteComponent),
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct LocalComponent {
	pub regions: Option<Vec<String>>,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RemoteComponent {
	pub name: String,
	pub regions: Option<Vec<String>>,
}

type Input<'a> = winnow::stream::LocatingSlice<&'a str>;

impl Arg {
	#[must_use]
	pub fn is_empty(&self) -> bool {
		self.0.is_empty()
	}

	#[must_use]
	pub fn to_location(&self) -> Option<Location> {
		let component = self.0.first()?;
		if self.0.len() != 1 {
			return None;
		}
		let location = match component {
			Component::Local(local) => {
				let region = match local.regions.as_deref() {
					None => None,
					Some([region]) => Some(region.to_owned()),
					Some(_) => {
						return None;
					},
				};
				Location::Local(Local { region })
			},
			Component::Remote(remote) => {
				let region = match remote.regions.as_deref() {
					None => None,
					Some([region]) => Some(region.to_owned()),
					Some(_) => {
						return None;
					},
				};
				Location::Remote(Remote {
					name: remote.name.clone(),
					region,
				})
			},
		};
		Some(location)
	}
}

impl std::fmt::Display for Arg {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		for (i, component) in self.0.iter().enumerate() {
			if i > 0 {
				write!(f, ",")?;
			}
			match component {
				Component::Local(local) => {
					write!(f, "local")?;
					if let Some(regions) =
						local.regions.as_ref().filter(|regions| !regions.is_empty())
					{
						write!(f, "(")?;
						for (i, region) in regions.iter().enumerate() {
							if i > 0 {
								write!(f, ",")?;
							}
							write!(f, "{region}")?;
						}
						write!(f, ")")?;
					}
				},
				Component::Remote(remote) => {
					write!(f, "remote")?;
					if remote.name != "default" {
						write!(f, ":{}", remote.name)?;
					}
					if let Some(regions) = remote
						.regions
						.as_ref()
						.filter(|regions| !regions.is_empty())
					{
						write!(f, "(")?;
						for (i, region) in regions.iter().enumerate() {
							if i > 0 {
								write!(f, ",")?;
							}
							write!(f, "{region}")?;
						}
						write!(f, ")")?;
					}
				},
			}
		}
		Ok(())
	}
}

impl std::str::FromStr for Arg {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		terminated(arg, eof)
			.parse(Input::new(s))
			.map_err(|error| tg::error!("{}", error.to_string()))
	}
}

impl From<tg::Location> for tg::location::Arg {
	fn from(value: tg::Location) -> Self {
		match value {
			tg::Location::Local(local) => Self(vec![Component::Local(LocalComponent {
				regions: local.region.map(|region| vec![region]),
			})]),
			tg::Location::Remote(remote) => Self(vec![Component::Remote(RemoteComponent {
				name: remote.name,
				regions: remote.region.map(|region| vec![region]),
			})]),
		}
	}
}

fn arg(input: &mut Input) -> ModalResult<Arg> {
	delimited(
		whitespace,
		separated(0.., component, (whitespace, ',', whitespace)).map(Arg),
		whitespace,
	)
	.parse_next(input)
}

fn component(input: &mut Input) -> ModalResult<Component> {
	alt((
		local_component.map(Component::Local),
		remote_component.map(Component::Remote),
	))
	.parse_next(input)
}

fn local_component(input: &mut Input) -> ModalResult<LocalComponent> {
	(
		"local",
		opt(delimited(
			(whitespace, '('),
			cut_err(separated(1.., region, (whitespace, ',', whitespace))),
			(whitespace, ')'),
		)),
	)
		.map(|(_, regions)| LocalComponent { regions })
		.parse_next(input)
}

fn remote_component(input: &mut Input) -> ModalResult<RemoteComponent> {
	(
		"remote",
		opt(preceded(':', cut_err(name))),
		opt(delimited(
			(whitespace, '('),
			cut_err(separated(1.., region, (whitespace, ',', whitespace))),
			(whitespace, ')'),
		)),
	)
		.map(|(_, name, regions)| RemoteComponent {
			name: name.unwrap_or_else(|| "default".to_owned()),
			regions,
		})
		.parse_next(input)
}

fn name(input: &mut Input) -> ModalResult<String> {
	take_while(1.., is_name_char)
		.map(ToOwned::to_owned)
		.parse_next(input)
}

fn region(input: &mut Input) -> ModalResult<String> {
	delimited(
		whitespace,
		take_while(1.., is_name_char).map(ToOwned::to_owned),
		whitespace,
	)
	.parse_next(input)
}

fn whitespace(input: &mut Input) -> ModalResult<()> {
	take_while(0.., char::is_whitespace)
		.void()
		.parse_next(input)
}

fn is_name_char(c: char) -> bool {
	c.is_ascii_alphanumeric() || c == '_' || c == '-'
}
