#![expect(dead_code)]

use {crate::Server, tangram_client::prelude::*};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Location {
	Local,
	Remote(String),
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct Locations {
	pub local: bool,
	pub remotes: Vec<String>,
}

impl Server {
	pub(crate) fn location(
		local: Option<bool>,
		remotes: Option<&[String]>,
	) -> tg::Result<Location> {
		let remotes = remotes.unwrap_or_default();
		let local = local.unwrap_or(remotes.is_empty());
		match (local, remotes) {
			(true, []) => Ok(Location::Local),
			(true, _) => Err(tg::error!("cannot specify both local and a remote")),
			(false, []) => Err(tg::error!("a remote is required when local is false")),
			(false, [remote]) => Ok(Location::Remote(remote.clone())),
			(false, _) => Err(tg::error!("only one remote is allowed")),
		}
	}

	pub(crate) async fn locations(
		&self,
		local: Option<bool>,
		remotes: Option<Vec<String>>,
	) -> tg::Result<Locations> {
		let local = match (local, remotes.as_ref()) {
			(None, None) => true,
			(Some(local), _) => local,
			(None, Some(_)) => false,
		};
		let remotes = if let Some(remotes) = remotes {
			remotes
		} else if local {
			Vec::new()
		} else {
			let output = self.list_remotes(tg::remote::list::Arg::default()).await?;
			output.data.into_iter().map(|remote| remote.name).collect()
		};
		let locations = Locations { local, remotes };
		Ok(locations)
	}
}
