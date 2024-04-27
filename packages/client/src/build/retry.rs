use crate as tg;

#[derive(
	Clone,
	Copy,
	Debug,
	Default,
	Eq,
	Ord,
	PartialEq,
	PartialOrd,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
)]
pub enum Retry {
	#[default]
	Canceled,
	Failed,
	Succeeded,
}

impl tg::Build {
	pub async fn retry<H>(&self, handle: &H) -> tg::Result<Retry>
	where
		H: tg::Handle,
	{
		self.try_get_retry(handle)
			.await?
			.ok_or_else(|| tg::error!("failed to get the build"))
	}

	pub async fn try_get_retry<H>(&self, handle: &H) -> tg::Result<Option<Retry>>
	where
		H: tg::Handle,
	{
		let Some(output) = handle.try_get_build(&self.id).await? else {
			return Ok(None);
		};
		Ok(Some(output.retry))
	}
}

impl std::fmt::Display for Retry {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Canceled => write!(f, "canceled"),
			Self::Failed => write!(f, "failed"),
			Self::Succeeded => write!(f, "succeeded"),
		}
	}
}

impl std::str::FromStr for Retry {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		match s {
			"canceled" => Ok(Self::Canceled),
			"failed" => Ok(Self::Failed),
			"succeeded" => Ok(Self::Succeeded),
			retry => Err(tg::error!(%retry, "invalid value")),
		}
	}
}
