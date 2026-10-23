use {crate::location::Remote, tangram_client::prelude::*};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum Query {
	List(tg::list::Arg),
	Match(tg::match_::Arg),
}

impl Query {
	#[must_use]
	pub fn with_remote(mut self, remote: &Remote) -> Self {
		let location = Some(tg::location::Arg(vec![
			tg::location::arg::Component::Local(tg::location::arg::LocalComponent {
				regions: remote.regions.clone(),
			}),
		]));
		match &mut self {
			Self::List(arg) => {
				if let Some(node) = &mut arg.node {
					node.options.tokens =
						super::tokens_for_remote(&node.options.tokens, &remote.name);
					if let Some(tg::Location::Remote(location)) = &node.options.location
						&& location.name == remote.name
					{
						node.options.location = Some(tg::Location::Local(tg::location::Local {
							region: location.region.clone(),
						}));
					}
				}
			},
			Self::Match(arg) => {
				arg.tokens = super::tokens_for_remote(&arg.tokens, &remote.name);
			},
		}
		let (cached, query_location, ttl) = match &mut self {
			Self::List(arg) => (&mut arg.cached, &mut arg.location, &mut arg.ttl),
			Self::Match(arg) => (&mut arg.cached, &mut arg.location, &mut arg.ttl),
		};
		*cached = false;
		*query_location = location;
		*ttl = tg::remote::cache::Ttl::default();

		self
	}
}
