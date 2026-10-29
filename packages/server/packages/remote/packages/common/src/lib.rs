use tangram_client::prelude::*;

#[must_use]
pub fn tokens_valid(
	tokens: &[tg::authorization::Token],
	clock: &tangram_server_clock_core::Clock,
) -> bool {
	let now = clock.unix_timestamp();
	tokens.is_empty() || tokens.iter().all(|token| token.body.expires_at > now)
}

pub fn remove_expired_tokens(
	tokens: &mut tg::authorization::Tokens,
	clock: &tangram_server_clock_core::Clock,
) {
	let now = clock.unix_timestamp();
	for token in tokens.remove_local_authorization() {
		if token.body.expires_at > now {
			tokens.insert_local_authorization(token);
		}
	}
}

#[cfg(test)]
mod tests {
	use tangram_client::prelude::*;

	#[test]
	fn expiration_preserves_other_proofs() {
		let clock = tangram_server_clock_core::Clock::new(None, None);
		let resource = tg::process::Id::new();
		let token = |expires_at| tg::authorization::Token {
			body: tg::authorization::Body {
				expires_at,
				permissions: vec![tg::authorization::Permission::Process(
					tg::authorization::permission::process::Permission::Node,
				)],
				resource: resource.clone().into(),
			},
			secret_id: 0,
			signature: vec![0; 16],
		};
		let mut expired = token(0);
		expired.body.resource = tg::process::Id::new().into();
		let valid = token(i64::MAX);
		let mut tokens =
			tg::authorization::Tokens::with_authorization([expired.clone(), valid.clone()]);
		let remote = tg::Location::Remote(tg::location::Remote {
			name: "default".into(),
			region: None,
		});
		tokens.insert_authorization(remote.clone(), expired.clone());
		let key = tg::authorization::Secret::generate(0, tg::authorization::Algorithm::Blake3);
		let sync = tg::authorization::Token::sign(
			tg::authorization::Body {
				expires_at: i64::MAX,
				permissions: vec![tg::authorization::Permission::Sync(
					tg::authorization::permission::sync::Permission::Read,
				)],
				resource: tg::sync::Id::new().into(),
			},
			&key,
		)
		.unwrap();
		let location = tg::Location::Local(tg::location::Local::default());
		tokens.insert_authorization(location, sync.clone());

		assert!(!super::tokens_valid(tokens.local_authorization(), &clock));
		super::remove_expired_tokens(&mut tokens, &clock);
		assert_eq!(tokens.local_authorization().len(), 2);
		assert!(tokens.local_authorization().contains(&valid));
		assert_eq!(tokens.authorization(&remote), &[expired]);
		assert!(super::tokens_valid(tokens.local_authorization(), &clock));
		assert!(tokens.local_authorization().contains(&sync));
	}
}
