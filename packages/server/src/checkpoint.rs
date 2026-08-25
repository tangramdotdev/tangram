use {
	crate::{Server, Session},
	std::{collections::BTreeMap, sync::Mutex},
	tangram_client::prelude::*,
};

mod continue_;
mod unwatch;
mod wait;
mod watch;

pub struct State {
	inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
	checkpoints: BTreeMap<String, Checkpoint>,
}

#[derive(Default)]
struct Checkpoint {
	next_watch: u64,
	watches: BTreeMap<u64, Watch>,
}

struct Watch {
	hits: BTreeMap<u64, Hit>,
	next_hit: u64,
	params: tg::checkpoint::Params,
	sender: tokio::sync::watch::Sender<u64>,
}

struct Hit {
	params: tg::checkpoint::Params,
	sender: Option<tokio::sync::oneshot::Sender<()>>,
}

impl State {
	#[must_use]
	pub fn new() -> Self {
		Self {
			inner: Mutex::new(Inner::default()),
		}
	}

	pub fn watch(&self, checkpoint: &str, params: tg::checkpoint::Params) -> u64 {
		let mut inner = self.inner.lock().unwrap();
		let checkpoint = inner.checkpoints.entry(checkpoint.to_owned()).or_default();
		let watch = checkpoint.next_watch;
		checkpoint.next_watch += 1;
		let (sender, _) = tokio::sync::watch::channel(0);
		let entry = Watch {
			hits: BTreeMap::new(),
			next_hit: 0,
			params,
			sender,
		};
		checkpoint.watches.insert(watch, entry);
		watch
	}

	pub async fn wait(
		&self,
		checkpoint: &str,
		watch: u64,
		hit: u64,
	) -> Option<tg::checkpoint::Params> {
		loop {
			let mut receiver = {
				let inner = self.inner.lock().unwrap();
				let checkpoint = inner.checkpoints.get(checkpoint)?;
				let watch = checkpoint.watches.get(&watch)?;
				if let Some(hit) = watch.hits.get(&hit) {
					return Some(hit.params.clone());
				}
				watch.sender.subscribe()
			};
			receiver.changed().await.ok()?;
		}
	}

	pub fn continue_hit(&self, checkpoint: &str, watch: u64, hit: u64) -> bool {
		let mut inner = self.inner.lock().unwrap();
		let Some(hit) = inner
			.checkpoints
			.get_mut(checkpoint)
			.and_then(|checkpoint| checkpoint.watches.get_mut(&watch))
			.and_then(|watch| watch.hits.get_mut(&hit))
		else {
			return false;
		};
		if let Some(sender) = hit.sender.take() {
			sender.send(()).ok();
		}
		true
	}

	pub fn unwatch(&self, checkpoint: &str, watch: u64) -> bool {
		let mut inner = self.inner.lock().unwrap();
		inner
			.checkpoints
			.get_mut(checkpoint)
			.and_then(|checkpoint| checkpoint.watches.remove(&watch))
			.is_some()
	}

	pub async fn hit(&self, checkpoint: &str, params: tg::checkpoint::Params) {
		let receivers = {
			let mut inner = self.inner.lock().unwrap();
			let Some(checkpoint) = inner.checkpoints.get_mut(checkpoint) else {
				return;
			};
			let mut receivers = Vec::new();
			for watch in checkpoint.watches.values_mut() {
				if !params_match(&watch.params, &params) {
					continue;
				}
				let hit = watch.next_hit;
				watch.next_hit += 1;
				let (sender, receiver) = tokio::sync::oneshot::channel();
				let entry = Hit {
					params: params.clone(),
					sender: Some(sender),
				};
				watch.hits.insert(hit, entry);
				watch.sender.send_replace(watch.next_hit);
				receivers.push(receiver);
			}
			receivers
		};
		for receiver in receivers {
			receiver.await.ok();
		}
	}
}

impl Server {
	pub(crate) async fn checkpoint<F>(&self, checkpoint: &str, params: F)
	where
		F: FnOnce() -> tg::checkpoint::Params,
	{
		let Some(checkpoints) = self.checkpoints.as_ref() else {
			return;
		};
		checkpoints.hit(checkpoint, params()).await;
	}
}

impl Session {
	fn checkpoint_state(&self) -> tg::Result<Option<&State>> {
		self.verify_request_from_host()?;
		if !matches!(self.context.principal, tg::Principal::Root) {
			return Err(tg::error!("unauthorized"));
		}
		Ok(self.server.checkpoints.as_ref())
	}
}

fn params_match(expected: &tg::checkpoint::Params, actual: &tg::checkpoint::Params) -> bool {
	expected
		.iter()
		.all(|(key, expected)| actual.get(key) == Some(expected))
}

#[macro_export]
macro_rules! checkpoint {
	({ $params:ident }) => {};
	({ $params:ident },) => {};
	({ $params:ident }, %$name:ident, $($arg:tt)*) => {
		$params.insert(stringify!($name).to_owned(), $name.to_string().into());
		$crate::checkpoint!({ $params }, $($arg)*);
	};
	({ $params:ident }, %$name:ident) => {
		$params.insert(stringify!($name).to_owned(), $name.to_string().into());
	};
	({ $params:ident }, ?$name:ident, $($arg:tt)*) => {
		$params.insert(stringify!($name).to_owned(), format!("{:?}", $name).into());
		$crate::checkpoint!({ $params }, $($arg)*);
	};
	({ $params:ident }, ?$name:ident) => {
		$params.insert(stringify!($name).to_owned(), format!("{:?}", $name).into());
	};
	({ $params:ident }, $name:ident = %$value:expr, $($arg:tt)*) => {
		$params.insert(stringify!($name).to_owned(), $value.to_string().into());
		$crate::checkpoint!({ $params }, $($arg)*);
	};
	({ $params:ident }, $name:ident = %$value:expr) => {
		$params.insert(stringify!($name).to_owned(), $value.to_string().into());
	};
	({ $params:ident }, $name:ident = ?$value:expr, $($arg:tt)*) => {
		$params.insert(stringify!($name).to_owned(), format!("{:?}", $value).into());
		$crate::checkpoint!({ $params }, $($arg)*);
	};
	({ $params:ident }, $name:ident = ?$value:expr) => {
		$params.insert(stringify!($name).to_owned(), format!("{:?}", $value).into());
	};
	({ $params:ident }, $name:ident = $value:expr, $($arg:tt)*) => {
		$params.insert(stringify!($name).to_owned(), $value.into());
		$crate::checkpoint!({ $params }, $($arg)*);
	};
	({ $params:ident }, $name:ident = $value:expr) => {
		$params.insert(stringify!($name).to_owned(), $value.into());
	};
	({ $params:ident }, $name:ident, $($arg:tt)*) => {
		$params.insert(stringify!($name).to_owned(), $name.clone().into());
		$crate::checkpoint!({ $params }, $($arg)*);
	};
	({ $params:ident }, $name:ident) => {
		$params.insert(stringify!($name).to_owned(), $name.clone().into());
	};
	($server:expr, $checkpoint:expr $(,)?) => {
		$server.checkpoint($checkpoint, tg::checkpoint::Params::new)
	};
	($server:expr, $checkpoint:expr, $($arg:tt)*) => {
		$server.checkpoint($checkpoint, || {
			let mut params = tg::checkpoint::Params::new();
			$crate::checkpoint!({ params }, $($arg)*);
			params
		})
	};
}
