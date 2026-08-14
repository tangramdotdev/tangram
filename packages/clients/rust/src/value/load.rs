use {
	crate::prelude::*,
	std::{collections::VecDeque, sync::Arc},
	tokio::{sync::Semaphore, task::JoinSet},
};

#[derive(Clone, Debug, Default)]
pub struct Options {
	pub blobs: bool,
	pub depth: Option<u64>,
	pub location: Option<tg::location::Arg>,
}

impl tg::Value {
	pub async fn load(&self, options: tg::value::load::Options) -> tg::Result<()> {
		let handle = tg::handle()?;
		self.load_with_handle(handle, options).await
	}

	pub async fn load_with_handle<H>(
		&self,
		handle: &H,
		options: tg::value::load::Options,
	) -> tg::Result<()>
	where
		H: tg::Handle + Clone + Send + Sync + 'static,
	{
		let tg::value::load::Options {
			blobs,
			depth,
			location,
		} = options;
		let arg = tg::object::get::Arg {
			location,
			metadata: false,
			stored: false,
			tokens: tg::authorization::Tokens::default(),
		};
		let semaphore = Arc::new(Semaphore::new(16));
		let mut join_set: JoinSet<tg::Result<(Vec<Self>, Option<u64>)>> = JoinSet::new();
		let mut queue = VecDeque::new();
		queue.push_back((self.clone(), depth));
		while !queue.is_empty() || !join_set.is_empty() {
			while let Some((value, depth)) = queue.pop_front() {
				let depth = match depth {
					Some(0) => continue,
					Some(depth) => Some(depth - 1),
					None => None,
				};
				if let Self::Object(object) = &value
					&& !blobs && object.is_blob()
				{
					continue;
				}
				let permit = semaphore.clone().acquire_owned().await.unwrap();
				let handle = handle.clone();
				let arg = arg.clone();
				join_set.spawn(async move {
					let _permit = permit;
					let children = value.children_with_arg_with_handle(&handle, arg).await?;
					Ok((children, depth))
				});
			}
			if let Some(result) = join_set.join_next().await {
				let (children, depth): (Vec<Self>, Option<u64>) = result
					.map_err(|error| tg::error!(!error, "the load task panicked"))
					.and_then(|result| result)?;
				for child in children {
					queue.push_back((child, depth));
				}
			}
		}

		Ok(())
	}
}
