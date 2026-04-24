use {
	std::{
		net::Ipv4Addr,
		sync::{Arc, Mutex},
	},
	tangram_client::prelude::*,
};

pub struct Network {
	inner: Arc<Mutex<Inner>>,
}

struct Inner {
	blocks: Vec<u32>,
	max: u32,
	next: u32,
}

pub struct Ip {
	pub addr: Ipv4Addr,
	_block: Arc<Block>,
}

struct Block {
	base: u32,
	inner: Arc<Mutex<Inner>>,
}

impl Network {
	pub fn new(min: u32, max: u32) -> Self {
		let next = (min + 3) & !3;
		Self {
			inner: Arc::new(Mutex::new(Inner {
				blocks: Vec::new(),
				max,
				next,
			})),
		}
	}

	pub fn try_reserve(&self) -> tg::Result<Ip> {
		let block = Arc::new(self.reserve_block()?);
		let addr = Ipv4Addr::from_bits(block.base + 1);
		Ok(Ip {
			addr,
			_block: block,
		})
	}

	pub fn try_reserve_pair(&self) -> tg::Result<(Ip, Ip)> {
		let block = Arc::new(self.reserve_block()?);
		let host = Ip {
			addr: Ipv4Addr::from_bits(block.base + 1),
			_block: Arc::clone(&block),
		};
		let guest = Ip {
			addr: Ipv4Addr::from_bits(block.base + 2),
			_block: block,
		};
		Ok((host, guest))
	}

	fn reserve_block(&self) -> tg::Result<Block> {
		let mut inner = self.inner.lock().unwrap();
		if let Some(base) = inner.blocks.pop() {
			return Ok(Block {
				base,
				inner: Arc::clone(&self.inner),
			});
		}
		let next = inner.next;
		let end = next
			.checked_add(4)
			.ok_or_else(|| tg::error!("out of ip addresses"))?;
		if end > inner.max {
			return Err(tg::error!("out of ip addresses"));
		}
		inner.next = end;
		Ok(Block {
			base: next,
			inner: Arc::clone(&self.inner),
		})
	}
}

impl Drop for Block {
	fn drop(&mut self) {
		if let Ok(mut inner) = self.inner.try_lock() {
			inner.blocks.push(self.base);
		}
	}
}
