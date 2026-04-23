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
	ips: Vec<u32>,
	max: u32,
	next: u32,
}

pub struct Ip {
	pub addr: Ipv4Addr,
	inner: Arc<Mutex<Inner>>,
}

impl Network {
	pub fn new(min: u32, max: u32) -> Self {
		Self {
			inner: Arc::new(Mutex::new(Inner {
				ips: Vec::new(),
				max,
				next: min,
			})),
		}
	}

	pub fn try_reserve(&self) -> tg::Result<Ip> {
		let mut inner = self.inner.lock().unwrap();
		if let Some(next) = inner.ips.pop() {
			return Ok(Ip {
				addr: Ipv4Addr::from_bits(next),
				inner: Arc::clone(&self.inner),
			});
		}
		if inner.next == inner.max {
			return Err(tg::error!("out of ip addresses"));
		}
		let next = inner.next;
		inner.next += 1;
		Ok(Ip {
			addr: Ipv4Addr::from_bits(next),
			inner: Arc::clone(&self.inner),
		})
	}
}

impl Drop for Ip {
	fn drop(&mut self) {
		if let Ok(mut inner) = self.inner.try_lock() {
			inner.ips.push(self.addr.to_bits());
		}
	}
}
