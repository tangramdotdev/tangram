use {
	std::{
		net::Ipv4Addr,
		sync::{Arc, Mutex},
	},
	tangram_client::prelude::*,
};

pub struct Network {
	inner: Arc<Mutex<Inner>>,
	ignored_ifaces: Vec<String>,
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
	pub fn new(min: u32, max: u32, ignored_ifaces: Vec<String>) -> Self {
		let next = (min + 3) & !3;
		Self {
			inner: Arc::new(Mutex::new(Inner {
				blocks: Vec::new(),
				max,
				next,
			})),
			ignored_ifaces,
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
		let used = host_used_ranges(&self.ignored_ifaces);
		loop {
			let mut inner = self.inner.lock().unwrap();
			if let Some(base) = inner.blocks.pop() {
				let overlaps = used
					.iter()
					.any(|&(start, last)| base <= last && base + 3 >= start);
				if !overlaps {
					return Ok(Block {
						base,
						inner: Arc::clone(&self.inner),
					});
				}
				continue;
			}
			let next = inner.next;
			let end = next
				.checked_add(4)
				.ok_or_else(|| tg::error!("out of ip addresses"))?;
			if end - 1 > inner.max {
				return Err(tg::error!("out of ip addresses"));
			}
			let conflict_end = used
				.iter()
				.filter_map(|&(start, last)| (next <= last && end > start).then_some(last))
				.max();
			if let Some(conflict_end) = conflict_end {
				let advanced = conflict_end
					.checked_add(1)
					.ok_or_else(|| tg::error!("out of ip addresses"))?;
				let aligned = advanced
					.checked_add(3)
					.ok_or_else(|| tg::error!("out of ip addresses"))?
					& !3;
				inner.next = aligned;
				continue;
			}
			inner.next = end;
			return Ok(Block {
				base: next,
				inner: Arc::clone(&self.inner),
			});
		}
	}
}

#[cfg(target_os = "linux")]
fn host_used_ranges(ignored: &[String]) -> Vec<(u32, u32)> {
	let is_ignored = |name: &str| ignored.iter().any(|s| s == name);
	let mut ranges = Vec::new();
	if let Ok(content) = std::fs::read_to_string("/proc/net/route") {
		for line in content.lines().skip(1) {
			let mut fields = line.split_whitespace();
			let (
				Some(iface),
				Some(dest),
				Some(_gw),
				Some(_flags),
				Some(_refcnt),
				Some(_use),
				Some(_metric),
				Some(mask),
			) = (
				fields.next(),
				fields.next(),
				fields.next(),
				fields.next(),
				fields.next(),
				fields.next(),
				fields.next(),
				fields.next(),
			)
			else {
				continue;
			};
			if is_ignored(iface) {
				continue;
			}
			let (Ok(dest), Ok(mask)) =
				(u32::from_str_radix(dest, 16), u32::from_str_radix(mask, 16))
			else {
				continue;
			};
			let dest = u32::from_be_bytes(dest.to_ne_bytes());
			let mask = u32::from_be_bytes(mask.to_ne_bytes());
			if mask == 0 {
				continue;
			}
			let start = dest & mask;
			let end = start | !mask;
			ranges.push((start, end));
		}
	}
	unsafe {
		let mut ifaddrs: *mut libc::ifaddrs = std::ptr::null_mut();
		if libc::getifaddrs(&raw mut ifaddrs) == 0 {
			let mut current = ifaddrs;
			while !current.is_null() {
				let entry = &*current;
				if entry.ifa_addr.is_null()
					|| i32::from((*entry.ifa_addr).sa_family) != libc::AF_INET
				{
					current = entry.ifa_next;
					continue;
				}
				let ignored_match = !entry.ifa_name.is_null()
					&& std::ffi::CStr::from_ptr(entry.ifa_name)
						.to_str()
						.is_ok_and(is_ignored);
				if !ignored_match {
					#[allow(clippy::cast_ptr_alignment)]
					let addr = entry.ifa_addr.cast::<libc::sockaddr_in>();
					if !addr.is_aligned() {
						tracing::warn!("misaligned ifa_addr");
						continue;
					}
					let addr = &*addr;
					let addr_bits = u32::from_be(addr.sin_addr.s_addr);
					let mask_bits = if entry.ifa_netmask.is_null() {
						u32::MAX
					} else {
						#[allow(clippy::cast_ptr_alignment)]
						let netmask = entry.ifa_netmask.cast::<libc::sockaddr_in>();
						if !netmask.is_aligned() {
							tracing::warn!("misaligned ifa_netmask");
							continue;
						}
						let m = &*netmask;
						u32::from_be(m.sin_addr.s_addr)
					};
					let start = addr_bits & mask_bits;
					let end = start | !mask_bits;
					ranges.push((start, end));
				}
				current = entry.ifa_next;
			}
			libc::freeifaddrs(ifaddrs);
		}
	}
	ranges
}

#[cfg(not(target_os = "linux"))]
fn host_used_ranges(_ignored: &[String]) -> Vec<(u32, u32)> {
	Vec::new()
}

impl Drop for Block {
	fn drop(&mut self) {
		if let Ok(mut inner) = self.inner.lock() {
			inner.blocks.push(self.base);
		}
	}
}
