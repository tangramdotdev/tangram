use {
	std::{
		ffi::CStr,
		net::Ipv4Addr,
		sync::{Arc, Mutex},
	},
	tangram_client::prelude::*,
};

#[derive(Clone, Debug)]
pub struct Pool {
	state: Arc<Mutex<State>>,
}

#[derive(Debug)]
struct State {
	ranges: Vec<Range>,
}

#[derive(Debug)]
struct Range {
	blocks: Vec<u32>,
	max: u32,
	next: u32,
}

pub(crate) struct Lease {
	pub(crate) addr: Ipv4Addr,
	#[expect(dead_code)]
	block: Arc<Block>,
}

struct Block {
	base: u32,
	range: usize,
	state: Arc<Mutex<State>>,
}

impl Pool {
	#[must_use]
	pub fn new(ranges: impl IntoIterator<Item = (u32, u32)>) -> Self {
		let ranges = ranges
			.into_iter()
			.map(|(min, max)| Range {
				blocks: Vec::new(),
				max,
				next: (min + 3) & !3,
			})
			.collect();
		Self {
			state: Arc::new(Mutex::new(State { ranges })),
		}
	}

	pub(crate) fn try_reserve(&self) -> tg::Result<Lease> {
		let block = Arc::new(self.reserve_block()?);
		let addr = Ipv4Addr::from_bits(block.base + 1);
		Ok(Lease { addr, block })
	}

	pub(crate) fn try_reserve_pair(&self) -> tg::Result<(Lease, Lease)> {
		let block = Arc::new(self.reserve_block()?);
		let host = Lease {
			addr: Ipv4Addr::from_bits(block.base + 1),
			block: Arc::clone(&block),
		};
		let guest = Lease {
			addr: Ipv4Addr::from_bits(block.base + 2),
			block,
		};
		Ok((host, guest))
	}

	fn reserve_block(&self) -> tg::Result<Block> {
		let used = host_used_ranges();
		let mut state = self.state.lock().unwrap();
		if state.ranges.is_empty() {
			return Err(tg::error!("no networks are configured"));
		}
		for index in 0..state.ranges.len() {
			loop {
				let range = &mut state.ranges[index];
				if let Some(base) = range.blocks.pop() {
					let overlaps = used
						.iter()
						.any(|&(start, last)| base <= last && base + 3 >= start);
					if !overlaps {
						return Ok(Block {
							base,
							range: index,
							state: Arc::clone(&self.state),
						});
					}
					continue;
				}
				let next = range.next;
				let end = next
					.checked_add(4)
					.ok_or_else(|| tg::error!("out of ip addresses"))?;
				if end - 1 > range.max {
					break;
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
					range.next = aligned;
					continue;
				}
				range.next = end;
				return Ok(Block {
					base: next,
					range: index,
					state: Arc::clone(&self.state),
				});
			}
		}
		Err(tg::error!("out of ip addresses"))
	}
}

fn host_used_ranges() -> Vec<(u32, u32)> {
	#[cfg(target_os = "linux")]
	{
		host_used_ranges_linux()
	}
	#[cfg(target_os = "macos")]
	{
		Vec::new()
	}
}

#[cfg(target_os = "linux")]
fn host_used_ranges_linux() -> Vec<(u32, u32)> {
	let ignore_bridge = super::root();
	let is_ignored = |name: &str| ignore_bridge && name == super::veth::BRIDGE_NAME;
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
					&& CStr::from_ptr(entry.ifa_name)
						.to_str()
						.is_ok_and(is_ignored);
				if !ignored_match {
					#[allow(clippy::cast_ptr_alignment)]
					let addr = entry.ifa_addr.cast::<libc::sockaddr_in>();
					if !addr.is_aligned() {
						tracing::warn!("misaligned ifa_addr");
						current = entry.ifa_next;
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
							current = entry.ifa_next;
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

impl Drop for Block {
	fn drop(&mut self) {
		if let Ok(mut state) = self.state.lock()
			&& let Some(range) = state.ranges.get_mut(self.range)
		{
			range.blocks.push(self.base);
		}
	}
}
