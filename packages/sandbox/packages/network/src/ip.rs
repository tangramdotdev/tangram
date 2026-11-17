use {
	std::{
		ffi::CStr,
		net::Ipv4Addr,
		os::unix::fs::{DirBuilderExt as _, OpenOptionsExt as _},
		sync::{Arc, Mutex},
	},
	tangram_client::prelude::*,
};

const HOST_LOCK_DIRECTORY: &str = "/run/tangram/network/ip";

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

pub struct Lease {
	pub addr: Ipv4Addr,
	#[expect(dead_code)]
	block: Arc<Block>,
}

struct Block {
	base: u32,

	/// The lock of the block's addresses across the host, which keeps another server that shares the bridge from leasing them.
	_lock: Option<std::fs::File>,

	range: usize,
	state: Arc<Mutex<State>>,
}

/// The result of an attempt to lock a block's addresses across the host.
enum HostLock {
	/// Another process holds the block.
	Held,

	/// This process holds the block, or the host has no lock directory that this process can use.
	Locked(Option<std::fs::File>),
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

	pub fn try_reserve_in(&self, min: Ipv4Addr, max: Ipv4Addr) -> tg::Result<Lease> {
		let block = Arc::new(self.reserve_block(Some((min.to_bits(), max.to_bits())))?);
		let addr = Ipv4Addr::from_bits(block.base + 1);
		Ok(Lease { addr, block })
	}

	pub fn try_reserve_pair(&self) -> tg::Result<(Lease, Lease)> {
		let block = Arc::new(self.reserve_block(None)?);
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

	fn reserve_block(&self, bounds: Option<(u32, u32)>) -> tg::Result<Block> {
		let used = host_used_ranges();
		let mut state = self.state.lock().unwrap();
		if state.ranges.is_empty() {
			return Err(tg::error!("no networks are configured"));
		}

		// Collect the blocks that another server holds, and return them to the pool after the search, so that they are tried again later.
		let mut held = Vec::new();
		for index in 0..state.ranges.len() {
			loop {
				let range = &mut state.ranges[index];
				if let Some(position) = range
					.blocks
					.iter()
					.rposition(|&base| block_fits_bounds(base, bounds))
				{
					let base = range.blocks.swap_remove(position);
					let overlaps = used
						.iter()
						.any(|&(start, last)| base <= last && base + 3 >= start);
					if overlaps {
						continue;
					}
					match lock_host_block(base) {
						HostLock::Held => {
							held.push((index, base));
							continue;
						},
						HostLock::Locked(lock) => {
							restore_held_blocks(&mut state, held);
							return Ok(Block {
								_lock: lock,
								base,
								range: index,
								state: Arc::clone(&self.state),
							});
						},
					}
				}
				let mut next = range.next;
				if let Some((min, _)) = bounds
					&& next < min
				{
					next = min
						.checked_add(3)
						.ok_or_else(|| tg::error!("out of ip addresses"))?
						& !3;
				}
				let end = next
					.checked_add(4)
					.ok_or_else(|| tg::error!("out of ip addresses"))?;
				if end - 1 > range.max {
					break;
				}
				if !block_fits_bounds(next, bounds) {
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
				let HostLock::Locked(lock) = lock_host_block(next) else {
					held.push((index, next));
					continue;
				};
				restore_held_blocks(&mut state, held);
				return Ok(Block {
					_lock: lock,
					base: next,
					range: index,
					state: Arc::clone(&self.state),
				});
			}
		}
		restore_held_blocks(&mut state, held);
		Err(tg::error!("out of ip addresses"))
	}
}

/// Lock a block's addresses across the host with a lock file, which the kernel releases when the process exits, so that servers that share the bridge do not lease the same addresses. Use no lock if the process cannot create the lock directory, such as when it is not root, in which case it does not use the shared bridge.
fn lock_host_block(base: u32) -> HostLock {
	let directory = std::path::Path::new(HOST_LOCK_DIRECTORY);
	if std::fs::DirBuilder::new()
		.recursive(true)
		.mode(0o755)
		.create(directory)
		.is_err()
	{
		return HostLock::Locked(None);
	}
	let path = directory.join(Ipv4Addr::from_bits(base).to_string());
	let Ok(file) = std::fs::OpenOptions::new()
		.create(true)
		.mode(0o600)
		.truncate(false)
		.write(true)
		.open(&path)
	else {
		return HostLock::Locked(None);
	};
	match file.try_lock() {
		Ok(()) => HostLock::Locked(Some(file)),
		Err(std::fs::TryLockError::WouldBlock) => HostLock::Held,
		Err(std::fs::TryLockError::Error(_)) => HostLock::Locked(None),
	}
}

/// Return the blocks that another server held during a search to the pool.
fn restore_held_blocks(state: &mut State, held: Vec<(usize, u32)>) {
	for (index, base) in held {
		state.ranges[index].blocks.push(base);
	}
}

fn block_fits_bounds(base: u32, bounds: Option<(u32, u32)>) -> bool {
	let Some((min, max)) = bounds else {
		return true;
	};
	base >= min && base.checked_add(3).is_some_and(|end| end <= max)
}

fn host_used_ranges() -> Vec<(u32, u32)> {
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
