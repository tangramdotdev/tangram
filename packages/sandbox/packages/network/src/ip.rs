use {
	std::{
		collections::{BTreeMap, BTreeSet},
		ffi::CStr,
		net::Ipv4Addr,
		path::PathBuf,
		sync::{Arc, Mutex, Weak},
	},
	tangram_client::prelude::*,
};

/// The number of addresses in a block, which a pool locks across the host before it leases addresses in the block.
const BLOCK_SIZE: u32 = 256;

/// The number of addresses in a slot, which is the unit that a pool leases.
const SLOT_SIZE: u32 = 4;

const _: () = assert!(BLOCK_SIZE / SLOT_SIZE <= u64::BITS);

#[derive(Clone, Debug)]
pub struct Pool {
	state: Arc<Mutex<State>>,
}

#[derive(Debug)]
struct State {
	blocks: BTreeMap<u32, Weak<Block>>,
	directory: Option<PathBuf>,
	ranges: Vec<(u32, u32)>,
}

pub struct Lease {
	pub addr: Ipv4Addr,
	#[expect(dead_code)]
	slot: Arc<Slot>,
}

struct Slot {
	base: u32,
	block: Arc<Block>,
	index: u32,
}

struct Block {
	free: Mutex<u64>,
	_lock: Option<std::fs::File>,
}

impl Pool {
	/// Create a pool which, when the server runs as root, locks its blocks across the host, so that servers on the same host lease different addresses.
	#[must_use]
	pub fn new(ranges: impl IntoIterator<Item = (u32, u32)>) -> Self {
		let directory = super::root().then(|| PathBuf::from(crate::lock::DIRECTORY).join("ip"));
		Self::with_directory(ranges, directory)
	}

	/// Create a pool which locks its blocks with files in a directory, so that pools in other processes that use the same directory lease different addresses.
	#[must_use]
	pub fn with_directory(
		ranges: impl IntoIterator<Item = (u32, u32)>,
		directory: Option<PathBuf>,
	) -> Self {
		let blocks = BTreeMap::new();
		let ranges = ranges.into_iter().collect();
		let state = State {
			blocks,
			directory,
			ranges,
		};
		Self {
			state: Arc::new(Mutex::new(state)),
		}
	}

	pub fn try_reserve_in(&self, min: Ipv4Addr, max: Ipv4Addr) -> tg::Result<Lease> {
		let slot = Arc::new(self.reserve_slot(Some((min.to_bits(), max.to_bits())))?);
		let addr = Ipv4Addr::from_bits(slot.base + 1);
		Ok(Lease { addr, slot })
	}

	pub fn try_reserve_pair(&self) -> tg::Result<(Lease, Lease)> {
		let slot = Arc::new(self.reserve_slot(None)?);
		let host = Lease {
			addr: Ipv4Addr::from_bits(slot.base + 1),
			slot: Arc::clone(&slot),
		};
		let guest = Lease {
			addr: Ipv4Addr::from_bits(slot.base + 2),
			slot,
		};
		Ok((host, guest))
	}

	fn reserve_slot(&self, bounds: Option<(u32, u32)>) -> tg::Result<Slot> {
		// Get the address ranges that the host uses.
		let used = host_used_ranges();

		let mut state = self.state.lock().unwrap();
		if state.ranges.is_empty() {
			return Err(tg::error!("no networks are configured"));
		}

		// Lease a free slot in a block that this pool holds.
		state.blocks.retain(|_, block| block.strong_count() > 0);
		for (&base, block) in &state.blocks {
			let Some(block) = block.upgrade() else {
				continue;
			};
			let mask = state.available(base, bounds, &used);
			if let Some(index) = block.take(mask) {
				let base = base + index * SLOT_SIZE;
				let slot = Slot { base, block, index };
				return Ok(slot);
			}
		}

		// Lock a block that no other pool holds, and lease a slot in it.
		for base in state.bases() {
			if state.blocks.contains_key(&base) {
				continue;
			}
			let mask = state.available(base, bounds, &used);
			if mask == 0 {
				continue;
			}
			let lock = match &state.directory {
				Some(directory) => {
					let name = format!(
						"{}-{}.lock",
						Ipv4Addr::from_bits(base),
						32 - BLOCK_SIZE.trailing_zeros()
					);
					let Some(lock) = crate::lock::try_lock(&directory.join(name))? else {
						continue;
					};
					Some(lock)
				},
				None => None,
			};
			let free = Mutex::new(state.initial(base));
			let block = Arc::new(Block { free, _lock: lock });
			let index = block
				.take(mask)
				.ok_or_else(|| tg::error!("expected a free slot in a new block"))?;
			state.blocks.insert(base, Arc::downgrade(&block));
			let base = base + index * SLOT_SIZE;
			let slot = Slot { base, block, index };
			return Ok(slot);
		}

		Err(tg::error!("out of ip addresses"))
	}
}

impl State {
	/// Get the bases of the blocks that overlap the configured ranges, in order.
	fn bases(&self) -> BTreeSet<u32> {
		let mut bases = BTreeSet::new();
		for &(min, max) in &self.ranges {
			let mut base = u64::from(min & !(BLOCK_SIZE - 1));
			while base <= u64::from(max) {
				bases.insert(u32::try_from(base).unwrap());
				base += u64::from(BLOCK_SIZE);
			}
		}
		bases
	}

	/// Get the slots of a block that are within the configured ranges.
	fn initial(&self, base: u32) -> u64 {
		self.ranges
			.iter()
			.fold(0, |mask, &(min, max)| mask | inside_mask(base, min, max))
	}

	/// Get the slots of a block that are within the configured ranges and the bounds, and that do not overlap an address range that the host uses.
	fn available(&self, base: u32, bounds: Option<(u32, u32)>, used: &[(u32, u32)]) -> u64 {
		let bounds = bounds.map_or(u64::MAX, |(min, max)| inside_mask(base, min, max));
		let used = used.iter().fold(0, |mask, &(start, last)| {
			mask | overlap_mask(base, start, last)
		});
		self.initial(base) & bounds & !used
	}
}

impl Block {
	/// Take the lowest free slot in a mask.
	fn take(&self, mask: u64) -> Option<u32> {
		let mut free = self.free.lock().unwrap();
		let available = *free & mask;
		if available == 0 {
			return None;
		}
		let index = available.trailing_zeros();
		*free &= !(1 << index);
		Some(index)
	}
}

impl Drop for Slot {
	fn drop(&mut self) {
		if let Ok(mut free) = self.block.free.lock() {
			*free |= 1 << self.index;
		}
	}
}

/// Get the slots of a block whose addresses are all within a range.
fn inside_mask(base: u32, min: u32, max: u32) -> u64 {
	let last = base + (BLOCK_SIZE - 1);
	if max < base || min > last {
		return 0;
	}
	let start = (min.max(base) - base).div_ceil(SLOT_SIZE);
	let end = (max.min(last) - base + 1) / SLOT_SIZE;
	slot_mask(start, end)
}

/// Get the slots of a block that have an address within a range.
fn overlap_mask(base: u32, min: u32, max: u32) -> u64 {
	let last = base + (BLOCK_SIZE - 1);
	if max < base || min > last {
		return 0;
	}
	let start = (min.max(base) - base) / SLOT_SIZE;
	let end = (max.min(last) - base) / SLOT_SIZE + 1;
	slot_mask(start, end)
}

/// Get a mask of the slots from a start index to an exclusive end index.
fn slot_mask(start: u32, end: u32) -> u64 {
	if start >= end {
		return 0;
	}
	let width = end - start;
	let ones = if width >= u64::BITS {
		u64::MAX
	} else {
		(1 << width) - 1
	};
	ones << start
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

#[cfg(test)]
mod tests {
	use super::*;

	struct Directory(PathBuf);

	impl Directory {
		fn new() -> Self {
			let name = format!("tangram-network-ip-{:016x}", rand::random::<u64>());
			Self(std::env::temp_dir().join(name))
		}
	}

	impl Drop for Directory {
		fn drop(&mut self) {
			std::fs::remove_dir_all(&self.0).ok();
		}
	}

	fn addr(value: &str) -> Ipv4Addr {
		value.parse().unwrap()
	}

	fn bits(value: &str) -> u32 {
		addr(value).to_bits()
	}

	fn reserve(pool: &Pool) -> tg::Result<Lease> {
		pool.try_reserve_in(addr("198.51.100.0"), addr("198.51.101.255"))
	}

	#[test]
	fn a_released_slot_is_reused() {
		let pool = Pool::with_directory([(bits("198.51.100.4"), bits("198.51.100.255"))], None);
		let first = reserve(&pool).unwrap();
		let second = reserve(&pool).unwrap();
		assert_eq!(first.addr, addr("198.51.100.5"));
		assert_eq!(second.addr, addr("198.51.100.9"));
		drop(first);
		let third = reserve(&pool).unwrap();
		assert_eq!(third.addr, addr("198.51.100.5"));
	}

	#[test]
	fn a_pair_shares_one_slot() {
		let pool = Pool::with_directory([(bits("198.51.100.4"), bits("198.51.100.255"))], None);
		let (host, guest) = pool.try_reserve_pair().unwrap();
		assert_eq!(host.addr, addr("198.51.100.5"));
		assert_eq!(guest.addr, addr("198.51.100.6"));
		let (host, guest) = pool.try_reserve_pair().unwrap();
		assert_eq!(host.addr, addr("198.51.100.9"));
		assert_eq!(guest.addr, addr("198.51.100.10"));
	}

	#[test]
	fn partial_slots_are_excluded() {
		let pool = Pool::with_directory([(bits("198.51.100.4"), bits("198.51.100.10"))], None);
		let lease = reserve(&pool).unwrap();
		assert_eq!(lease.addr, addr("198.51.100.5"));
		let error = reserve(&pool).err().unwrap();
		assert!(error.to_string().contains("out of ip addresses"));
	}

	#[test]
	fn bounds_restrict_the_slots() {
		let pool = Pool::with_directory([(bits("198.51.100.0"), bits("198.51.101.255"))], None);
		let lease = pool
			.try_reserve_in(addr("198.51.100.130"), addr("198.51.100.138"))
			.unwrap();
		assert_eq!(lease.addr, addr("198.51.100.133"));
		let error = pool
			.try_reserve_in(addr("198.51.100.130"), addr("198.51.100.138"))
			.err()
			.unwrap();
		assert!(error.to_string().contains("out of ip addresses"));
	}

	#[test]
	fn a_full_block_extends_to_the_next_block() {
		let pool = Pool::with_directory([(bits("198.51.100.0"), bits("198.51.101.255"))], None);
		let leases = (0..64).map(|_| reserve(&pool).unwrap()).collect::<Vec<_>>();
		assert_eq!(leases.last().unwrap().addr, addr("198.51.100.253"));
		let lease = reserve(&pool).unwrap();
		assert_eq!(lease.addr, addr("198.51.101.1"));
	}

	#[test]
	fn pools_that_share_a_directory_lease_from_different_blocks() {
		let directory = Directory::new();
		let ranges = [(bits("198.51.100.0"), bits("198.51.101.255"))];
		let a = Pool::with_directory(ranges, Some(directory.0.clone()));
		let b = Pool::with_directory(ranges, Some(directory.0.clone()));
		let first = reserve(&a).unwrap();
		let second = reserve(&b).unwrap();
		let third = reserve(&a).unwrap();
		assert_eq!(first.addr, addr("198.51.100.1"));
		assert_eq!(second.addr, addr("198.51.101.1"));
		assert_eq!(third.addr, addr("198.51.100.5"));
		let error = reserve(&Pool::with_directory(ranges, Some(directory.0.clone())))
			.err()
			.unwrap();
		assert!(error.to_string().contains("out of ip addresses"));
	}

	#[test]
	fn a_block_is_held_until_its_last_lease_is_released() {
		let directory = Directory::new();
		let ranges = [(bits("198.51.100.0"), bits("198.51.100.255"))];
		let a = Pool::with_directory(ranges, Some(directory.0.clone()));
		let b = Pool::with_directory(ranges, Some(directory.0.clone()));
		let (host, guest) = a.try_reserve_pair().unwrap();
		let lease = reserve(&a).unwrap();
		assert!(reserve(&b).is_err());
		drop(host);
		drop(lease);
		assert!(reserve(&b).is_err());
		drop(guest);
		let lease = reserve(&b).unwrap();
		assert_eq!(lease.addr, addr("198.51.100.1"));
		assert!(reserve(&a).is_err());
	}

	#[test]
	fn masks_cover_the_expected_slots() {
		let base = bits("198.51.100.0");
		assert_eq!(inside_mask(base, base, base + 255), u64::MAX);
		assert_eq!(inside_mask(base, base + 4, base + 10), 0b10);
		assert_eq!(inside_mask(base, base + 1, base + 6), 0);
		assert_eq!(inside_mask(base, base + 256, base + 511), 0);
		assert_eq!(overlap_mask(base, base + 3, base + 4), 0b11);
		assert_eq!(overlap_mask(base, base - 10, base), 0b1);
		assert_eq!(overlap_mask(base, base + 252, base + 300), 1 << 63);
	}
}
