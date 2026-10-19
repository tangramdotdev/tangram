use {
	std::{io::Read as _, os::unix::fs::MetadataExt as _, path::Path},
	tangram_client::prelude::*,
};

struct Entry {
	address: u64,
	header: [u8; size_of::<libc::Elf64_Ehdr>()],
	headers: *const libc::Elf64_Phdr,
	offset: Option<u64>,
}

pub(crate) fn entrypoint(path: &Path) -> tg::Result<Option<u64>> {
	// Only use the running image's entrypoint when the source is still that image.
	let mut file = std::fs::File::open(path)
		.map_err(|error| tg::error!(!error, "failed to open the tangram executable"))?;
	let source = file
		.metadata()
		.map_err(|error| tg::error!(!error, "failed to stat the tangram executable"))?;
	let current = std::fs::metadata("/proc/self/exe")
		.map_err(|error| tg::error!(!error, "failed to stat the running executable"))?;
	if source.dev() != current.dev() || source.ino() != current.ino() {
		return Ok(None);
	}

	// An embedded wrapper can replace the file entrypoint while restoring the auxiliary vector before starting Tangram.
	let mut header = [0; size_of::<libc::Elf64_Ehdr>()];
	file.read_exact(&mut header)
		.map_err(|error| tg::error!(!error, "failed to read the executable header"))?;
	if &header[..6] != b"\x7fELF\x02\x01" {
		return Err(tg::error!("expected a little-endian ELF64 executable"));
	}
	let offset = std::mem::offset_of!(libc::Elf64_Ehdr, e_entry);
	let stored = u64::from_le_bytes(header[offset..offset + 8].try_into().unwrap());
	// SAFETY: These auxiliary vector entries contain values and require no caller-provided storage.
	let (address, headers) = unsafe {
		(
			libc::getauxval(libc::AT_ENTRY),
			libc::getauxval(libc::AT_PHDR),
		)
	};
	let mut entry = Entry {
		address,
		header,
		headers: headers as *const libc::Elf64_Phdr,
		offset: None,
	};
	// SAFETY: The callback runs synchronously and receives a pointer to entry, which remains valid for the call.
	unsafe {
		libc::dl_iterate_phdr(Some(callback), std::ptr::from_mut(&mut entry).cast());
	}
	let entrypoint = entry.offset.filter(|offset| *offset != stored);

	Ok(entrypoint)
}

unsafe extern "C" fn callback(
	info: *mut libc::dl_phdr_info,
	_size: usize,
	data: *mut libc::c_void,
) -> libc::c_int {
	// SAFETY: dl_iterate_phdr provides valid program header information, and data points to the caller's Entry.
	let (info, entry) = unsafe { (&*info, &mut *data.cast::<Entry>()) };
	if info.dlpi_phdr != entry.headers {
		return 0;
	}
	// Explicit loader invocation makes /proc/self/exe identify the loader, so verify that the file header belongs to the main image.
	// SAFETY: The loader provides dlpi_phnum valid program headers for the duration of this callback.
	let headers =
		unsafe { std::slice::from_raw_parts(info.dlpi_phdr, usize::from(info.dlpi_phnum)) };
	let segment = headers.iter().find(|header| {
		header.p_type == libc::PT_LOAD
			&& header.p_offset == 0
			&& header.p_filesz >= entry.header.len() as u64
			&& header.p_flags & libc::PF_R != 0
	});
	let Some(segment) = segment else {
		return 1;
	};
	let address = (info.dlpi_addr + segment.p_vaddr) as *const u8;
	// SAFETY: The readable PT_LOAD segment contains at least one complete ELF header.
	let header = unsafe { std::slice::from_raw_parts(address, entry.header.len()) };
	if header != entry.header {
		return 1;
	}
	entry.offset = entry.address.checked_sub(info.dlpi_addr);
	1
}
