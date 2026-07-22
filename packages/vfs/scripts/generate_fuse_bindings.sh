#!/usr/bin/env bash

set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
header=${FUSE_HEADER:-/usr/include/linux/fuse.h}
output="$root/packages/vfs/src/fuse/sys.rs"

if [[ $(bindgen --version) != "bindgen 0.72.1" ]]; then
	echo "bindgen 0.72.1 is required" >&2
	exit 1
fi

types=(
	fuse_attr
	fuse_attr_out
	fuse_backing_map
	fuse_batch_forget_in
	fuse_entry_out
	fuse_flush_in
	fuse_forget_in
	fuse_forget_one
	fuse_getattr_in
	fuse_getxattr_in
	fuse_getxattr_out
	fuse_in_header
	fuse_init_in
	fuse_init_out
	fuse_interrupt_in
	fuse_kstatfs
	fuse_open_in
	fuse_open_out
	fuse_opcode
	fuse_out_header
	fuse_read_in
	fuse_release_in
	fuse_statfs_out
	fuse_statx
	fuse_statx_in
	fuse_statx_out
	fuse_sx_time
	fuse_uring_cmd
	fuse_uring_cmd_req
	fuse_uring_ent_in_out
	fuse_uring_req_header
)

variables=(
	FOPEN_CACHE_DIR
	FOPEN_KEEP_CACHE
	FOPEN_NOFLUSH
	FOPEN_PASSTHROUGH
	FUSE_ASYNC_READ
	FUSE_CACHE_SYMLINKS
	FUSE_COMPAT_22_INIT_OUT_SIZE
	FUSE_COMPAT_INIT_OUT_SIZE
	FUSE_DO_READDIRPLUS
	FUSE_INIT_EXT
	FUSE_KERNEL_MINOR_VERSION
	FUSE_KERNEL_VERSION
	FUSE_MAP_ALIGNMENT
	FUSE_MAX_PAGES
	FUSE_NO_OPENDIR_SUPPORT
	FUSE_OVER_IO_URING
	FUSE_PARALLEL_DIROPS
	FUSE_PASSTHROUGH
	FUSE_READDIRPLUS_AUTO
	FUSE_SPLICE_MOVE
	FUSE_SPLICE_READ
	fuse_opcode_FUSE_BATCH_FORGET
	fuse_opcode_FUSE_DESTROY
	fuse_opcode_FUSE_FLUSH
	fuse_opcode_FUSE_FORGET
	fuse_opcode_FUSE_GETATTR
	fuse_opcode_FUSE_GETXATTR
	fuse_opcode_FUSE_INIT
	fuse_opcode_FUSE_INTERRUPT
	fuse_opcode_FUSE_LISTXATTR
	fuse_opcode_FUSE_LOOKUP
	fuse_opcode_FUSE_OPEN
	fuse_opcode_FUSE_OPENDIR
	fuse_opcode_FUSE_READ
	fuse_opcode_FUSE_READDIR
	fuse_opcode_FUSE_READDIRPLUS
	fuse_opcode_FUSE_READLINK
	fuse_opcode_FUSE_RELEASE
	fuse_opcode_FUSE_RELEASEDIR
	fuse_opcode_FUSE_STATFS
	fuse_opcode_FUSE_STATX
	fuse_uring_cmd_FUSE_IO_URING_CMD_COMMIT_AND_FETCH
	fuse_uring_cmd_FUSE_IO_URING_CMD_REGISTER
)

args=(
	"$header"
	--formatter none
	--generate types,vars
	--no-doc-comments
	--no-layout-tests
	--raw-line '#![allow(clippy::pub_underscore_fields, clippy::unreadable_literal, non_camel_case_types, non_upper_case_globals)]'
	--with-derive-custom-struct '^fuse_.*=zerocopy::FromBytes,zerocopy::Immutable,zerocopy::IntoBytes'
)
for type in "${types[@]}"; do
	args+=(--allowlist-type "^${type}$")
done
for variable in "${variables[@]}"; do
	args+=(--allowlist-var "^${variable}$")
done

bindgen "${args[@]}" --output "$output"
rustfmt --config-path "$root/rustfmt.toml" "$output"
