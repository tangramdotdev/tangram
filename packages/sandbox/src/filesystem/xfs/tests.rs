use super::*;

#[test]
fn quota_preserves_the_linux_abi() {
	assert_eq!(std::mem::size_of::<XfsDiskQuota>(), 112);
	assert_eq!(std::mem::offset_of!(XfsDiskQuota, version), 0);
	assert_eq!(std::mem::offset_of!(XfsDiskQuota, fieldmask), 2);
	assert_eq!(std::mem::offset_of!(XfsDiskQuota, id), 4);
	assert_eq!(std::mem::offset_of!(XfsDiskQuota, block_hard), 8);
}
