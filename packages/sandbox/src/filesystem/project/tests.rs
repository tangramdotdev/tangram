use super::*;

#[test]
fn attributes_preserve_the_linux_abi() {
	assert_eq!(std::mem::size_of::<FsXAttr>(), 28);
	assert_eq!(std::mem::offset_of!(FsXAttr, xflags), 0);
	assert_eq!(std::mem::offset_of!(FsXAttr, project), 12);
}
