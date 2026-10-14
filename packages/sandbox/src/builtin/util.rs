use std::ffi::CString;

pub struct CStringVec {
	_strings: Vec<CString>,
	pointers: Vec<*const libc::c_char>,
}

impl CStringVec {
	pub fn as_ptr(&self) -> *const *const libc::c_char {
		self.pointers.as_ptr()
	}
}

impl FromIterator<CString> for CStringVec {
	fn from_iter<T: IntoIterator<Item = CString>>(iter: T) -> Self {
		let mut strings = Vec::new();
		let mut pointers = Vec::new();
		for cstring in iter {
			pointers.push(cstring.as_ptr());
			strings.push(cstring);
		}
		pointers.push(std::ptr::null());
		Self {
			_strings: strings,
			pointers,
		}
	}
}
