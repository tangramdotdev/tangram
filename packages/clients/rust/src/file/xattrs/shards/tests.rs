use {super::*, tangram_util::fs::Temp};

#[test]
fn round_trip_and_replace() {
	let path = Temp::new().unwrap();
	std::fs::write(&path, "").unwrap();
	xattr::set(&path, "user.example", b"preserved").unwrap();
	assert_eq!(read_sharded(&path, "user.tangram.output").unwrap(), None);
	for size in [0, 4, 20_000, 131_072, 4, 0] {
		let value = (0..size)
			.map(|index| u8::try_from(index % 256).unwrap())
			.collect::<Vec<_>>();
		write_sharded(&path, "user.tangram.output", &value).unwrap();
		assert_eq!(
			read_sharded(&path, "user.tangram.output").unwrap(),
			Some(value)
		);
	}
	assert_eq!(
		xattr::get(&path, "user.example").unwrap(),
		Some(b"preserved".to_vec())
	);
	assert!(
		!xattr::list(&path)
			.unwrap()
			.any(|name| name.to_string_lossy().starts_with("user.tangram.output."))
	);
}

#[test]
fn numeric_order() {
	let path = Temp::new().unwrap();
	std::fs::write(&path, "").unwrap();
	for index in (0..12).rev() {
		xattr::set(&path, format!("user.tangram.output.{index}"), &[index]).unwrap();
	}
	assert_eq!(
		read_sharded(&path, "user.tangram.output").unwrap(),
		Some((0..12).collect())
	);
}

#[test]
fn invalid_shards() {
	for suffixes in [["", ".0"], [".0", ".2"], [".0", ".01"], [".0", ".invalid"]] {
		let path = Temp::new().unwrap();
		std::fs::write(&path, "").unwrap();
		for suffix in suffixes {
			xattr::set(&path, format!("user.tangram.output{suffix}"), b"part").unwrap();
		}
		assert_eq!(
			read_sharded(&path, "user.tangram.output")
				.unwrap_err()
				.kind(),
			io::ErrorKind::InvalidData
		);
	}
}
