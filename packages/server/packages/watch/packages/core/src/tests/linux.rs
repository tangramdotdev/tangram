use {
	super::State,
	notify::Watcher as _,
	std::{collections::BTreeMap, sync::mpsc, time::Duration},
	tangram_util::fs::Temp,
};

#[test]
fn replacing_a_watched_directory_releases_the_moved_inode() {
	// Watch the directory without watching its parent.
	let temp = Temp::new().unwrap();
	let path = temp.path().join("directory");
	std::fs::create_dir_all(&path).unwrap();
	let (sender, receiver) = mpsc::channel();
	let mut watcher = notify::RecommendedWatcher::new(
		move |event| {
			sender.send(event).ok();
		},
		notify::Config::default(),
	)
	.unwrap();
	let mut paths = BTreeMap::new();
	State::add_path_linux(&mut *watcher.paths_mut(), &mut paths, &path).unwrap();

	// Replace the directory repeatedly while keeping the moved directories alive.
	for index in 0..3 {
		let moved = temp.path().join(format!("moved{index}"));
		std::fs::rename(&path, &moved).unwrap();
		std::fs::create_dir(&path).unwrap();
		State::add_path_linux(&mut *watcher.paths_mut(), &mut paths, &path).unwrap();
	}

	// Changes in the moved directories must not be reported as changes in the replacement.
	for index in 0..3 {
		std::fs::write(temp.path().join(format!("moved{index}/old")), "old").unwrap();
	}
	let current = path.join("current");
	std::fs::write(&current, "current").unwrap();
	loop {
		let event = receiver
			.recv_timeout(Duration::from_secs(10))
			.unwrap()
			.unwrap();
		assert!(!event.paths.iter().any(|path| path.ends_with("old")));
		if event.paths.contains(&current) {
			break;
		}
	}
}
