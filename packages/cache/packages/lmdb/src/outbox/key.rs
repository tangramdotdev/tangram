#[derive(Debug)]
pub enum Key {
	Fragment {
		batch: [u8; 16],
		index: u64,
		partition: u64,
	},
}
