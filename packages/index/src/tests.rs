/// Get every order of the indices below the length.
pub fn permutations(length: usize) -> Vec<Vec<usize>> {
	if length == 0 {
		return vec![Vec::new()];
	}
	let mut output = Vec::new();
	for order in permutations(length - 1) {
		for position in 0..=order.len() {
			let mut order = order.clone();
			order.insert(position, length - 1);
			output.push(order);
		}
	}

	output
}
