#[derive(Debug)]
pub struct Tree {
	pub title: String,
	pub children: Vec<Self>,
}

impl Tree {
	pub fn print(&self) {
		self.print_inner("");
		println!();
	}

	fn print_inner(&self, prefix: &str) {
		print!("{}", self.title);
		for (n, child) in self.children.iter().enumerate() {
			print!("\n{prefix}");
			if n == self.children.len() - 1 {
				print!("└── ");
				child.print_inner(&format!("{prefix}   "));
			} else {
				print!("├── ");
				child.print_inner(&format!("{prefix}│   "));
			}
		}
	}
}
