use ../../test.nu *

# Spawn a server with small max_leaf_entries to trigger branch directories.
let server = spawn --config {
	checkin: {
		directory: {
			max_leaf_entries: 4
			max_branch_children: 2
		}
	}
}

# Create a directory with 6 entries to trigger branching.
let path = artifact {
	tangram.ts: '
		import directory from "./data" with { type: "directory" };
		export default async () => {
			let entries: Array<string> = [];
			for await (let [name, _artifact] of directory) {
				entries.push(name);
			}
			entries.sort();
			return entries.join(",");
		};
	'
	data: {
		a.txt: 'File A'
		b.txt: 'File B'
		c.txt: 'File C'
		d.txt: 'File D'
		e.txt: 'File E'
		f.txt: 'File F'
	}
}

let output = tg build $path
snapshot $output
