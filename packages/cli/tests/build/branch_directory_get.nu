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
			let file = await directory.get("d.txt");
			tg.File.assert(file);
			return file.text;
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
