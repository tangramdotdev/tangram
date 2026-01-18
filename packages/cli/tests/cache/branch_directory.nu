use ../../test.nu *

# Spawn a server with a small max_leaf_entries to trigger branch directories with few files.
let server = spawn --config {
	checkin: {
		directory: {
			max_leaf_entries: 4
			max_branch_children: 2
		}
	}
}

# Create a directory with 6 entries to trigger branching (more than max_leaf_entries of 4).
let path = artifact {
	a.txt: 'File A'
	b.txt: 'File B'
	c.txt: 'File C'
	d.txt: 'File D'
	e.txt: 'File E'
	f.txt: 'File F'
}

let id = tg checkin $path

# Cache the branch directory.
tg cache $id

# Snapshot the artifacts directory.
snapshot --path ($server.directory | path join "artifacts")
