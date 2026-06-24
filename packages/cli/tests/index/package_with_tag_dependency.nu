use ../../test.nu *

# Indexing correctly computes metadata for packages with tagged dependencies.
# This test creates a package that imports a tagged dependency, then incrementally puts
# the directory, file, blob, and tag to a remote server, verifying that the metadata
# transitions from incomplete to complete as all objects become available. Also verifies
# that a full push produces identical metadata.

# Create the local server.
let local = spawn --name local

# Create the remote server (receives incremental puts).
let remote = spawn --cloud --name remote

# Create another server (receives a full push for comparison).
let other = spawn --cloud --name other

# Tag a dependency on the local server.
let dep_path = artifact {
	tangram.ts: '
		export default function () { return "dependency"; }
	'
}
tg --url $local.url tag dep $dep_path

# Create a package that imports the tagged dependency.
let path = artifact {
	tangram.ts: '
		import dep from "dep";
		export default function () { return dep(); }
	'
}

# Check in the package on the local server.
let id = tg --url $local.url checkin $path
let dir_id = $id

# Get the file id.
let output = tg --url $local.url children $id
let fil_id = $output | from json | get 0

# Get the blob id.
let output = tg --url $local.url children $fil_id
let blb_id = $output | from json | get 0

# Index the local server to get the expected metadata.
tg --url $local.url index
let expected_metadata = tg --url $local.url object metadata $dir_id --pretty

# Put only the directory to the remote server.
tg --url $local.url get --bytes $dir_id | tg --url $remote.url put --bytes --kind dir

# The directory should exist on remote server, but file should not.
let output = tg --url $remote.url get $dir_id | complete
success $output "directory should exist on remote server"

let output = tg --url $remote.url get $fil_id | complete
failure $output "file should NOT exist on remote server"

# Index and check metadata - should be incomplete.
tg --url $remote.url index
let incomplete_metadata = tg --url $remote.url object metadata $dir_id --pretty
snapshot --name incomplete_metadata $incomplete_metadata '
	{
	  "node": {
	    "size": 60,
	    "solvable": false,
	    "solved": true,
	  },
	}
'

# Now put the file
tg --url $local.url get --bytes $fil_id | tg --url $remote.url put --bytes --kind fil

# Index and check metadata - should still be incomplete because blob is missing.
tg --url $remote.url index
let partial_metadata = tg --url $remote.url object metadata $dir_id --pretty
snapshot --name partial_metadata $partial_metadata '
	{
	  "node": {
	    "size": 60,
	    "solvable": false,
	    "solved": true,
	  },
	}
'

# Now put the blob.
tg --url $local.url get --bytes $blb_id | tg --url $remote.url put --bytes --kind blob

# Put the tag.
tg --url $remote.url tag dep $dep_path

# Index and check metadata - should now be complete.
tg --url $remote.url index
let complete_metadata = tg --url $remote.url object metadata $dir_id --pretty
snapshot --name complete_metadata $complete_metadata '
	{
	  "node": {
	    "size": 60,
	    "solvable": false,
	    "solved": true,
	  },
	  "subtree": {
	    "count": 6,
	    "depth": 5,
	    "size": 435,
	    "solvable": true,
	    "solved": true,
	  },
	}
'

# Verify the complete metadata matches the expected metadata from the local server.
assert equal $complete_metadata $expected_metadata

# Now test push: add the other server as a remote and push the directory.
tg --url $local.url remote put push $other.url
tg --url $local.url push --remote=push $dir_id

# Also push the tag to the other server.
tg --url $other.url tag dep $dep_path

# Index the other server and verify metadata matches.
tg --url $other.url index
let other_metadata = tg --url $other.url object metadata $dir_id --pretty

# All three servers should have identical metadata.
assert equal $other_metadata $expected_metadata
assert equal $other_metadata $complete_metadata
