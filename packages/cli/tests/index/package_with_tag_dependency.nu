use ../../test.nu *

# Test that indexing correctly computes metadata for packages with tagged dependencies.
# This test creates a package that imports a tagged dependency, then incrementally puts
# the directory, file, blob, and tag to a remote server, verifying that the metadata
# transitions from incomplete to complete as all objects become available. Also verifies
# that a full push produces identical metadata.

# Create the local server.
let local = spawn -n local

# Create the remote server (receives incremental puts).
let remote = spawn -n remote

# Create another server (receives a full push for comparison).
let other = spawn -n other

# Tag a dependency on the local server.
let dep_path = artifact {
	tangram.ts: '
		export default () => "dependency";
	'
}
tg -u $local.url tag dep $dep_path

# Create a package that imports the tagged dependency.
let path = artifact {
	tangram.ts: '
		import dep from "dep";
		export default () => dep();
	'
}

# Check in the package on the local server.
let id = tg -u $local.url checkin $path
let dir_id = $id

# Get the file id.
let output = tg -u $local.url children $id
let fil_id = $output | from json | get 0

# Get the blob id.
let output = tg -u $local.url children $fil_id
let blb_id = $output | from json | get 0

# Index the local server to get the expected metadata.
tg -u $local.url index
let expected_metadata = tg -u $local.url object metadata $dir_id --pretty

# Put only the directory to the remote server.
tg -u $local.url get --bytes $dir_id | tg -u $remote.url put --bytes -k dir

# The directory should exist on remote server, but file should not.
let output = tg -u $remote.url get $dir_id | complete
success $output "directory should exist on remote server"

let output = tg -u $remote.url get $fil_id | complete
failure $output "file should NOT exist on remote server"

# Index and check metadata - should be incomplete.
tg -u $remote.url index
let incomplete_metadata = tg -u $remote.url object metadata $dir_id --pretty
snapshot -n incomplete_metadata $incomplete_metadata '
	{
	  "node": {
	    "size": 58,
	    "solvable": false,
	    "solved": true,
	  },
	}
'

# Now put the file
tg -u $local.url get --bytes $fil_id | tg -u $remote.url put --bytes -k fil

# Index and check metadata - should still be incomplete because blob is missing.
tg -u $remote.url index
let partial_metadata = tg -u $remote.url object metadata $dir_id --pretty
snapshot -n partial_metadata $partial_metadata '
	{
	  "node": {
	    "size": 58,
	    "solvable": false,
	    "solved": true,
	  },
	}
'

# Now put the blob.
tg -u $local.url get --bytes $blb_id | tg -u $remote.url put --bytes -k blob

# Put the tag.
tg -u $remote.url tag dep $dep_path

# Index and check metadata - should now be complete.
tg -u $remote.url index
let complete_metadata = tg -u $remote.url object metadata $dir_id --pretty
snapshot -n complete_metadata $complete_metadata '
	{
	  "node": {
	    "size": 58,
	    "solvable": false,
	    "solved": true,
	  },
	  "subtree": {
	    "count": 6,
	    "depth": 5,
	    "size": 397,
	    "solvable": true,
	    "solved": true,
	  },
	}
'

# Verify the complete metadata matches the expected metadata from the local server.
assert equal $complete_metadata $expected_metadata

# Now test push: add the other server as a remote and push the directory.
tg -u $local.url remote put push $other.url
tg -u $local.url push --remote push $dir_id

# Also push the tag to the other server.
tg -u $other.url tag dep $dep_path

# Index the other server and verify metadata matches.
tg -u $other.url index
let other_metadata = tg -u $other.url object metadata $dir_id --pretty

# All three servers should have identical metadata.
assert equal $other_metadata $expected_metadata
assert equal $other_metadata $complete_metadata
