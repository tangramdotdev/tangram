use ../../test.nu *

# Test that indexing correctly computes metadata for incomplete objects.
# This test puts a directory object with a missing file child, verifies the metadata
# shows incomplete state, then adds the missing children and verifies the metadata
# becomes complete and matches the expected values. Also verifies that a full push
# produces identical metadata.

# Create the local server (has the complete artifacts).
let local = spawn -n local

# Create the remote server (receives partial puts for testing).
let remote = spawn --cloud -n remote

# Create another server (receives a full push for comparison).
let other = spawn --cloud -n other

let path = artifact {
	tangram.ts: '
		export default () => {
			return tg.directory({
				"hello.txt": tg.file("Hello, World!")
			})
		}
	'
}

# Build the module on the local server.
let id = tg -u $local.url build $path
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

# Put only the directory to the remote server (file is missing).
tg -u $local.url get --bytes $dir_id | tg -u $remote.url put --bytes -k dir

# Verify: the directory should exist on remote server, but file should not.
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
	    "size": 59,
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
	    "size": 59,
	    "solvable": false,
	    "solved": true,
	  },
	}
'

# Now put the blob.
tg -u $local.url get --bytes $blb_id | tg -u $remote.url put --bytes -k blob

# Index and check metadata - should now be complete.
tg -u $remote.url index
let complete_metadata = tg -u $remote.url object metadata $dir_id --pretty
snapshot -n complete_metadata $complete_metadata '
	{
	  "node": {
	    "size": 59,
	    "solvable": false,
	    "solved": true,
	  },
	  "subtree": {
	    "count": 3,
	    "depth": 3,
	    "size": 117,
	    "solvable": false,
	    "solved": true,
	  },
	}
'

# Verify the complete metadata matches the expected metadata from the local server.
assert equal $complete_metadata $expected_metadata

# Now test push: add the other server as a remote and push the directory.
tg -u $local.url remote put push $other.url
tg -u $local.url push --remote push $dir_id

# Index the other server and verify metadata matches.
tg -u $other.url index
let other_metadata = tg -u $other.url object metadata $dir_id --pretty

# All three servers should have identical metadata.
assert equal $other_metadata $expected_metadata
assert equal $other_metadata $complete_metadata
