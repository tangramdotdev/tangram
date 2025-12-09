use ../../test.nu *

# Test that indexing correctly computes metadata for incomplete objects.
# This test puts a directory object with a missing file child, verifies the metadata
# shows incomplete state, then adds the missing children and verifies the metadata
# becomes complete and matches the expected values. Also verifies that a full push
# produces identical metadata.

# Create the local server (has the complete artifacts).
let local_server = spawn -n local

# Create the remote server (receives partial puts for testing).
let remote_server = spawn -n remote

# Create a push server (receives a full push for comparison).
let push_server = spawn -n push

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
let id = tg -u $local_server.url build $path
let dir_id = $id

# Get the file id.
let output = tg -u $local_server.url children $id
let fil_id = $output | from json | get 0

# Get the blob id.
let output = tg -u $local_server.url children $fil_id
let blb_id = $output | from json | get 0

# Index the local server to get the expected metadata.
tg -u $local_server.url index
let expected_metadata = tg -u $local_server.url object metadata $dir_id --pretty

# Put only the directory to the remote server (file is missing).
tg -u $local_server.url get --bytes $dir_id | tg -u $remote_server.url put --bytes -k dir

# Verify: the directory should exist on remote server, but file should not.
let output = tg -u $remote_server.url get $dir_id | complete
success $output "directory should exist on remote server"

let output = tg -u $remote_server.url get $fil_id | complete
failure $output "file should NOT exist on remote server"

# Index and check metadata - should be incomplete.
tg -u $remote_server.url index
let incomplete_metadata = tg -u $remote_server.url object metadata $dir_id --pretty
snapshot -n incomplete_metadata $incomplete_metadata '
	{
	  "node": {
	    "size": 57,
	    "solvable": false,
	    "solved": true,
	  },
	}
'

# Now put the file
tg -u $local_server.url get --bytes $fil_id | tg -u $remote_server.url put --bytes -k fil

# Index and check metadata - should still be incomplete because blob is missing.
tg -u $remote_server.url index
let partial_metadata = tg -u $remote_server.url object metadata $dir_id --pretty
snapshot -n partial_metadata $partial_metadata '
	{
	  "node": {
	    "size": 57,
	    "solvable": false,
	    "solved": true,
	  },
	}
'

# Now put the blob.
tg -u $local_server.url get --bytes $blb_id | tg -u $remote_server.url put --bytes -k blob

# Index and check metadata - should now be complete.
tg -u $remote_server.url index
let complete_metadata = tg -u $remote_server.url object metadata $dir_id --pretty
snapshot -n complete_metadata $complete_metadata '
	{
	  "node": {
	    "size": 57,
	    "solvable": false,
	    "solved": true,
	  },
	  "subtree": {
	    "count": 3,
	    "depth": 3,
	    "size": 115,
	    "solvable": false,
	    "solved": true,
	  },
	}
'

# Verify the complete metadata matches the expected metadata from the local server.
assert equal $complete_metadata $expected_metadata

# Now test push: add the push server as a remote and push the directory.
tg -u $local_server.url remote put push $push_server.url
tg -u $local_server.url push --remote push $dir_id

# Index the push server and verify metadata matches.
tg -u $push_server.url index
let push_metadata = tg -u $push_server.url object metadata $dir_id --pretty

# All three servers should have identical metadata.
assert equal $push_metadata $expected_metadata
assert equal $push_metadata $complete_metadata
