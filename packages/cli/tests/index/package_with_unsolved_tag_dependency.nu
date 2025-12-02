use ../../test.nu *

# Test incremental indexing for a package with an unsolved tag dependency.
# Puts directory, file, and blob incrementally and verifies metadata at each step.
# Also verifies that a full push produces identical metadata.

# Create the local server.
let local_server = spawn -n local

# Create the remote server (receives incremental puts).
let remote_server = spawn -n remote

# Create the push server (receives a full push for comparison).
let push_server = spawn -n push

# Create a package that imports the tagged dependency.
let path = artifact {
	tangram.ts: '
		import dep from "dep";
		export default () => dep();
	'
}

# Check in the package on the local server.
let id = run tg -u $local_server.url checkin --unsolved-dependencies $path
let dir_id = $id

# Get the file id.
let output = run tg -u $local_server.url children $id
let fil_id = $output | from json | get 0

# Get the blob id.
let output = run tg -u $local_server.url children $fil_id
let blb_id = $output | from json | get 0

# Index the local server to get the expected metadata.
run tg -u $local_server.url index
let expected_metadata = run tg -u $local_server.url object metadata $dir_id --pretty

# Put only the directory to the remote server.
run tg -u $local_server.url get --bytes $dir_id | tg -u $remote_server.url put --bytes -k dir

# The directory should exist on remote server, but file should not.
let output = tg -u $remote_server.url get $dir_id | complete
success $output "directory should exist on remote server"

let output = tg -u $remote_server.url get $fil_id | complete
failure $output "file should NOT exist on remote server"

# Index and check metadata - should be incomplete.
run tg -u $remote_server.url index
let incomplete_metadata = run tg -u $remote_server.url object metadata $dir_id --pretty
snapshot -n incomplete_metadata $incomplete_metadata '
	{
	  "self_solvable": false,
	  "self_solved": true,
	}

'

# Now put the file
run tg -u $local_server.url get --bytes $fil_id | tg -u $remote_server.url put --bytes -k fil

# Index and check metadata - should still be incomplete because blob is missing.
run tg -u $remote_server.url index
let partial_metadata = run tg -u $remote_server.url object metadata $dir_id --pretty
snapshot -n partial_metadata $partial_metadata '
	{
	  "self_solvable": false,
	  "self_solved": true,
	}

'

# Now put the blob.
run tg -u $local_server.url get --bytes $blb_id | tg -u $remote_server.url put --bytes -k blob

# Index and check metadata - should now be complete.
run tg -u $remote_server.url index
let complete_metadata = run tg -u $remote_server.url object metadata $dir_id --pretty
snapshot -n complete_metadata $complete_metadata '
	{
	  "count": 3,
	  "depth": 3,
	  "self_solvable": false,
	  "self_solved": true,
	  "solvable": true,
	  "solved": false,
	  "weight": 162,
	}

'

# Verify the complete metadata matches the expected metadata from the local server.
assert equal $complete_metadata $expected_metadata

# Now test push: add the push server as a remote and push the directory.
run tg -u $local_server.url remote put push $push_server.url
run tg -u $local_server.url push --remote push $dir_id

# Index the push server and verify metadata matches.
run tg -u $push_server.url index
let push_metadata = run tg -u $push_server.url object metadata $dir_id --pretty

# All three servers should have identical metadata.
assert equal $push_metadata $expected_metadata
assert equal $push_metadata $complete_metadata
