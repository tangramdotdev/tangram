use ../../test.nu *

# Indexing fails when the indexer is disabled.

let server = spawn --config { indexer: false }

let output = tg index | complete
failure $output
snapshot --normalize $output.stderr '
	error an error occurred
	-> failed to index
	-> the request failed
	   status = 500 Internal Server Error
	-> failed to start the index task
	-> cannot index when the indexer is disabled

'
