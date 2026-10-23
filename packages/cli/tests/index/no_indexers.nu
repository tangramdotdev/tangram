use ../lib/test.nu *

# An empty indexer list must not imply that pending indexing work is complete.
let local = server spawn --config {
	advanced: { multi_server: true },
	roles: [api],
}
tg --url $local.url group create project | ignore

let output = tg --url $local.url index | complete
failure $output 'indexing must fail when no indexers are registered'
snapshot --normalize $output.stderr '
	error an error occurred
	-> no indexers are available

'
