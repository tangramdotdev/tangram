use ../lib/test.nu *

# An empty indexer list must not imply that pending indexing work is complete.
let local = server spawn --config {
	advanced: { single_process: false },
	database: { kind: 'sqlite', path: 'database.sqlite3' },
	roles: [api],
}
tg --url $local.url group create project | ignore
let pending = (
	open ($local.directory | path join database.sqlite3)
	| query db 'select count(*) as count from index_queue'
	| get count.0
)
assert ($pending > 0)

let output = tg --url $local.url index | complete
failure $output 'indexing must fail when no indexers are registered'
snapshot --normalize $output.stderr '
	error an error occurred
	-> no indexers are available

'
