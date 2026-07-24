use ../../test.nu *

# A direct sandbox create times out after the scheduler accepts it when no runner can start it.

let server = spawn --config {
	roles: [cleaner finalizer http indexer scheduler],
	sandbox: {
		create_connection_timeout: 1,
	},
}

let output = tg sandbox create | complete
failure $output "timed out waiting for the sandbox control connection"
