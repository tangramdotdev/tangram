use ../../test.nu *

# Entities are listed with tg list by specifier, so there is no tg organization list subcommand, just as there is none for groups.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose alice | from json

let output = tg --token $alice.token organization list | complete
failure $output "tg organization list should not be a command"
snapshot --normalize $output.stderr r#'
	error: unrecognized subcommand 'list'
	
	Usage: tg organization <COMMAND>
	
	For more information, try '--help'.

'#
