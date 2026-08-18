use ../../test.nu *

# Process status is available only as a process subcommand.

let output = tg status | complete
failure $output "tg status should not be a command"
snapshot --normalize $output.stderr r#'
	error: unrecognized subcommand 'status'
	
	Usage: tg [OPTIONS] <COMMAND>
	
	For more information, try '--help'.

'#
