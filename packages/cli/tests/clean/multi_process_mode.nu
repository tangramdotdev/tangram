use ../../test.nu *

# Cleaning fails when the server is not in single process mode.

let server = spawn --config { advanced: { single_process: false } }

let output = tg clean | complete
failure $output
snapshot --normalize $output.stderr '
	error an error occurred
	-> failed to clean
	-> the request failed
	   status = 500 Internal Server Error
	-> failed to start the clean task
	-> cannot clean in multi-process mode

'
