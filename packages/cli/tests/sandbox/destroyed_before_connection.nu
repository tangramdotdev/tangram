use ../../test.nu *

# A sandbox that dies on the runner before its control stream connects must fail creation instead of waiting forever.

let server = spawn --config {
	advanced: {
		checkpoints: true,
	},
}

# Hold the sandbox control stream before it announces the connection, so the runner's creation failure always precedes the connection.
let watch = (
	tg checkpoint watch sandbox.control.connect
	| from json
	| get watch
)

# vm isolation is not configured, so the runner accepts the sandbox and then fails to create it.
let output = tg sandbox create --isolation vm | complete
failure $output
snapshot --normalize $output.stderr '
	error an error occurred
	-> failed to create the sandbox
	-> the request failed
	   status = 500 Internal Server Error
	-> the sandbox was destroyed before it connected
	   sandbox = sbx_0000000000000000000000000000

'

tg checkpoint unwatch sandbox.control.connect $watch
