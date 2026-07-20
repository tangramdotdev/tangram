use ../../test.nu *

# Getting a reference that does not parse fails with a parse error.

let server = spawn

let output = tg get 'not a reference' | complete
failure $output
snapshot --normalize $output.stderr r#'
	error: invalid value 'not a reference' for '<REFERENCE>': failed to parse the reference item
	
	For more information, try '--help'.

'#
