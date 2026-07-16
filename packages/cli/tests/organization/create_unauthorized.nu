use ../../test.nu *

# An anonymous client cannot create an organization.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let config = mktemp
{} | to json | save -f $config
let output = with-env { TANGRAM_CONFIG: $config } { tg organization create anon | complete }
failure $output "an anonymous client should not be able to create an organization"
snapshot ($output.stderr | redact) '
	error an error occurred
	-> failed to create the organization
	   specifier = anon
	-> the request failed
	   status = 500 Internal Server Error
	-> unauthorized

'
