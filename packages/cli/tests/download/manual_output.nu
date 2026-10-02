use ../lib/test.nu *
use ../lib/http.nu *

# A manual builtin invocation reports the reason on stderr without writing to a process output path.

let http = spawn_http_server { '/does-not-exist': { status: 404 } }

let directory = mktemp -d
let output_path = $directory | path join 'output'
let process_output_path = $directory | path join 'process_output'

# Without a process output path, the failure is reported and nothing is written.
let output = with-env { TANGRAM_OUTPUT: null } {
	tg builtin download --output $output_path $'($http.url)/does-not-exist' | complete
}
failure $output
snapshot --normalize --redact $http.url $output.stderr '
	-> expected a success status
	   url = <redacted>/does-not-exist
	-> HTTP status client error (404 Not Found) for url (<redacted>/does-not-exist)

'
assert (not ($output_path | path exists)) "the output should not be written"

# With an unrelated process output path, the failure is not written to it.
let output = with-env { TANGRAM_OUTPUT: $process_output_path } {
	tg builtin download --output $output_path $'($http.url)/does-not-exist' | complete
}
failure $output
snapshot --normalize --redact $http.url $output.stderr '
	-> expected a success status
	   url = <redacted>/does-not-exist
	-> HTTP status client error (404 Not Found) for url (<redacted>/does-not-exist)

'
assert (not ($process_output_path | path exists)) "the process output should not be written"
