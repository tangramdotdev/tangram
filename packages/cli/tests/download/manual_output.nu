use ../../test.nu *

# A manual builtin invocation reports the reason on stderr without writing to a process output path.

skip_if_offline

let directory = mktemp -d
let output_path = $directory | path join 'output'
let process_output_path = $directory | path join 'process_output'

# Without a process output path, the failure is reported and nothing is written.
let output = with-env { TANGRAM_OUTPUT: null } {
	tg builtin download --output $output_path "http://www.example.com/does-not-exist" | complete
}
failure $output
snapshot --normalize $output.stderr '
	-> expected a success status
	   url = http://www.example.com/does-not-exist
	-> HTTP status client error (404 Not Found) for url (http://www.example.com/does-not-exist)

'
assert (not ($output_path | path exists)) "the output should not be written"

# With an unrelated process output path, the failure is not written to it.
let output = with-env { TANGRAM_OUTPUT: $process_output_path } {
	tg builtin download --output $output_path "http://www.example.com/does-not-exist" | complete
}
failure $output
snapshot --normalize $output.stderr '
	-> expected a success status
	   url = http://www.example.com/does-not-exist
	-> HTTP status client error (404 Not Found) for url (http://www.example.com/does-not-exist)

'
assert (not ($process_output_path | path exists)) "the process output should not be written"
