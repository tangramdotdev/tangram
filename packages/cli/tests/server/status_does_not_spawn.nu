use ../../test.nu *

# Checking the status of a stopped server does not spawn it.

let directory = mktemp -d
let output = with-env { TANGRAM_MODE: auto } { tg -d $directory server status | complete }
success $output
assert equal ($output.stdout | from json) 'stopped'
assert not (($directory | path join 'socket') | path exists)
