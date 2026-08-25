use ../../test.nu *

# Stopping a server that has already stopped succeeds. A clean shutdown truncates the lock file rather than removing it, so the second stop reads an empty lock file.

let directory = mktemp -d
let output = tg -d $directory server stop | complete
success $output 'stopping a server that has never started should succeed'

let server = server spawn

let output = tg -d $server.directory server stop | complete
success $output

let output = tg -d $server.directory server stop | complete
success $output 'stopping an already stopped server should succeed'
