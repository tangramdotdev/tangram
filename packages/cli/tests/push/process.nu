use ../../test.nu *
use std assert

# Create a remote server.
let remote_server = spawn -n remote

# Create a local server.
let local_server = spawn -n local

# Add the remote.
let output = tg remote put default $remote_server.url | complete
success $output

# Create some test content.
let path = artifact {
    tangram.ts: r#'
        export default () => {
           return 5
        }
    '#
}

# Build the module.
let output = run tg build -d $path | from json

# Parse the process ID.
let process_id = $output.process

# Wait for the process to complete.
tg wait $process_id

# Push the process.
run tg push --eager $process_id

# Confirm the process is on the remote and the same.
let local_process = tg get $process_id --blobs --depth=inf --pretty | complete | get stdout
let remote_process = tg --url $remote_server.url get $process_id --blobs --depth=inf --pretty | complete | get stdout

assert equal $local_process $remote_process
