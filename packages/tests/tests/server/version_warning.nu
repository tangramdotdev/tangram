use ../../util/test.nu *

# Commands report a server version mismatch, while quiet mode suppresses the warning.

let port = mktemp
let supervisor = mktemp -d
track_supervisor $supervisor
let source = '
import { createServer } from "node:http2";
import { closeSync, writeFileSync, writeSync } from "node:fs";
const server = createServer();
server.on("stream", stream => {
    stream.respond({ ":status": 200, "content-type": "application/json" });
    stream.end(JSON.stringify({ version: "old-test-version", diagnostics: [] }));
});
server.listen(0, "127.0.0.1", () => {
    writeFileSync(process.argv[1], String(server.address().port));
    writeSync(3, new Uint8Array([0]));
    closeSync(3);
});
'
tangram_tests_supervisor start --directory $supervisor --name server --ready-fd 3 -- node --input-type=module -e $source $port
let url = $'http://127.0.0.1:(open --raw $port | str trim)'
let output = tg --no-quiet --url $url --mode client health --fields version | complete
success $output
assert ($output.stderr | str contains 'old-test-version') $output.stderr
assert ($output.stderr | str contains 'differs from the CLI version') $output.stderr
let output = tg --quiet --url $url --mode client health --fields version | complete
success $output
assert (not ($output.stderr | str contains 'differs from the CLI version')) $output.stderr
