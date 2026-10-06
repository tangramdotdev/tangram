use ../lib/test.nu *

let remote = server spawn --cloud --name remote
let local = server spawn --name local
tg remote put default $remote.url
let path = artifact { tangram.ts: 'export default function () { console.log("stdout"); console.error("stderr"); }' }
let process = tg build --no-tokens --detach $path | referent node
timeout 10s tg wait $process
let data = tg get --no-tokens $process | from json
for iteration in [0 1] {
 tg push --eager --process-log-objects $process
 tg --url $remote.url process put $process ($data | to json)
 assert equal (tg --url $remote.url get --no-tokens $process | from json | get log) $data.log
 let output = tg --url $remote.url log $process --no-timeout | complete
 success $output
 assert equal $output.stdout "stdout\n"
 assert equal $output.stderr "stderr\n"
}
