use ../lib/test.nu *

for mode in [--eager --lazy] {
 let remote = server spawn --cloud --name remote
 let local = server spawn --name local
 tg remote put default $remote.url
 let path = artifact { tangram.ts: 'export default function () { console.log("stdout"); console.error("stderr"); }' }
 let process = tg build --no-tokens --detach $path | referent node
 timeout 10s tg wait $process
 let log = tg get --no-tokens $process | from json | get log
 let push_job = job spawn {
 let job_id = job id
 let output = tg --url $local.url push $process --process-log-objects $mode | complete
 $output | job send --tag $job_id 0
 }
 tg push $process --process-log-objects $mode
 success (job recv --tag $push_job --timeout 10sec)
 assert equal (tg --url $remote.url get --no-tokens $process | from json | get log) $log
 let output = tg --url $remote.url log $process --no-timeout | complete
 success $output
 assert equal $output.stdout "stdout\n"
 assert equal $output.stderr "stderr\n"
}
