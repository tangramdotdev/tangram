use ../lib/test.nu *

# A finished process already includes its final log blob.

let local = server spawn --name local

let path = artifact {
	tangram.ts: r#'
		export default function () {}
	'#
}
let id = tg build --no-tokens --detach $path | referent node
tg wait --source=index $id

timeout 10 tg index

let process = tg get $id | from json
let log_id = $process.log
let log = tg read $log_id | encode hex
snapshot --name log $log '000B0A03000800010800020800'
