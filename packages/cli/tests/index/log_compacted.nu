use ../../test.nu *

let local = spawn -n local

let path = artifact {
	tangram.ts: r#'
		export default () => {};
	'#
}
let id = tg build -d $path | str trim
tg wait $id

tg index

let process = tg get $id | from json
let log_id = $process.log
let log = tg get $log_id --blobs
snapshot -n log $log 'tg.blob("\u0000\u000b\n\u0003\u0000\b\u0000\u0001\b\u0000\u0002\b\u0000")'
