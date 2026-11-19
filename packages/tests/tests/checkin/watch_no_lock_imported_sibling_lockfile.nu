use ../../util/test.nu *

# A watched checkin without a lock of a module that imports its sibling .tg.lock file as a file reflects an edit to that file.

let local = server spawn

let path = artifact {
	main.tg.ts: '
		import "./main.tg.lock" with { type: "file" };
	'
	main.tg.lock: 'a'
}
let module_path = $path | path join main.tg.ts
let data_path = $path | path join main.tg.lock
tg checkin --no-tokens --watch --no-lock $module_path | ignore

# Edit the imported file and synchronously deliver its event.
'b' | save --force $data_path
tg watch touch $module_path $data_path

let cold = tg checkin --no-tokens --no-watch --no-lock $module_path | referent node
let watched = tg checkin --no-tokens --watch --no-lock $module_path | referent node
assert equal $watched $cold "the watched checkin should reflect the edit"
