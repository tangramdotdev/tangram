use ../../util/test.nu *

# A watched checkin without a lock of a file whose name ends with .lock reflects an edit to the file.

let local = server spawn

let path = artifact {
	data.lock: 'a'
}
let file_path = $path | path join data.lock
tg checkin --no-tokens --watch --no-lock $file_path | ignore

# Edit the file and synchronously deliver its event.
'b' | save --force $file_path
tg watch touch $file_path $file_path

let cold = tg checkin --no-tokens --no-watch --no-lock $file_path | referent node
let watched = tg checkin --no-tokens --watch --no-lock $file_path | referent node
assert equal $watched $cold "the watched checkin should reflect the edit"
