use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: 'export let a = () => tg.file("a");'
}

# Build the export.
let a_output = tg build ($path + '#a') | complete
success $a_output
let a_id = $a_output.stdout | str trim

# Tag a with the same tag twice.
tg tag mytag $a_id
tg tag mytag $a_id

# Delete the tag.
tg tag delete mytag

# Clean.
let clean_output = tg clean | complete
success $clean_output

# Verify a was cleaned (no longer tagged).
let a_get = tg object get $a_id | complete
failure $a_get
