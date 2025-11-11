use std assert
use ../../test.nu *

let server = spawn

let path = artifact {
	'tangram.ts': 'export let a = () => tg.file("a");'
}

# Build the export.
let a_output = tg build ($path + '#a') | complete
assert equal $a_output.exit_code 0
let a_id = $a_output.stdout | str trim

# Tag a with the same tag twice.
tg tag mytag $a_id
tg tag mytag $a_id

# Delete the tag.
tg tag delete mytag

# Clean.
let clean_output = tg clean | complete
assert equal $clean_output.exit_code 0

# Verify a was cleaned (no longer tagged).
let a_get = tg object get $a_id | complete
assert ($a_get.exit_code != 0)
