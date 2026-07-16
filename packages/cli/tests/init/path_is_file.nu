use ../../test.nu *

# Initializing a path that exists as a regular file fails.

let dir = mktemp --directory
let target = $dir | path join "target"
"file" | save $target

let output = tg init $target | complete
failure $output
snapshot --normalize --redact $target $output.stderr '
	error an error occurred
	-> failed to create the directory
	   path = "<redacted>"
	-> File exists (os error 17)

'
