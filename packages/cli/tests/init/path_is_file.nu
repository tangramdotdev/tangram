use ../../test.nu *

# Initializing a path that exists as a regular file fails.

let dir = mktemp --directory
let target = $dir | path join "target"
"file" | save $target

let output = tg init $target | complete
failure $output
snapshot ($output.stderr | redact $target) '
	error an error occurred
	-> failed to create the directory
	   path = "<path>"
	-> File exists (os error 17)

'
