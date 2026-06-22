use ../../test.nu *

# Displaying a tree for an object that does not exist renders a load failure marker.

let server = spawn

let output = tg tree fil_010000000000000000000000000000000000000000000000000000 | complete
snapshot ($output.stdout | redact | normalize_ids) '
	? failed to load the object

'
