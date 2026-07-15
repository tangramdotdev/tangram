use ../../test.nu *

# Displaying a tree for an object that does not exist renders a load failure marker.

let server = spawn

let output = tg tree fil_010000000000000000000000000000000000000000000000000000 | complete
snapshot --normalize-ids $output.stdout '
	? failed to load the object

'
