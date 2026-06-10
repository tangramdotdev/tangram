use ../../test.nu *

# Displaying a tree for an object that does not exist renders a load failure marker.

let server = spawn

let output = tg tree fil_010000000000000000000000000000000000000000000000000000 | complete
assert ($output.stdout | str contains "failed to load the object") "the tree should render the load failure"
