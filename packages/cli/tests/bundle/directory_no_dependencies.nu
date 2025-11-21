use ../../test.nu *

let server = spawn

let path = artifact {
	file: 'hello'
	link: (symlink 'link')
}

# Check in the directory.
let id = tg checkin $path

# Bundle the directory.
let bundle_id = tg bundle $id

# Get the bundled object.
let output = run tg object get $bundle_id --blobs --depth=inf --pretty
snapshot $output
