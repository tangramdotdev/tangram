use ../../test.nu *

let server = spawn

let path = artifact {
	file: 'hello'
	link: (symlink 'link')
}

# Check in the directory.
let id = tg checkin $path | complete | get stdout | str trim

# Bundle the directory.
let bundle_id = tg bundle $id | complete | get stdout | str trim

# Get the bundled object.
let output = tg object get $bundle_id --blobs --depth=inf --pretty | complete
success $output

snapshot $output.stdout
