use ../../test.nu *

let server = spawn

# Return a simple string instead of a dict.
# Dict returns require proper tg::value::Data serialization.
let path = artifact {
	tangram.py: 'def default(): return "hello world"'
}

let output = tg build $path
snapshot $output
