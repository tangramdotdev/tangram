use ../lib/test.nu *

# The installed Python client uses its default host and the inherited Tangram URL.

const python_path = path self '../../../../.venv/bin/python'
const script_path = path self 'client.py'

let local = server spawn
let xattrs = artifact (file --xattrs {
	"user.example": ''
	"user.tangram.output.0": '{"PATH":'
	"user.tangram.output.1": '"bin"}'
} '')

let output = ^$python_path $script_path $xattrs | complete
success $output
assert equal ($output.stdout | str trim) 'hello from the Python client' 'the Python client should receive the server health'
