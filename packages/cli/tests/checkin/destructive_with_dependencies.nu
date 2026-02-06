use ../../test.nu *

let server = spawn

# Create a file artifact that will be referenced as a dependency.
let dep_id = tg put 'tg.file("dependency")'

# Create a directory containing a file with a dependency xattr pointing to
# the artifact above. This simulates the output of a build that uses tgld:
# the linked binary has user.tangram.dependencies referencing external
# artifacts like the interpreter, libraries, etc.
let dep_xattr = [$dep_id] | to json -r
let path = artifact {
	myfile: (file --xattrs { 'user.tangram.dependencies': $dep_xattr } 'hello world')
}

# Destructive checkin. The index task runs in the background and its error
# is logged but not propagated to the CLI.
let id = tg checkin --destructive --ignore=false $path

# Index
tg index

# Read back the file to confirm the checkin itself succeeded.
let contents = tg read $"($id)?path=myfile"
assert equal $contents 'hello world'
