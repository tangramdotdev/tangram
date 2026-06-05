use ../../test.nu *

let server = spawn

# Create a directory with nested structure.
let path = artifact {
	foo: {
		bar: {
			file.txt: 'Hello, World!'
		}
	}
}

# Check in and tag the directory.
let dir_id = tg checkin $path
tg tag test $dir_id

# Get the nested file using the path option with a resolved tag reference.
let output = tg --no-quiet get -R --pretty "test?get=foo/bar/file.txt" | complete

# Verify the output is a file ID.
snapshot $output.stdout '
	tg.file({
	  "contents": blb_01b7mbpwtwk7vv4n50rn5cab07zcxvpq8d7pggwc2g54d0cjd8nnm0,
	})

'
snapshot $output.stderr '
	info test?get=foo/bar/file.txt

'
