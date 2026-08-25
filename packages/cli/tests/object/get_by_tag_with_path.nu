use ../../test.nu *

# Getting an object by a tag reference with a get path option resolves to the nested file and reports the resolved referent on stderr.

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
let output = tg --no-quiet get --pretty "test?follow=true&get=foo/bar/file.txt" | complete

# Verify the output is a file ID.
snapshot $output.stdout '
	tg.file({
	  "contents": blb_01b7mbpwtwk7vv4n50rn5cab07zcxvpq8d7pggwc2g54d0cjd8nnm0,
	})

'
assert ($output.stderr | str contains "fil_0161g41yea30wb48ta1dt778xfgfxrm09e1p1dznezech34e27tp60") "the referent should include the file ID"
assert ($output.stderr | str contains "id=dir_01dsqh18mkjvps1bsynv883g6h70xtem9p12yexpbn0dcxz5xygnsg") "the referent should include the directory ID"
assert ($output.stderr | str contains "location=local") "the referent should include its location"
assert ($output.stderr | str contains "path=foo/bar/file.txt") "the referent should include its path"
assert ($output.stderr | str contains "tag=test") "the referent should include its tag"
assert ($output.stderr | str contains "tokens[local]") "the referent should include its token"
