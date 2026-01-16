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

# Check in the directory.
let dir_id = tg checkin $path

# Get the nested file using the path option.
let output = tg get --pretty $"($dir_id)?path=foo/bar/file.txt" | complete

snapshot $output.stdout '
	tg.file({
	  "contents": blb_01b7mbpwtwk7vv4n50rn5cab07zcxvpq8d7pggwc2g54d0cjd8nnm0,
	})

'
snapshot $output.stderr '
	info fil_0161g41yea30wb48ta1dt778xfgfxrm09e1p1dznezech34e27tp60?id=dir_01pwzdtbwm2m6e0a5fm77bb066dx78qhee6dmj7qmnr688hc1yagcg&path=foo/bar/file.txt

'