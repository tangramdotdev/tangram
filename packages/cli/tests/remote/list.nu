use ../../test.nu *

let server = spawn -c {
	remotes: [
		{ name: "origin", url: "http://localhost:9999" }
	]
}

let output = tg remote list | complete
success $output
snapshot -n list ($output.stdout | str trim) '[{"name":"origin","url":"http://localhost:9999"}]'
