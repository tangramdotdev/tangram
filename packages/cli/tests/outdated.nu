use ../test.nu *

let server = spawn

# Write the artifact to a temp.
let path = artifact 'Hello, World!'

# Check in.
let id = tg checkin $path

# Tag it a couple times.
for version in ["1.0.0" "1.1.0" "2.0.0"] {
	tg tag put $"hello/($version)" $id
}

# Create another dependency that is already on the latest version.
let another = artifact 'Hello again!'
let another_id = tg checkin $another
tg tag put "another/1.0.0" $another_id

# Create something that uses it.
let path = artifact {
	tangram.ts: '
		import another from "another/^1.0";
		import hello from "hello/^1.0";
	'
}

tg checkin $path
let output = (
	do --env {
		cd $path
		tg outdated .
	}
	| str trim
)
snapshot $output '
	! hello/1.1.0 is latest compatible (newest hello/2.0.0) referrer tangram.ts
'

let output = (
	do --env {
		cd $path
		tg outdated --json --pretty .
	}
)
snapshot $output '
	[
	  {
	    "compatible": "hello/1.1.0",
	    "current": "hello/1.1.0",
	    "latest": "hello/2.0.0",
	    "referrer": {
	      "item": null,
	      "options": {
	        "path": "tangram.ts",
	      },
	    },
	  },
	]
'
