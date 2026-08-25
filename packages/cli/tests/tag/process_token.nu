use ../../test.nu *

# A process is subject to tag authorization when its sandbox has network access.

let server = server spawn

# Create a tag so the failure cannot be attributed to a missing tag.
let path = artifact "test"
let id = tg checkin $path
tg tag put test $id

let module = artifact {
	tangram.ts: '
		export default function () {
			return tg.run`
				if output=$(tg tag get test 2>&1); then
					exit 1
				fi
				printf "%s\\n" "$output" > "$TANGRAM_OUTPUT"
				printf "%s\\n" "$output"
			`
				.network()
				.sandbox()
				.then(tg.File.expect);
		}
	'
}
let output = tg run --sandbox --network=true $module | complete
success $output
assert ($output.stdout | str contains "failed to find the tag")
