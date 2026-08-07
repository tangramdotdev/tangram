use ../../test.nu *

# A process may not list watches from its sandbox.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default function () {
			return tg.run`
				if tg watch list > /dev/null 2> "$TANGRAM_OUTPUT"; then
					exit 1
				fi
			`
				.sandbox()
				.then(tg.File.expect);
		}
	'
}

let output = tg build $path | str trim | tg cat $in
assert ($output | str contains "the operation is not available from a sandbox")
