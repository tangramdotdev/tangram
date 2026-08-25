use ../../test.nu *

# A sandbox can authenticate as root and access tags only when it has network access.

let root_token = "root-token"
let server = server spawn --busybox --config {
	authentication: { root: { token: $root_token } },
}

let path = artifact "test"
let id = tg --token $root_token checkin $path
tg --token $root_token tag put test $id

let no_network = artifact {
	tangram.ts: '
		import busybox from "busybox";

		export default function () {
			return tg.run`
				if tg --token root-token tag get test > /dev/null 2> "$TANGRAM_OUTPUT"; then
					exit 1
				fi
			`
				.env(tg.build(busybox))
				.sandbox()
				.then(tg.File.expect);
		}
	',
}
let output = tg --token $root_token build $no_network | str trim | tg cat $in
assert ($output | str contains "network access is disabled for the origin sandbox")

let network = artifact {
	tangram.ts: '
		import busybox from "busybox";

		export default function () {
			return tg.run`tg --token root-token tag get test > "$TANGRAM_OUTPUT"`
				.env(tg.build(busybox))
				.sandbox()
				.network();
		}
	',
}
success (tg --token $root_token run --sandbox --network=true $network | complete)
