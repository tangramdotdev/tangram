use ../../util/test.nu *

# The builds of a publish follow --public: without it, an anonymous client cannot read the build tag or its process, and with it, an anonymous client can read them through the public build tag, even after the process's own temporary public permission expires.

let host = current_host
let root_token = random chars
let remote = server spawn --name remote --now '2026-01-01T00:00:00Z' --config {
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } }
	process: { permission_ttl: 60 }
	scheduler: { runner_ttl: 300 }
}
let alice = tg --url $remote.url login --verbose --name alice | from json
let local = server spawn --name local --config {
	remotes: { default: { url: $remote.url, token: $alice.token } },
}

# Create a configuration for an anonymous client.
let anonymous = mktemp
{} | to json | save -f $anonymous

# Without --public, the build tag exists for the publisher, but an anonymous client cannot read it or its process.
let path = artifact {
	tangram.ts: '
		export default function () { return "private"; }

		export let metadata = {
			tag: "builds-private/1.0.0",
			builds: true,
		};
	'
}
let output = tg --url $local.url publish --host $host $path | complete
success $output "the publish should succeed"
let tag = $'builds-private/builds/1.0.0/default/($host)'
let process = tg --url $remote.url --token $alice.token tag get $tag | from json | get target.id
assert ($process | str starts-with 'pcs_') "the build tag should target a process for the publisher"
let output = with-env { TANGRAM_CONFIG: $anonymous } { tg --url $remote.url tag get $tag | complete }
failure $output "an anonymous client should not read a private build tag"
let output = with-env { TANGRAM_CONFIG: $anonymous } { tg --url $remote.url process output $process | complete }
failure $output "an anonymous client should not read a private build process"

# With --public, an anonymous client can read the build tag and the output of its process.
let path = artifact {
	tangram.ts: '
		export default function () { return "public"; }

		export let metadata = {
			tag: "builds-public/1.0.0",
			builds: true,
		};
	'
}
let output = tg --url $local.url publish --public --host $host $path | complete
success $output "the publish should succeed"
let tag = $'builds-public/builds/1.0.0/default/($host)'
let process = with-env { TANGRAM_CONFIG: $anonymous } { tg --url $remote.url tag get $tag | from json | get target.id }
let output = with-env { TANGRAM_CONFIG: $anonymous } { tg --url $remote.url process output $process | from json }
assert equal $output "public" "an anonymous client should read the output of a public build"

# After the process's temporary public permission expires, the public build tag still gives an anonymous client the process and its output.
advance_time $remote 2min
tg --url $remote.url --token $root_token index
let target = with-env { TANGRAM_CONFIG: $anonymous } { tg --url $remote.url tag get $tag | from json | get target.id }
assert equal $target $process "the public build tag should still target the process"
let output = with-env { TANGRAM_CONFIG: $anonymous } { tg --url $remote.url process output $process | complete }
success $output "an anonymous client should read the output of a public build through its public tag after the process's permission expires"
assert equal ($output.stdout | from json) "public"
