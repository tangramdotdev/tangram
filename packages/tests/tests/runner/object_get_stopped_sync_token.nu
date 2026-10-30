use ../../util/test.nu *

# A remote object get must not wait on a sync whose service has stopped when the request carries the sync's token.
let root_token = random chars
let remote = server spawn --cloud --preserve-keys --name remote --config {
	advanced: { checkpoints: true, multi_server: true },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	remotes: { default: { token: $created.token.token, trusted: true, url: $remote.url } },
	roles: [api indexer runner],
	runner: { id: $created.data.id, remote: "default", token: $created.token.token },
}
let alice = tg --url $remote.url login --verbose --name alice | from json
let local = server spawn --name local --config {
	remotes: { default: { token: $alice.token, url: $remote.url } },
}

# The modules import each other so they are checked in as a graph.
let path = artifact {
	tangram.ts: 'import { value } from "./value.tg.ts";
export default () => value;
',
	value.tg.ts: 'import "./tangram.ts";
export const value = 42;
',
}
# Watch the sync services stop, before any sync starts.
let stopped_watch = tg --url $remote.url --token $root_token checkpoint watch sync.service.stopped | from json | get watch
let output = tg --url $local.url build --remote --cached=false --detach $path | complete
success $output "the build should start"
let process = $output.stdout | str trim | split row '?' | first
let output = tg --url $local.url wait --remote $process | complete
success $output "the build should finish"

# Read the graph with the token of the build's sync after the sync's service has stopped.
let data = tg --url $local.url process get --remote $process | from json
let token = $data.command.options.tokens.remote | first
let graph = $data.command.node | to json --raw | parse --regex '(?<id>gph_[0-9a-z]+)' | get id.0

# Wait for the service of the token's sync to stop.
let subject = $"syncs.($token | token body | get resource).service"
mut hit = 0
loop {
	let params = tg --url $remote.url --token $root_token checkpoint wait --timeout 30s sync.service.stopped $stopped_watch $hit | from json | get params
	tg --url $remote.url --token $root_token checkpoint continue sync.service.stopped $stopped_watch $hit
	if $params.subject == $subject {
		break
	}
	$hit += 1
}
tg --url $remote.url --token $root_token checkpoint unwatch sync.service.stopped $stopped_watch
let token = $token | url encode --all
let output = timeout 30s tg --url $local.url object get --remote --bytes $graph | complete
success $output "the object get without the token should succeed"
let output = timeout 30s tg --url $local.url object get --remote --bytes $'($graph)?tokens[remote][0]=($token)' | complete
success $output "the object get must not wait on the stopped sync"
