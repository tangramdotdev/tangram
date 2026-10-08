use ../lib/test.nu *

# A trusted runner that dies while pushing a finished child's output to the remote cleans up its previous sandboxes after it restarts and accepts new work, when neither the remote nor the runner may search for authorization.

let searches = {
	ancestor: { max_depth: 0, max_edges: 0, max_nodes: 0 }
	descendant: { max_depth: 0, max_edges: 0, max_nodes: 0 }
	subtree: { max_depth: 0, max_objects: 0, max_processes: 0 }
}
let root_token = random chars
let remote = server spawn --cloud --preserve-keys --name remote --config {
	advanced: { checkpoints: true, single_process: false },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
	scheduler: { runner_ttl: 300 },
	verification: { permissions: { initial: $searches, final: $searches } },
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --preserve-keys --config {
	remotes: { default: { token: $created.token.token, trusted: true, url: $remote.url } },
	roles: [api indexer runner],
	runner: { cpus: 1, id: $created.data.id, remote: "default", token: $created.token.token },
	verification: { permissions: { initial: $searches, final: $searches } },
}
let alice = tg --url $remote.url login --verbose --name alice | from json
let local = server spawn --name local --config {
	remotes: { default: { token: $alice.token, url: $remote.url } },
}

# The child's output contains a blob that only the runner creates.
let path = artifact {
	tangram.ts: 'import { child } from "./child.tg.ts";
export default async () => await tg.build(child);
',
	child.tg.ts: 'import "./tangram.ts";
export const child = async () => await tg.file("restart during output push");
',
}

# Hold the remote's store of the output blob, so the runner dies with the child's output push in progress.
let blob = tg --url $local.url put --no-tokens 'tg.blob("restart during output push")' | referent node
let store = tg --url $remote.url --token $root_token checkpoint watch sync.get.store.object --params ({ id: $blob } | to json --raw) | from json | get watch
let id = tg --url $local.url build --remote --detach $path | referent node
success (timeout 60s tg --url $remote.url --token $root_token checkpoint wait sync.get.store.object $store 0 | complete) "the runner must push the child's output"

let pid = open ($runner.directory | path join 'lock') | into int
kill --signal 9 $pid
wait_until { ps | where pid == $pid | is-empty } "the runner must exit"
tg --url $remote.url --token $root_token checkpoint unwatch sync.get.store.object $store
let runner = server start $runner

let trivial = artifact { tangram.ts: 'export default () => 42' }
success (timeout 60s tg --url $local.url build --remote $trivial | complete) "the restarted runner must accept work after cleanup"
let data = tg --url $remote.url --token $root_token get $id | from json
assert equal $data.status "finished"
