use ../lib/test.nu *

# A process on a trusted runner whose verification search budget is smaller than the package spawns a child with an artifact another process built, so the runner must push the child's command, including that artifact and its module files, to the remote.

let root_token = random chars
let remote = server spawn --cloud --preserve-keys --name remote --config {
	advanced: { single_process: false },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	remotes: { default: { token: $created.token.token, trusted: true, url: $remote.url } },
	roles: [api indexer runner],
	runner: { id: $created.data.id, remote: "default", token: $created.token.token },
	tracing: { filter: 'tangram=info', stderr_format: 'json' },
	verification: {
		permissions: {
			final: {
				ancestor: { max_depth: 4, max_edges: 4, max_nodes: 4 }
				descendant: { max_depth: 4, max_edges: 4, max_nodes: 4 }
				subtree: { max_objects: 4 }
			}
			initial: {
				ancestor: { max_depth: 4, max_edges: 4, max_nodes: 4 }
				descendant: { max_depth: 4, max_edges: 4, max_nodes: 4 }
				subtree: { max_objects: 4 }
			}
		}
	},
}
let alice = tg --url $remote.url login --verbose --name alice | from json
let local = server spawn --name local --config {
	remotes: { default: { token: $alice.token, url: $remote.url } },
}

# The parent passes the directory one child builds to a second child defined in its own module, so the runner must push the second child's command with an artifact it received from another process.
let path = artifact {
	tangram.ts: 'import { child } from "./child.tg.ts";
export default async () => {
	const directory = await tg.build(make);
	return await tg.build(child, directory);
};
export const make = () => tg.directory({ file: tg.file("hello") });
',
	child.tg.ts: 'import { a } from "./a.tg.ts";
import { b } from "./b.tg.ts";
export const child = (directory: tg.Directory) => a + b;
',
	a.tg.ts: 'export const a = 1;
',
	b.tg.ts: 'import { c } from "./c.tg.ts";
export const b = c + 1;
',
	c.tg.ts: 'export const c = 1;
',
}

let output = tg --url $local.url build --remote $path | complete
success $output "the build should succeed"
assert equal ($output.stdout | str trim) "3"
