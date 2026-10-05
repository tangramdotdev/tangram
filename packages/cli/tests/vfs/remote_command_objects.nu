use ../lib/test.nu *
use ../lib/vfs.nu

vfs skip_unless_supported

# Read remote command objects through the VFS without requiring local authorization for an unused checkout shortcut.

let root_token = random chars
let remote = server spawn --cloud --preserve-keys --name remote --config {
	advanced: { single_process: false },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	vfs: true,
	remotes: { default: { token: $created.token.token, trusted: true, url: $remote.url } },
	roles: [api indexer runner],
	runner: { id: $created.data.id, remote: "default", token: $created.token.token },
	tracing: { filter: 'tangram=info', stderr_format: 'json' },
	verification: {
		permissions: {
			final: {
				ancestor: { max_depth: 1, max_edges: 1, max_nodes: 1 }
				descendant: { max_depth: 1, max_edges: 1, max_nodes: 1 }
				subtree: { max_objects: 1 }
			}
			initial: {
				ancestor: { max_depth: 1, max_edges: 1, max_nodes: 1 }
				descendant: { max_depth: 1, max_edges: 1, max_nodes: 1 }
				subtree: { max_objects: 1 }
			}
		}
	},
}
let alice = tg --url $remote.url login --verbose --name alice | from json
let local = server spawn --name local --config {
	remotes: { default: { token: $alice.token, url: $remote.url } },
}

# The two modules import each other so they are checked in as a graph, and the parent reads its child's directory output.
let path = artifact {
	tangram.ts: 'import "./args.tg.ts";
export default async () => {
	const directory = await tg.build(child);
	return await directory.get("file");
};
export const child = () => tg.directory({ file: tg.file("hello") });
',
	args.tg.ts: 'import "./util.tg.ts";
export const args = 1;
',
	util.tg.ts: 'import "./args.tg.ts";
export const util = 2;
',
}

let output = tg --url $local.url build --remote $path | complete
success $output "the build should succeed"
