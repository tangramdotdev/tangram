use ../lib/test.nu *
use ../lib/vfs.nu

vfs skip_unless_supported

# A sandbox can read a directory assembled from imported artifacts.
let root_token = random chars
let remote = server spawn --preserve-keys --name remote --config {
	advanced: { single_process: false },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
	verification: { permissions: { initial: false, final: false } },
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	remotes: { default: { token: $created.token.token, trusted: true, url: $remote.url } },
	roles: [api indexer runner],
	runner: { id: $created.data.id, remote: "default", token: $created.token.token },
	verification: { permissions: { initial: false, final: false } },
	vfs: true,
}
let alice = tg --url $remote.url login --verbose --name alice | from json
let local = server spawn --name local --config {
	verification: { permissions: { initial: false, final: false } },
	remotes: { default: { token: $alice.token, url: $remote.url } },
}

let path = artifact {
 tangram.ts: '
  import "./cycle.tg.ts";
  import data from "./data" with { type: "directory" };
  import manifest from "./manifest" with { type: "file" };
  export default async () => {
   const source = await tg.directory({ manifest, packages: tg.directory({ data }) });
   await tg.build(child, source);
   return 1;
  };
  export async function child(source: tg.Directory) {
   await tg.run({
    executable: "/bin/sh",
    args: ["-u", "-c", tg`value=; read value < ${source}/manifest; test "$value" = hello || exit 1; value=; read value < ${source}/packages/data/file; test "$value" = hello`],
    host: tg.host.current,
   }).sandbox(true);
   return 1;
  }
 ',
 cycle.tg.ts: 'import "./tangram.ts";',
 manifest: "hello\n",
 data: { file: "hello\n" },
}
let output = timeout 45s tg --url $local.url build --remote $path | complete
success $output "the sandbox should read the assembled directory"
assert equal ($output.stdout | str trim) "1"
