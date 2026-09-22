use ../lib/test.nu *

# The host reads exactly one xattr without interpreting Tangram shards.

let server = server spawn
let input = artifact (file --xattrs {
	"user.tangram.output.0": first
	"user.tangram.output.1": second
	"user.tangram.error": raw
	"user.tangram.error.invalid": unrelated
} input)
let path = artifact {
	tangram.ts: '
		export default async function (path: string) {
			const names = await tg.host.listxattr(path);
			for (const name of ["user.tangram.output.0", "user.tangram.output.1", "user.tangram.error", "user.tangram.error.invalid"]) {
				tg.assert(names.includes(name));
			}
			tg.assert(!names.includes("user.tangram.output"));
			tg.assert(await tg.host.getxattr(path, "user.tangram.output") === null);
			const shard = await tg.host.getxattr(path, "user.tangram.output.0");
			tg.assert(shard !== null && tg.encoding.utf8.decode(shard) === "first");
			const raw = await tg.host.getxattr(path, "user.tangram.error");
			tg.assert(raw !== null && tg.encoding.utf8.decode(raw) === "raw");
		}
	'
}
let output = tg run --no-sandbox $path --arg-string $input | complete
success $output
