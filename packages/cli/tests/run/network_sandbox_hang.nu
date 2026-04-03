use ../../test.nu *

# Regression test: a sandboxed JS process that spawns a child with
# `network: true` via `tg.run` hangs indefinitely. The server creates
# the internal HTTP socket in the host network namespace, but the
# network-enabled sandbox runs with a different network configuration
# that prevents the JS process from connecting to it.
#
# Trigger: tg.run({ ..., network: true, checksum: "sha256:any" })
# inside a tg.build (sandboxed JS function).

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			return await tg.build(inner);
		};

		export const inner = async () => {
			return await tg.run({
				executable: "/bin/sh",
				args: ["-c", tg`echo hello > ${tg.output}`],
				checksum: "sha256:any",
				network: true,
			});
		};
	',
}

let output = tg build $path | complete
success $output
