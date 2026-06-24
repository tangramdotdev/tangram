use ../../../test.nu *

# Minimal, deterministic reproduction of the nested-sandbox pipe data-loss bug.
#
# A sandboxed parent (tg build) spawns a sandboxed child, pipes its stdout, and
# reads it. The child's stdout is lost: readAll returns 0 bytes instead of 2,
# even though the child exits 0.
#
# The bug requires BOTH levels of sandboxing:
#   - parent sandboxed:  run via `tg build` (not `tg run`)
#   - child sandboxed:   `.sandbox()` on the spawn
# Drop either one and the read succeeds. This is the mechanism behind the flaky
# js/process/stdio_read_all.nu and stdio_write_all.nu.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let child = await tg.spawn`printf hi`.stdout("pipe").sandbox();
			return (await child.stdout.readAll()).length;
		}
	'
}

assert ((tg build $path | from json) == 2) "nested-sandbox pipe read lost the child stdout"
