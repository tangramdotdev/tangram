use ../../test.nu *

# A failing tg.assert in an imported module causes the run to fail and produces the expected diagnostic on stderr.

let server = spawn

let path = artifact {
	tangram.ts: '
		import foo from "./foo.tg.ts";
		export default () => foo();
	',
	foo.tg.ts: '
		export default () => tg.assert(false);
	',
}

let output = do { cd $path; tg run } | complete
failure $output
let stderr = $output.stderr
let stderr = $stderr | redact | normalize_ids
snapshot $stderr
