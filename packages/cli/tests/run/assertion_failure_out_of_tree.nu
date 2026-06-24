use ../../test.nu *

# A failing tg.assert in a process spawned via tg.run from an out-of-tree dependency causes the run to fail and produces the expected diagnostic on stderr.

let server = spawn

let path = artifact {
	foo: {
		tangram.ts: '
			import bar from "../bar";
			export default function () { return tg.run(bar); }
		'
	}
	bar: {
		tangram.ts: '
			export default function () { return tg.assert(false); }
		'
	}
}

let output = do { cd $path; tg run ./foo }| complete
failure $output
let stderr = $output.stderr
let stderr = $stderr | redact | normalize_ids
snapshot $stderr
