use ../../test.nu *

# A failing tg.assert in a path dependency causes the run to fail and produces the expected diagnostic on stderr.

let server = spawn

let path = artifact {
	foo: {
		tangram.ts: '
			import foo from "../bar";
			export default function () { return foo(); }
		'
	}
	bar: {
		tangram.ts: '
			export default function () { return tg.assert(false, "error"); }'
	}
}

let output = do { cd $path; tg run ./foo }| complete
print $output
failure $output
let stderr = $output.stderr
snapshot --normalize-ids $stderr
