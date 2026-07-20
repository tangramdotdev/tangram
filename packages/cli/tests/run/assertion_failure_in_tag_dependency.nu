use ../../test.nu *

# A failing tg.assert in a tagged dependency causes the run to fail and produces the expected diagnostic on stderr.

let server = spawn

# Create and tag the foo dependency.
let foo_path = artifact {
	tangram.ts: '
		export default function () { return tg.assert(false, "error in foo"); }
	'
}
tg tag foo $foo_path

let path = artifact {
	tangram.ts: '
		import foo from "foo";
		export default function () { return foo(); }
	'
}

let output = do { cd $path; tg run } | complete
failure $output
let stderr = $output.stderr
snapshot --normalize-ids $stderr
