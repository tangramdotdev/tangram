use ../../test.nu *

let server = spawn

let path = artifact {
	foo: {
		tangram.ts: '
			import foo from "../bar";
			export default () => foo();
		'
	}
	bar: {
		tangram.ts: '
			export default () => tg.assert(false, "error")
		'
	}
}

let output = do { cd $path; tg run ./foo }| complete
print $output
failure $output
let stderr = $output.stderr
let stderr = $stderr | str replace -ar 'id = (pcs_00[0-9a-z]{26}|[0-9]+)' 'id = PROCESS'
snapshot $stderr
