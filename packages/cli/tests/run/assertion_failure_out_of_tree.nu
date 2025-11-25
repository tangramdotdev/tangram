use ../../test.nu *

let server = spawn

let path = artifact {
	foo: {
		tangram.ts: '
			import bar from "../bar";
			export default () => tg.run(bar);
		'
	}
	bar: {
		tangram.ts: '
			export default () => tg.assert(false);
		'
	}
}

let output = do { cd $path; tg run ./foo }| complete
failure $output
let stderr = $output.stderr | lines | skip 1 | str join "\n"
let stderr = $stderr | str replace -ar 'pcs_00[0-9a-z]{26}' 'PROCESS'
snapshot $stderr
