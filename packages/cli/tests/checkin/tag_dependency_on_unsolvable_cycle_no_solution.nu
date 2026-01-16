use ../../test.nu *

let server = spawn

# Tag the dependencies.
let c1_path = artifact {
	tangram.ts: '
        // c/1.0.0
        import * as foo from "./foo.tg.ts";
    ',
    foo.tg.ts: '
        import * as root from "./tangram.ts";
    '
}
tg tag c/1.0.0 ($c1_path | path join 'foo.tg.ts')

let c2_path = artifact {
    tangram.ts: '
        // c/2.0.0
        import * as foo from "./foo.tg.ts";
    ',
    foo.tg.ts: '
        import * as root from "./tangram.ts";
    '
}
tg tag c/2.0.0 ($c2_path | path join 'foo.tg.ts')
tg index

let a_path = artifact {
	tangram.ts: '
		import * as c from "c/^1"
	'
}
tg tag a/1.0.0 $a_path

let b_path = artifact {
	tangram.ts: '
		import * as c from "c/^2"
	'
}
tg tag b/1.0.0 $b_path

let path = artifact {
	tangram.ts: '
		import * as a from "a/*";
		import * as b from "b/*";
	'
}

let output = tg checkin $path | complete
failure $output "the checkin should fail when no solution exists"

let stdout = $output.stdout | str replace -a $path ''
let stderr = $output.stderr | str replace -a $path ''

snapshot -n stderr $stderr
snapshot -n stdout $stdout
