use ../../test.nu *

let server = spawn

# Check that we cannot destructively checkin artifacts with external paths.
let path = artifact {
	foo: {
		tangram.ts: 'import * as bar from "../bar";'
	}
	bar: {
		tangram.ts: ''
	}
}
let output = tg checkin --destructive ($path | path join 'foo') --ignore=false | complete
failure $output "destructive checkin with external path dependencies should fail"
