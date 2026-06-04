use ../../test.nu *

# A destructive checkin of a package with a path dependency outside the checked-in root fails.

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
