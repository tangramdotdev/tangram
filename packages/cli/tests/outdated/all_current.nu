use ../../test.nu *

# Outdated prints nothing when every dependency is on its latest version.

let server = spawn

let dep = artifact { tangram.ts: 'export default function () { return "dep"; }' }
tg tag dep/1.0.0 $dep

let root = artifact {
	tangram.ts: 'import dep from "dep/^1";'
}
tg checkin $root

let output = do --env { cd $root; tg outdated . } | complete
success $output
assert equal $output.stdout "" "outdated should print nothing"
