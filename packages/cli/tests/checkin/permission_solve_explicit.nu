use ../../test.nu *
use ../lib/checkin.nu checkin-output

# Solving an explicit unsolved artifact retrieves its permissions as part of the normal solve traversal.

let server = server spawn

let target = artifact {
	tangram.ts: 'export default 1;'
}
tg tag -p dependency/1.0.0 $target

let dependency_path = artifact {
	tangram.ts: 'import dependency from "dependency/^1"; export default dependency;'
}
let dependency = tg checkin --no-lock --no-solve --root $dependency_path | str trim
let metadata = tg metadata $dependency | from json
assert equal $metadata.subtree.solvable true "the dependency should be solvable"
assert equal $metadata.subtree.solved false "the dependency should be unsolved"

let dependencies = [$dependency] | to json
let directory = artifact {
	input: (file --xattrs { "user.tangram.dependencies": $dependencies } explicit)
}
let path = $directory | path join input
let output = checkin-output $server $path
assert equal $output.permissions [object_subtree] "the solved artifact should have subtree permission"
let object = tg get --depth=inf --pretty $output.reference
assert ($object | str contains '"tag": "dependency/1.0.0"') "the explicit dependency should be solved"
