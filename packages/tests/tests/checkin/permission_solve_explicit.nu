use ../../util/test.nu *
use ../../util/checkin.nu reference-access

# Solving an explicit unsolved artifact produces a reference that grants another user read access to the solved subtree.

let local = server spawn --config {
	authentication: { users: { providers: { insecure: true } } }
}
let alice = tg login --verbose --name alice | from json
let eve = tg login --verbose --name eve | from json

let target = artifact {
	tangram.ts: 'export default 1;'
}
tg --token $alice.token tag -p dependency/1.0.0 $target

let dependency_path = artifact {
	tangram.ts: 'import dependency from "dependency/^1"; export default dependency;'
}
let dependency = tg --token $alice.token checkin --no-tokens --no-lock --no-solve --root $dependency_path | referent node
let metadata = tg --token $alice.token metadata $dependency | from json
assert equal $metadata.subtree.solvable true "the dependency should be solvable"
assert equal $metadata.subtree.solved false "the dependency should be unsolved"

let dependencies = [$dependency] | to json
let directory = artifact {
	input: (file --xattrs { "user.tangram.dependencies": $dependencies } explicit)
}
let path = $directory | path join input
let reference = tg --token $alice.token checkin --no-lock --root $path | str trim
assert equal (reference-access $reference --token $eve.token) subtree "the reference should grant the solved subtree"
let object = tg --token $eve.token get --depth=inf --pretty $reference
assert ($object | str contains '"tag": "dependency/1.0.0"') "the explicit dependency should be solved"
