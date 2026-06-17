use ../../test.nu *

# A process_node grant confers the node but not its child processes, so a node grantee reads the parent while its children stay masked.

let server = spawn --config { authentication: true }

let alice = tg login --verbose alice | from json
let eve = tg login --verbose eve | from json

# Alice builds a private process that builds a child process.
let path = artifact { tangram.ts: 'export default () => tg.build(child); export const child = () => 42;' }
let parent = tg --token $alice.token build --detach $path | str trim
tg --token $alice.token wait $parent
tg --token $alice.token index
let child = (tg --token $alice.token get $parent | from json | get children | get 0.process)

# Alice grants Eve only the process node of the parent.
tg --token $alice.token grant $eve.user.id process_node $parent | ignore

# Eve can read the parent process node she was granted.
let parent_read = tg --token $eve.token get $parent | complete
success $parent_read "Eve should read the parent process node she was granted."

# Eve cannot read the child process: a node grant does not confer the subtree.
let child_read = tg --token $eve.token get $child | complete
failure $child_read "a process_node grant must not confer read on child processes."
