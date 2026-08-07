use ../../test.nu *

# Cleaning a process must delete its grants. Otherwise a grant revoked while the process is absent from the index leaves the materialized subtree grant behind, and re-putting the process restores the access the revocation removed.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose alice | from json
let eve = tg login --verbose eve | from json

# Alice builds a private process that builds a child process.
let path = artifact { tangram.ts: 'export default function () { return tg.build(child); } export function child() { return 42; }' }
let parent = tg --token $alice.token build --detach $path | str trim
tg --token $alice.token wait $parent
tg --token $alice.token index
let child = (tg --token $alice.token get $parent | from json | get children | get 0.process)

# Alice grants Eve the parent node and the child subtree, which materializes a subtree grant on the parent.
tg --token $alice.token grant $eve.user.id process_node $parent | ignore
tg --token $alice.token grant $eve.user.id process_subtree $child | ignore
tg --token $alice.token index

let granted = tg --token $eve.token get $parent | complete
success $granted "Eve should read the parent she was granted"

# Save both processes so that they can be restored after the clean.
let parent_data = tg --token $alice.token process get $parent
let child_data = tg --token $alice.token process get $child

# Clean deletes both processes from the index.
tg clean

# Alice revokes Eve's parent node grant while the parent is absent from the index.
tg --token $alice.token revoke $eve.user.id process_node $parent
tg --token $alice.token index

# Restoring the processes must not restore Eve's access.
$child_data | tg --token $alice.token process put $child
$parent_data | tg --token $alice.token process put $parent
tg --token $alice.token index

let revoked = tg --token $eve.token get $parent | complete
failure $revoked "cleaning must delete the process's grants so that re-putting the process does not undo the revocation"
