use ../../test.nu *
use ../lib/permissions.nu *

# A user can claim a top-level group and a nested group, and the intermediate parent of a nested group is auto-created and gettable.

let server = spawn --config { authentication: true }
tg user login alice
let alice = current_token

tg --token $alice group create project
tg --token $alice group get project
tg --token $alice grants list project
tg --token $alice group create project/pkg

tg --token $alice group create nested/claimed
tg --token $alice group get nested
tg --token $alice group get nested/claimed
