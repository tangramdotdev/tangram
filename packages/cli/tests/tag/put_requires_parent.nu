use ../../test.nu *

# Tagging a nested specifier requires its parent to exist.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose alice | from json

let path = artifact "test"
let id = tg checkin $path

let output = tg tag company/team/pkg $id | complete
failure $output "tagging should fail when the parent does not exist"

tg group create company
tg group create company/team
tg tag company/team/pkg $id
let tag = tg tag get company/team/pkg | from json
assert equal $tag.specifier company/team/pkg

tg group create existing
let existing_team = tg group create existing/team | from json
tg tag existing/team/pkg $id

let reused_team = tg group get existing/team | from json
assert equal $reused_team.id $existing_team.id
assert equal $reused_team.specifier existing/team

let reused_tag = tg tag get existing/team/pkg | from json
assert equal $reused_tag.specifier existing/team/pkg

tg organization create org
tg group create org/team
tg tag org/team/pkg $id

let org_team = tg group get org/team | from json
assert equal $org_team.specifier org/team

let org_tag = tg tag get org/team/pkg | from json
assert equal $org_tag.specifier org/team/pkg

tg group create alice/team
tg tag alice/team/pkg $id

let user_team = tg group get alice/team | from json
assert equal $user_team.specifier alice/team

let user_tag = tg tag get alice/team/pkg | from json
assert equal $user_tag.specifier alice/team/pkg
