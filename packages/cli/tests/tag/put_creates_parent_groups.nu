use ../../test.nu *

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose alice | from json

let path = artifact "test"
let id = tg checkin $path

tg tag company/team/pkg $id

let company = tg group get company | from json
assert equal $company.specifier company

let team = tg group get company/team | from json
assert equal $team.specifier company/team

let tag = tg tag get company/team/pkg | from json
assert equal $tag.specifier company/team/pkg

let existing_team = tg group create existing/team | from json
tg tag existing/team/pkg $id

let reused_team = tg group get existing/team | from json
assert equal $reused_team.id $existing_team.id
assert equal $reused_team.specifier existing/team

let reused_tag = tg tag get existing/team/pkg | from json
assert equal $reused_tag.specifier existing/team/pkg

tg organization create org
tg tag org/team/pkg $id

let org_team = tg group get org/team | from json
assert equal $org_team.specifier org/team

let org_tag = tg tag get org/team/pkg | from json
assert equal $org_tag.specifier org/team/pkg

tg tag alice/team/pkg $id

let user_team = tg group get alice/team | from json
assert equal $user_team.specifier alice/team

let user_tag = tg tag get alice/team/pkg | from json
assert equal $user_tag.specifier alice/team/pkg
