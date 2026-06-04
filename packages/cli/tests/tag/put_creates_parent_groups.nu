use ../../test.nu *

let server = spawn

let path = artifact "test"
let id = tg checkin $path

tg tag company/team/pkg $id

let company = tg group get company | from json
assert equal $company.specifier company

let team = tg group get company/team | from json
assert equal $team.specifier company/team

let tag = tg tag get company/team/pkg | from json
assert equal $tag.specifier company/team/pkg
