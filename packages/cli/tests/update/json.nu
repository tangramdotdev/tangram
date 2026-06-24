use ../../test.nu *

# The json flag prints updates as structured data.

let server = spawn

let a1 = artifact { tangram.ts: 'export default function () { return "a1"; }' }
tg tag a/1.0.0 $a1

let root = artifact {
	tangram.ts: 'import a from "a/^1";'
}
tg checkin $root

let a2 = artifact { tangram.ts: 'export default function () { return "a2"; }' }
tg tag a/1.1.0 $a2
tg index

let output = do --env { cd $root; tg update . --json } | complete
success $output

let updates = $output.stdout | from json
assert equal ($updates | length) 1 "there should be one update"
assert equal $updates.0.old "a/1.0.0" "the old tag should be reported"
assert equal $updates.0.new "a/1.1.0" "the new tag should be reported"
assert equal $updates.0.pattern.item "a/^1" "the pattern should be reported"
