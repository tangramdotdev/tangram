use ../lib/test.nu *

# A module file checked in by its identifier can be built directly and returns its default export value.

let local = server spawn

let path = artifact {
    file.tg.ts: r#'export default function () { return "hello, world!"; }'#
};

let id = tg checkin --no-tokens ($path + '/file.tg.ts') | referent node
tg index

let output = tg build $id
snapshot $output '"hello, world!"'
