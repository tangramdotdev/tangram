use ../../test.nu *

# A module file checked in and referenced by its object id can be run directly after indexing.

let server = spawn

let path = artifact {
    file.tg.ts: r#'export default () => "hello, world!"'#
};

let id = tg checkin ($path + '/file.tg.ts')
tg index

let output = tg run $id
snapshot $output '"hello, world!"'