use ../../test.nu *

let server = spawn

let path = artifact {
    file.tg.ts: r#'export default () => "hello, world!"'#
};

let id = tg checkin ($path + '/file.tg.ts')
tg index

let output = tg build $id
snapshot $output '"hello, world!"'
