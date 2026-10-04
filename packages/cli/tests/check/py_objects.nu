use ../lib/test.nu *

let local = server spawn
let dependency = artifact {'hello.txt': 'hello'}
tg tag put -p py-assets/1.0.0 $dependency
rm --recursive $dependency
let path = artifact {
    'main.tg.py': '
        # /// script
        # [tool.tangram.imports.local]
        # specifier = "./local.txt"
        # attributes = { type = "file" }
        # [tool.tangram.imports.remote]
        # specifier = "py-assets/^1"
        # attributes = { get = "hello.txt", type = "file" }
        # [tool.tangram.imports.directory]
        # specifier = "py-assets/^1"
        # attributes = { type = "directory" }
        # ///
        from typing import assert_type
        from local import default as local
        from remote import default as remote
        from directory import default as directory
        assert_type(local, tg.File)
        assert_type(remote, tg.File)
        assert_type(directory, tg.Directory)
        async def default() -> str:
            return await remote.text()
    '
    'local.txt': 'local'
}
let file = $path | path join main.tg.py
success (tg check $file | complete)
let checked = tg checkin $file
let source = open --raw $file
$source | str replace 'assert_type(remote, tg.File)' 'value: tg.Directory = remote' | save --force $file
let output = tg check $file | complete
failure $output
assert ($output.stderr | str contains 'File')
assert ($output.stderr | str contains 'Directory')
rm --recursive $path
success (tg check $checked | complete)
let output = tg run $checked | complete
success $output
assert equal ($output.stdout | str trim) '"hello"'

# Every object kind shares the runtime's generated module and the client's types.
for case in [
    {kind: artifact, expression: 'tg.file("hello")', annotation: 'tg.Directory | tg.File | tg.Symlink', class: Artifact}
    {kind: blob, expression: 'tg.blob("hello")', annotation: 'tg.Blob', class: Blob}
    {kind: command, expression: 'tg.command({ executable: "echo" })', annotation: 'tg.Command', class: Command}
    {kind: directory, expression: 'tg.directory()', annotation: 'tg.Directory', class: Directory}
    {kind: error, expression: 'tg.error("example")', annotation: 'tg.Error', class: Error}
    {kind: file, expression: 'tg.file("hello")', annotation: 'tg.File', class: File}
    {kind: graph, expression: 'tg.graph({ nodes: [] })', annotation: 'tg.Graph', class: Graph}
    {kind: object, expression: 'tg.file("hello")', annotation: 'tg.Object', class: Object}
    {kind: symlink, expression: 'tg.symlink("target")', annotation: 'tg.Symlink', class: Symlink}
] {
    let source = ([
        '# /// script'
        '# [tool.tangram.imports.asset]'
        '# specifier = "asset"'
        ('# attributes = { type = "' + $case.kind + '" }')
        '# ///'
        'from typing import assert_type'
        'from asset import default as asset'
        ('assert_type(asset, ' + $case.annotation + ')')
        ('assert isinstance(asset, tg.' + $case.class + ')')
        'def default() -> int:'
        '    return 42'
    ] | str join (char nl))
    let builder = artifact {
        'tangram.ts': ('export default async function () { const object = await ' + $case.expression + '; return tg.directory({ "tangram.py": tg.file(' + ($source | to json -r) + ').module("py").dependency("asset", { node: object }) }); }')
    }
    let module = tg run $builder
    success (tg check $module | complete)
    let output = tg run $module | complete
    success $output
    assert equal ($output.stdout | str trim) '42'
}
