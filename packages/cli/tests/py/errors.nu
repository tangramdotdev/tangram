use ../lib/test.nu *

let local = server spawn
let path = artifact {
    'tangram.py': 'pass'
    'main.tg.py': '
        from .helper import fail
        def run():
            fail()
    '
    'helper.tg.py': '
        def fail():
            try:
                raise ValueError("inner")
            except ValueError as error:
                raise RuntimeError("outer") from error
    '
}
let output_path = mktemp
let output = with-env {TANGRAM_OUTPUT: $output_path} {
    tg py --export run ($path | path join main.tg.py) | complete
}
failure $output
assert ($output.stderr | str contains 'RuntimeError: outer')
assert ($output.stderr | str contains 'helper.tg.py')
let outcome = open --raw $output_path | from json
assert equal $outcome.exit 1
assert equal $outcome.error.message 'RuntimeError: outer'
assert equal $outcome.error.location.file.value.kind 'py'
assert ($outcome.error.location.file.value.referent.node | str ends-with 'helper.tg.py')
assert equal $outcome.error.source.node.message 'ValueError: inner'

let output = tg py --export missing ($path | path join main.tg.py) | complete
failure $output
assert ($output.stderr | str contains 'failed to find the export named missing')

let syntax = artifact {'main.tg.py': 'def broken('}
let output_path = mktemp
let output = with-env {TANGRAM_OUTPUT: $output_path} { tg py ($syntax | path join main.tg.py) | complete }
failure $output
let outcome = open --raw $output_path | from json
assert equal $outcome.error.location.range.start.line 0

# Checkin parser errors include the source path and the exact token location.
let syntax = artifact {'main.tg.py': 'pass
x = )
'}
let output = tg checkin ($syntax | path join main.tg.py) | complete
failure $output
assert ($output.stderr | str contains 'main.tg.py:2:5')
assert ($output.stderr | str contains 'failed to parse the python module')

let syntax = artifact {'main.tg.py': 'pass
x = "🙂"; )
'}
let output = tg run ($syntax | path join main.tg.py) | complete
failure $output
assert ($output.stderr | str contains 'main.tg.py:2:13')

let exit = artifact {'main.tg.py': 'raise SystemExit(7)'}
let output = tg py ($exit | path join main.tg.py) | complete
assert equal $output.exit_code 7

for code in ['2**100', '-(2**100)'] {
    let exit = artifact {'main.tg.py': ('raise SystemExit(' + $code + ')')}
    let output = tg py ($exit | path join main.tg.py) | complete
    assert equal $output.exit_code 255
}

let plain = artifact {'main.py': 'print("wrong extension")'}
let output = tg py ($plain | path join main.py) | complete
failure $output
assert ($output.stderr | str contains 'expected tangram.py or a .tg.py module')

let unicode = artifact {'main.tg.py': '
    def run():
        text = "🙂"; return 1 / 0
'}
let output_path = mktemp
let output = with-env {TANGRAM_OUTPUT: $output_path} {
    tg py --export run ($unicode | path join main.tg.py) | complete
}
failure $output
let outcome = open --raw $output_path | from json
assert equal $outcome.error.location.range.start {line: 1, character: 24}

# Runtime parser errors retain the artifact module descriptor rather than its display filename.
let source = artifact {
    'tangram.ts': '
        export default async function () {
            const file = await tg.file("def broken(").module("py");
            return new tg.Module({ kind: "py", referent: { node: file } });
        }
    '
}
let module = tg run $source
let output_path = mktemp
let output = with-env {TANGRAM_OUTPUT: $output_path} { tg py $module | complete }
failure $output
let outcome = open --raw $output_path | from json
assert equal $outcome.error.location.range.start.line 0
assert equal $outcome.error.location.file.value.kind 'py'
assert ($outcome.error.location.file.value.referent.node | str starts-with 'fil_')
