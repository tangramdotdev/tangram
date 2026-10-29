use ../../lib/test.nu *

# CLI, file, and inherited-descriptor inputs preserve the workload's standard streams.
if $nu.os-info.name != 'linux' { skip_test 'this test requires linux' }
let tangram = which tangram | first | get path
let program = which 'true' | first | get path
const helper = path self ../../lib/run_config.py
let script = 'read line; test "$line" = provided; test "$TEST" = "value with spaces"; printf "output\n"; printf "error\n" >&2; exit 7'
{
    backend: { kind: builtin },
    command: { args: [/bin/sh -ec $script], env: { TEST: 'value with spaces' } },
} | to json | save config.json
let outputs = [
    ("provided\n" | ^tangram sandbox run --backend builtin --clearenv --setenv TEST 'value with spaces' -- /bin/sh -ec $script | complete),
    ("provided\n" | ^tangram sandbox run --config config.json | complete),
    ("provided\n" | ^python3 $helper $tangram config.json | complete),
]
for output in $outputs {
    assert equal $output.exit_code 7
    assert equal $output.stdout "output\n"
    assert equal $output.stderr "error\n"
}

# Configuration sources cannot be combined with each other or with launch options.
for arguments in [[--config config.json --config-fd 3] [--config config.json --backend builtin] [--config-fd 3 --nice 1] [--config config.json -- /bin/true]] {
    let output = ^tangram sandbox run ...$arguments | complete
    failure $output
    assert ($output.stderr | str contains 'cannot be used with')
}
for fd in [0 1 2 999999] {
    failure (^tangram sandbox run --config-fd $fd | complete)
}
'{invalid' | save invalid.json
failure (^tangram sandbox run --config invalid.json | complete)
{ backend: { kind: builtin }, unknown: true, command: { args: [/bin/true] } } | to json | save unknown.json
failure (^tangram sandbox run --config unknown.json | complete)

# Reading a configuration larger than a pipe buffer does not block launch.
let environment = 0..63 | each { |index| { key: $'KEY_($index)', value: ('' | fill --width 4096 --character x) } } | transpose -r -d
{ backend: { kind: builtin }, command: { args: [$program], env: $environment } } | to json | save large.json
success (^python3 $helper $tangram large.json | complete)
