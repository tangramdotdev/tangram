use ../lib/test.nu *

# The per-sandbox pasta helper shares the workload's resource limits.
if $nu.os-info.name != 'linux' { skip_test 'this test requires linux' }
if (id -u | into int) == 0 { skip_test 'this test requires unprivileged pasta networking' }
if (which pasta | is-empty) { skip_test 'this test requires pasta' }
let local = server spawn --config { runner: { resources: { max_pids: 256 }, sandbox_pool_size: 0 } }
let sandbox = tg sandbox create --network --no-tokens --no-ttl | referent node
let server_pid = glob ($local.directory | path join '**/lock')
    | each { |path| open --raw $path | str trim }
    | where { |pid| (^ps -o command= -p $pid | complete).stdout | str contains $local.directory }
    | first
let helpers = ^pgrep -P $server_pid -f '(^|/)pasta([.[:space:]]|$)' | lines
assert equal ($helpers | length) 1
let helper = $helpers | first
let helper_cgroup = open --raw $'/proc/($helper)/cgroup' | str trim | str replace '0::' ''
let server_cgroup = open --raw $'/proc/($server_pid)/cgroup' | str trim | str replace '0::' ''
assert ($helper_cgroup != $server_cgroup) 'pasta must enter the sandbox cgroup before exec'
assert equal ($helper_cgroup | path basename) 'sidecars' 'pasta must enter the sidecars cgroup of the sandbox'
let path = $'/sys/fs/cgroup($helper_cgroup | path dirname)'
assert equal (open --raw ($path | path join pids.max) | str trim) '256'
tg sandbox destroy $sandbox
tg sandbox wait $sandbox | ignore
assert (not ($path | path exists)) 'destroying the sandbox must remove the helper cgroup'
