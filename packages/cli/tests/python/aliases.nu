use ../lib/test.nu *

let local = server spawn
let path = artifact {
    'main.tg.py': 'def default():
    print("python alias")
'
}
for language in [python py] {
    let output = tg $language --export default ($path | path join main.tg.py) | complete
    success $output
    assert equal ($output.stdout | str trim) 'python alias'
}
