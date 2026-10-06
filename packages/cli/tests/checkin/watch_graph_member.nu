use ../lib/test.nu *

let local = server spawn
for package in [
    {
        root: tangram.ts
        child: child.tg.ts
        files: {
            tangram.ts: 'import "./child.tg.ts";'
            child.tg.ts: 'import "./tangram.ts";'
        }
    }
    {
        root: tangram.py
        child: child.tg.py
        files: {
            tangram.py: 'from . import child'
            child.tg.py: 'from . import value'
        }
    }
] {
    let path = artifact $package.files
    tg checkin --watch --no-checkout-pointers ($path | path join $package.root) | ignore
    let child = tg checkin --watch --no-checkout-pointers ($path | path join $package.child)
    assert equal (tg read $child) ($package.files | get $package.child)
}
