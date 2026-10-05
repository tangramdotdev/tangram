use ../lib/test.nu *

let local = server spawn
let message = 'star imports are not supported in python modules; use explicit imports'
for source in [
    'from math import *'
    'if False:
    from math import *'
    '__all__ = ["helper"]
from . import *'
] {
    let path = artifact {'main.tg.py': $source}
    let file = $path | path join main.tg.py
    for output in [(tg checkin $file | complete) (tg check $file | complete) (tg python $file | complete)] {
        failure $output
        assert ($output.stderr | str contains $message) $output.stderr
        assert ($output.stderr | str contains 'main.tg.py:') $output.stderr
    }

    # Programmatically constructed modules are validated when loaded as well.
    let builder = artifact {
        'tangram.ts': ('export default () => tg.directory({ "tangram.py": tg.file(' + ($source | to json -r) + ').module("python") });')
    }
    let module = tg run $builder
    for output in [(tg check $module | complete) (tg run $module | complete)] {
        failure $output
        assert ($output.stderr | str contains $message) $output.stderr
        assert ($output.stderr | str contains 'tangram.py:') $output.stderr
        assert not ($output.stderr | str contains '500 Internal Server Error')
    }
    let importer = artifact {
        'tangram.ts': ('import { default as foreign } from "' + $module + '"; export default () => foreign();')
    }
    let output = tg run $importer | complete
    failure $output
    assert ($output.stderr | str contains $message) $output.stderr
}
