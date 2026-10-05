use ../lib/test.nu *

let local = server spawn
let path = artifact {
    'tangram.py': '
        import importlib
        import importlib.util
        from . import helper
        namespace = importlib.import_module(".namespace", __package__)
        def default():
            assert importlib.util.find_spec(name=".spec", package=__package__) is not None
            assert importlib.import_module(name=".keyword", package=__package__).value == 42
            assert importlib.import_module("..helper", namespace.__name__) is helper
            name = ".namespace.nested.leaf"
            assert importlib.import_module(name, __package__).value == 43
            try:
                importlib.import_module("...helper", namespace.__name__)
            except ImportError as error:
                assert "beyond top-level package" in str(error)
            else:
                assert False, "crossed the top-level package boundary"
            return helper.value
    '
    'helper.tg.py': 'value = 42'
    'keyword.tg.py': 'value = 42'
    'spec.tg.py': 'value = 1'
    namespace: {nested: {'leaf.tg.py': 'value = 43'}}
}
let file = $path | path join tangram.py
success (tg python --export default $file | complete)
let checked = tg checkin $file
rm --recursive $path
assert equal (tg run $checked | str trim) '42'

# A namespace reached through a directory import keeps access to that directory's members.
let package = artifact {
    'tangram.py': 'from .namespace import leaf'
    'helper.tg.py': 'value = 42'
    namespace: {
        'leaf.tg.py': '
            def touch() -> int:
                from . import sibling
                return sibling.value
        '
        'other.tg.py': 'value = 44'
        'sibling.tg.py': 'value = 43'
    }
}
tg tag put -p dynamic/1.0.0 $package
let path = artifact {
    'tangram.py': '
        # /// script
        # [tool.tangram.imports.pkg]
        # specifier = "dynamic/^1"
        # ///
        import pkg.namespace
        import importlib
        def default():
            namespace = pkg.namespace
            assert importlib.import_module(".other", namespace.__name__).value == 44
            assert pkg.leaf.touch() == 43
            assert pkg.namespace is namespace
            assert importlib.import_module(".other", namespace.__name__).value == 44
            return importlib.import_module("..helper", pkg.namespace.__name__).value
    '
}
let file = $path | path join tangram.py
success (tg check $file | complete)
success (tg python --export default $file | complete)
let checked = tg checkin $file
rm --recursive $path $package
success (tg check $checked | complete)
assert equal (tg run $checked | str trim) '42'
