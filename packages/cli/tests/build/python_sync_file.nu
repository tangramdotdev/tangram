use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.py: '
def default():
    return tg.file("hello from sync")
'
}

let output = tg build $path
snapshot $output
