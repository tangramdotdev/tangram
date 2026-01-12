use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.py: '
async def default():
    return await tg.build(foo)

def foo():
    return tg.directory({ "hello": tg.file("hello") })
'
}

let output = tg build $path
snapshot $output
