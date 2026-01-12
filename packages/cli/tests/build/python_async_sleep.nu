use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.py: '
async def default():
    await tg.sleep(0.1)
    return "slept"
'
}

let output = tg build $path
snapshot $output
