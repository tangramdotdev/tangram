use ../../util/test.nu *

const interrupt_path = path self ../../util/interrupt.py

# Ctrl-C lets the child finish cleanup, then restores the parent interrupt handler.
let local = server spawn
let path = artifact {
    'tangram.py': r#'
import asyncio
import os
import signal

async def default():
    asyncio.get_running_loop().add_signal_handler(signal.SIGINT, lambda: os._exit(130))
    await tg.run({"executable": "sh", "args": ["-c", 'trap \'trap "" INT TERM; printf "stopping one\\n"; sleep 0.2; printf "stopping two\\n"; exit 0\' INT\nprintf "child ready\\n"\nwhile :; do sleep 0.1; done']})
    print("parent ready", flush=True)
    await asyncio.sleep(30)

'#
}
python3 $interrupt_path tg --quiet run $path
