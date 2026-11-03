use ../../../util/test.nu *

# Queued buffered uploads must preserve their contents across an idle connection closing.
let local = server spawn --config { http: { idle_timeout: 0.25 } }

let path = artifact {
	'test.tg.py': '
		import asyncio
		import time

		async def default():
			await tg.client.write("warm")
			values = [f"write {index}" for index in range(100)]
			writes = [asyncio.create_task(tg.client.write(value if index % 2 == 0 else value.encode())) for index, value in enumerate(values)]
			await asyncio.sleep(0)
			time.sleep(1)
			ids = await asyncio.gather(*writes)
			for id, value in zip(ids, values):
				assert await tg.Blob.with_id(id).text() == value
			return "done"
	'
}

let output = tg build ($path | path join test.tg.py) | complete
success $output 'the queued requests should preserve their contents'
assert equal ($output.stdout | from json) 'done'
