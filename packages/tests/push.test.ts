import { describe, expect, test } from "bun:test";
import { $ } from "bun";
import { startServer } from "./setup.ts";
import path from "node:path";

describe("objects", () => {
  test("repeated pushes of random data", async () => {
		await using remote = await startServer({ registry: true });

		await using server = await startServer({ remotePath: remote.serverPath });

    // Generate random data
    for (let i = 0; i < 50; i++) {
      let filePath = path.join(server.path, `random_data_${i}`);
      let file = Bun.file(filePath);

  		await $`head -c 4096 /dev/urandom > ${file}`.quiet();

      // Check it in.
  		let id = await server.tg`checkin ${filePath}`.text().then((t) => t.trim());
  		let serverOut = await server.tg`get ${id}`.text();

  		// Push it.
  		await server.tg`push ${id}`.quiet();

  		// Get it from the remote.
  		let remoteOut = await remote.tg`get ${id}`.text();
  		expect(serverOut).toBe(remoteOut);
    }
  });
});
