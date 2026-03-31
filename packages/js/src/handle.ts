import * as tg from "@tangramdotdev/client";

export let handle: tg.Handle = {
	checkin(arg: tg.Handle.CheckinArg): Promise<tg.Artifact.Id> {
		return syscall("handle_checkin", arg);
	},

	checkout(arg: tg.Handle.CheckoutArg): Promise<string> {
		return syscall("handle_checkout", arg);
	},

	checksum(
		input: string | Uint8Array,
		algorithm: tg.Checksum.Algorithm,
	): tg.Checksum {
		return syscall("handle_checksum", input, algorithm);
	},

	getObject(id) {
		return syscall("handle_object_get", id);
	},

	getProcess(
		id: tg.Process.Id,
		remote: string | undefined,
	): Promise<tg.Process.Data> {
		return syscall("handle_process_get", id, remote);
	},

	objectId(object: tg.Object.Data): tg.Object.Id {
		return syscall("handle_object_id", object);
	},

	parseValue(value: string): tg.Value.Data {
		return syscall("handle_value_parse", value);
	},

	postObjectBatch(arg: tg.Handle.PostObjectBatchArg): Promise<void> {
		return syscall("handle_object_batch", arg);
	},

	processId(): tg.Process.Id {
		return syscall("handle_process_id", undefined);
	},

	read(arg: tg.Handle.ReadArg): Promise<Uint8Array> {
		return syscall("handle_read", arg);
	},

	async readProcessStdio(
		id: tg.Process.Id,
		arg: tg.Handle.ProcessStdioReadArg,
	): Promise<AsyncIterableIterator<tg.Process.Stdio.Read.Event> | undefined> {
		let token = await syscall("handle_process_stdio_read_open", id, arg);
		if (token === undefined) {
			return undefined;
		}
		return (async function* () {
			try {
				while (true) {
					let eventData = await syscall(
						"handle_process_stdio_read_read",
						token,
					);
					if (eventData === undefined) {
						break;
					}
					let event = tg.Process.Stdio.Read.Event.fromData(eventData);
					yield event;
					if (event.kind === "end") {
						break;
					}
				}
			} finally {
				await syscall("handle_process_stdio_read_close", token);
			}
		})();
	},

	setProcessTtySize(
		id: tg.Process.Id,
		arg: tg.Handle.ProcessTtySizePutArg,
	): Promise<void> {
		return syscall("handle_process_tty_size_put", id, arg);
	},

	signalProcess(id: tg.Process.Id, arg: tg.Handle.SignalArg): Promise<void> {
		return syscall("handle_process_signal", id, arg);
	},

	spawnProcess(arg: tg.Handle.SpawnArg): Promise<tg.Handle.SpawnOutput> {
		return syscall("handle_process_spawn", arg);
	},

	waitProcess(
		id: tg.Process.Id,
		arg: tg.Handle.WaitArg,
	): Promise<tg.Process.Wait.Data> {
		return syscall("handle_process_wait", id, arg);
	},

	stringifyValue(value: tg.Value.Data): string {
		return syscall("handle_value_stringify", value);
	},

	write(bytes: string | Uint8Array): Promise<tg.Blob.Id> {
		return syscall("handle_write", bytes);
	},

	async writeProcessStdio(
		id: tg.Process.Id,
		arg: tg.Handle.ProcessStdioWriteArg,
		input: AsyncIterableIterator<tg.Process.Stdio.Read.Event>,
	): Promise<void> {
		let token = await syscall("handle_process_stdio_write_open", id, arg);
		try {
			for await (let event of input) {
				if (event.kind === "end") {
					break;
				}
				if (!arg.streams.includes(event.value.stream)) {
					throw new Error("invalid process stdio stream");
				}
				let eventData = tg.Process.Stdio.Read.Event.toData(event);
				if (eventData.kind !== "chunk") {
					throw new Error("invalid process stdio event");
				}
				await syscall(
					"handle_process_stdio_write_write",
					token,
					eventData.value,
				);
			}
		} finally {
			await syscall("handle_process_stdio_write_close", token);
		}
	},
};
