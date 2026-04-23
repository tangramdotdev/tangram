import * as tg from "@tangramdotdev/client";

type LocationWire = string;

type LocationArgWire<T extends { location?: tg.Location.Arg | undefined }> = Omit<
	T,
	"location"
> & {
	location?: LocationWire | undefined;
};

type ProcessGetOutputWire = Omit<tg.Handle.ProcessGetOutput, "location"> & {
	location?: LocationWire | undefined;
};

type SandboxGetOutputWire = Omit<tg.Handle.SandboxGetOutput, "location"> & {
	location?: LocationWire | undefined;
};

type SpawnArgWire = Omit<
	tg.Handle.SpawnArg,
	"cache_location" | "command" | "location"
> & {
	cache_location?: LocationWire | undefined;
	command: Omit<tg.Referent<tg.Command.Id>, "options"> & {
		options?: tg.Referent.Data.Options | undefined;
	};
	location?: LocationWire | undefined;
};

type SpawnOutputWire = Omit<tg.Handle.SpawnOutput, "location"> & {
	location?: LocationWire | undefined;
};

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
		arg?: tg.Handle.ProcessGetArg | undefined,
	): Promise<tg.Handle.ProcessGetOutput> {
		let wireArg = arg === undefined ? undefined : normalizeLocationArg(arg);
		return syscall("handle_process_get", id, wireArg).then(
			(output) => normalizeProcessGetOutput(output),
		);
	},

	getSandbox(id: tg.Sandbox.Id): Promise<tg.Handle.SandboxGetOutput> {
		return syscall("handle_sandbox_get", id).then((output) =>
			normalizeSandboxGetOutput(output),
		);
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

	read(arg: tg.Handle.ReadArg): Promise<Uint8Array> {
		return syscall("handle_read", arg);
	},

	async readProcessStdio(
		id: tg.Process.Id,
		arg: tg.Handle.ProcessStdioReadArg,
	): Promise<AsyncIterableIterator<tg.Process.Stdio.Read.Event> | undefined> {
		let token = await syscall(
			"handle_process_stdio_read_open",
			id,
			normalizeLocationArg(arg),
		);
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
		return syscall("handle_process_tty_size_put", id, normalizeLocationArg(arg));
	},

	signalProcess(id: tg.Process.Id, arg: tg.Handle.SignalArg): Promise<void> {
		return syscall("handle_process_signal", id, normalizeLocationArg(arg));
	},

	spawnProcess(arg: tg.Handle.SpawnArg): Promise<tg.Handle.SpawnOutput> {
		return syscall("handle_process_spawn", normalizeSpawnArg(arg)).then(
			(output) => normalizeSpawnOutput(output),
		);
	},

	waitProcess(
		id: tg.Process.Id,
		arg: tg.Handle.WaitArg,
	): Promise<tg.Process.Wait.Data> {
		return syscall("handle_process_wait", id, normalizeLocationArg(arg));
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
		let token = await syscall(
			"handle_process_stdio_write_open",
			id,
			normalizeLocationArg(arg),
		);
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

function normalizeLocationArg<T extends { location?: tg.Location.Arg | undefined }>(
	arg: T,
): LocationArgWire<T>;
function normalizeLocationArg(arg: undefined): undefined;
function normalizeLocationArg<T extends { location?: tg.Location.Arg | undefined }>(
	arg: T | undefined,
): LocationArgWire<T> | undefined {
	if (arg === undefined) {
		return undefined;
	}
	return {
		...arg,
		location:
			arg.location === undefined
				? undefined
				: tg.Location.Arg.toDataString(arg.location),
	};
}

let normalizeReferentOptions = (
	options: tg.Referent.Options | undefined,
): tg.Referent.Data.Options | undefined => {
	if (options === undefined) {
		return undefined;
	}
	let output: tg.Referent.Data.Options = {};
	if (options.artifact !== undefined) {
		output.artifact = options.artifact;
	}
	if (options.id !== undefined) {
		output.id = options.id;
	}
	if (options.location !== undefined) {
		output.location = tg.Location.Arg.toDataString(options.location);
	}
	if (options.name !== undefined) {
		output.name = options.name;
	}
	if (options.path !== undefined) {
		output.path = options.path;
	}
	if (options.tag !== undefined) {
		output.tag = options.tag;
	}
	return output;
};

let normalizeSpawnArg = (
	arg: tg.Handle.SpawnArg,
): SpawnArgWire => {
	return {
		...normalizeLocationArg(arg),
		cache_location:
			arg.cache_location === undefined
				? undefined
				: tg.Location.Arg.toDataString(arg.cache_location),
		command: {
			...arg.command,
			options: normalizeReferentOptions(arg.command.options),
		},
	};
};

let normalizeLocation = (
	location: string | tg.Location | undefined,
): tg.Location | undefined => {
	if (location === undefined) {
		return undefined;
	}
	if (tg.Location.is(location)) {
		return location;
	}
	return tg.Location.fromDataString(location);
};

let normalizeProcessGetOutput = (
	output: ProcessGetOutputWire,
): tg.Handle.ProcessGetOutput => {
	return {
		...output,
		location: normalizeLocation(output.location),
	};
};

let normalizeSandboxGetOutput = (
	output: SandboxGetOutputWire,
): tg.Handle.SandboxGetOutput => {
	return {
		...output,
		location: normalizeLocation(output.location),
	};
};

let normalizeSpawnOutput = (
	output: SpawnOutputWire,
): tg.Handle.SpawnOutput => {
	return {
		...output,
		location: normalizeLocation(output.location),
	};
};
