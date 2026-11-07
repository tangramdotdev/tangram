export const task = async (
	label: string,
	action: string,
	command: tg.Process.Builder<"run">,
) => {
	const started = Date.now();
	const status = async (message: string) => {
		await writeLine(2, `${labelText(2, label)} ${message}`);
	};
	await status(action);
	try {
		await using process = await command.stdout("pipe").stderr("pipe").spawn();
		const results = await Promise.allSettled([
			process.output(),
			prefix(process.stdout, 1, label),
			prefix(process.stderr, 2, label),
		]);
		for (const result of results) {
			if (result.status === "rejected") {
				throw result.reason;
			}
		}
	} catch (error) {
		await status(`${color(2, "✗", "31")} ${action} (${elapsed(started)})`);
		throw error;
	}
	await status(`${color(2, "✓", "32")} ${action} (${elapsed(started)})`);
};

const elapsed = (started: number) =>
	`${((Date.now() - started) / 1000).toFixed(2)}s`;

const prefix = async (
	reader: tg.Process.Stdio.Reader,
	fd: number,
	label: string,
) => {
	let pending = new Uint8Array();
	const write = async (bytes: Uint8Array) => {
		const line = tg.encoding.utf8.decode(bytes).replace(/\r/g, "");
		await writeLine(fd, `${labelText(fd, label)} ${line}`);
	};
	while (true) {
		const chunk = await reader.read();
		if (chunk === null) {
			break;
		}
		const bytes = new Uint8Array(pending.length + chunk.length);
		bytes.set(pending);
		bytes.set(chunk, pending.length);
		let start = 0;
		for (let index = 0; index < bytes.length; index++) {
			if (bytes[index] === 10) {
				await write(bytes.subarray(start, index));
				start = index + 1;
			}
		}
		pending = bytes.slice(start);
	}
	if (pending.length > 0) {
		await write(pending);
	}
};

let writing = Promise.resolve();

const writeLine = (fd: number, line: string) => {
	const written = writing.then(() =>
		tg.host.write(fd, tg.encoding.utf8.encode(`${line}\n`)),
	);
	writing = written.catch(() => {});
	return written;
};

const labelText = (fd: number, label: string) => {
	const colors = ["36", "35", "34", "33", "92", "94"];
	let hash = 0;
	for (const character of label) {
		hash = (hash * 31 + character.codePointAt(0)!) >>> 0;
	}
	return color(fd, `[${label}]`, `1;${colors[hash % colors.length]}`);
};

const color = (fd: number, text: string, code: string) => {
	const enabled =
		tg.host.isTty(fd) &&
		tg.process.env.NO_COLOR === undefined &&
		tg.process.env.TERM !== "dumb";
	return enabled ? `\x1b[${code}m${text}\x1b[0m` : text;
};
