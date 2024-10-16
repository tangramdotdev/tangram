import * as path from "node:path";
import { $ } from "bun";
import * as Bun from "bun";

const cliPath = path.join(import.meta.dir, "../../../target/debug/tangram");

export default class Server {
	readonly path: string;
	readonly process: Bun.Subprocess;

	static async start(config?: object) {
		const path = await $`mktemp -d`.text().then((t) => t.trim());
		const configPath = `${path}/.config/tangram/config.json`;
		const dataPath = `${path}/.tangram`;

		// Write the config.
		if (config !== undefined) {
			await Bun.write(configPath, JSON.stringify(config));
		}

		// Spawn the process.
		const command = [
			cliPath,
			"--config",
			configPath,
			"--path",
			dataPath,
			"serve",
		];
		const process = Bun.spawn(command);

		return new Server(path, process);
	}

	private constructor(path: string, process: Bun.Subprocess) {
		this.path = path;
		this.process = process;
	}

	async [Symbol.asyncDispose]() {
		await this.stop();
	}

	async stop() {
		this.process.kill(2);
		await this.process.exited;
	}

	get configPath() {
		return `${this.path}/.config/tangram/config.json`;
	}

	get dataPath() {
		return `${this.path}/.tangram`;
	}

	get url() {
		const socketPath = path.join(this.dataPath, "socket");
		const authority = encodeURIComponent(socketPath);
		return `http+unix://${authority}`;
	}

	tg(strings: TemplateStringsArray, ...values: any[]) {
		let args = "";
		strings.forEach((string, i) => {
			args += string;
			if (i < values.length) {
				args += values[i];
			}
		});
		return $`${cliPath} --mode client --url ${this.url} ${{ raw: args }}`;
	}
}
