import { $ } from "bun";
import * as path from "node:path";

class ServerInstance {
	readonly path: string;
	readonly process;

	private constructor(path: string, process) {
		this.path = path;
		this.process = process;
	}

	get configPath() {
		return `${this.path}/config.json`
	}

	get serverPath() {
		return `${this.path}/.tangram`
	}

	get stdoutLog() {
		return Bun.file(`${this.path}/stdout.log`)
	}

	get stderrLog() {
		return Bun.file(`${this.path}/stderr.log`)
	}

	static async create(path: string, configJson: string) {
		const configPath = `${path}/config.json`;
		const serverPath = `${path}/.tangram`;


		// Write the config.
		await Bun.write(configPath, configJson);

		// Spawn the process.
		const process = Bun.spawn(["tg", "--config", configPath, "--path", serverPath, "serve"], {
			stdout: Bun.file(`${path}/stdout.log`),
			stderr: Bun.file(`${path}/stderr.log`),
		});

		// Wait for the server to mount.
		const initTimeout = 1000;
		const initInterval = 50;
		const start = Date.now();
		const initMessage = "serving on";
		let initialized = false;

		while (Date.now() - start < initTimeout) {
			try {
				const stderrLog = await Bun.file(`${path}/stderr.log`).text();
				if (stderrLog.includes(initMessage)) {
					initialized = true;
					break;
				}
			} catch (err) {
				console.error("Could not read log file: ", err);
			}
			await new Promise(resolve => setTimeout(resolve, initInterval));
		}

		if (!initialized) {
			throw new Error("Server initialization timed out");
		}

		// Construct instance.
		const instance = new ServerInstance(path, process);

		// Add the dispose symbol and return.
		return Object.assign(instance, {
			[Symbol.asyncDispose]: async () => {
				await instance.stop();
			}
		});
	}
	
	async stop() {
		this.process.kill();
		await $`rm -rf ${this.path}`;
	}

	tg(strings: TemplateStringsArray, ...values: any[]) {
		let argString = "";

		strings.forEach((string, i) => {
			argString += string;
			if (i < values.length) {
				argString += values[i];
			}
		});

		return $`tg --config ${this.configPath} ${{ raw: `${argString}` }}`;
	}
}

export type ServerArg = {
	concurrency?: number,
	config?: string,
	dbConnections?: number,
	registry?: boolean,
	remotePath?: string,
}

export const startServer = async (arg?: ServerArg) => {
	const { concurrency, config, dbConnections, registry, remotePath } = arg ?? {};

	if (config && (registry || remotePath)) {
		throw new Error("Setting config will override the registry and remotePath options. Either provide a config or use the options.");
	}

	if (registry && remotePath) {
		console.warn("Both registry: true and remotePath are set. This may not be what you intended.");
	}
	
	// Create a tempdir for the server data.
	const path = (await $`mktemp -d`.text()).trim();
		

	const isRegistry = registry ? "true" : "false";

	const build = concurrency ? `
		{
			"concurrency": ${concurrency}
		}
	` : "{}";

	const database = dbConnections ? `
		"database": {
			"kind": "sqlite",
			"connections": ${dbConnections}
		},` : ``;

	const remotes = remotePath ? `{
		"default": {
			"url": "http+unix://${encodeURIComponent(remotePath + "/socket")}"
		}
	}` : "null";
	const configJson = config ?? `
			{
				"advanced": {
					"error_trace_options": {
						"internal": true
					}
				},
				"build": ${build},
				${database}
				"path": "${path}/.tangram",
				"registry": ${isRegistry},
				"remotes": ${remotes},
				"tracing": {
					"filter": "tg=trace,tangram_server=trace",
					"format": "json"
				},
				"vfs": null
			}`;
	
	return await ServerInstance.create(path, configJson);
}

/** Get the path to a pacakge defined in the test packages dir. */
export const getTestPackage = (name: string) => path.join(import.meta.dir, "packages", name)

/** Extract a build ID from a string, if present. Returns `undefined` if not found. */
export const extractBuildId = (s: string): string | undefined => {
	const match = s.match(/\bbld_\w+/);
	return match ? match[0] : undefined;
}
