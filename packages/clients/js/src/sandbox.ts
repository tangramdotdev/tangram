import * as tg from "./index.ts";

const defaultTtl = 5 * 60;

export class Sandbox {
	#id: tg.Sandbox.Id;
	#location: tg.Location.Arg | null;
	#owned: boolean;
	#state: tg.Sandbox.Get.Output | null;

	constructor(arg: tg.Sandbox.ConstructorArg) {
		this.#id = arg.id;
		this.#location = arg.location ?? null;
		this.#owned = arg.owned ?? false;
		this.#state = arg.state ?? null;
	}

	static create(...args: tg.Args<tg.Sandbox.Arg>): tg.Sandbox.Builder {
		return new tg.Sandbox.Builder(...args);
	}

	static withId(
		id: tg.Sandbox.Id,
		location?: tg.Location.Arg | null,
	): tg.Sandbox {
		return new tg.Sandbox({
			id,
			...(location !== undefined ? { location } : {}),
		});
	}

	/** Expect that a value is a `tg.Sandbox`. */
	static expect(value: unknown): tg.Sandbox {
		tg.assert(value instanceof Sandbox);
		return value;
	}

	/** Assert that a value is a `tg.Sandbox`. */
	static assert(value: unknown): asserts value is tg.Sandbox {
		tg.assert(value instanceof Sandbox);
	}

	/** Load the sandbox's state. */
	async load(): Promise<void> {
		let arg: tg.Sandbox.Get.Arg = {};
		if (this.#location !== null) {
			arg.location = this.#location;
		}
		let output = await tg.client.getSandbox(this.#id, arg);
		this.#location =
			output.location === undefined || output.location === null
				? null
				: tg.Location.Arg.fromLocation(output.location);
		this.#state = output;
	}

	/** Reload the sandbox's state. */
	async reload(): Promise<void> {
		await this.load();
	}

	/** Destroy this sandbox. */
	async destroy(): Promise<void> {
		let arg: tg.Sandbox.Destroy.Arg = {};
		if (this.#location !== null) {
			arg.location = this.#location;
		}
		await tg.client.destroySandbox(this.#id, arg);
		this.detach();
	}

	/** Detach this sandbox from this handle's lifetime. */
	detach(): void {
		if (!this.#owned) {
			return;
		}
		this.#owned = false;
	}

	async [Symbol.asyncDispose](): Promise<void> {
		if (!this.#owned) {
			return;
		}
		let arg: tg.Sandbox.Destroy.Arg = {};
		if (this.#location !== null) {
			arg.location = this.#location;
		}
		await tg.client.tryDestroySandbox(this.#id, arg);
		this.detach();
	}

	/** Get this sandbox's ID. */
	get id(): tg.Sandbox.Id {
		return this.#id;
	}

	/** Get this sandbox's location arg. */
	get location(): tg.Location.Arg | null {
		return this.#location;
	}

	/** Get this sandbox's loaded state. */
	get state(): tg.Sandbox.Get.Output | null {
		return this.#state;
	}

	run<A extends tg.UnresolvedArgs<Array<tg.Value>>, O extends tg.ReturnValue>(
		function_: (...args: A) => O,
	): tg.Process.Builder<"run", [], tg.ResolvedReturnValue<O>>;
	run<A extends tg.UnresolvedArgs<Array<tg.Value>>, O extends tg.ReturnValue>(
		function_: (...args: A) => O,
		...args: tg.UnresolvedArgs<tg.ResolvedArgs<A>>
	): tg.Process.Builder<"run", [], tg.ResolvedReturnValue<O>>;
	run(
		strings: TemplateStringsArray,
		...placeholders: tg.Args<tg.Template.Arg>
	): tg.Process.Builder<"run", Array<tg.Value>, tg.Value>;
	run(
		...args: tg.Args<tg.Process.Arg>
	): tg.Process.Builder<"run", Array<tg.Value>, tg.Value>;
	run(...args: any): any {
		let builder = tg.Process.run(...args).sandbox(this.#id);
		if (this.#location !== null) {
			builder.location(this.#location);
		}
		return builder;
	}
}

export namespace Sandbox {
	export type Id = string;

	export type DataArg = {
		cpu?: number | null;
		host?: string | null;
		hostname?: string | null;
		isolation?: tg.Sandbox.Isolation.Data | null;
		location?: string | null;
		memory?: number | null;
		mounts?: Array<tg.Sandbox.Mount.Data>;
		network?: tg.Sandbox.Network.Data | null;
		owner?: string | null;
		ttl?: number | null;
	};

	export namespace Get {
		export type Arg = {
			location?: tg.Location.Arg | null;
		};

		export type Output = {
			cpu?: number | null;
			creator?: string | null;
			hostname?: string | null;
			id: tg.Sandbox.Id;
			isolation?: tg.Sandbox.Isolation.Data | null;
			location?: tg.Location | null;
			memory?: number | null;
			mounts?: Array<tg.Sandbox.Mount.Data>;
			network?: tg.Sandbox.Network.Data | null;
			owner?: string | null;
			status: tg.Sandbox.Status;
			ttl?: number | null;
		};
	}

	export namespace Id {
		export let is = (value: unknown): value is tg.Sandbox.Id => {
			return typeof value === "string" && value.startsWith("sbx_");
		};
	}

	export type Arg = {
		cpu?: number | null;
		host?: string | null;
		hostname?: string | null;
		isolation?: tg.Sandbox.Isolation | null;
		location?: tg.Location.Arg | null;
		memory?: number | null;
		mounts?: Array<tg.Sandbox.Mount> | null;
		network?: boolean | tg.Sandbox.Network | null;
		owner?: string | null;
		ports?: Array<tg.Sandbox.Port> | null;
		ttl?: number | null;
	};

	export namespace Arg {
		export let toData = (arg: tg.Sandbox.Arg): tg.Sandbox.DataArg => {
			let output: tg.Sandbox.DataArg = {};
			if (arg.cpu !== undefined) {
				output.cpu = arg.cpu;
			}
			if (arg.host !== undefined) {
				output.host = arg.host;
			}
			if (arg.hostname !== undefined) {
				output.hostname = arg.hostname;
			}
			if (arg.isolation !== undefined) {
				output.isolation =
					arg.isolation === null
						? null
						: tg.Sandbox.Isolation.toData(arg.isolation);
			}
			if (arg.location !== undefined) {
				output.location =
					arg.location === null
						? null
						: tg.Location.Arg.toDataString(arg.location);
			}
			if (arg.memory !== undefined) {
				output.memory = arg.memory;
			}
			if (arg.mounts !== undefined && arg.mounts !== null) {
				output.mounts = arg.mounts.map(tg.Sandbox.Mount.toDataString);
			}
			let network = normalizeNetwork(arg.network, arg.ports ?? []);
			if (network !== undefined) {
				output.network = network;
			}
			if (arg.owner !== undefined) {
				output.owner = arg.owner;
			}
			if (arg.ttl !== undefined) {
				output.ttl = arg.ttl;
			}
			return output;
		};
	}

	export class Builder {
		#args: tg.Args<tg.Sandbox.Arg>;

		constructor(...args: tg.Args<tg.Sandbox.Arg>) {
			this.#args = args;
		}

		cpu(cpu: tg.Unresolved<tg.MaybeMutation<number> | null>): this {
			this.#args.push({ cpu });
			return this;
		}

		host(host: tg.Unresolved<tg.MaybeMutation<string> | null>): this {
			this.#args.push({ host });
			return this;
		}

		hostname(hostname: tg.Unresolved<tg.MaybeMutation<string> | null>): this {
			this.#args.push({ hostname });
			return this;
		}

		isolation(
			isolation: tg.Unresolved<tg.MaybeMutation<tg.Sandbox.Isolation> | null>,
		): this {
			this.#args.push({ isolation });
			return this;
		}

		location(
			location: tg.Unresolved<tg.MaybeMutation<tg.Location.Arg> | null>,
		): this {
			this.#args.push({ location });
			return this;
		}

		memory(memory: tg.Unresolved<tg.MaybeMutation<number> | null>): this {
			this.#args.push({ memory });
			return this;
		}

		mount(...mounts: Array<tg.Unresolved<tg.Sandbox.Mount>>): this {
			this.#args.push({ mounts });
			return this;
		}

		mounts(
			...mounts: Array<
				tg.Unresolved<tg.MaybeMutation<Array<tg.Sandbox.Mount>> | null>
			>
		): this {
			this.#args.push(...mounts.map((mounts) => ({ mounts })));
			return this;
		}

		network(): this;
		network(
			network: tg.Unresolved<tg.MaybeMutation<
				boolean | tg.Sandbox.Network
			> | null>,
		): this;
		network(
			network?: tg.Unresolved<tg.MaybeMutation<
				boolean | tg.Sandbox.Network
			> | null>,
		): this {
			this.#args.push({ network: network === undefined ? true : network });
			return this;
		}

		owner(owner: tg.Unresolved<tg.MaybeMutation<string> | null>): this {
			this.#args.push({ owner });
			return this;
		}

		port(...ports: Array<tg.Unresolved<tg.Sandbox.Port>>): this {
			this.#args.push({ ports });
			return this;
		}

		ports(
			...ports: Array<
				tg.Unresolved<tg.MaybeMutation<Array<tg.Sandbox.Port>> | null>
			>
		): this {
			this.#args.push(...ports.map((ports) => ({ ports })));
			return this;
		}

		ttl(ttl: tg.Unresolved<tg.MaybeMutation<number> | null>): this {
			this.#args.push({ ttl });
			return this;
		}

		then<TResult1 = tg.Sandbox, TResult2 = never>(
			onfulfilled?:
				| ((value: tg.Sandbox) => TResult1 | PromiseLike<TResult1>)
				| undefined
				| null,
			onrejected?:
				| ((reason: any) => TResult2 | PromiseLike<TResult2>)
				| undefined
				| null,
		): PromiseLike<TResult1 | TResult2> {
			return this.#thenInner().then(onfulfilled, onrejected);
		}

		async #thenInner(): Promise<tg.Sandbox> {
			let arg = await tg.Args.apply<tg.Sandbox.Arg, tg.Sandbox.Arg>({
				args: [{ host: tg.host.current, ttl: defaultTtl }, ...this.#args],
				map: async (arg) => arg,
				reduce: {
					mounts: "append",
					ports: "append",
				},
			});
			let output = await tg.client.createSandbox(tg.Sandbox.Arg.toData(arg));
			return new tg.Sandbox({
				id: output.id,
				...(arg.location !== undefined ? { location: arg.location } : {}),
				owned: true,
			});
		}
	}

	export type ConstructorArg = {
		id: tg.Sandbox.Id;
		location?: tg.Location.Arg | null;
		owned?: boolean;
		state?: tg.Sandbox.Get.Output | null;
	};

	export namespace Create {
		export type Arg = tg.Sandbox.DataArg;

		export type Output = {
			id: tg.Sandbox.Id;
		};
	}

	export namespace Destroy {
		export type Arg = {
			location?: tg.Location.Arg | null;
		};
	}

	export type Isolation = "container" | "seatbelt" | "vm";

	export namespace Isolation {
		export type Data = { kind: tg.Sandbox.Isolation };

		export let toData = (
			value: tg.Sandbox.Isolation,
		): tg.Sandbox.Isolation.Data => {
			return { kind: value };
		};

		export let fromData = (
			data: tg.Sandbox.Isolation.Data,
		): tg.Sandbox.Isolation => {
			return data.kind;
		};
	}

	export type Network =
		| "default"
		| "host"
		| "bridge"
		| tg.Sandbox.Network.Bridge;

	export namespace Network {
		export type Bridge = {
			kind?: "bridge";
			ports?: Array<tg.Sandbox.Port>;
		};

		export type Data =
			| { kind: "default" }
			| { kind: "host" }
			| {
					kind: "bridge";
					ports?: Array<tg.Sandbox.Port.Data>;
			  };

		export let toData = (
			value: tg.Sandbox.Network,
		): tg.Sandbox.Network.Data => {
			if (typeof value === "string") {
				return { kind: value };
			}
			if (value.ports === undefined) {
				return { kind: "bridge" };
			}
			return {
				kind: "bridge",
				ports: value.ports.map(tg.Sandbox.Port.toDataString),
			};
		};

		export let fromData = (
			data: tg.Sandbox.Network.Data,
		): tg.Sandbox.Network => {
			if (data.kind !== "bridge") {
				return data.kind;
			}
			if (data.ports === undefined || data.ports.length === 0) {
				return "bridge";
			}
			return {
				kind: "bridge",
				ports: data.ports.map(tg.Sandbox.Port.fromDataString),
			};
		};
	}

	export type Status = "created" | "started" | "destroyed";

	export type Port = string;

	export namespace Port {
		export type Data = string;

		export let toDataString = (
			value: tg.Sandbox.Port,
		): tg.Sandbox.Port.Data => {
			return value;
		};

		export let fromDataString = (
			data: tg.Sandbox.Port.Data,
		): tg.Sandbox.Port => {
			return data;
		};
	}

	export type Mount = {
		source: string;
		target: string;
		readonly?: boolean;
	};

	export namespace Mount {
		export type Data = string;

		export let toDataString = (
			value: tg.Sandbox.Mount,
		): tg.Sandbox.Mount.Data => {
			let output = `${value.source}:${value.target}`;
			if (value.readonly === true) {
				output += ",ro";
			}
			return output;
		};

		export let fromDataString = (
			data: tg.Sandbox.Mount.Data,
		): tg.Sandbox.Mount => {
			let readonly = false;
			let string = data;
			let separator = data.indexOf(",");
			if (separator !== -1) {
				string = data.slice(0, separator);
				let option = data.slice(separator + 1);
				if (option === "ro") {
					readonly = true;
				} else if (option !== "rw") {
					throw new Error(`unknown option: ${option}`);
				}
			}
			let split = string.indexOf(":");
			if (split === -1) {
				throw new Error("expected a target path");
			}
			let source = string.slice(0, split);
			let target = string.slice(split + 1);
			if (!target.startsWith("/")) {
				throw new Error("expected an absolute path");
			}
			return {
				source,
				target,
				readonly,
			};
		};
	}
}

let normalizeNetwork = (
	value: boolean | tg.Sandbox.Network | null | undefined,
	ports: Array<tg.Sandbox.Port>,
): tg.Sandbox.Network.Data | undefined => {
	if (ports.length > 0) {
		if (value === false) {
			throw new Error("ports require networking");
		}
		if (value === "host") {
			throw new Error("ports are not supported with host networking");
		}
		let network =
			value === undefined || value === null || value === true
				? undefined
				: tg.Sandbox.Network.toData(value);
		return {
			kind: "bridge",
			ports: [
				...(network?.kind === "bridge" ? (network.ports ?? []) : []),
				...ports.map(tg.Sandbox.Port.toDataString),
			],
		};
	}
	if (value === undefined || value === null || value === false) {
		return undefined;
	}
	if (value === true) {
		return { kind: "default" };
	}
	return tg.Sandbox.Network.toData(value);
};
