import * as tg from "./index.ts";

export namespace Sandbox {
	export type Id = string;

	export namespace Id {
		export let is = (value: unknown): value is tg.Sandbox.Id => {
			return typeof value === "string" && value.startsWith("sbx_");
		};
	}

	export type Arg = {
		cpu?: number | undefined;
		hostname?: string | undefined;
		isolation?: tg.Sandbox.Isolation | undefined;
		location?: tg.Location.Arg | undefined;
		memory?: number | undefined;
		mounts?: Array<tg.Sandbox.Mount> | undefined;
		namespace?: string | undefined;
		network?: boolean | tg.Sandbox.Network | undefined;
		ttl?: number | undefined;
		user?: string | undefined;
	};

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
			kind?: "bridge" | undefined;
			ports?: Array<tg.Sandbox.Port> | undefined;
		};

		export type Data =
			| { kind: "default" }
			| { kind: "host" }
			| {
					kind: "bridge";
					ports?: Array<tg.Sandbox.Port.Data> | undefined;
			  };

		export let toData = (
			value: tg.Sandbox.Network,
		): tg.Sandbox.Network.Data => {
			if (typeof value === "string") {
				return { kind: value };
			}
			return {
				kind: "bridge",
				ports: value.ports?.map(tg.Sandbox.Port.toDataString),
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
		readonly?: boolean | undefined;
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
