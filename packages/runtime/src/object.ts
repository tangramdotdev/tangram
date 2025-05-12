import * as tg from "./index.ts";

export type Object =
	| tg.Blob
	| tg.Directory
	| tg.File
	| tg.Symlink
	| tg.Graph
	| tg.Command;

export namespace Object {
	export type Id =
		| tg.Blob.Id
		| tg.Directory.Id
		| tg.File.Id
		| tg.Symlink.Id
		| tg.Graph.Id
		| tg.Command.Id;

	export type Object_ =
		| { kind: "blob"; value: tg.Blob.Object }
		| { kind: "directory"; value: tg.Directory.Object }
		| { kind: "file"; value: tg.File.Object }
		| { kind: "symlink"; value: tg.Symlink.Object }
		| { kind: "graph"; value: tg.Graph.Object }
		| { kind: "command"; value: tg.Command.Object };

	export namespace Object_ {
		export let toData = (object: Object_): Data => {
			switch (object.kind) {
				case "blob": {
					let value = tg.Blob.Object.toData(object.value);
					return { kind: "blob", value };
				}
				case "directory": {
					let value = tg.Directory.Object.toData(object.value);
					return { kind: "directory", value };
				}
				case "file": {
					let value = tg.File.Object.toData(object.value);
					return { kind: "file", value };
				}
				case "symlink": {
					let value = tg.Symlink.Object.toData(object.value);
					return { kind: "symlink", value };
				}
				case "graph": {
					let value = tg.Graph.Object.toData(object.value);
					return { kind: "graph", value };
				}
				case "command": {
					let value = tg.Command.Object.toData(object.value);
					return { kind: "command", value };
				}
			}
		};

		export let fromData = (data: Data): Object_ => {
			switch (data.kind) {
				case "blob": {
					let value = tg.Blob.Object.fromData(data.value);
					return { kind: "blob", value };
				}
				case "directory": {
					let value = tg.Directory.Object.fromData(data.value);
					return { kind: "directory", value };
				}
				case "file": {
					let value = tg.File.Object.fromData(data.value);
					return { kind: "file", value };
				}
				case "symlink": {
					let value = tg.Symlink.Object.fromData(data.value);
					return { kind: "symlink", value };
				}
				case "graph": {
					let value = tg.Graph.Object.fromData(data.value);
					return { kind: "graph", value };
				}
				case "command": {
					let value = tg.Command.Object.fromData(data.value);
					return { kind: "command", value };
				}
			}
		};

		export let children = (object: Object_): Array<tg.Object> => {
			switch (object.kind) {
				case "blob": {
					return tg.Blob.Object.children(object.value);
				}
				case "directory": {
					return tg.Directory.Object.children(object.value);
				}
				case "file": {
					return tg.File.Object.children(object.value);
				}
				case "symlink": {
					return tg.Symlink.Object.children(object.value);
				}
				case "graph": {
					return tg.Graph.Object.children(object.value);
				}
				case "command": {
					return tg.Command.Object.children(object.value);
				}
			}
		};
	}

	export type Kind =
		| "blob"
		| "directory"
		| "file"
		| "symlink"
		| "graph"
		| "command";

	export type State<I, O> = {
		id?: I | undefined;
		object?: O | undefined;
		stored: boolean;
	};

	export let withId = (id: tg.Object.Id): tg.Object => {
		let prefix = id.substring(0, 3);
		if (prefix === "blb") {
			return tg.Blob.withId(id);
		} else if (prefix === "dir") {
			return tg.Directory.withId(id);
		} else if (prefix === "fil") {
			return tg.File.withId(id);
		} else if (prefix === "sym") {
			return tg.Symlink.withId(id);
		} else if (prefix === "gph") {
			return tg.Graph.withId(id);
		} else if (prefix === "cmd") {
			return tg.Command.withId(id);
		} else {
			throw new Error(`invalid object id: ${id}`);
		}
	};

	export let is = (value: unknown): value is Object => {
		return (
			value instanceof tg.Blob ||
			value instanceof tg.Directory ||
			value instanceof tg.File ||
			value instanceof tg.Symlink ||
			value instanceof tg.Graph ||
			value instanceof tg.Command
		);
	};

	export let expect = (value: unknown): Object => {
		tg.assert(is(value));
		return value;
	};

	export let assert = (value: unknown): asserts value is Object => {
		tg.assert(is(value));
	};

	export let kind = (object: tg.Object): tg.Object.Kind => {
		if (object instanceof tg.Blob) {
			return "blob";
		} else if (object instanceof tg.Directory) {
			return "directory";
		} else if (object instanceof tg.File) {
			return "file";
		} else if (object instanceof tg.Symlink) {
			return "symlink";
		} else if (object instanceof tg.Graph) {
			return "graph";
		} else if (object instanceof tg.Command) {
			return "command";
		} else {
			return tg.unreachable();
		}
	};

	export type Data =
		| { kind: "blob"; value: tg.Blob.Data }
		| { kind: "directory"; value: tg.Directory.Data }
		| { kind: "file"; value: tg.File.Data }
		| { kind: "symlink"; value: tg.Symlink.Data }
		| { kind: "graph"; value: tg.Graph.Data }
		| { kind: "command"; value: tg.Command.Data };
}
