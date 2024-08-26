import * as tg from "./index.ts";
import { flatten } from "./util.ts";

export let file = async (...args: tg.Args<File.Arg>) => {
	return await File.new(...args);
};

export class File {
	#state: File.State;

	constructor(state: File.State) {
		this.#state = state;
	}

	get state(): File.State {
		return this.#state;
	}

	static withId(id: File.Id): File {
		return new File({ id });
	}

	static async new(...args: tg.Args<File.Arg>): Promise<File> {
		let arg = await File.arg(...args);
		let contents = await tg.blob(arg.contents);
		let dependencies = arg.dependencies ?? [];
		let executable = arg.executable ?? false;
		return new File({
			object: { contents, dependencies, executable },
		});
	}

	static async arg(...args: tg.Args<File.Arg>): Promise<File.ArgObject> {
		let resolved = await Promise.all(args.map(tg.resolve));
		let flattened = flatten(resolved);
		let objects = await Promise.all(
			flattened.map(async (arg) => {
				if (arg === undefined) {
					return {};
				} else if (
					typeof arg === "string" ||
					arg instanceof Uint8Array ||
					tg.Blob.is(arg)
				) {
					return { contents: arg };
				} else if (arg instanceof File) {
					let object = await arg.object();
					return {
						contents: object.contents,
						dependencies: object.dependencies,
					};
				} else {
					return arg;
				}
			}),
		);
		let mutations = await tg.Args.createMutations(objects, {
			contents: "append",
			dependencies: "append",
		});
		let arg = await tg.Args.applyMutations(mutations);
		return arg;
	}

	static expect(value: unknown): File {
		tg.assert(value instanceof File);
		return value;
	}

	static assert(value: unknown): asserts value is File {
		tg.assert(value instanceof File);
	}

	async id(): Promise<File.Id> {
		await this.store();
		return this.#state.id!;
	}

	async object(): Promise<File.Object> {
		await this.load();
		return this.#state.object!;
	}

	async load() {
		if (this.#state.object === undefined) {
			let object = await syscall("load", this.#state.id!);
			tg.assert(object.kind === "file");
			this.#state.object = object.value;
		}
	}

	async store() {
		if (this.#state.id === undefined) {
			this.#state.id = await syscall("store", {
				kind: "file",
				value: this.#state.object!,
			});
		}
	}

	async contents(): Promise<tg.Blob> {
		return (await this.object()).contents;
	}

	async dependencies(): Promise<File.Dependencies | undefined> {
		return (await this.object()).dependencies;
	}

	async executable(): Promise<boolean> {
		return (await this.object()).executable;
	}

	async size(): Promise<number> {
		return (await this.contents()).size();
	}

	async bytes(): Promise<Uint8Array> {
		return (await this.contents()).bytes();
	}

	async text(): Promise<string> {
		return (await this.contents()).text();
	}
}

export namespace File {
	export type Arg =
		| undefined
		| string
		| Uint8Array
		| tg.Blob
		| tg.File
		| ArgObject;

	export type ArgObject = {
		contents?: tg.Blob.Arg | undefined;
		dependencies?: Array<tg.Artifact> | undefined;
		executable?: boolean | undefined;
	};

	export type Id = string;

	export type Object = {
		contents: tg.Blob;
		dependencies: Dependencies | undefined;
		executable: boolean;
	};

	export type Dependencies =
		| Array<tg.Object>
		| { [reference: string]: tg.Object };

	export type State = tg.Object.State<File.Id, File.Object>;
}
