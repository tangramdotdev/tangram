import { Args } from "./args.ts";
import type { Artifact } from "./artifact.ts";
import { assert as assert_ } from "./assert.ts";
import { Blob, blob } from "./blob.ts";
import type { Object_ } from "./object.ts";
import { resolve } from "./resolve.ts";
import { flatten } from "./util.ts";
import type { Value } from "./value.ts";

export let file = async (...args: Args<File.Arg>) => {
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

	static async new(...args: Args<File.Arg>): Promise<File> {
		let arg = await File.arg(...args);
		let contents = await blob(arg.contents);
		let dependencies = arg.dependencies ?? [];
		let executable = arg.executable ?? false;
		return new File({
			object: { contents, dependencies, executable, metadata },
		});
	}

	static async arg(...args: Args<File.Arg>): Promise<File.ArgObject> {
		let resolved = await Promise.all(args.map(resolve));
		let flattened = flatten(resolved);
		let objects = await Promise.all(
			flattened.map(async (arg) => {
				if (arg === undefined) {
					return {};
				} else if (
					typeof arg === "string" ||
					arg instanceof Uint8Array ||
					Blob.is(arg)
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
		let mutations = await Args.createMutations(objects, {
			contents: "append",
			dependencies: "append",
		});
		let arg = await Args.applyMutations(mutations);
		return arg;
	}

	static expect(value: unknown): File {
		assert_(value instanceof File);
		return value;
	}

	static assert(value: unknown): asserts value is File {
		assert_(value instanceof File);
	}

	async id(): Promise<File.Id> {
		await this.store();
		return this.#state.id!;
	}

	async object(): Promise<File.Object_> {
		await this.load();
		return this.#state.object!;
	}

	async load() {
		if (this.#state.object === undefined) {
			let object = await syscall("load", this.#state.id!);
			assert_(object.kind === "file");
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

	async contents(): Promise<Blob> {
		return (await this.object()).contents;
	}

	async dependencies(): Promise<Array<Artifact>> {
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
	export type Arg = undefined | string | Uint8Array | Blob | File | ArgObject;

	export type ArgObject = {
		contents?: Blob.Arg | undefined;
		dependencies?: Array<Artifact> | undefined;
		executable?: boolean | undefined;
		metadata: { [key: string]: Value };
	};

	export type Id = string;

	export type Object_ = {
		contents: Blob;
		dependencies: Array<Artifact>;
		executable: boolean;
		metadata: { [key: string]: Value };
	};

	export type State = Object_.State<File.Id, File.Object_>;
}
