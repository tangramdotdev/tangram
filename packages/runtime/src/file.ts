import { Args } from "./args.ts";
import type { Artifact } from "./artifact.ts";
import { assert as assert_ } from "./assert.ts";
import { Blob, blob } from "./blob.ts";
import type { Object_ } from "./object.ts";
import { resolve } from "./resolve.ts";
import { flatten } from "./util.ts";

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
		let executable = arg.executable ?? false;
		let references = arg.references ?? [];
		return new File({
			object: { contents, executable, references },
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
						references: object.references,
					};
				} else {
					return arg;
				}
			}),
		);
		let mutations = await Args.createMutations(objects, {
			contents: "append",
			references: "append",
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
			let object = await syscall("object_load", this.#state.id!);
			assert_(object.kind === "file");
			this.#state.object = object.value;
		}
	}

	async store() {
		if (this.#state.id === undefined) {
			this.#state.id = await syscall("object_store", {
				kind: "file",
				value: this.#state.object!,
			});
		}
	}

	async contents(): Promise<Blob> {
		return (await this.object()).contents;
	}

	async executable(): Promise<boolean> {
		return (await this.object()).executable;
	}

	async references(): Promise<Array<Artifact>> {
		return (await this.object()).references;
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
		contents?: Blob.Arg;
		executable?: boolean;
		references?: Array<Artifact>;
	};

	export type Id = string;

	export type Object_ = {
		contents: Blob;
		executable: boolean;
		references: Array<Artifact>;
	};

	export type State = Object_.State<File.Id, File.Object_>;
}
