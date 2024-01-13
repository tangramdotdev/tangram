import { assert as assert_ } from "./assert.ts";
import { Object_ } from "./object.ts";
import * as syscall from "./syscall.ts";

export class Lock {
	#state: Lock.State;

	constructor(state: Lock.State) {
		this.#state = state;
	}

	get state(): Lock.State {
		return this.#state;
	}

	static withId(id: Lock.Id): Lock {
		return new Lock({ id });
	}

	static is(value: unknown): value is Lock {
		return value instanceof Lock;
	}

	static expect(value: unknown): Lock {
		assert_(Lock.is(value));
		return value;
	}

	static assert(value: unknown): asserts value is Lock {
		assert_(Lock.is(value));
	}

	async id(): Promise<Lock.Id> {
		await this.store();
		return this.#state.id!;
	}

	async object(): Promise<Lock.Object_> {
		await this.load();
		return this.#state.object!;
	}

	async load() {
		if (this.#state.object === undefined) {
			let object = await syscall.load(this.#state.id!);
			assert_(object.kind === "lock");
			this.#state.object = object.value;
		}
	}

	async store() {
		if (this.#state.id === undefined) {
			this.#state.id = await syscall.store({
				kind: "lock",
				value: this.#state.object!,
			});
		}
	}


	async root(): Promise<number> {
		return (await this.object()).root
	}

	async nodes(): Promise<Array<Lock.Node>> {
		return (await this.object()).nodes
	}
}

export namespace Lock {
	export type Arg = Lock | ArgObject | Array<Arg>;

	export type ArgObject = {
		dependencies?: { [dependency: string]: Lock.Arg };
	};

	export type Id = string;

	export type Object_ = {
		root: number,
		nodes: Array<Node>,
	};

	export type Node = {
		dependencies: { [dependency: string]: Lock | number };
	};

	export type State = Object_.State<Lock.Id, Lock.Object_>;
}
