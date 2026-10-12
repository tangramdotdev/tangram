import * as tg from "../index.ts";
import type { Pool as Options } from "../config.ts";
import type { Host } from "../host.ts";

type Entry = {
	discarded: boolean;
	expiresAt: number;
	shared: number;
	value: Host.Http2.ClientHttp2Session;
};
export type Lease = {
	discard(): void;
	release(): void;
	value: Host.Http2.ClientHttp2Session;
};

export class Pool {
	#entries: Entry[] = [];
	#epoch = 0;
	#expirationTask: { canceled: boolean; stopper?: Host.Stopper } | undefined;
	#pending = 0;
	#waiters: (() => void)[] = [];
	constructor(
		readonly options: Options,
		readonly create: () => Promise<Host.Http2.ClientHttp2Session>,
	) {}

	async get(): Promise<Lease> {
		while (true) {
			this.#expire();
			let entry = this.#entries.find(
				(entry) => entry.shared < this.options.shared,
			);
			if (entry !== undefined) {
				entry.shared++;
				return this.#lease(entry);
			}
			if (this.#entries.length + this.#pending < this.options.max) {
				let epoch = this.#epoch;
				this.#pending++;
				try {
					let value = await this.create();
					if (epoch !== this.#epoch) {
						void value.close();
						continue;
					}
					entry = {
						discarded: false,
						expiresAt: this.#expiration(),
						shared: 1,
						value,
					};
					this.#entries.push(entry);
					return this.#lease(entry);
				} finally {
					this.#pending--;
					this.#notify();
				}
			}
			await new Promise<void>((resolve) => this.#waiters.push(resolve));
		}
	}

	clear(): void {
		this.#epoch++;
		this.#cancelExpiration();
		for (let entry of this.#entries) {
			entry.discarded = true;
			if (entry.shared === 0) void entry.value.close();
		}
		this.#entries = [];
		this.#notify();
	}

	#lease(entry: Entry): Lease {
		let released = false;
		return {
			discard: () => {
				entry.discarded = true;
				this.#entries = this.#entries.filter((value) => value !== entry);
				this.#scheduleExpiration();
				this.#notify();
			},
			release: () => {
				if (released) return;
				released = true;
				entry.shared--;
				entry.expiresAt = this.#expiration();
				if (entry.discarded && entry.shared === 0) void entry.value.close();
				this.#scheduleExpiration();
				this.#notify();
			},
			value: entry.value,
		};
	}

	#expire(): void {
		for (let entry of this.#entries) {
			if (
				!entry.value.closed &&
				(entry.shared !== 0 ||
					this.#entries.length <= this.options.min ||
					Date.now() < entry.expiresAt)
			)
				continue;
			entry.discarded = true;
			this.#entries = this.#entries.filter((value) => value !== entry);
			if (entry.shared === 0) void entry.value.close();
		}
	}
	#cancelExpiration(): void {
		let task = this.#expirationTask;
		if (task === undefined) return;
		task.canceled = true;
		if (task.stopper !== undefined) void tg.host.stopperStop(task.stopper);
		this.#expirationTask = undefined;
	}
	#scheduleExpiration(): void {
		this.#cancelExpiration();
		if (this.#entries.length <= this.options.min) return;
		let expiresAt = Math.min(
			...this.#entries
				.filter((entry) => entry.shared === 0)
				.map((entry) => entry.expiresAt),
		);
		if (!Number.isFinite(expiresAt)) return;
		let task: { canceled: boolean; stopper?: Host.Stopper } = {
			canceled: false,
		};
		this.#expirationTask = task;
		void (async () => {
			let stopper = await tg.host.stopperOpen();
			task.stopper = stopper;
			try {
				if (task.canceled) return;
				try {
					await tg.host.sleep(
						Math.max(0, expiresAt - Date.now()) / 1000,
						stopper,
					);
				} catch (error) {
					if (!task.canceled) throw error;
				}
				if (task.canceled) return;
				this.#expirationTask = undefined;
				this.#expire();
				this.#notify();
				this.#scheduleExpiration();
			} finally {
				await tg.host.stopperClose(stopper);
			}
		})();
	}
	#expiration(): number {
		return this.options.ttl === undefined
			? Infinity
			: Date.now() + this.options.ttl * 1000;
	}
	#notify(): void {
		for (let waiter of this.#waiters.splice(0)) waiter();
	}
}
