export type Limits = {
	bytes: number;
	messages: number;
};

export namespace Limits {
	export function validate(limits: Limits): void {
		if (
			!valid(limits.bytes) ||
			limits.bytes === 0 ||
			!valid(limits.messages) ||
			limits.messages === 0 ||
			limits.messages > Math.floor((Number.MAX_SAFE_INTEGER - 4) / 2)
		)
			throw new Error("invalid flow limits");
	}
}

export type Consumption = {
	bytes: number;
	messages: number;
};

export class Sender {
	#consumption: Consumption = { bytes: 0, messages: 0 };
	#limits: Limits;
	#sent: Consumption = { bytes: 0, messages: 0 };

	constructor(limits: Limits) {
		Limits.validate(limits);
		this.#limits = { ...limits };
	}

	remainingBytes(): number {
		return this.#limits.bytes - (this.#sent.bytes - this.#consumption.bytes);
	}

	available(bytes: number): boolean {
		return (
			valid(bytes) &&
			bytes <= this.remainingBytes() &&
			this.#sent.messages - this.#consumption.messages < this.#limits.messages
		);
	}

	send(bytes: number): void {
		if (!this.available(bytes)) throw new Error("the flow window was exceeded");
		this.#sent = add(this.#sent, bytes);
	}

	update(consumption: Consumption): void {
		if (
			!valid(consumption.bytes) ||
			!valid(consumption.messages) ||
			consumption.bytes < this.#consumption.bytes ||
			consumption.messages < this.#consumption.messages ||
			consumption.bytes > this.#sent.bytes ||
			consumption.messages > this.#sent.messages
		)
			throw new Error("invalid flow consumption");
		this.#consumption = { ...consumption };
	}
}

export class Receiver {
	#consumption: Consumption = { bytes: 0, messages: 0 };
	#limits: Limits;
	#received: Consumption = { bytes: 0, messages: 0 };
	#reported: Consumption = { bytes: 0, messages: 0 };

	constructor(limits: Limits) {
		Limits.validate(limits);
		this.#limits = { ...limits };
	}

	receive(bytes: number): void {
		let received = add(this.#received, bytes);
		if (
			received.bytes - this.#consumption.bytes > this.#limits.bytes ||
			received.messages - this.#consumption.messages > this.#limits.messages
		)
			throw new Error("the flow window was exceeded");
		this.#received = received;
	}

	consume(bytes: number): Consumption | null {
		let consumption = add(this.#consumption, bytes);
		if (
			consumption.bytes > this.#received.bytes ||
			consumption.messages > this.#received.messages
		)
			throw new Error("invalid flow consumption");
		this.#consumption = consumption;
		if (
			this.#consumption.bytes - this.#reported.bytes <
				Math.ceil(this.#limits.bytes / 2) &&
			this.#consumption.messages - this.#reported.messages <
				Math.ceil(this.#limits.messages / 2)
		)
			return null;
		return this.flush();
	}

	flush(): Consumption | null {
		if (
			this.#consumption.bytes === this.#reported.bytes &&
			this.#consumption.messages === this.#reported.messages
		)
			return null;
		this.#reported = { ...this.#consumption };
		return { ...this.#consumption };
	}
}

function add(value: Consumption, bytes: number): Consumption {
	if (!valid(bytes) || !valid(value.bytes + bytes))
		throw new Error("the flow byte count overflowed");
	if (!valid(value.messages + 1))
		throw new Error("the flow message count overflowed");
	return { bytes: value.bytes + bytes, messages: value.messages + 1 };
}

function valid(value: number): boolean {
	return Number.isSafeInteger(value) && value >= 0;
}
