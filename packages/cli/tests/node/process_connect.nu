use ../../test.nu *

const js_path = path self '../../../js'
cd $js_path

# Receipt, operation completion, process exit, and read EOF are independent handshakes.
let output = timeout 15 node --input-type=module -e '
    import assert from "node:assert/strict";
    import * as tg from "@tangramdotdev/client";
    tg.setEncoding({
        base64: { decode: value => new Uint8Array(Buffer.from(value, "base64")), encode: value => Buffer.from(value).toString("base64") },
        utf8: { decode: value => new TextDecoder().decode(value), encode: value => new TextEncoder().encode(value) },
    });
    const id = "pcs_010000000000000000000000000000000000000000000000000000";
    const tick = () => new Promise(resolve => setImmediate(resolve));
    class Queue {
        values = []; waiters = [];
        push(value) { const waiter = this.waiters.shift(); if (waiter) waiter({value, done: false}); else this.values.push(value); }
        next() { return this.values.length ? Promise.resolve({value: this.values.shift(), done: false}) : new Promise(resolve => this.waiters.push(resolve)); }
        [Symbol.asyncIterator]() { return this; }
    }
    let requests = [];
    let input;
    let events = new Queue();
    tg.client.send = async request => {
        requests.push(request.uri.path);
        input = request.body.sse();
        return new tg.Response(200, {"content-type": "text/event-stream"}, {sse: async function* () {
            for await (let event of events) { if (event instanceof Error) throw event; yield event; }
        }});
    };
    const emit = (event, value) => events.push({event, data: JSON.stringify(value)});
    const response = (id, kind, value) => emit("response", {id, error: null, output: {kind, ...(value === undefined ? {} : {value})}});
    const next = async () => {
        while (true) {
            let event = (await input.next()).value;
            if (event.event !== "ack") return {kind: event.event, value: JSON.parse(event.data)};
        }
    };
    let connected = false;
    let opening = tg.Process.connect(id, {reads: [{streams: ["stdout"]}]}).then(value => {connected = true; return value});
    await tick();
    let initial = await next();
    assert.equal(initial.value.arg.kind, "connect");
    emit("ack", {id: 0});
    await tick();
    assert.equal(connected, false);
    response(0, "connect", {cached: false, lease: null, location: null, process: id, tokens: {}, wait: null});
    let process = await opening;
    let written = false;
    let writing = process.stdin.write(new TextEncoder().encode("abc")).then(value => {written = true; return value});
    let write = await next();
    assert.equal(write.value.arg.kind, "write");
    emit("ack", {id: write.value.id});
    await tick();
    assert.equal(written, false);
    response(write.value.id, "write");
    let chunk = await next();
    assert.equal(chunk.value.value.message.value.kind, "chunk");
    await tick();
    assert.equal(written, false);
    emit("notification", {kind: "write", value: {id: write.value.id, message: {kind: "response", value: {kind: "write", value: {closed: false, length: 3}}}}});
    assert.equal(await writing, 3);
    let closing = process.stdin.close();
    let end = await next();
    assert.deepEqual(end.value.value.message, {kind: "request", value: {kind: "end", value: {position: 3}}});
    emit("notification", {kind: "write", value: {id: write.value.id, message: {kind: "response", value: {kind: "end"}}}});
    await closing;
    emit("notification", {kind: "wait", value: {exit: 0}});
    assert.equal((await process.wait()).exit, 0);
    let reading = process.stdout.text();
    emit("notification", {kind: "read", value: {id: 1, message: {kind: "notification", value: {kind: "chunk", value: {bytes: "YWJj", combined_position: 0, stream: "stdout", stream_position: 0}}}}});
    let read = await next();
    assert.deepEqual(read.value.value.message, {kind: "notification", value: {kind: "read", value: {position: 3}}});
    emit("notification", {kind: "read", value: {id: 1, message: {kind: "request", value: {kind: "end"}}}});
    assert.equal(await reading, "abc");
    let eof = await next();
    assert.deepEqual(eof.value.value.message, {kind: "response", value: {kind: "end"}});
    assert.equal((await process.wait()).exit, 0);
    assert.deepEqual(requests, ["/processes/connect"]);

    events = new Queue();
    let reopening = tg.Process.connect(id);
    await tick();
    await next();
    response(0, "connect", {cached: false, lease: null, location: null, process: id, tokens: {}, wait: null});
    let failed = await reopening;
    let signaling = failed.signal(tg.Process.Signal.TERM);
    let waiting = failed.wait();
    let signalError = assert.rejects(signaling, /transport failed/);
    let waitError = assert.rejects(waiting, /transport failed/);
    let signal = await next();
    assert.equal(signal.value.arg.kind, "signal");
    emit("ack", {id: signal.value.id});
    events.push(new Error("transport failed"));
    await Promise.all([signalError, waitError]);
    assert.deepEqual(requests, ["/processes/connect", "/processes/connect"]);

' | complete
success $output
