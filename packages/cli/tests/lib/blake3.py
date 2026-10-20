# A pure Python implementation of BLAKE3, following the reference implementation, for test helpers that must reproduce hashes that the server computes.

OUT_LEN = 32
BLOCK_LEN = 64
CHUNK_LEN = 1024

CHUNK_START = 1 << 0
CHUNK_END = 1 << 1
PARENT = 1 << 2
ROOT = 1 << 3
DERIVE_KEY_CONTEXT = 1 << 5
DERIVE_KEY_MATERIAL = 1 << 6

IV = [0x6A09E667, 0xBB67AE85, 0x3C6EF372, 0xA54FF53A, 0x510E527F, 0x9B05688C, 0x1F83D9AB, 0x5BE0CD19]
MSG_PERMUTATION = [2, 6, 3, 10, 7, 0, 4, 13, 1, 11, 12, 5, 9, 14, 15, 8]


def rotate_right(x, n):
    return ((x >> n) | (x << (32 - n))) & 0xFFFFFFFF


def g(state, a, b, c, d, mx, my):
    state[a] = (state[a] + state[b] + mx) & 0xFFFFFFFF
    state[d] = rotate_right(state[d] ^ state[a], 16)
    state[c] = (state[c] + state[d]) & 0xFFFFFFFF
    state[b] = rotate_right(state[b] ^ state[c], 12)
    state[a] = (state[a] + state[b] + my) & 0xFFFFFFFF
    state[d] = rotate_right(state[d] ^ state[a], 8)
    state[c] = (state[c] + state[d]) & 0xFFFFFFFF
    state[b] = rotate_right(state[b] ^ state[c], 7)


def round_(state, m):
    g(state, 0, 4, 8, 12, m[0], m[1])
    g(state, 1, 5, 9, 13, m[2], m[3])
    g(state, 2, 6, 10, 14, m[4], m[5])
    g(state, 3, 7, 11, 15, m[6], m[7])
    g(state, 0, 5, 10, 15, m[8], m[9])
    g(state, 1, 6, 11, 12, m[10], m[11])
    g(state, 2, 7, 8, 13, m[12], m[13])
    g(state, 3, 4, 9, 14, m[14], m[15])


def compress(chaining_value, block_words, counter, block_len, flags):
    state = chaining_value[:] + IV[:4] + [counter & 0xFFFFFFFF, counter >> 32, block_len, flags]
    block = block_words[:]
    for index in range(7):
        round_(state, block)
        if index < 6:
            block = [block[i] for i in MSG_PERMUTATION]
    for i in range(8):
        state[i] ^= state[i + 8]
        state[i + 8] ^= chaining_value[i]
    return state


def words(block):
    block = block.ljust(BLOCK_LEN, b"\0")
    return [int.from_bytes(block[i : i + 4], "little") for i in range(0, BLOCK_LEN, 4)]


def chunk_output(key, chunk, counter, flags):
    chaining_value = key
    blocks = [chunk[i : i + BLOCK_LEN] for i in range(0, len(chunk), BLOCK_LEN)] or [b""]
    for index, block in enumerate(blocks):
        block_flags = flags
        if index == 0:
            block_flags |= CHUNK_START
        if index == len(blocks) - 1:
            return chaining_value, words(block), counter, len(block), block_flags | CHUNK_END
        chaining_value = compress(chaining_value, words(block), counter, BLOCK_LEN, block_flags)[:8]


def hash_(input, key=IV, flags=0, length=OUT_LEN):
    # Compute the output of each chunk, merging completed subtrees as the chunks arrive.
    chunks = [input[i : i + CHUNK_LEN] for i in range(0, len(input), CHUNK_LEN)] or [b""]
    stack = []
    for counter, chunk in enumerate(chunks):
        output = chunk_output(key, chunk, counter, flags)
        if counter == len(chunks) - 1:
            break
        chaining_value = compress(*output)[:8]
        total = counter + 1
        while total & 1 == 0:
            chaining_value = compress(key, stack.pop() + chaining_value, 0, BLOCK_LEN, PARENT | flags)[:8]
            total >>= 1
        stack.append(chaining_value)
    while stack:
        chaining_value = compress(*output)[:8]
        output = (key, stack.pop() + chaining_value, 0, BLOCK_LEN, PARENT | flags)

    # Produce the root output.
    chaining_value, block_words, _, block_len, output_flags = output
    result = b""
    counter = 0
    while len(result) < length:
        state = compress(chaining_value, block_words, counter, block_len, output_flags | ROOT)
        result += b"".join(word.to_bytes(4, "little") for word in state)
        counter += 1
    return result[:length]


def derive_key(context, material):
    context_key = hash_(context.encode(), flags=DERIVE_KEY_CONTEXT)
    key = [int.from_bytes(context_key[i : i + 4], "little") for i in range(0, 32, 4)]
    return hash_(material, key=key, flags=DERIVE_KEY_MATERIAL)
