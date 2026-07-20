# Copyright (c) clast-project. All rights reserved.
# Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

"""Validates ORC bloom filters written by EngineeredWood.

Uses PyArrow to read the ORC file structure and manually parses bloom filter
protobuf streams to probe with the C++ murmur3_64 hash.

Usage: python validate_orc_bloom.py <path> <column_name> <present_value> <absent_value>
Output: JSON with {present: bool, absent: bool}
"""
import sys
import struct
import json
import zlib
import ctypes

import pyarrow.orc as orc


# ─── C++ ORC murmur3_64 (single accumulator, 8-byte blocks) ───

def rotl64(x, r):
    x &= 0xFFFFFFFFFFFFFFFF
    return ((x << r) | (x >> (64 - r))) & 0xFFFFFFFFFFFFFFFF

def fmix64(k):
    k &= 0xFFFFFFFFFFFFFFFF
    k ^= k >> 33
    k = (k * 0xff51afd7ed558ccd) & 0xFFFFFFFFFFFFFFFF
    k ^= k >> 33
    k = (k * 0xc4ceb9fe1a85ec53) & 0xFFFFFFFFFFFFFFFF
    k ^= k >> 33
    return k

def murmur3_64_cpp(data, seed=104729):
    """C++ ORC murmur3_64: single accumulator, 8-byte blocks."""
    c1 = 0x87c37b91114253d5
    c2 = 0x4cf5ad432745937f
    h = seed & 0xFFFFFFFFFFFFFFFF
    blocks = len(data) >> 3

    for i in range(blocks):
        k = struct.unpack_from('<Q', data, i * 8)[0]
        k = (k * c1) & 0xFFFFFFFFFFFFFFFF
        k = rotl64(k, 31)
        k = (k * c2) & 0xFFFFFFFFFFFFFFFF
        h ^= k
        h = rotl64(h, 27)
        h = (h * 5 + 0x52dce729) & 0xFFFFFFFFFFFFFFFF

    tail = 0
    idx = blocks << 3
    remaining = len(data) - idx
    for j in range(remaining - 1, -1, -1):
        tail = ((tail << 8) | data[idx + j]) & 0xFFFFFFFFFFFFFFFF
    if remaining > 0:
        tail = (tail * c1) & 0xFFFFFFFFFFFFFFFF
        tail = rotl64(tail, 31)
        tail = (tail * c2) & 0xFFFFFFFFFFFFFFFF
        h ^= tail

    h ^= len(data)
    h = fmix64(h)
    return h


def bloom_test_hash(hash64, num_hash_functions, num_bits):
    """ORC bloom filter probe using the 'Less Hashing' technique."""
    # Convert to signed for int32 arithmetic
    hash64_signed = ctypes.c_int64(hash64).value
    hash1 = ctypes.c_int32(hash64_signed & 0xFFFFFFFF).value
    hash2 = ctypes.c_int32((hash64 >> 32) & 0xFFFFFFFF).value

    positions = []
    for i in range(1, num_hash_functions + 1):
        combined = ctypes.c_int32(hash1 + i * hash2).value
        if combined < 0:
            combined = ~combined
        pos = combined % num_bits
        positions.append(pos)
    return positions


def bloom_might_contain(bitset_longs, num_hash_functions, data_bytes):
    """Check if data might be in the bloom filter."""
    num_bits = len(bitset_longs) * 64
    hash64 = murmur3_64_cpp(data_bytes)
    positions = bloom_test_hash(hash64, num_hash_functions, num_bits)

    for pos in positions:
        word_idx = pos >> 6
        bit_idx = pos & 63
        if (bitset_longs[word_idx] & (1 << bit_idx)) == 0:
            return False
    return True


def parse_protobuf_varint(data, pos):
    result = 0
    shift = 0
    while pos < len(data):
        b = data[pos]; pos += 1
        result |= (b & 0x7F) << shift
        shift += 7
        if not (b & 0x80):
            return result, pos
    return result, pos


def parse_bloom_filter_index(data):
    """Parse a BloomFilterIndex protobuf message, extracting bloom filters."""
    filters = []
    pos = 0
    while pos < len(data):
        tag = data[pos]; pos += 1
        field_num = tag >> 3
        wire_type = tag & 7

        if field_num == 1 and wire_type == 2:  # BloomFilter sub-message
            length, pos = parse_protobuf_varint(data, pos)
            bf_data = data[pos:pos + length]
            pos += length

            # Parse BloomFilter message
            num_hash = 0
            bitset_longs = []
            bf_pos = 0
            while bf_pos < len(bf_data):
                bf_tag = bf_data[bf_pos]; bf_pos += 1
                bf_fn = bf_tag >> 3
                bf_wt = bf_tag & 7

                if bf_fn == 1 and bf_wt == 0:  # num_hash_functions (varint)
                    num_hash, bf_pos = parse_protobuf_varint(bf_data, bf_pos)
                elif bf_fn == 3 and bf_wt == 2:  # utf8bitset (bytes, LE)
                    length, bf_pos = parse_protobuf_varint(bf_data, bf_pos)
                    bitset_bytes = bf_data[bf_pos:bf_pos + length]
                    bf_pos += length
                    # Parse as little-endian int64 array
                    num_longs = len(bitset_bytes) // 8
                    bitset_longs = list(struct.unpack_from(f'<{num_longs}q', bitset_bytes))
                elif bf_wt == 0:
                    _, bf_pos = parse_protobuf_varint(bf_data, bf_pos)
                elif bf_wt == 1:
                    bf_pos += 8
                elif bf_wt == 2:
                    l, bf_pos = parse_protobuf_varint(bf_data, bf_pos)
                    bf_pos += l
                elif bf_wt == 5:
                    bf_pos += 4

            filters.append((num_hash, bitset_longs))
        else:
            # Skip unknown field
            if wire_type == 0:
                _, pos = parse_protobuf_varint(data, pos)
            elif wire_type == 1:
                pos += 8
            elif wire_type == 2:
                l, pos = parse_protobuf_varint(data, pos)
                pos += l
            elif wire_type == 5:
                pos += 4

    return filters


def main():
    if len(sys.argv) != 5:
        print("Usage: validate_orc_bloom.py <path> <column_name> <present_value> <absent_value>")
        sys.exit(1)

    path = sys.argv[1]
    column_name = sys.argv[2]
    present_value = sys.argv[3]
    absent_value = sys.argv[4]

    with open(path, 'rb') as f:
        file_data = f.read()

    # Read the ORC file structure
    ps_len = file_data[-1]
    ps_bytes = file_data[-(1 + ps_len):-1]

    # Parse postscript
    ps = {}
    pos = 0
    while pos < len(ps_bytes):
        tag = ps_bytes[pos]; pos += 1
        fn = tag >> 3; wt = tag & 7
        if wt == 0:
            val, pos = parse_protobuf_varint(ps_bytes, pos)
            ps[fn] = val
        elif wt == 2:
            l, pos = parse_protobuf_varint(ps_bytes, pos)
            ps[fn] = ps_bytes[pos:pos + l]
            pos += l

    footer_len = ps[1]
    compression = ps[2]  # 0=none, 1=zlib
    comp_block_size = ps.get(3, 65536)

    # Read footer
    footer_start = len(file_data) - 1 - ps_len - footer_len
    footer_compressed = file_data[footer_start:footer_start + footer_len]

    if compression == 0:
        footer_bytes = footer_compressed
    else:
        # ORC block compression: 3-byte header
        hdr = footer_compressed[0] | (footer_compressed[1] << 8) | (footer_compressed[2] << 16)
        is_original = hdr & 1
        chunk_len = hdr >> 1
        if is_original:
            footer_bytes = footer_compressed[3:3 + chunk_len]
        else:
            footer_bytes = zlib.decompress(footer_compressed[3:3 + chunk_len], -15)

    # Parse footer to find stripes
    # field 3 = repeated StripeInformation, field 8 = repeated Type
    stripes = []
    types = []
    ft_pos = 0
    while ft_pos < len(footer_bytes):
        tag = footer_bytes[ft_pos]; ft_pos += 1
        fn = tag >> 3; wt = tag & 7
        if fn == 3 and wt == 2:  # StripeInformation
            l, ft_pos = parse_protobuf_varint(footer_bytes, ft_pos)
            stripe_data = footer_bytes[ft_pos:ft_pos + l]
            ft_pos += l
            # Parse stripe info
            si = {}
            sp = 0
            while sp < len(stripe_data):
                st = stripe_data[sp]; sp += 1
                sfn = st >> 3; swt = st & 7
                if swt == 0:
                    val, sp = parse_protobuf_varint(stripe_data, sp)
                    si[sfn] = val
                elif swt == 2:
                    l2, sp = parse_protobuf_varint(stripe_data, sp)
                    sp += l2
            stripes.append(si)
        elif fn == 8 and wt == 2:  # Type
            l, ft_pos = parse_protobuf_varint(footer_bytes, ft_pos)
            type_data = footer_bytes[ft_pos:ft_pos + l]
            ft_pos += l
            # Parse type
            tp = {}
            sp = 0
            while sp < len(type_data):
                st = type_data[sp]; sp += 1
                sfn = st >> 3; swt = st & 7
                if swt == 0:
                    val, sp = parse_protobuf_varint(type_data, sp)
                    tp[sfn] = val
                elif swt == 2:
                    l2, sp = parse_protobuf_varint(type_data, sp)
                    tp[sfn] = type_data[sp:sp + l2]
                    sp += l2
            types.append(tp)
        else:
            if wt == 0: _, ft_pos = parse_protobuf_varint(footer_bytes, ft_pos)
            elif wt == 1: ft_pos += 8
            elif wt == 2:
                l, ft_pos = parse_protobuf_varint(footer_bytes, ft_pos)
                ft_pos += l
            elif wt == 5: ft_pos += 4

    # Find column ID for the target column name
    # Types[0] is root struct, subtypes field (4) has children, fieldNames field (6) has names
    # But simpler: use PyArrow to get the schema
    reader = orc.ORCFile(path)
    schema_names = reader.schema.names
    if column_name not in schema_names:
        print(json.dumps({"error": f"Column '{column_name}' not found"}))
        sys.exit(1)
    col_index = schema_names.index(column_name) + 1  # +1 for root struct at index 0

    # Read stripe 0
    stripe = stripes[0]
    stripe_offset = stripe[1]  # offset
    index_length = stripe[2]   # index_length
    data_length = stripe[3]    # data_length
    footer_length = stripe[4]  # footer_length

    # Read index section and stripe footer
    index_data = file_data[stripe_offset:stripe_offset + index_length]
    sf_start = stripe_offset + index_length + data_length
    sf_data = file_data[sf_start:sf_start + footer_length]

    if compression != 0:
        hdr = sf_data[0] | (sf_data[1] << 8) | (sf_data[2] << 16)
        is_original = hdr & 1
        chunk_len = hdr >> 1
        if is_original:
            sf_bytes = sf_data[3:3 + chunk_len]
        else:
            sf_bytes = zlib.decompress(sf_data[3:3 + chunk_len], -15)
    else:
        sf_bytes = sf_data

    # Parse stripe footer to find bloom filter streams
    streams = []
    sp = 0
    while sp < len(sf_bytes):
        tag = sf_bytes[sp]; sp += 1
        fn = tag >> 3; wt = tag & 7
        if fn == 1 and wt == 2:  # Stream message
            l, sp = parse_protobuf_varint(sf_bytes, sp)
            stream_data = sf_bytes[sp:sp + l]
            sp += l
            s = {}
            ssp = 0
            while ssp < len(stream_data):
                st = stream_data[ssp]; ssp += 1
                sfn = st >> 3; swt = st & 7
                if swt == 0:
                    val, ssp = parse_protobuf_varint(stream_data, ssp)
                    s[sfn] = val
            streams.append(s)
        else:
            if wt == 0: _, sp = parse_protobuf_varint(sf_bytes, sp)
            elif wt == 1: sp += 8
            elif wt == 2:
                l, sp = parse_protobuf_varint(sf_bytes, sp)
                sp += l
            elif wt == 5: sp += 4

    # Find bloom filter stream for our column (kind=8 = BLOOM_FILTER_UTF8)
    offset = 0
    bf_stream = None
    bf_offset = None
    for s in streams:
        kind = s.get(1, 0)
        col = s.get(2, 0)
        length = s.get(3, 0)
        if kind in (6, 7, 8):  # index streams
            if kind == 8 and col == col_index:
                bf_stream = s
                bf_offset = offset
            offset += length
        else:
            break  # past index section

    if bf_stream is None:
        print(json.dumps({"error": f"No bloom filter stream for column {col_index}"}))
        sys.exit(1)

    bf_length = bf_stream.get(3, 0)
    bf_raw = index_data[bf_offset:bf_offset + bf_length]

    if compression != 0:
        hdr = bf_raw[0] | (bf_raw[1] << 8) | (bf_raw[2] << 16)
        is_original = hdr & 1
        chunk_len = hdr >> 1
        if is_original:
            bf_decompressed = bf_raw[3:3 + chunk_len]
        else:
            bf_decompressed = zlib.decompress(bf_raw[3:3 + chunk_len], -15)
    else:
        bf_decompressed = bf_raw

    # Parse BloomFilterIndex protobuf
    filters = parse_bloom_filter_index(bf_decompressed)

    if not filters:
        print(json.dumps({"error": "No bloom filter entries found"}))
        sys.exit(1)

    num_hash, bitset_longs = filters[0]

    present_bytes = present_value.encode('utf-8')
    absent_bytes = absent_value.encode('utf-8')

    present_result = bloom_might_contain(bitset_longs, num_hash, present_bytes)
    absent_result = bloom_might_contain(bitset_longs, num_hash, absent_bytes)

    print(json.dumps({
        "present": present_result,
        "absent": absent_result,
        "num_hash_functions": num_hash,
        "num_bits": len(bitset_longs) * 64,
    }))


if __name__ == '__main__':
    main()
