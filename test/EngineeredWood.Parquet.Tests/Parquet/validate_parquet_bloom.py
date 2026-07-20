# Copyright (c) clast-project. All rights reserved.
# Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

"""Validates Parquet bloom filters written by EngineeredWood.

Reads a Parquet file, extracts bloom filter blocks from the metadata,
and probes them with known present and absent values.

Usage: python validate_parquet_bloom.py <path> <column> <present_value> <absent_value>
Output: JSON with {present: bool, absent: bool}

The present value should be found (true), the absent value should not (false).
"""
import sys
import struct
import json
import xxhash  # pip install xxhash


def read_parquet_footer(data):
    """Parse the Parquet footer to extract row group metadata."""
    # Last 4 bytes = PAR1 magic
    assert data[-4:] == b'PAR1', "Not a Parquet file"
    # 4 bytes before magic = footer length (LE int32)
    footer_len = struct.unpack_from('<i', data, len(data) - 8)[0]
    footer_start = len(data) - 8 - footer_len
    return footer_start, data[footer_start:footer_start + footer_len]


def decode_thrift_varint(data, pos):
    """Decode a Thrift compact protocol varint (unsigned)."""
    result = 0
    shift = 0
    while True:
        b = data[pos]; pos += 1
        result |= (b & 0x7F) << shift
        shift += 7
        if not (b & 0x80):
            return result, pos


def decode_zigzag(n):
    return (n >> 1) ^ -(n & 1)


def parse_thrift_column_metadata(data, pos, end):
    """Parse ColumnMetaData Thrift struct, extracting bloom filter offset/length."""
    last_fid = 0
    bf_offset = None
    bf_length = None

    while pos < end:
        b = data[pos]; pos += 1
        if b == 0:  # stop
            break
        type_id = b & 0x0F
        delta = (b >> 4) & 0x0F
        if delta == 0:
            fid, pos = decode_thrift_varint(data, pos)
            fid = decode_zigzag(fid)
        else:
            fid = last_fid + delta
        last_fid = fid

        if fid == 14 and type_id == 6:  # i64 bloom_filter_offset
            val, pos = decode_thrift_varint(data, pos)
            bf_offset = decode_zigzag(val)
        elif fid == 15 and type_id == 5:  # i32 bloom_filter_length
            val, pos = decode_thrift_varint(data, pos)
            bf_length = decode_zigzag(val)
        else:
            pos = skip_thrift_field(data, pos, type_id)

    return bf_offset, bf_length


def skip_thrift_field(data, pos, type_id):
    """Skip a Thrift field value."""
    if type_id in (1, 2):  # bool true/false
        return pos
    elif type_id in (3,):  # byte
        return pos + 1
    elif type_id in (4,):  # i16
        _, pos = decode_thrift_varint(data, pos)
        return pos
    elif type_id in (5,):  # i32
        _, pos = decode_thrift_varint(data, pos)
        return pos
    elif type_id in (6,):  # i64
        _, pos = decode_thrift_varint(data, pos)
        return pos
    elif type_id == 7:  # double
        return pos + 8
    elif type_id == 8:  # binary/string
        length, pos = decode_thrift_varint(data, pos)
        return pos + length
    elif type_id in (9, 10):  # list, set
        header = data[pos]; pos += 1
        elem_type = header & 0x0F
        size = (header >> 4) & 0x0F
        if size == 15:
            size, pos = decode_thrift_varint(data, pos)
        for _ in range(size):
            pos = skip_thrift_field(data, pos, elem_type)
        return pos
    elif type_id == 11:  # map
        size, pos = decode_thrift_varint(data, pos)
        if size == 0:
            pos += 1  # empty map has 0 byte for types
            return pos
        types = data[pos]; pos += 1
        kt = (types >> 4) & 0x0F
        vt = types & 0x0F
        for _ in range(size):
            pos = skip_thrift_field(data, pos, kt)
            pos = skip_thrift_field(data, pos, vt)
        return pos
    elif type_id == 12:  # struct
        last = 0
        while pos < len(data):
            b = data[pos]; pos += 1
            if b == 0:
                break
            tid = b & 0x0F
            d = (b >> 4) & 0x0F
            if d == 0:
                _, pos = decode_thrift_varint(data, pos)
            pos = skip_thrift_field(data, pos, tid)
        return pos
    else:
        raise ValueError(f"Unknown Thrift type {type_id}")


def parse_bloom_filter_header(data, pos):
    """Parse the Thrift BloomFilterHeader to get numBytes and header length."""
    start = pos
    last_fid = 0
    num_bytes = 0

    while pos < len(data):
        b = data[pos]; pos += 1
        if b == 0:
            break
        type_id = b & 0x0F
        delta = (b >> 4) & 0x0F
        if delta == 0:
            fid, pos = decode_thrift_varint(data, pos)
            fid = decode_zigzag(fid)
        else:
            fid = last_fid + delta
        last_fid = fid

        if fid == 1 and type_id == 5:  # i32 numBytes
            val, pos = decode_thrift_varint(data, pos)
            num_bytes = decode_zigzag(val)
        else:
            pos = skip_thrift_field(data, pos, type_id)

    return num_bytes, pos - start


def sbbf_might_contain(bitset, num_blocks, value_bytes):
    """Probe a Split Block Bloom Filter."""
    hash_val = xxhash.xxh64(value_bytes).intdigest()

    SALT = [0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d,
            0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31]

    upper = (hash_val >> 32) & 0xFFFFFFFF
    block_index = (upper * num_blocks) >> 32
    key = hash_val & 0xFFFFFFFF
    block_offset = block_index * 32

    for i in range(8):
        mask = 1 << (((key * SALT[i]) & 0xFFFFFFFF) >> 27)
        word = struct.unpack_from('<I', bitset, block_offset + i * 4)[0]
        if (word & mask) == 0:
            return False
    return True


def find_bloom_filter_in_footer(file_data):
    """Walk the Parquet footer Thrift to find bloom filter offset for the first column."""
    footer_start, footer = read_parquet_footer(file_data)

    # The footer is a Thrift FileMetaData struct. We need to find the first
    # RowGroup → first ColumnChunk → ColumnMetaData → bloom_filter_offset.
    # This is a simplified parser that walks the Thrift structure.
    pos = 0
    last_fid = 0

    while pos < len(footer):
        b = footer[pos]; pos += 1
        if b == 0:
            break
        type_id = b & 0x0F
        delta = (b >> 4) & 0x0F
        if delta == 0:
            fid, pos = decode_thrift_varint(footer, pos)
            fid = decode_zigzag(fid)
        else:
            fid = last_fid + delta
        last_fid = fid

        if fid == 4 and type_id == 9:  # list of RowGroups
            # Parse list header
            header = footer[pos]; pos += 1
            elem_type = header & 0x0F
            size = (header >> 4) & 0x0F
            if size == 15:
                size, pos = decode_thrift_varint(footer, pos)

            # Parse first RowGroup struct
            if size > 0:
                # RowGroup is a struct
                rg_last_fid = 0
                while pos < len(footer):
                    b = footer[pos]; pos += 1
                    if b == 0:
                        break
                    tid = b & 0x0F
                    d = (b >> 4) & 0x0F
                    if d == 0:
                        rg_fid, pos = decode_thrift_varint(footer, pos)
                        rg_fid = decode_zigzag(rg_fid)
                    else:
                        rg_fid = rg_last_fid + d
                    rg_last_fid = rg_fid

                    if rg_fid == 1 and tid == 9:  # list of ColumnChunks
                        cc_header = footer[pos]; pos += 1
                        cc_elem_type = cc_header & 0x0F
                        cc_size = (cc_header >> 4) & 0x0F
                        if cc_size == 15:
                            cc_size, pos = decode_thrift_varint(footer, pos)

                        # For each column chunk, find the one matching our column
                        results = []
                        for _ in range(cc_size):
                            cc_start = pos
                            cc_last_fid = 0
                            bf_offset = None
                            bf_length = None
                            while pos < len(footer):
                                b = footer[pos]; pos += 1
                                if b == 0:
                                    break
                                tid2 = b & 0x0F
                                d2 = (b >> 4) & 0x0F
                                if d2 == 0:
                                    cc_fid, pos = decode_thrift_varint(footer, pos)
                                    cc_fid = decode_zigzag(cc_fid)
                                else:
                                    cc_fid = cc_last_fid + d2
                                cc_last_fid = cc_fid

                                if cc_fid == 3 and tid2 == 12:  # ColumnMetaData struct
                                    bf_offset, bf_length = parse_thrift_column_metadata(footer, pos, len(footer))
                                    # Skip past the struct we just parsed
                                    pos = skip_thrift_field(footer, pos, 12)
                                    # Undo the extra struct we skipped
                                    # Actually, parse_thrift_column_metadata consumed until stop
                                    # but skip_thrift_field will re-consume. Let me fix this.
                                    # Just use skip for the whole field.
                                else:
                                    pos = skip_thrift_field(footer, pos, tid2)

                            results.append((bf_offset, bf_length))
                        return results
                    else:
                        pos = skip_thrift_field(footer, pos, tid)
        else:
            pos = skip_thrift_field(footer, pos, type_id)

    return []


def main():
    if len(sys.argv) != 5:
        print("Usage: validate_parquet_bloom.py <path> <column_index> <present_value> <absent_value>")
        sys.exit(1)

    path = sys.argv[1]
    col_index = int(sys.argv[2])
    present_value = sys.argv[3]
    absent_value = sys.argv[4]

    with open(path, 'rb') as f:
        file_data = f.read()

    # Find bloom filter offsets for all columns
    bf_info = find_bloom_filter_in_footer(file_data)

    if col_index >= len(bf_info):
        print(json.dumps({"error": f"Column {col_index} not found, only {len(bf_info)} columns"}))
        sys.exit(1)

    bf_offset, bf_length = bf_info[col_index]
    if bf_offset is None:
        print(json.dumps({"error": "No bloom filter for this column"}))
        sys.exit(1)

    # Read and parse the bloom filter block
    bf_data = file_data[bf_offset:bf_offset + (bf_length or 4096)]
    num_bytes, header_len = parse_bloom_filter_header(bf_data, 0)
    bitset = bf_data[header_len:header_len + num_bytes]
    num_blocks = num_bytes // 32

    # Probe with present and absent values
    present_bytes = present_value.encode('utf-8')
    absent_bytes = absent_value.encode('utf-8')

    present_result = sbbf_might_contain(bitset, num_blocks, present_bytes)
    absent_result = sbbf_might_contain(bitset, num_blocks, absent_bytes)

    print(json.dumps({
        "present": present_result,
        "absent": absent_result,
        "bf_offset": bf_offset,
        "bf_length": bf_length,
        "num_bytes": num_bytes,
        "num_blocks": num_blocks,
    }))


if __name__ == '__main__':
    main()
