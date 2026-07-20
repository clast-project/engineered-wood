# Copyright (c) clast-project. All rights reserved.
# Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

"""
Generates Avro test data files using fastavro for cross-validation with EngineeredWood.Avro.
Run: python generate_test_data.py
"""
import os
import fastavro

OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))

def write_avro(filename, schema, records, codec='null'):
    path = os.path.join(OUTPUT_DIR, filename)
    with open(path, 'wb') as f:
        fastavro.writer(f, schema, records, codec=codec)
    print(f"  Written: {filename} ({len(records)} records, codec={codec})")

# --- Primitives ---
primitives_schema = {
    "type": "record",
    "name": "Primitives",
    "fields": [
        {"name": "bool_col", "type": "boolean"},
        {"name": "int_col", "type": "int"},
        {"name": "long_col", "type": "long"},
        {"name": "float_col", "type": "float"},
        {"name": "double_col", "type": "double"},
        {"name": "string_col", "type": "string"},
        {"name": "bytes_col", "type": "bytes"},
    ]
}

primitives_records = []
for i in range(100):
    primitives_records.append({
        "bool_col": i % 2 == 0,
        "int_col": i * 7 - 50,
        "long_col": i * 100000 - 500000,
        "float_col": float(i) * 0.5,
        "double_col": float(i) * 1.23456789,
        "string_col": f"row_{i}",
        "bytes_col": bytes([i % 256, (i * 3) % 256]),
    })

print("Generating primitive type files...")
write_avro("primitives_null.avro", primitives_schema, primitives_records, codec='null')
write_avro("primitives_deflate.avro", primitives_schema, primitives_records, codec='deflate')
write_avro("primitives_snappy.avro", primitives_schema, primitives_records, codec='snappy')

# --- Nullable fields ---
nullable_schema = {
    "type": "record",
    "name": "Nullable",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "nullable_int", "type": ["null", "int"]},
        {"name": "nullable_string", "type": ["null", "string"]},
    ]
}

nullable_records = []
for i in range(50):
    nullable_records.append({
        "id": i,
        "nullable_int": None if i % 3 == 0 else i * 10,
        "nullable_string": None if i % 5 == 0 else f"value_{i}",
    })

print("Generating nullable field files...")
write_avro("nullable_null.avro", nullable_schema, nullable_records, codec='null')

# --- Edge cases ---
edge_schema = {
    "type": "record",
    "name": "EdgeCases",
    "fields": [
        {"name": "int_col", "type": "int"},
        {"name": "long_col", "type": "long"},
        {"name": "string_col", "type": "string"},
    ]
}

edge_records = [
    {"int_col": 0, "long_col": 0, "string_col": ""},
    {"int_col": 2147483647, "long_col": 9223372036854775807, "string_col": "max"},
    {"int_col": -2147483648, "long_col": -9223372036854775808, "string_col": "min"},
    {"int_col": 1, "long_col": -1, "string_col": "hello 🌍"},
]

print("Generating edge case files...")
write_avro("edge_cases.avro", edge_schema, edge_records, codec='null')

# --- Empty file ---
empty_schema = {
    "type": "record",
    "name": "Empty",
    "fields": [
        {"name": "x", "type": "int"},
    ]
}

print("Generating empty file...")
write_avro("empty.avro", empty_schema, [], codec='null')

# --- Enum ---
enum_schema = {
    "type": "record",
    "name": "EnumTest",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "color", "type": {
            "type": "enum",
            "name": "Color",
            "symbols": ["RED", "GREEN", "BLUE"]
        }},
    ]
}

enum_records = [
    {"id": i, "color": ["RED", "GREEN", "BLUE"][i % 3]}
    for i in range(30)
]

print("Generating enum files...")
write_avro("enum.avro", enum_schema, enum_records, codec='null')

# --- Array (list) ---
array_schema = {
    "type": "record",
    "name": "ArrayTest",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "tags", "type": {"type": "array", "items": "string"}},
        {"name": "scores", "type": {"type": "array", "items": "int"}},
    ]
}

array_records = [
    {"id": i, "tags": [f"tag_{j}" for j in range(i % 4)], "scores": list(range(i % 5))}
    for i in range(20)
]

print("Generating array files...")
write_avro("array.avro", array_schema, array_records, codec='null')

# --- Map ---
map_schema = {
    "type": "record",
    "name": "MapTest",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "labels", "type": {"type": "map", "values": "string"}},
        {"name": "counts", "type": {"type": "map", "values": "int"}},
    ]
}

map_records = [
    {
        "id": i,
        "labels": {f"key_{j}": f"val_{j}" for j in range(i % 3)},
        "counts": {f"c_{j}": j * 10 for j in range(i % 4)},
    }
    for i in range(20)
]

print("Generating map files...")
write_avro("map.avro", map_schema, map_records, codec='null')

# --- Fixed ---
fixed_schema = {
    "type": "record",
    "name": "FixedTest",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "hash", "type": {"type": "fixed", "name": "Hash", "size": 16}},
    ]
}

fixed_records = [
    {"id": i, "hash": bytes([(i + j) % 256 for j in range(16)])}
    for i in range(20)
]

print("Generating fixed files...")
write_avro("fixed.avro", fixed_schema, fixed_records, codec='null')

# --- Nested record (struct) ---
struct_schema = {
    "type": "record",
    "name": "StructTest",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "address", "type": {
            "type": "record",
            "name": "Address",
            "fields": [
                {"name": "street", "type": "string"},
                {"name": "city", "type": "string"},
                {"name": "zip", "type": "int"},
            ]
        }},
    ]
}

struct_records = [
    {"id": i, "address": {"street": f"{i} Main St", "city": f"City_{i}", "zip": 10000 + i}}
    for i in range(20)
]

print("Generating struct files...")
write_avro("struct.avro", struct_schema, struct_records, codec='null')

# --- Logical types ---
import datetime

logical_schema = {
    "type": "record",
    "name": "LogicalTypes",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "date_col", "type": {"type": "int", "logicalType": "date"}},
        {"name": "time_millis_col", "type": {"type": "int", "logicalType": "time-millis"}},
        {"name": "time_micros_col", "type": {"type": "long", "logicalType": "time-micros"}},
        {"name": "ts_millis_col", "type": {"type": "long", "logicalType": "timestamp-millis"}},
        {"name": "ts_micros_col", "type": {"type": "long", "logicalType": "timestamp-micros"}},
    ]
}

logical_records = []
base_date = datetime.date(2024, 1, 1)
for i in range(20):
    d = base_date + datetime.timedelta(days=i)
    epoch = datetime.date(1970, 1, 1)
    days_since_epoch = (d - epoch).days
    millis_of_day = (i * 3600 + i * 60 + i) * 1000  # h:m:s in millis
    micros_of_day = millis_of_day * 1000
    ts_millis = days_since_epoch * 86400000 + millis_of_day
    ts_micros = ts_millis * 1000

    logical_records.append({
        "id": i,
        "date_col": days_since_epoch,
        "time_millis_col": millis_of_day,
        "time_micros_col": micros_of_day,
        "ts_millis_col": ts_millis,
        "ts_micros_col": ts_micros,
    })

print("Generating logical type files...")
write_avro("logical_types.avro", logical_schema, logical_records, codec='null')

# --- Nullable enum ---
nullable_enum_schema = {
    "type": "record",
    "name": "NullableEnumTest",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "color", "type": ["null", {
            "type": "enum",
            "name": "Color2",
            "symbols": ["RED", "GREEN", "BLUE"]
        }]},
    ]
}

nullable_enum_records = [
    {"id": i, "color": None if i % 3 == 0 else ["RED", "GREEN", "BLUE"][i % 3]}
    for i in range(15)
]

print("Generating nullable enum files...")
write_avro("nullable_enum.avro", nullable_enum_schema, nullable_enum_records, codec='null')

# --- Decimal (bytes-based) ---
import decimal as dec

decimal_bytes_schema = {
    "type": "record",
    "name": "DecimalBytesTest",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "amount", "type": {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}},
    ]
}

decimal_bytes_records = [
    {"id": i, "amount": dec.Decimal(f"{i * 100 + i}.{i:02d}")}
    for i in range(20)
]

print("Generating decimal (bytes) files...")
write_avro("decimal_bytes.avro", decimal_bytes_schema, decimal_bytes_records, codec='null')

# --- Decimal (fixed-based) ---
decimal_fixed_schema = {
    "type": "record",
    "name": "DecimalFixedTest",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "price", "type": {
            "type": "fixed",
            "name": "Price",
            "size": 8,
            "logicalType": "decimal",
            "precision": 16,
            "scale": 4,
        }},
    ]
}

decimal_fixed_records = [
    {"id": i, "amount": dec.Decimal(f"{i * 1000 + i}.{i:04d}")}
    for i in range(20)
]
# fastavro uses "amount" as the field name in the record but schema says "price"
decimal_fixed_records2 = [
    {"id": i, "price": dec.Decimal(f"{i * 1000 + i}.{i:04d}")}
    for i in range(20)
]

print("Generating decimal (fixed) files...")
write_avro("decimal_fixed.avro", decimal_fixed_schema, decimal_fixed_records2, codec='null')

# --- UUID ---
import uuid

uuid_schema = {
    "type": "record",
    "name": "UuidTest",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "uid", "type": {"type": "string", "logicalType": "uuid"}},
    ]
}

uuid_records = [
    {"id": i, "uid": str(uuid.UUID(int=i + 1))}
    for i in range(20)
]

print("Generating UUID files...")
write_avro("uuid.avro", uuid_schema, uuid_records, codec='null')

# --- LZ4 compression ---
print("Generating LZ4 compressed files...")
try:
    write_avro("primitives_lz4.avro", primitives_schema, primitives_records, codec='lz4')
except Exception as e:
    print(f"  LZ4 not available: {e}")

print("Done!")
