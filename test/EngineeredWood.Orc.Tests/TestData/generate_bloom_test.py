# Copyright (c) clast-project. All rights reserved.
# Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

"""Generate ORC test file with bloom filters using PyArrow."""
import pyarrow as pa
import pyarrow.orc as orc
import os

# Create test data with known values
n = 5000
names = [f"name_{i}" for i in range(n)]
ids = list(range(n))
values = [float(i) * 1.5 for i in range(n)]

table = pa.table({
    'id': pa.array(ids, type=pa.int32()),
    'name': pa.array(names, type=pa.string()),
    'value': pa.array(values, type=pa.float64()),
})

output_path = os.path.join(os.path.dirname(__file__), 'bloom_test.orc')

# Write with bloom filters enabled
# PyArrow ORC writer supports bloom_filter_columns parameter
orc.write_table(
    table,
    output_path,
    bloom_filter_columns=[0, 1, 2],
    bloom_filter_fpp=0.05,
    stripe_size=16 * 1024,  # Small stripes to get multiple
)

# Verify by reading back
reader = orc.ORCFile(output_path)
print(f"Rows: {reader.nrows}")
print(f"Stripes: {reader.nstripes}")
print(f"Schema: {reader.schema}")

# Read first few rows
result = reader.read()
print(f"First 3 rows:")
print(result.slice(0, 3).to_pandas())
print(f"\nFile written to: {output_path}")
