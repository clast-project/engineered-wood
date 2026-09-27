# Parquet Modular Encryption: Implementation Plan

**Status:** Not implemented. Re-verified against `main` @ `170c6c7` (2026-09-26).
`EngineeredWood.Parquet` has no crypto code. An encrypted file either fails with an
unhelpful error or is misread (see [Phase 0](#phase-0-refuse-clearly)). The Vortex
reader already refuses encrypted segments explicitly, and that is the model for Phase 0.

**Scope:** Parquet Modular Encryption (PARQUET-1375, `Encryption.md` in parquet-format).
ORC encryption is a separate, much smaller-demand feature and is parked in the
[appendix](#appendix-orc-encryption-deferred).

What changed since the March draft:

- The old recommendation, a separate `net8.0+`-only `EngineeredWood.Parquet.Encryption`
  package, is **withdrawn**. Decryption has to live inside the page loops of
  `ColumnChunkReader`, and the netstandard2.0 build is exercised by net472 tests. Crypto
  ships in the main package on every TFM ([D1](#d1-crypto-on-every-tfm)).
- The spec details are now exact (module types, framing, AAD byte layout), and one gap the
  spec leaves open was **measured** against a fixture (`compressed_page_size` includes the
  framing).
- The plan now maps each phase to concrete code sites and tests. It covers the second
  writer (`BufferedParquetWriter`), the batch reader's `PageMapBuilder`, the FSST
  symbol-table page (which has no module type), and the fact that today's test sweeps
  never open the encrypted fixtures at all.
- Packaging is layered: spec-level crypto in the main package (it adds no dependencies),
  key management and KMS adapters in separate packages ([D10](#d10-packaging-core-crypto-in-the-main-package-key-management-outside-it)).
- Key management is outside the spec but decides interop: pyarrow and Spark only exchange
  encrypted files through the PKMT1 key-tools format, so KeyTools is now a required phase
  ([D11](#d11-key-management-outside-the-spec-but-required-for-interop)).
- The shared reader/writer refactors come first, and page-index support is recommended
  before encryption because page skipping decides how the AAD page ordinal is derived
  ([D8](#d8-page-indexes-come-first-recommended-not-required)).

---

## 1. The spec, precisely

### 1.1 File layouts

| Mode | Leading / trailing magic | Tail layout |
|---|---|---|
| **Encrypted footer** | `PARE` / `PARE` | `FileCryptoMetaData` (plaintext Thrift) ‖ encrypted-footer module ‖ `len` (4 B LE, covers both) ‖ `PARE` |
| **Plaintext footer** | `PAR1` / `PAR1` | `FileMetaData` (plaintext, with fields 8/9 set) ‖ signature `nonce(12) ‖ tag(16)` ‖ `len` (4 B LE, covers both) ‖ `PAR1` |

Plaintext-footer signature: GCM-encrypt the serialized `FileMetaData` bytes with the footer
key, the stored nonce and the footer AAD, then compare the resulting tag with the stored
one. The ciphertext itself is discarded. A reader must therefore know where the Thrift
struct ends inside the footer. `ThriftCompactReader.Position` already exposes that.

### 1.2 Module framing

| Cipher | Module on disk |
|---|---|
| AES-GCM | `len` (4 B LE) ‖ `nonce` (12) ‖ `ciphertext` ‖ `tag` (16), where `len` = 12 + ciphertext + 16 |
| AES-CTR | `len` (4 B LE) ‖ `nonce` (12) ‖ `ciphertext`, where `len` = 12 + ciphertext. The CTR IV is `nonce ‖ 00 00 00 01` (the 32-bit big-endian counter starts at 1) |

`AES_GCM_V1` uses GCM for every module. `AES_GCM_CTR_V1` uses **CTR for data and
dictionary pages only** and GCM for everything else: headers, metadata, footer, indexes
and bloom filters. Both algorithms therefore need GCM. Key sizes are 128, 192 or 256 bits.

**Measured, not in the spec:** a page header's `compressed_page_size` counts the **whole
page module, length prefix included**. In `uniform_encryption.parquet.encrypted`, the first
data page header decrypts to `compressed_page_size = 46`, and the page module that follows
is `4 + 42` bytes. The header itself is its own module (`4 + 45`). Consequences:

- The reader's existing `pos += headerSize; slice(CompressedPageSize)` arithmetic survives
  unchanged. Only the header-size computation changes (`4 + len` instead of the Thrift
  byte count).
- `ColumnMetaData.TotalCompressedSize` and the chunk ranges include all framing, so
  `PrepareRowGroupAsync`'s range math needs no changes.
- A writer must put the **post-encryption** size into the header before encrypting the header.

### 1.3 Module types and AAD

`AAD = aad_prefix ‖ aad_file_unique ‖ module_type (1 B) ‖ rg_ordinal (i16 LE) ‖ col_ordinal (i16 LE) ‖ page_ordinal (i16 LE)`

| # | Module | RG + column ordinal | Page ordinal | EW touch-point today |
|---|---|---|---|---|
| 0 | Footer | – | – | `ParquetFileReader.ReadMetadataAsync` / both writers' footer code |
| 1 | ColumnMetaData | ✓ | – | `MetadataDecoder` ColumnChunk field 9 (skipped today) |
| 2 | Data Page | ✓ | ✓ | 3 read loops + `*FromEntry`; 4 write helpers |
| 3 | Dictionary Page | ✓ | – | `ReadDictionaryPage` / `WriteDictionaryPage` |
| 4 | Data Page Header | ✓ | ✓ | `PageHeaderDecoder.Decode` call sites |
| 5 | Dictionary Page Header | ✓ | – | same |
| 6 | ColumnIndex | ✓ | – | none yet: EW neither writes nor reads page indexes (decoder skips ColumnChunk fields 4–7); see D8 |
| 7 | OffsetIndex | ✓ | – | none yet, as above |
| 8 | BloomFilter Header | ✓ | – | `BloomFilterReader.Parse` (2 read sites), writer bloom block |
| 9 | BloomFilter Bitset | ✓ | – | same |

- The page ordinal counts **data pages** within the column chunk, starting at 0. The
  dictionary page does not advance it.
- The ordinals are signed 16-bit. The writer must refuse more than 32,767 row groups,
  leaf columns or data pages per chunk rather than wrap around. `RowGroup.Ordinal` is
  already a `checked((short)…)` cast, but the page count is not.
- `aad_file_unique` is random per file. Both the parquet-mr and the arrow-cpp fixtures use
  8 bytes.
- `supply_aad_prefix = true` means the prefix was not stored, so the reader must be given it.
- One key must not exceed 2^32 module encryptions (random 96-bit nonces). This limit is
  documented, not enforced.

### 1.4 Thrift additions

- `FileMetaData` 8 `encryption_algorithm: EncryptionAlgorithm` and 9 `footer_signing_key_metadata: binary`. Both are plaintext-footer mode only.
- `ColumnChunk` 8 `crypto_metadata: ColumnCryptoMetaData` and 9 `encrypted_column_metadata: binary`.
- `union EncryptionAlgorithm { 1: AesGcmV1, 2: AesGcmCtrV1 }`. Both structs are `{1: aad_prefix binary, 2: aad_file_unique binary, 3: supply_aad_prefix bool}`.
- `union ColumnCryptoMetaData { 1: EncryptionWithFooterKey {}, 2: EncryptionWithColumnKey {1: path_in_schema list<string>, 2: key_metadata binary} }`.
- `struct FileCryptoMetaData { 1: required EncryptionAlgorithm, 2: key_metadata binary }`.

### 1.5 ColumnMetaData placement

- **Plaintext footer:** `meta_data` (field 3) is present for every column, but for
  encrypted columns it is **stripped of statistics**. The full copy lives in the
  encrypted field 9.
- **Encrypted footer:** footer-key columns keep `meta_data` inline, because the footer is
  already encrypted. Column-key columns have **no** `meta_data`, only field 9. Without that
  column's key, even its offsets are unknown, so the column cannot be read at all.
- Unencrypted columns (no `crypto_metadata`) may coexist with encrypted ones in either
  mode. The first module of `encrypt_columns_and_footer.parquet.encrypted` is a plaintext
  page header.

---

## 2. Current code: every site that must change

**Read path**

| Site | Change |
|---|---|
| `ParquetFileReader.cs:58-100` `ReadMetadataAsync` | Accepts only a trailing `PAR1`. It must branch on `PARE`, decrypt the footer, verify the plaintext-footer signature, and resolve keys (async) before decoding the column metadata |
| `MetadataDecoder.cs:24-75` (FileMetaData), `:520-545` (ColumnChunk) | Decode fields 8/9 of both, plus `FileCryptoMetaData`. Field 9 is kept as raw bytes and decrypted once keys are known |
| `ParquetFileReader.cs:812` | `"Column chunk {i} has no inline metadata"` becomes a typed "no key for column X" error |
| `ColumnChunkReader.cs:81` `ReadColumn` loop, `:250` `TryReadFixedListColumn` loop, `PageMapBuilder.cs:120` `Build` loop | The three independent page walkers. Each must decrypt the header module, then the page / dictionary module, tracking the page ordinal |
| `ColumnChunkReader.cs:640/702` `ReadDataPageV{1,2}FromEntry` | The batch path decodes lazily from `PageMapEntry.Offset`. Add `PageOrdinal` to `PageMapEntry` and decrypt here. `PageMapBuilder.DeriveRowCountV1` also decompresses V1 pages of repeated columns at map time, so those pages get decrypted twice (accept this, or cache the decrypted payload) |
| `ParquetFileReader.GetCandidateRowGroupsAsync` (by value) and `MembershipPredicateEvaluator.Decode` (for pruning, reached from `ReadSetAsync` and `MembershipPrefetch`) | Two bloom-filter read sites. `BloomFilterReader.Parse` assumes header and bitset are contiguous and plaintext. Under encryption they are two modules (8, 9) |
| `MembershipPredicateEvaluator.Decode` for dictionaries (reached from `ReadSetAsync` and `MembershipPrefetch`) | Reads a chunk's dictionary page for pruning (#57), outside the column read path. Under encryption the page header and payload are modules too, so this site needs the same decryption as the reader |
| `ParquetFileReader.GetCandidateRowGroupsAsync` / `ParquetStatisticsAccessor` | Need no change as long as stripped stats read as "absent": pruning returns Unknown, which is correct but weaker |
| `src/EngineeredWood.Parquet.TestTool/Program.cs:571` | Decodes the footer directly, so it should refuse `PARE` |

**Write path**

| Site | Change |
|---|---|
| `ColumnChunkWriter` page emitters: `WriteSymbolTablePage` :451, `WriteDictionaryPage` :640, `WriteDictDataPageV2` :686, `WriteDictDataPageV1` :769, `WriteDataPageV2` :963, `WriteDataPageV1` :1052 | Six copies of "build `PageHeader` → `EncodePageHeader` → write header + payload". **Refactor these into one `EmitPage(...)` helper first**, with no behavior change, so that encryption is added in one place |
| `ColumnChunkWriter.WriteColumn` (called from `Parallel.For` in `ParquetFileWriter.cs:157`) | Takes only `_options`. It needs an encryption context: row-group ordinal, column ordinal, and the key/cipher for that column. All of these are known before the parallel loop, so pages can be encrypted in parallel |
| `ParquetFileWriter.cs:205-275` ↔ `BufferedParquetWriter.cs:200-253` (chunk placement, bloom block, `ColumnChunk` build); `ParquetFileWriter.cs:575-642` ↔ `BufferedParquetWriter.cs:275-311` (footer + magic) | **The two writers duplicate this code.** Extract a shared `RowGroupAssembler` / `FooterWriter` before adding encryption, or it will be implemented twice and drift (the "sibling writer" trap) |
| Bloom filter block (`result.BloomFilterData`) | Split into two modules |

**Tests (currently dead)**

`TestData.GetAllParquetFiles()` globs `*.parquet` non-recursively, so it never yields a
`.parquet.encrypted` file or anything under `data/aes256/`. The "skip encrypted" checks
at `ReadRowGroupTests.cs:464/615`, `MetadataDecoderTests.cs:219` and
`ParquetFileReaderTests.cs:155` are **dead code**. Removing them adds no coverage. The
encrypted fixtures need their own enumerator (see Phase 0).

**Available fixtures** (`parquet-testing` @ `09f3cdb`, keys in `data/README.md`)

| File | Footer | Cipher | Notes |
|---|---|---|---|
| `uniform_encryption` | PARE | GCM | every column under the footer key |
| `encrypt_columns_and_footer` | PARE | GCM | `double_field`/`float_field` under column keys, the rest plaintext |
| `encrypt_columns_and_footer_aad` | PARE | GCM | AAD prefix `tester`, stored |
| `encrypt_columns_and_footer_disable_aad_storage` | PARE | GCM | prefix `tester` **not stored**, so the reader must supply it |
| `encrypt_columns_and_footer_ctr` | PARE | GCM_CTR | |
| `encrypt_columns_plaintext_footer` | PAR1 | GCM | signed footer |
| `encrypt_columns_and_footer_bloom_filter` | PARE | GCM | bloom filters on the encrypted columns |
| `external_key_material_java` + `_KEY_MATERIAL_FOR_…json` | – | – | parquet-mr key tools (PKMT1, double wrapping), Phase 7 only (D11) |
| `aes256/*` (5 files) | mixed | mixed | 256-bit keys, 8 column keys incl. `int64_field.list.element` (nested), INT96, FLBA |

Key metadata in these files is the key ID as UTF-8 (`kf`, `kc1`, …), so a simple
dictionary-backed key retriever reads all of them. The `aes256` set is from parquet-mr and
the rest are from arrow-cpp, which gives two writer lineages without running either.

---

## 3. Decisions

### D1. Crypto on every TFM

GCM is needed by both algorithms (§1.2), so a CTR-only fallback is not an option.

- **net8.0 / net10.0:** `System.Security.Cryptography.AesGcm(key, tagSizeInBytes: 16)`.
  For CTR, generate counter blocks and use the one-shot `Aes.EncryptEcb`, XOR'd in 4 KiB
  batches.
- **netstandard2.0** (covers the net472 test run): the ECB block cipher comes from
  `Aes.Create()` with `CipherMode.ECB` / `PaddingMode.None` via `ICryptoTransform`. On top
  of it, a **managed GHASH** (4-bit table multiply, ~200 lines) gives GCM, and CTR is
  built the same way as above. Document that the managed GHASH is not hardened against
  timing side channels. It is used only where the platform has nothing better.
- One internal seam, `ParquetCipher` (`EncryptModule` / `DecryptModule` over spans, one
  instance per key and cipher), selected by `#if NET8_0_OR_GREATER`.
- Validation: NIST GCM test vectors on every TFM, plus a net10 test that cross-checks the
  managed GHASH path against `AesGcm` on random inputs. The managed code compiles on net10
  as well so that this test can run.
- No new package dependency (BouncyCastle is rejected, as it is heavy and unnecessary).
  The AOT gate is unaffected.

`AesGcm` instances are not documented as thread-safe. Create one per column-chunk read
(the reader decodes columns in parallel via `ForEachColumn`) and one per column on write.

### D2. Read both footer modes and both ciphers from the start

The fixtures are six `PARE` files and one `PAR1`. A "plaintext footer first" phase would
exercise almost nothing, and the marginal work for `PARE` is small (one module decrypt
before `DecodeFileMetaData`).

### D3. Write defaults

- `AES_GCM_V1` and an **encrypted footer**. These match both parquet-mr's and arrow-cpp's
  defaults.
- CTR and the plaintext footer are opt-in.
- A random `aad_file_unique` (8 bytes, `RandomNumberGenerator`), with the prefix stored
  unless the caller asks otherwise.

### D4. Key API: explicit keys plus an async retriever

Key lookup is resolved **asynchronously and up front**, not inside span-based decoding:

1. Read the tail.
2. Resolve the footer key from `FileCryptoMetaData.key_metadata` or
   `footer_signing_key_metadata`.
3. Decrypt or verify the footer.
4. Collect the distinct column `key_metadata` values and resolve them all.
5. Decrypt each field 9.

Keys are then available synchronously to the page loops.

```csharp
public sealed class ParquetDecryptionOptions
{
    /// <summary>Explicit footer key; consulted before <see cref="KeyRetriever"/>.</summary>
    public ReadOnlyMemory<byte>? FooterKey { get; init; }

    /// <summary>Explicit column keys by dotted path (e.g. "int64_field.list.element").</summary>
    public IReadOnlyDictionary<string, ReadOnlyMemory<byte>>? ColumnKeys { get; init; }

    /// <summary>Resolves key_metadata (opaque bytes) to a key; null result = key unavailable.</summary>
    public IParquetKeyRetriever? KeyRetriever { get; init; }

    /// <summary>Required when the file was written with supply_aad_prefix.</summary>
    public ReadOnlyMemory<byte>? AadPrefix { get; init; }

    /// <summary>Plaintext-footer files: verify the GCM signature (default true).</summary>
    public bool VerifyFooterSignature { get; init; } = true;
}

public interface IParquetKeyRetriever
{
    ValueTask<byte[]?> GetKeyAsync(ReadOnlyMemory<byte> keyMetadata, CancellationToken cancellationToken);
}

public sealed class ParquetEncryptionOptions
{
    public required ReadOnlyMemory<byte> FooterKey { get; init; }
    public ReadOnlyMemory<byte> FooterKeyMetadata { get; init; }
    public bool PlaintextFooter { get; init; }
    public ParquetEncryptionAlgorithm Algorithm { get; init; } = ParquetEncryptionAlgorithm.AesGcmV1;

    /// <summary>
    /// Columns to encrypt, by dotted path. Null = every column under the footer key (uniform).
    /// A column mapped to a key uses that key; mapped to null uses the footer key.
    /// Columns not listed stay plaintext.
    /// </summary>
    public IReadOnlyDictionary<string, ParquetColumnKey?>? Columns { get; init; }

    public ReadOnlyMemory<byte>? AadPrefix { get; init; }
    public bool StoreAadPrefix { get; init; } = true;
}

public sealed record ParquetColumnKey(ReadOnlyMemory<byte> Key, ReadOnlyMemory<byte> KeyMetadata);
```

These hang off `ParquetReadOptions.Decryption` and `ParquetWriteOptions.Encryption`. The
public `ColumnChunk` gains `CryptoMetaData`, and `FileMetaData` gains `EncryptionAlgorithm`
/ `FooterSigningKeyMetadata`, so the metadata stays inspectable. After decryption,
`ColumnChunk.MetaData` holds the **full** decrypted metadata, which means everything
downstream of `ReadMetadataAsync` stays unchanged.

The API is pre-1.0, so the names can move. The shape to keep is the async up-front resolution.

### D5. Missing-key behavior

The schema is always readable, including from a decrypted `PARE` footer, provided the
footer key is available.

- Projecting away a keyless column works.
- Selecting one throws a typed `ParquetKeyUnavailableException` that names the column,
  instead of failing a GCM tag deep in a page loop.
- A missing footer key on a `PARE` file fails at `ReadMetadataAsync`.
- A plaintext-footer file with no keys at all remains readable for its plaintext columns.
  Signature verification is then impossible, so this path requires
  `VerifyFooterSignature = false`, matching arrow-cpp's `plaintext_files_allowed`-style
  explicit opt-out.

A GCM tag failure is always an error. It means tampering, a wrong key or a wrong AAD
prefix, and the message should list those three causes.

### D6. FSST symbol-table pages cannot be encrypted

`PageType.SymbolTablePage` is an unratified EW/FSST-proposal page type with no module
type in the encryption spec.

- **Write:** refuse `ByteArrayEncoding.Fsst` (and its per-column form) on an encrypted
  column with a clear error. Do not invent a module type.
- **Read:** an encrypted chunk containing one is a format error.

ALP and PFOR are value encodings inside ordinary data pages, so they need no special
handling.

### D7. Page CRC under encryption: settle by measurement

The spec is silent on this. `parquet.thrift` says the CRC covers the page "as written to
disk", which under encryption would be the ciphertext module, but implementations may
differ. **Before writing any CRC code:** have pyarrow write an encrypted file with
`write_page_checksum=True`, then check which bytes the CRC matches (the ciphertext module
or the compressed plaintext).

Until then:

- The writer refuses `PageChecksumEnabled` together with encryption. GCM already
  authenticates each page, and CTR pages are authenticated only through the header.
- The reader skips CRC validation on encrypted pages.

### D8. Page indexes come first (recommended, not required)

EW does not read or write ColumnIndex/OffsetIndex today (`MetadataDecoder` skips
ColumnChunk fields 4–7). Encryption does not *need* them, and the extra encryption work
is the same whichever feature lands second: two more module types (6/7). The recommended
order is still page index first, for three reasons:

- **The same refactors come first either way.** Both features need one page reader in
  place of today's three walkers, and both need the six page emitters merged and the two
  writers de-duplicated. Page-index writing needs each page's offset, and that offset is
  exactly what a single `EmitPage` produces.
- **Page skipping changes where the page ordinal comes from.** The AAD binds each data
  page to its ordinal. With OffsetIndex-driven skipping, the reader jumps to page *k*
  without walking pages 0 to *k*−1, so the ordinal must be the page's **position in
  `OffsetIndex.page_locations`**, not a running counter. A page reader designed for
  skipping takes encryption without rework. One built around a counter would need
  reworking.
- **The encrypted fixtures carry encrypted page indexes.** `data/README.md` notes that the
  column keys cover "column and offset index". Without page-index read support, that part
  of every fixture goes untested.

The case against: page index pays off only on data clustered on the predicate column
(see `predicate-pushdown-design.md`, "Page index: common in files…"). If encryption
becomes the priority, do not block on it. Build the Phase 1a page reader to take ordinals
from position, and add a comment at the `MetadataDecoder` ColumnChunk skip that says
encrypted page indexes use module types 6/7.

### D9. Delta Lake: out of scope

The Delta protocol does not describe Parquet encryption, and no EW Delta code path passes
encryption options. Encrypted *reads* of Delta data files would work transparently once
`ParquetReadOptions` flows through (CDF/compaction already take read options). Nothing is
planned.

### D10. Packaging: core crypto in the main package, key management outside it

Encryption at the spec level adds **no dependencies**. AES, AES-GCM and the RNG are in the
BCL on all three TFMs (netstandard2.0 included), and the netstandard2.0 GCM layer is our
own code. A consumer that never encrypts pays only for roughly 1,500–2,000 lines of IL in
`EngineeredWood.Parquet`.

Splitting that core out would not buy much. The parts that must change sit inside the
reader and writer: the page loops, the footer read, and the Thrift decoding (which even
Phase 0 needs). A separate assembly could only hook in through a new public interface
inside those loops, a permanent API surface that exists only to move code between
assemblies.

What *does* bring dependencies is key management (D11). The layering is:

| Package | Contents | New dependencies |
|---|---|---|
| `EngineeredWood.Parquet` | Read/write encrypted files given keys or an `IParquetKeyRetriever` (D4) | none |
| `EngineeredWood.Parquet.KeyTools` | PKMT1 key material, single/double wrapping, external key material, an `IKmsClient` interface, a caching retriever | none beyond System.Text.Json source-gen (already used elsewhere) |
| One package per KMS, e.g. Azure Key Vault, AWS KMS, GCP KMS | `IKmsClient` adapters | that provider's SDK (`Azure.Security.KeyVault.Keys`, `AWSSDK.KeyManagementService`, …) |

The KMS adapters do **not** go into the existing `EngineeredWood.Azure`/`Aws`/`Gcs`
storage packages. A storage user should not inherit a key-service SDK.

### D11. Key management: outside the spec, but required for interop

The encryption spec leaves key management out on purpose: every key reference is an opaque
`key_metadata` byte string. The fixtures store bare key IDs (`"kf"`, `"kc1"`), and D4's
explicit keys plus retriever is exactly the spec-level interface.

In practice there is a **de facto standard** layered on top, the "Parquet key management
tools" design. parquet-mr (and so Spark) and arrow-cpp (and so pyarrow) both implement it:

- **Envelope encryption.** A random data key (DEK) per file or per column is wrapped by a
  master key held in a KMS.
- **Double wrapping (the default).** The DEK is wrapped by a key-encryption key (KEK), and
  only the KEK goes to the KMS. This cuts KMS round trips.
- **Key material** is stored as `PKMT1` JSON, either inside `key_metadata` ("internal") or
  in a sidecar `_KEY_MATERIAL_FOR_<file>.json` ("external"). External material is what
  makes key rotation possible without rewriting data files.
- **The KMS plug-in** is a client with two operations, wrap and unwrap. There is also a
  "local wrap" variant that fetches master keys and wraps locally.

**This decides interop, not the spec.** pyarrow 24's Python API exposes *only* this route
(`CryptoFactory`, `KmsClient`, `EncryptionConfiguration`, `DecryptionConfiguration`,
verified on the box); it cannot take raw keys. Spark's configuration works the same way
(`parquet.crypto.factory.class`). Without KeyTools, an EW-encrypted file is unreadable
through pyarrow's or Spark's normal APIs, and the reverse holds too. Only the C++ and
Java low-level APIs can exchange files with EW on explicit keys.

Table formats are the counterweight: Iceberg's encryption spec manages keys at the table
level and hands Parquet the keys directly, without PKMT1. That is why the core API stays
"keys or a retriever" and KeyTools stays optional. KeyTools is no longer "possibly
never": it is **required before EW claims interop with Spark or pyarrow** (Phase 7).

---

## 4. Phases (each one PR unless noted)

Overall order:

| # | Phase | Size | Notes |
|---|---|---|---|
| 0 | Refuse encrypted files clearly | small | independent; ship first |
| 1a | Reader: one page walker, position-derived ordinals, skip-capable | medium | shared prerequisite (D8) |
| 1b | Writer: `EmitPage` + de-duplicate the two writers | medium | shared prerequisite (D8) |
| 2 | Page index: write, then read | medium | **not in this doc**: [`parquet-page-index.md`](parquet-page-index.md) (#390). Recommended, not required (D8) |
| 3 | Crypto primitives | small–medium | |
| 4 | Encrypted metadata (footer, column metadata, keys) | medium | |
| 5 | Encrypted pages, bloom filters, page indexes | medium | |
| 6 | Encrypted writes | medium | |
| 7 | KeyTools (+ KMS adapter packages) | large | required for Spark/pyarrow interop (D11) |

### Phase 0: refuse clearly

Small, and useful on its own.

- In `ReadMetadataAsync`, a trailing `PARE` throws `ParquetEncryptedFileException`
  ("encrypted footer; encryption is not supported"), not "missing trailing PAR1 magic".
- A `PAR1` footer that carries `encryption_algorithm` (field 8) throws the same, instead of
  returning metadata whose encrypted columns then fail obscurely (or decode wrongly) in
  the page loop. This needs only a presence check for field 8 in `ReadFileMetaData`.
- Add the `EncryptedFixtures` test enumerator (`*.parquet.encrypted`, recursive, which
  picks up `aes256/`). Assert that all 13 fixture files (8 in `data/`, 5 in
  `data/aes256/`) throw the typed exception. Delete the dead skip checks.

### Phase 1a: reader page-walker unification

No behavior change.

- One internal `PageReader` that yields `(PageHeader, ReadOnlySpan<byte> payload,
  pageOrdinal)`. It replaces the loops in `ColumnChunkReader.ReadColumn`,
  `TryReadFixedListColumn` and `PageMapBuilder.Build`, and serves the `*FromEntry` path.
  The plaintext path stays zero-copy.
- `pageOrdinal` is the data page's **position** in the chunk. That is what OffsetIndex
  `page_locations` indexes and what the encryption AAD needs, so do not implement it as a
  counter that only works when walking from page 0.
- `PageMapEntry` records its ordinal.
- Existing tests are the oracle, plus a benchmark check that the column-read path did not
  regress.

### Phase 1b: writer refactor

No behavior change.

- Merge the six page emitters into `EmitPage`. It returns each page's offset in the chunk
  and its ordinal; page-index writing consumes the offsets and encryption consumes the
  ordinals.
- Extract the row-group assembly and footer writing that `ParquetFileWriter` and
  `BufferedParquetWriter` both duplicate.
- The output must be byte-identical: round-trip a corpus and hash it.

### Phase 2: page index

Specified in [`parquet-page-index.md`](parquet-page-index.md), not here. Page-index writing is blocked on
[#389](https://github.com/clast-project/engineered-wood/issues/389) (row-aligned pages). Its only obligation
to this plan: route ColumnIndex/OffsetIndex bytes through one read helper and one write
helper, so that Phase 5 and Phase 6 can wrap them in module types 6/7.

### Phase 3: primitives

Internal only, with no reader changes.

- `ParquetCipher` (D1), module framing (encrypt/decrypt into caller buffers, or
  `ArrayPool`-rented ones), and an `AadBuilder` (§1.3) that reuses a buffer per chunk and
  patches only the page ordinal.
- Tests: NIST vectors on net10/net8/net472; the managed-vs-`AesGcm` cross-check; decrypt
  the footer module of `uniform_encryption` with key `kf` (this is exactly the probe that
  produced the §1.2 measurement).

### Phase 4: metadata

- Thrift decode for the §1.4 structs. Keep field 9 raw.
- `PARE` footer decrypt, plaintext-footer signature verify, the D4 async key resolution,
  and field-9 decryption that replaces `ColumnChunk.MetaData`.
- The public options and exception types from D4 and D5.
- Tests:
  - `ReadMetadataAsync` succeeds on every fixture. The schema, row counts and column
    chunk offsets agree across the arrow-cpp GCM files, which hold the same data.
  - The plaintext-footer file exposes stripped stats without a column key and full stats
    with one.
  - A wrong footer key fails, and so does a tampered signature (flip one footer byte).
  - `disable_aad_storage` fails without `AadPrefix` and succeeds with it.

### Phase 5: pages, bloom filters and page indexes

- Decryption inside the Phase 1a `PageReader`: header module, then page or dictionary
  module, decrypted into a buffer reused per chunk. The `*FromEntry` path decrypts at
  decode time from the recorded module offset and ordinal.
- Decrypt the two bloom-filter modules at both read sites. Route both through one helper,
  since the sites are duplicated today.
- If Phase 2 has landed: decrypt ColumnIndex/OffsetIndex (module types 6/7), and test page
  skipping on an encrypted chunk. Skipping is the case that proves the ordinals are
  position-derived.
- Tests: read every fixture (both `data/` and `aes256/`) and check values against the
  deterministic generator in arrow's `cpp/src/parquet/encryption/test_encryption_util.cc`
  / `read_configurations_test.cc`, ported as a C# expectation. The coverage includes:
  - CTR;
  - the nested `int64_field.list.element`;
  - bloom-filter pruning on the `_bloom_filter` file (it must prune a value that is
    absent and keep one that is present);
  - a missing column key with that column projected away (succeeds) and selected (typed
    error);
  - the batch path (`ReadRowGroupBatchesAsync` with a small `BatchSize`), the fixed-list
    fast path and `ReadAllAsync`.

### Phase 6: writer encryption

- Per-column encryption context threaded into `WriteColumn`; header and page modules;
  dictionary modules; bloom modules; page-index modules (if Phase 2 has landed); both
  footer modes; stats stripping on plaintext-footer encrypted columns; the D6 FSST
  refusal; the D7 CRC refusal; and the ordinal-overflow refusals.
- Tests:
  - EW round trip across both ciphers, both footer modes, uniform / per-column / mixed
    plaintext keys, V1/V2 pages, dictionary and non-dictionary, nested columns and
    bloom filters, for both writers.
  - Tamper tests: swap two page modules, which must fail by AAD, not by parse error.
  - **Cross-engine (pyarrow 24, already on the box):** `pyarrow.parquet.encryption.CryptoFactory`
    with a test `KmsClient` whose `wrap_key` returns `base64(key)`, run with
    `double_wrapping=False`. pyarrow → EW: a test-only `IParquetKeyRetriever` parses the
    PKMT1 JSON in `key_metadata` and base64-decodes `wrappedDEK`. EW → pyarrow: EW writes
    that same JSON as its key metadata. This test-only shim is the simplest case of
    Phase 7, so it is a starting point for Phase 7 rather than a substitute. Put it in
    the existing interop tier with honest *Skipped* reporting when pyarrow is absent.
  - Settle D7 here, using the same pyarrow harness.
  - Optional Spark tier: parquet-mr's `PropertiesDrivenCryptoFactory` + `InMemoryKMS`
    (the combination Spark documents for columnar encryption). This is only worth it if
    the pyarrow tier leaves doubt about parquet-mr compatibility; the `aes256` fixtures
    already cover parquet-mr on read.

### Phase 7: KeyTools

In `EngineeredWood.Parquet.KeyTools` (D10). Required for Spark/pyarrow interop (D11).

- **7a:** PKMT1 internal key material, single wrapping, the `IKmsClient` interface and a
  caching `IParquetKeyRetriever`. This alone makes EW ↔ pyarrow work through pyarrow's
  public API with `double_wrapping=False`.
- **7b:** double wrapping (KEK cache) and external key material files. This matches the
  parquet-mr/pyarrow *defaults*. Validate against
  `external_key_material_java.parquet.encrypted` + its `_KEY_MATERIAL_…json` (InMemoryKMS
  master keys as in parquet-mr's mocks), and against pyarrow with `double_wrapping=True`.
- **7c:** KMS adapter packages, one per provider, added on demand.

JSON types must be registered in a source-gen context to pass the net10 AOT gate. This is
the only phase that is plausibly "large". Everything before it is medium or small.

---

## 5. Open questions for the maintainer

1. **D1:** is a managed GHASH on netstandard2.0 acceptable, or would you rather have
   netstandard2.0 throw `PlatformNotSupportedException` for encryption (and skip the
   fixture tests on net472)? The recommendation is managed GHASH, because the net472 test
   run is otherwise blind to the feature.
2. **D8:** page index before encryption, or encryption first with a skip-ready page
   reader? The recommendation is page index first.
3. Should Phase 0 ship immediately as a standalone PR? The recommendation is yes: today a
   plaintext-footer encrypted file opens with no hint that it is encrypted, and failure
   comes later and obscurely.

---

## Appendix: ORC encryption (deferred)

Unchanged from the March draft and not planned: no ORC encryption code exists and there
is no known demand. Summary for when it comes up:

- ORC uses AES-CTR only, with no authentication.
- Each encrypted column is stored twice: encrypted-unmasked, plus a plaintext masked
  variant (nullify / redact / SHA-256).
- The IV is deterministic from (column id, stream kind, stripe id, block counter).
- Per-file local keys are wrapped by the KMS and stored in the stripe information.

It would reuse the CTR half of the `ParquetCipher` primitive from Phase 1, which is a
reason to put that primitive in `EngineeredWood.Core` rather than in the Parquet project.
Decide that when ORC work actually starts.
