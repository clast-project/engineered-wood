// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Vortex.Encodings;

/// <summary>
/// Well-known array encoding ids registered by
/// <c>vortex_file::register_default_encodings</c>. Match exactly the strings
/// produced by the canonical Rust impl as of vortex 0.70.
/// </summary>
internal static class VortexArrayEncodings
{
    public const string Primitive = "vortex.primitive";
    public const string Bool = "vortex.bool";
    public const string Constant = "vortex.constant";
    public const string Null = "vortex.null";
    public const string Dict = "vortex.dict";
    public const string Chunked = "vortex.chunked";
    public const string Struct_ = "vortex.struct";
    public const string List = "vortex.list";
    public const string ListView = "vortex.listview";
    public const string VarBin = "vortex.varbin";
    public const string VarBinView = "vortex.varbinview";
    public const string FixedSizeList = "vortex.fixed_size_list";
    public const string Sparse = "vortex.sparse";
    public const string Sequence = "vortex.sequence";
    public const string Decimal = "vortex.decimal";
    public const string DecimalByteParts = "vortex.decimal_byte_parts";
    public const string Extension = "vortex.ext";
    public const string Masked = "vortex.masked";
    public const string ByteBool = "vortex.bytebool";
    public const string DateTimeParts = "vortex.datetimeparts";
    public const string FsstString = "vortex.fsst";
    public const string Alp = "vortex.alp";
    public const string AlpRD = "vortex.alprd";
    public const string Pco = "vortex.pco";
    public const string RunEnd = "vortex.runend";

    public const string FastlanesBitPacked = "fastlanes.bitpacked";
    public const string FastlanesFor = "fastlanes.for";
    public const string FastlanesDelta = "fastlanes.delta";
    public const string FastlanesRle = "fastlanes.rle";
}
