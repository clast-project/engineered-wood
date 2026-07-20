// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Compatibility;

var cacheDir = Path.Combine(Path.GetTempPath(), "ew-compat-data");
Directory.CreateDirectory(cacheDir);

using var http = new HttpClient { Timeout = TimeSpan.FromMinutes(2) };
http.DefaultRequestHeaders.UserAgent.ParseAdd("EngineeredWood-Compatibility/1.0");

var files = FileManifest.Files;
var reports = new List<ValidationReport>(files.Count);

Console.WriteLine($"EngineeredWood Compatibility Suite — {files.Count} files");
Console.WriteLine($"Cache: {cacheDir}");
Console.WriteLine();

foreach (var entry in files)
{
    var localPath = Path.Combine(cacheDir, entry.LocalName);

    // Download if not cached
    if (!File.Exists(localPath) || new FileInfo(localPath).Length == 0)
    {
        try
        {
            using var response = await http.GetAsync(entry.Url, HttpCompletionOption.ResponseHeadersRead);
            response.EnsureSuccessStatusCode();
            await using var fs = File.Create(localPath);
            await response.Content.CopyToAsync(fs);
        }
        catch (Exception ex)
        {
            var report = new ValidationReport(entry, ValidationResult.Fail,
                $"Download failed: {ex.GetType().Name}: {ex.Message}", TimeSpan.Zero);
            reports.Add(report);
            PrintResult(report);
            continue;
        }
    }

    // Validate
    var result = await ParquetValidator.ValidateAsync(entry, localPath);
    reports.Add(result);
    PrintResult(result);
}

// Summary
Console.WriteLine();
Console.WriteLine(new string('─', 72));

int passed = reports.Count(r => r.Result == ValidationResult.Pass);
int failed = reports.Count(r => r.Result == ValidationResult.Fail);
int skipped = reports.Count(r => r.Result == ValidationResult.Skip);

SetColor(ConsoleColor.Green);
Console.Write($"  PASSED: {passed}");
Console.ResetColor();
Console.Write("  |  ");
SetColor(failed > 0 ? ConsoleColor.Red : ConsoleColor.Gray);
Console.Write($"FAILED: {failed}");
Console.ResetColor();
Console.Write("  |  ");
SetColor(ConsoleColor.Yellow);
Console.Write($"SKIPPED: {skipped}");
Console.ResetColor();
Console.WriteLine($"  |  TOTAL: {reports.Count}");

if (failed > 0)
{
    Console.WriteLine();
    SetColor(ConsoleColor.Red);
    Console.WriteLine("Failures:");
    Console.ResetColor();
    foreach (var r in reports.Where(r => r.Result == ValidationResult.Fail))
    {
        Console.WriteLine($"  [{r.Entry.Source}] {r.Entry.LocalName}");
        Console.WriteLine($"    {r.Detail}");
    }
}

Console.WriteLine();
return failed > 0 ? 1 : 0;

static void PrintResult(ValidationReport r)
{
    var (color, label) = r.Result switch
    {
        ValidationResult.Pass => (ConsoleColor.Green, "PASS"),
        ValidationResult.Fail => (ConsoleColor.Red, "FAIL"),
        ValidationResult.Skip => (ConsoleColor.Yellow, "SKIP"),
        _ => (ConsoleColor.Gray, "????"),
    };

    Console.Write($"  [{r.Entry.Source,-14}] {r.Entry.LocalName,-50} ");
    SetColor(color);
    Console.Write(label);
    Console.ResetColor();

    var detail = r.Detail.Length > 60 ? r.Detail[..57] + "..." : r.Detail;
    Console.Write($"  {detail}");

    if (r.Elapsed > TimeSpan.Zero)
        Console.Write($"  [{r.Elapsed.TotalMilliseconds:F0}ms]");

    Console.WriteLine();
}

static void SetColor(ConsoleColor color)
{
    try { Console.ForegroundColor = color; }
    catch { /* non-interactive terminal */ }
}
