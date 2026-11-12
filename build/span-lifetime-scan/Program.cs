// Type-aware scan for spans taken off Apache.Arrow objects after which the owner is never used again (#834).
//
// Usage: span-lifetime-scan <project dir> <bin dir> [<preprocessor symbols, comma-separated>]; see README.md.
//
// For each member access or invocation whose symbol belongs to an Apache.Arrow type and whose type is Span<T> or
// ReadOnlySpan<T>, the receiver chain is walked to its root: a local, a parameter, `this`, or a temporary (a call
// or object creation). A site is reported when the root is a temporary, or a local or parameter that is not read
// anywhere after the site within the enclosing body (method, local function, lambda or accessor) and is not a
// `using` local. Output: path(line): owner kind, owner, how the span is consumed, the source line.
using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

if (args.Length < 2)
{
    Console.Error.WriteLine("usage: spanscan <project dir> <bin dir> [symbols]");
    return 2;
}

string projectDir = Path.GetFullPath(args[0]);
string binDir = Path.GetFullPath(args[1]);
// The preprocessor symbols the SDK defines for a target, cumulative (`NET6_0_OR_GREATER` holds on net10.0 too), so the
// branches scanned are the ones that target builds (review round 1). `net472` scans .NET Framework's #else branches;
// anything else is taken as a comma-separated list.
const string net10 = "NET,NETCOREAPP,NET10_0,NETCOREAPP1_0_OR_GREATER,NETCOREAPP1_1_OR_GREATER,NETCOREAPP2_0_OR_GREATER," +
    "NETCOREAPP2_1_OR_GREATER,NETCOREAPP2_2_OR_GREATER,NETCOREAPP3_0_OR_GREATER,NETCOREAPP3_1_OR_GREATER," +
    "NET5_0_OR_GREATER,NET6_0_OR_GREATER,NET7_0_OR_GREATER,NET8_0_OR_GREATER,NET9_0_OR_GREATER,NET10_0_OR_GREATER,RELEASE";
const string net472 = "NETFRAMEWORK,NET472,NET20_OR_GREATER,NET30_OR_GREATER,NET35_OR_GREATER,NET40_OR_GREATER," +
    "NET45_OR_GREATER,NET451_OR_GREATER,NET452_OR_GREATER,NET46_OR_GREATER,NET461_OR_GREATER,NET462_OR_GREATER," +
    "NET47_OR_GREATER,NET471_OR_GREATER,NET472_OR_GREATER,RELEASE";
string symbolArg = args.Length > 2 ? args[2] : "net10.0";
var symbols = (symbolArg switch { "net10.0" => net10, "net472" => net472, _ => symbolArg })
    .Split(',', StringSplitOptions.RemoveEmptyEntries);
var parse = new CSharpParseOptions(LanguageVersion.Preview, preprocessorSymbols: symbols);

var files = Directory.EnumerateFiles(projectDir, "*.cs", SearchOption.AllDirectories)
    .Where(f => !f.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}")
             && !f.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}"))
    .ToList();

// Linked compile items (e.g. test/Shared/*.cs) are left out; they are scanned with their own project if any.
var trees = files.Select(f => CSharpSyntaxTree.ParseText(File.ReadAllText(f), parse, f)).ToList();

// Global usings the SDK generates (ImplicitUsings) and the project's own <Using> items live in obj; add the common set.
const string globals = """
    global using global::System;
    global using global::System.Collections.Generic;
    global using global::System.IO;
    global using global::System.Linq;
    global using global::System.Net.Http;
    global using global::System.Threading;
    global using global::System.Threading.Tasks;
    global using global::Xunit;
    """;
trees.Add(CSharpSyntaxTree.ParseText(globals, parse, "globals.cs"));

var refs = new List<MetadataReference>();
string runtimeDir = Path.GetDirectoryName(typeof(object).Assembly.Location)!;
foreach (var dll in Directory.EnumerateFiles(runtimeDir, "*.dll"))
{
    try { refs.Add(MetadataReference.CreateFromFile(dll)); } catch { }
}

string projectName = Path.GetFileName(projectDir.TrimEnd(Path.DirectorySeparatorChar));
foreach (var dll in Directory.EnumerateFiles(binDir, "*.dll"))
{
    // The project's own assembly would duplicate the sources being compiled.
    if (Path.GetFileNameWithoutExtension(dll).Equals(projectName, StringComparison.OrdinalIgnoreCase))
        continue;
    try { refs.Add(MetadataReference.CreateFromFile(dll)); } catch { }
}

// Signed with the repo's key, so InternalsVisibleTo grants resolve and the tests' use of internals binds.
string? snk = Environment.GetEnvironmentVariable("SPANSCAN_SNK");
var options = new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary, allowUnsafe: true,
    nullableContextOptions: NullableContextOptions.Enable);
if (snk is not null)
    options = options.WithCryptoKeyFile(snk).WithStrongNameProvider(new DesktopStrongNameProvider());
refs.RemoveAll(r => r.Display is { } d && d.EndsWith(".Native.dll", StringComparison.OrdinalIgnoreCase));
var compilation = CSharpCompilation.Create(projectName, trees, refs, options);

var errors = compilation.GetDiagnostics().Where(d => d.Severity == DiagnosticSeverity.Error).ToList();
Console.Error.WriteLine($"{projectName}: {files.Count} files, {errors.Count} compile errors (unresolved symbols are skipped)");
if (Environment.GetEnvironmentVariable("SPANSCAN_ERRORS") == "1")
{
    foreach (var g in errors.GroupBy(d => d.Id).OrderByDescending(g => g.Count()).Take(6))
        Console.Error.WriteLine($"  {g.Key} x{g.Count()}: {g.First().GetMessage()}");
}

int reported = 0;
bool fix = Environment.GetEnvironmentVariable("SPANSCAN_FIX") == "1";
foreach (var tree in trees)
{
    var model = compilation.GetSemanticModel(tree);
    var root = tree.GetRoot();

    // Fix mode: per (body, owner), the latest position a span off the owner is read, so a GC.KeepAlive(owner) can
    // follow the statement holding it.
    var latest = new Dictionary<(SyntaxNode Body, ISymbol Owner), int>();
    var firstSite = new Dictionary<(SyntaxNode Body, ISymbol Owner), int>();
    var manual = new List<string>();
    foreach (var node in root.DescendantNodes())
    {
        ExpressionSyntax? site = node switch
        {
            InvocationExpressionSyntax inv => inv,
            MemberAccessExpressionSyntax ma when ma.Parent is not InvocationExpressionSyntax { Expression: var e } || e != ma => ma,
            _ => null,
        };
        if (site is null)
            continue;

        var symbol = model.GetSymbolInfo(site).Symbol;
        ITypeSymbol? type = symbol switch
        {
            IPropertySymbol p => p.Type,
            IMethodSymbol m => m.ReturnType,
            _ => null,
        };
        if (symbol is null || type is null || !IsSpan(type) || !IsArrow(symbol.ContainingType))
            continue;

        ExpressionSyntax? receiver = site switch
        {
            InvocationExpressionSyntax { Expression: MemberAccessExpressionSyntax ma } => ma.Expression,
            MemberAccessExpressionSyntax ma => ma.Expression,
            _ => null,
        };
        if (receiver is null)
            continue;

        var (ownerKind, ownerName, ownerSymbol) = Root(receiver, model);
        var body = EnclosingBody(site);
        if (body is null)
            continue;

        bool dead;
        if (ownerSymbol is null)
        {
            dead = ownerKind is "temporary";
        }
        else if (ownerSymbol is ILocalSymbol local && IsUsingLocal(local, body))
        {
            dead = false;
        }
        else
        {
            // In fix mode a keep-alive is not a use: one placed too early must be found to be moved.
            bool fixing = Environment.GetEnvironmentVariable("SPANSCAN_FIX") == "1";
            dead = !body.DescendantNodes().OfType<IdentifierNameSyntax>()
                .Where(id => id.SpanStart >= site.Span.End && id.Identifier.ValueText == ownerName)
                .Where(id => !fixing || id.Ancestors().OfType<ExpressionStatementSyntax>().FirstOrDefault() is not { } s
                             || !IsKeepAliveOf(s, ownerName))
                .Any(id => SymbolEqualityComparer.Default.Equals(model.GetSymbolInfo(id).Symbol, ownerSymbol));
            if (ownerKind == "this")
                dead = false; // an instance method's `this` is the caller's problem
        }

        if (!dead)
            continue;

        var line = tree.GetLineSpan(site.Span).StartLinePosition.Line + 1;
        string text = tree.GetText().Lines[line - 1].ToString().Trim();
        string consumption = Consumption(site);
        Console.WriteLine($"{tree.FilePath}({line}): {ownerKind} {ownerName} | {consumption} | {text}");
        reported++;

        // Indexed or measured where taken: no call between taking the span and reading it. Left alone.
        if (!fix || consumption is "indexed" or "length")
            continue;
        if (ownerSymbol is null)
        {
            manual.Add($"{tree.FilePath}({line}): owner is a {ownerKind}");
            continue;
        }

        // A span stored in a span-typed local, directly or through an expression (MemoryMarshal.Cast, Slice), is read
        // until that local's last use; so is any span local initialized from one of those.
        var tracked = new HashSet<ISymbol>(SymbolEqualityComparer.Default);
        foreach (var declarator in site.Ancestors().OfType<VariableDeclaratorSyntax>().Take(1))
        {
            if (declarator.Initializer is { } init && init.Span.Contains(site.Span)
                && model.GetDeclaredSymbol(declarator) is ILocalSymbol local && IsSpan(local.Type))
            {
                tracked.Add(local);
            }
        }

        foreach (var assignment in site.Ancestors().OfType<AssignmentExpressionSyntax>().Take(1))
        {
            if (assignment.Right.Span.Contains(site.Span)
                && model.GetSymbolInfo(assignment.Left).Symbol is ILocalSymbol local && IsSpan(local.Type))
            {
                tracked.Add(local);
            }
        }

        bool grew = tracked.Count > 0;
        while (grew)
        {
            grew = false;
            foreach (var declarator in body.DescendantNodes().OfType<VariableDeclaratorSyntax>())
            {
                if (declarator.Initializer is not { } init
                    || model.GetDeclaredSymbol(declarator) is not ILocalSymbol local || !IsSpan(local.Type)
                    || tracked.Contains(local))
                {
                    continue;
                }

                if (init.DescendantNodes().OfType<IdentifierNameSyntax>()
                    .Any(id => model.GetSymbolInfo(id).Symbol is ILocalSymbol l && tracked.Contains(l)))
                {
                    tracked.Add(local);
                    grew = true;
                }
            }
        }

        int end = site.Span.End;
        foreach (var id in body.DescendantNodes().OfType<IdentifierNameSyntax>())
        {
            if (model.GetSymbolInfo(id).Symbol is ILocalSymbol l && tracked.Contains(l))
                end = Math.Max(end, id.Span.End);
        }

        var key = (body, ownerSymbol);
        latest[key] = latest.TryGetValue(key, out int seen) ? Math.Max(seen, end) : end;
        firstSite[key] = firstSite.TryGetValue(key, out int first) ? Math.Min(first, site.SpanStart) : site.SpanStart;
    }

    if (!fix)
        continue;

    // One insertion per statement, each naming its owners once; and keep-alives placed too early, to remove.
    var inserts = new SortedDictionary<int, (string Indent, List<string> Owners)>();
    var removals = new List<StatementSyntax>();
    string newline = tree.GetText().ToString().Contains("\r\n") ? "\r\n" : "\n";
    foreach (var ((body, owner), position) in latest)
    {
        var token = root.FindToken(Math.Max(0, position - 1));
        StatementSyntax? statement = null;
        for (var n = token.Parent; n is not null && n != body; n = n.Parent)
        {
            if (n is StatementSyntax s && s.Parent is BlockSyntax)
            {
                statement = s;
                break;
            }
        }

        var where = tree.GetLineSpan(token.Span).StartLinePosition.Line + 1;
        if (statement is null || !body.Span.Contains(statement.Span))
        {
            manual.Add($"{tree.FilePath}({where}): {owner.Name}: no enclosing block statement (expression body)");
            continue;
        }

        if (statement is ReturnStatementSyntax or ThrowStatementSyntax or YieldStatementSyntax)
        {
            manual.Add($"{tree.FilePath}({where}): {owner.Name}: read in a {statement.Kind()}");
            continue;
        }

        // Whether the owner is in scope after the statement (a pattern variable is not) is left to the compiler: an
        // inserted line that does not compile is removed afterwards, and the site handled by hand.

        // A keep-alive of this owner between its first span and this statement was placed too early (an earlier pass
        // did not follow spans through Cast or Slice): it moves here.
        foreach (var early in body.DescendantNodes().OfType<ExpressionStatementSyntax>())
        {
            if (early.SpanStart > firstSite[(body, owner)] && early.SpanStart < statement.Span.End
                && IsKeepAliveOf(early, owner.Name) && !statement.Span.Contains(early.Span))
            {
                removals.Add(early);
            }
        }

        // Already kept alive right after.
        if (statement.Parent is BlockSyntax block)
        {
            int index = block.Statements.IndexOf(statement);
            if (index + 1 < block.Statements.Count
                && block.Statements[index + 1] is ExpressionStatementSyntax after && IsKeepAliveOf(after, owner.Name))
            {
                continue;
            }
        }

        string lineText = tree.GetText().Lines.GetLineFromPosition(statement.SpanStart).ToString();
        string indent = lineText[..(lineText.Length - lineText.TrimStart().Length)];
        int at = statement.FullSpan.End;
        if (!inserts.TryGetValue(at, out var entry))
            inserts[at] = entry = (indent, new List<string>());
        if (!entry.Owners.Contains(owner.Name))
            entry.Owners.Add(owner.Name);
    }

    foreach (var m in manual)
        Console.Error.WriteLine("MANUAL " + m);

    if (inserts.Count == 0 && removals.Count == 0)
        continue;

    // Edits applied from the end, so earlier positions stay valid: (position, length removed, text inserted).
    string source = tree.GetText().ToString();
    var edits = new List<(int At, int Remove, string Insert)>();
    foreach (var (at, (indent, owners)) in inserts)
    {
        // FullSpan.End follows the statement's trailing newline, unless the statement ends the file.
        string prefix = at > 0 && source[at - 1] == '\n' ? "" : newline;
        edits.Add((at, 0, prefix + string.Concat(owners.Select(o => $"{indent}GC.KeepAlive({o});{newline}"))));
    }

    foreach (var removal in removals.Distinct())
        edits.Add((removal.FullSpan.Start, removal.FullSpan.Length, ""));
    foreach (var (at, remove, insert) in edits.OrderByDescending(e => e.At).ThenBy(e => e.Remove))
        source = source[..at] + insert + source[(at + remove)..];

    File.WriteAllText(tree.FilePath, source, new System.Text.UTF8Encoding(encoderShouldEmitUTF8Identifier: false));
    Console.Error.WriteLine($"FIXED {tree.FilePath}: {inserts.Sum(i => i.Value.Owners.Count)} keep-alives, {removals.Distinct().Count()} moved");
}

Console.Error.WriteLine($"{projectName}: {reported} sites");
return 0;

static bool IsKeepAliveOf(ExpressionStatementSyntax statement, string name) =>
    statement.Expression is InvocationExpressionSyntax
    {
        Expression: MemberAccessExpressionSyntax { Expression: IdentifierNameSyntax { Identifier.ValueText: "GC" }, Name.Identifier.ValueText: "KeepAlive" },
        ArgumentList.Arguments: [{ Expression: IdentifierNameSyntax arg }],
    } && arg.Identifier.ValueText == name;

static bool IsSpan(ITypeSymbol type) =>
    type is INamedTypeSymbol { IsGenericType: true } n
    && n.ContainingNamespace?.ToDisplayString() == "System"
    && n.Name is "Span" or "ReadOnlySpan";

static bool IsArrow(INamedTypeSymbol? type)
{
    for (var t = type; t is not null; t = t.BaseType)
    {
        var ns = t.ContainingNamespace?.ToDisplayString() ?? "";
        if (ns == "Apache.Arrow" || ns.StartsWith("Apache.Arrow.", StringComparison.Ordinal))
            return true;
    }

    return false;
}

static (string Kind, string Name, ISymbol? Symbol) Root(ExpressionSyntax expr, SemanticModel model)
{
    while (true)
    {
        switch (expr)
        {
            case MemberAccessExpressionSyntax ma:
                // A static member (Foo.Bar) roots nothing.
                if (model.GetSymbolInfo(ma).Symbol is { IsStatic: true } s && s is not IMethodSymbol)
                    return ("static", ma.ToString(), null);
                expr = ma.Expression;
                break;
            case InvocationExpressionSyntax inv:
                if (inv.Expression is MemberAccessExpressionSyntax target
                    && model.GetSymbolInfo(inv).Symbol is IMethodSymbol { IsStatic: false, IsExtensionMethod: false })
                {
                    // The receiver of an instance call: whatever it returns is a temporary owned elsewhere, but
                    // a call returning a child of the receiver (Column(i), Slice) keeps the root relevant.
                    expr = target.Expression;
                    break;
                }

                // Only a static call known to hand back its first argument, or an element of it (Assert.IsType<T>(x),
                // Assert.Single(xs)), makes that argument the owner. Any other call's result is a temporary: a
                // ReadWhole(chunk) builds a new array, which keeping `chunk` alive does nothing for (review round 1).
                if (model.GetSymbolInfo(inv).Symbol is IMethodSymbol { IsStatic: true } identity
                    && IsIdentityHelper(identity) && inv.ArgumentList.Arguments.Count > 0)
                {
                    expr = inv.ArgumentList.Arguments[0].Expression;
                    break;
                }

                return ("temporary", inv.ToString().Length > 60 ? inv.ToString()[..60] + "…" : inv.ToString(), null);
            case ElementAccessExpressionSyntax ea:
                expr = ea.Expression;
                break;
            case CastExpressionSyntax c:
                expr = c.Expression;
                break;
            case ParenthesizedExpressionSyntax p:
                expr = p.Expression;
                break;
            case PostfixUnaryExpressionSyntax { RawKind: (int)SyntaxKind.SuppressNullableWarningExpression } bang:
                expr = bang.Operand;
                break;
            case BinaryExpressionSyntax { RawKind: (int)SyntaxKind.AsExpression } asExpr:
                expr = asExpr.Left;
                break;
            case ThisExpressionSyntax:
                return ("this", "this", null);
            case ObjectCreationExpressionSyntax or ImplicitObjectCreationExpressionSyntax:
                return ("temporary", expr.ToString().Length > 60 ? expr.ToString()[..60] + "…" : expr.ToString(), null);
            case IdentifierNameSyntax id:
                var sym = model.GetSymbolInfo(id).Symbol;
                return sym switch
                {
                    ILocalSymbol l => ("local", l.Name, l),
                    IParameterSymbol p => ("parameter", p.Name, p),
                    IFieldSymbol f => (f.IsStatic ? "static" : "this", f.Name, null),
                    IPropertySymbol pr => (pr.IsStatic ? "static" : "this", pr.Name, null),
                    _ => ("unknown", id.Identifier.ValueText, null),
                };
            default:
                return ("unknown", expr.Kind().ToString(), null);
        }
    }
}

static SyntaxNode? EnclosingBody(SyntaxNode node)
{
    for (var n = node.Parent; n is not null; n = n.Parent)
    {
        switch (n)
        {
            case LocalFunctionStatementSyntax or AnonymousFunctionExpressionSyntax or AccessorDeclarationSyntax
                or BaseMethodDeclarationSyntax or PropertyDeclarationSyntax:
                return n;
        }
    }

    return null;
}

// Whether the local is itself a using resource (`using var x = …`, or `using (var x = …)`), whose disposal at the end of
// its scope is a use. A local merely declared inside a using statement's body is not (review round 1).
static bool IsUsingLocal(ILocalSymbol local, SyntaxNode body)
{
    foreach (var decl in local.DeclaringSyntaxReferences)
    {
        if (decl.GetSyntax() is not VariableDeclaratorSyntax { Parent: VariableDeclarationSyntax declaration })
            continue;
        if (declaration.Parent is LocalDeclarationStatementSyntax lds && lds.UsingKeyword.RawKind != 0)
            return true;
        if (declaration.Parent is UsingStatementSyntax us && us.Declaration == declaration)
            return true;
    }

    return false;
}

// Static helpers that return their first argument, or an element of it, so that argument owns the result.
static bool IsIdentityHelper(IMethodSymbol method)
{
    string type = method.ContainingType?.ToDisplayString() ?? "";
    return (type == "Xunit.Assert" && method.Name is "IsType" or "IsAssignableFrom" or "Single")
        || (type == "System.Runtime.CompilerServices.Unsafe" && method.Name == "As");
}

// How the span is used right where it is taken: indexed (no call between), passed on, or stored.
static string Consumption(ExpressionSyntax site)
{
    var parent = site.Parent;
    while (parent is ParenthesizedExpressionSyntax)
        parent = parent.Parent;
    return parent switch
    {
        ElementAccessExpressionSyntax => "indexed",
        MemberAccessExpressionSyntax ma when ma.Name.Identifier.ValueText is "Length" or "IsEmpty" => "length",
        MemberAccessExpressionSyntax ma => "." + ma.Name.Identifier.ValueText,
        ArgumentSyntax => "argument",
        EqualsValueClauseSyntax => "stored",
        AssignmentExpressionSyntax => "stored",
        ArrowExpressionClauseSyntax or ReturnStatementSyntax => "returned",
        ForEachStatementSyntax => "foreach",
        _ => parent?.Kind().ToString() ?? "?",
    };
}
