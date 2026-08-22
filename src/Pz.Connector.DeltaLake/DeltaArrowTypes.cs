using Apache.Arrow;
using Apache.Arrow.Types;

namespace Pz.Connector.DeltaLake;

/// <summary>The two Arrow-type translations this connector owns, plus the one way it names a type in a
/// message. delta-rs owns Arrow→Delta conversion; these describe what it does with the schema it is
/// handed, so the connector can hand it a schema it accepts and can compare a table's shape against the
/// shape a write will actually take there.
///
/// <see cref="Canonical(Schema)"/> is applied to the incoming schema ONCE, at BeginWriteAsync, before
/// anything else looks at it — so the create, the type refusal, the reconcile and every insert see one
/// spelling. <see cref="Stored"/> is what a reconcile compares against, because a Delta table does not
/// necessarily come back shaped like the Arrow schema that created it.
///
/// Both recurse into EVERY container Arrow has, which is wider than
/// <see cref="DeltaTypeSupport.IsWritable"/>'s recursion set and deliberately so: refusing on an
/// unobserved inner type is a claim about what delta-rs rejects, which is why that predicate is
/// cautious, whereas rewriting a spelling or naming a stored shape inside a container is the same
/// answer at every depth. An extension type is left alone by <see cref="Canonical"/> — its storage type
/// is its own business — and reported as its storage type by <see cref="Stored"/>, which is what
/// delta-rs puts in the table.</summary>
internal static class DeltaArrowTypes
{
    /// <summary>Rewrites a schema into the spelling delta-rs accepts, changing no value and no buffer.
    ///
    /// One rewrite exists, and pz is the reason it has to. Every Arrow schema pz produces spells a UTC
    /// timestamp's timezone <c>"+00:00"</c> — its own convention, applied to every TIMESTAMP column it
    /// hands a sink. delta-rs accepts <c>"UTC"</c>, an empty timezone and no timezone, and refuses
    /// every other spelling outright: measured, <c>Timestamp(µs, "+00:00")</c> fails the CREATE with
    /// "Invalid data type for Delta Lake", and against a table an earlier writer created as
    /// <c>timestamp(µs, UTC)</c> the two spellings differ and a reconcile refuses the write. Both
    /// symptoms are one cause, and the two strings denote the same zone, so the connector normalises
    /// rather than reporting a difference the user cannot act on: pz overwrites the timezone whatever
    /// the pipeline SQL says, so "cast it in the pipeline SQL" would be advice nobody can follow.
    ///
    /// A timezone that does NOT denote UTC is left exactly as it is — it is a real difference, not a
    /// spelling, and <see cref="DeltaTypeSupport"/> refuses it with a next step that says so.
    ///
    /// The batches themselves are NOT rewritten and do not need to be: measured against DeltaLake.Net
    /// 0.33.0, an insert whose declared schema says <c>"UTC"</c> accepts batch arrays typed
    /// <c>"+00:00"</c> — the two carry identical buffers, and the declared schema is what the write is
    /// validated against.</summary>
    public static Schema Canonical(Schema schema)
    {
        Field[]? rewritten = null;
        for (var i = 0; i < schema.FieldsList.Count; i++)
        {
            var field = schema.FieldsList[i];
            var canonical = Canonical(field.DataType);
            if (ReferenceEquals(canonical, field.DataType))
            {
                continue;
            }

            rewritten ??= schema.FieldsList.ToArray();
            rewritten[i] = new Field(field.Name, canonical, field.IsNullable, field.Metadata);
        }

        return rewritten is null ? schema : new Schema(rewritten, schema.Metadata);
    }

    /// <summary>Returns the argument itself when nothing needs rewriting, so a caller can tell a
    /// rewrite from a no-op by reference and leave an untouched schema untouched.</summary>
    public static IArrowType Canonical(IArrowType type) => type switch
    {
        TimestampType t when t.Timezone is { Length: > 0 } tz && DenotesUtc(tz) && tz != "UTC" =>
            new TimestampType(t.Unit, "UTC"),
        ExtensionType e => e,
        ListType l => Rewrap(l, Canonical),
        LargeListType l => Rewrap(l, Canonical),
        ListViewType l => Rewrap(l, Canonical),
        LargeListViewType l => Rewrap(l, Canonical),
        FixedSizeListType f => Rewrap(f, Canonical),
        StructType s => Rewrap(s, Canonical),
        MapType m => Rewrap(m, Canonical),
        _ => type,
    };

    /// <summary>What delta-rs actually stores for an Arrow type it accepts — which is not always the
    /// type it was given. A reconcile that compares a table's schema against the schema requested,
    /// rather than against this, refuses the very first write to a table it has just created from that
    /// same schema, and leaves an empty table behind.
    ///
    /// Every mapping below is OBSERVED against DeltaLake.Net 0.33.0, by creating a real table from the
    /// type and reading its schema back — DeltaStoredTypeTests does exactly that for every writable
    /// candidate in DeltaTypeSupportTests, so a delta-rs that changes one of these fails the build
    /// rather than reaching a user as a refusal they cannot act on.
    ///
    /// The write itself is unaffected: an insert declaring the ORIGINAL type against a table storing
    /// the normalised one succeeds — measured, twice over, for the create run and for a later run that
    /// only loads the table.</summary>
    public static IArrowType Stored(IArrowType type) => type switch
    {
        // Delta has no unsigned integer type.
        UInt8Type => Int8Type.Default,
        UInt16Type => Int16Type.Default,
        UInt32Type => Int32Type.Default,
        UInt64Type => Int64Type.Default,
        // Delta has one date type: a day count.
        Date64Type => Date32Type.Default,
        // An extension type is stored as the type it is built on; the extension name does not survive.
        ExtensionType e => Stored(e.StorageType),
        // A dictionary-encoded column is stored as its VALUE type; the encoding does not survive. As a
        // PARTITION column it cannot be stored at all — see DeltaTypeSupport.AssertPartitionable, which
        // is the refusal this mapping makes necessary.
        DictionaryType d => Stored(d.ValueType),
        // Delta has one string type and one binary type; every Arrow encoding of either collapses onto
        // it. Every Decimal*Type derives from FixedSizeBinaryType, so the decimal arm must precede the
        // binary one or a decimal column would be described as binary — a subsumed arm is CS8510,
        // which TreatWarningsAsErrors makes a build error, so the order cannot regress silently.
        StringViewType or LargeStringType => StringType.Default,
        Decimal128Type or Decimal256Type or Decimal32Type or Decimal64Type => type,
        BinaryViewType or LargeBinaryType or FixedSizeBinaryType => BinaryType.Default,
        // Delta stores one timestamp precision, microseconds, whatever unit was offered. An empty
        // timezone comes back as no timezone at all.
        TimestampType t => new TimestampType(
            TimeUnit.Microsecond, t.Timezone is { Length: > 0 } tz ? tz : null),
        // Delta has one list type, and all four Arrow list encodings collapse onto it.
        ListType l => AsList(l.ValueField, Stored),
        LargeListType l => AsList(l.ValueField, Stored),
        ListViewType l => AsList(l.ValueField, Stored),
        LargeListViewType l => AsList(l.ValueField, Stored),
        StructType s => Rewrap(s, Stored),
        MapType m => Rewrap(m, Stored),
        // NOT observed, and unreachable through this sink: DeltaTypeSupport refuses FixedSizeList at
        // BeginWriteAsync, so no create is ever made from one and there is no stored shape to have
        // measured. Recursing keeps the inner type honest if that refusal is ever lifted; it does not
        // claim to know what Delta does with the container.
        FixedSizeListType f => Rewrap(f, Stored),
        _ => type,
    };

    /// <summary>A fully parameterized name for an Arrow type. Apache.Arrow's own <c>Name</c> drops the
    /// parameters that decide whether two same-id types are actually the same ("decimal128" for any
    /// precision and scale, "timestamp" for any unit and timezone), and no Arrow type implements value
    /// equality, so a comparison and an error message both need this.</summary>
    public static string Describe(IArrowType type) => type switch
    {
        Decimal128Type d => $"decimal128({d.Precision}, {d.Scale})",
        Decimal256Type d => $"decimal256({d.Precision}, {d.Scale})",
        TimestampType t => $"timestamp[{t.Unit}{(t.Timezone is null ? string.Empty : $", tz={t.Timezone}")}]",
        Time32Type t => $"time32[{t.Unit}]",
        Time64Type t => $"time64[{t.Unit}]",
        DurationType d => $"duration[{d.Unit}]",
        IntervalType i => $"interval[{i.Unit}]",
        FixedSizeBinaryType f => $"fixed_size_binary[{f.ByteWidth}]",
        FixedSizeListType f => $"fixed_size_list<{Describe(f.ValueDataType)}>[{f.ListSize}]",
        ListType l => $"list<{Describe(l.ValueDataType)}>",
        LargeListType l => $"large_list<{Describe(l.ValueDataType)}>",
        MapType m => $"map<{Describe(m.KeyField.DataType)}, {Describe(m.ValueField.DataType)}>",
        StructType s => $"struct<{string.Join(", ", s.Fields.Select(f => $"{f.Name}: {Describe(f.DataType)}"))}>",
        _ => type.Name,
    };

    /// <summary>Whether an Arrow timezone string names the UTC zone. Deliberately a short, closed set
    /// of unambiguous spellings — the IANA name, the ISO designator, and the zero offset in the three
    /// widths Arrow's own specification allows — rather than a general offset parse: anything outside
    /// it is left alone and refused by name, which is the safe direction. A named zone that happens to
    /// sit at UTC today (<c>Etc/UTC</c>, <c>GMT</c>) is NOT in the set: it is a zone, not an offset,
    /// and rewriting one is a decision about the user's data rather than about its spelling.</summary>
    public static bool DenotesUtc(string timezone) =>
        string.Equals(timezone, "UTC", StringComparison.OrdinalIgnoreCase)
        || string.Equals(timezone, "Z", StringComparison.OrdinalIgnoreCase)
        || (timezone.Length > 1
            && (timezone[0] == '+' || timezone[0] == '-')
            && timezone[1..] is "00:00" or "0000" or "00");

    /// <summary>The one list shape Delta has, carrying the mapped element type.</summary>
    private static IArrowType AsList(Field valueField, Func<IArrowType, IArrowType> map) =>
        new ListType(Rewrap(valueField, map(valueField.DataType)));

    private static IArrowType Rewrap(ListType type, Func<IArrowType, IArrowType> map)
    {
        var inner = map(type.ValueDataType);
        return ReferenceEquals(inner, type.ValueDataType)
            ? type
            : new ListType(Rewrap(type.ValueField, inner));
    }

    private static IArrowType Rewrap(LargeListType type, Func<IArrowType, IArrowType> map)
    {
        var inner = map(type.ValueDataType);
        return ReferenceEquals(inner, type.ValueDataType)
            ? type
            : new LargeListType(Rewrap(type.ValueField, inner));
    }

    private static IArrowType Rewrap(ListViewType type, Func<IArrowType, IArrowType> map)
    {
        var inner = map(type.ValueDataType);
        return ReferenceEquals(inner, type.ValueDataType)
            ? type
            : new ListViewType(Rewrap(type.ValueField, inner));
    }

    private static IArrowType Rewrap(LargeListViewType type, Func<IArrowType, IArrowType> map)
    {
        var inner = map(type.ValueDataType);
        return ReferenceEquals(inner, type.ValueDataType)
            ? type
            : new LargeListViewType(Rewrap(type.ValueField, inner));
    }

    private static IArrowType Rewrap(FixedSizeListType type, Func<IArrowType, IArrowType> map)
    {
        var inner = map(type.ValueDataType);
        return ReferenceEquals(inner, type.ValueDataType)
            ? type
            : new FixedSizeListType(Rewrap(type.ValueField, inner), type.ListSize);
    }

    private static IArrowType Rewrap(StructType type, Func<IArrowType, IArrowType> map)
    {
        var mapped = type.Fields.Select(f => (Field: f, Type: map(f.DataType))).ToList();
        return mapped.All(m => ReferenceEquals(m.Type, m.Field.DataType))
            ? type
            : new StructType([.. mapped.Select(m => Rewrap(m.Field, m.Type))]);
    }

    private static IArrowType Rewrap(MapType type, Func<IArrowType, IArrowType> map)
    {
        var key = map(type.KeyField.DataType);
        var value = map(type.ValueField.DataType);
        return ReferenceEquals(key, type.KeyField.DataType) && ReferenceEquals(value, type.ValueField.DataType)
            ? type
            : new MapType(Rewrap(type.KeyField, key), Rewrap(type.ValueField, value), type.KeySorted);
    }

    private static Field Rewrap(Field field, IArrowType type) =>
        new(field.Name, type, field.IsNullable, field.Metadata);
}
