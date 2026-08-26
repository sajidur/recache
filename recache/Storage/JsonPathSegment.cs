namespace recache.Storage
{
    public abstract record JsonPathSegment;

    public sealed record JsonFieldSegment(string Name) : JsonPathSegment;

    public sealed record JsonIndexSegment(int Index) : JsonPathSegment;

    public sealed record JsonWildcardSegment : JsonPathSegment;
}
