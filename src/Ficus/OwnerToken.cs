namespace Ficus;

// TODO: Just remove the string? value because it's useless. Just have an empty class.
// TODO: Or just use int64s, perhaps with a struct wrapper just so that we can't overlap, and use Interlocked.Increment to get a new one.
// If you increment an int64 every nanosecond, it would take 213,504 years before it wrapped around. So no problem whatsoever.

public sealed class OwnerToken
{
    public string? Value;

    public OwnerToken(string? value)
    {
        Value = value;
    }
}

public static class OwnerTokens
{
    public static readonly OwnerToken NullOwner = new OwnerToken(null);

    public static OwnerToken MkOwnerToken()
    {
        return new OwnerToken(string.Empty);
    }
}
