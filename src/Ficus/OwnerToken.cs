namespace Ficus;

// TODO: Just remove the string? value because it's useless. Just have an empty class.

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
