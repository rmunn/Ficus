namespace Ficus;

// TODO: Turn this into a simple tuple to avoid so many allocations, once we're confident we have the logic right

// This is the equivalent of the following F# code:
//
// type SlideResult<'a> =
//     | SimpleInsertion of newCurrent: 'a
//     | SlidItemsLeft of newLeft: 'a * newCurrent: 'a
//     | SlidItemsRight of newCurrent: 'a * newRight: 'a
//     | SplitNode of newCurrent: 'a * newRight: 'a

public abstract class SlideResult<T> { }

public sealed class SimpleInsertion<T> : SlideResult<T>
{
    public T NewCurrent { get; }

    public SimpleInsertion(T newCurrent)
    {
        NewCurrent = newCurrent;
    }
    public void Deconstruct(out T newCurrent)
    {
        newCurrent = NewCurrent;
    }
}

public sealed class SlidItemsLeft<T> : SlideResult<T>
{
    public T NewLeft { get; }
    public T NewCurrent { get; }

    public SlidItemsLeft(T newLeft, T newCurrent)
    {
        NewLeft = newLeft;
        NewCurrent = newCurrent;
    }
    public void Deconstruct(out T newLeft, out T newCurrent)
    {
        newLeft = NewLeft;
        newCurrent = NewCurrent;
    }
}

public sealed class SlidItemsRight<T> : SlideResult<T>
{
    public T NewCurrent { get; }
    public T NewRight { get; }

    public SlidItemsRight(T newCurrent, T newRight)
    {
        NewCurrent = newCurrent;
        NewRight = newRight;
    }
    public void Deconstruct(out T newCurrent, out T newRight)
    {
        newCurrent = NewCurrent;
        newRight = NewRight;
    }
}

public sealed class SplitNode<T> : SlideResult<T>
{
    public T NewCurrent { get; }
    public T NewRight { get; }

    public SplitNode(T newCurrent, T newRight)
    {
        NewCurrent = newCurrent;
        NewRight = newRight;
    }
    public void Deconstruct(out T newCurrent, out T newRight)
    {
        newCurrent = NewCurrent;
        newRight = NewRight;
    }
}
