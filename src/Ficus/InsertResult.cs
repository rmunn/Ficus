namespace Ficus;

// This is the equivalent of the following F# code:
//
// type SlideResult<'a> =
//     | SimpleInsertion of newCurrent: 'a
//     | SlidItemsLeft of newLeft: 'a * newCurrent: 'a
//     | SlidItemsRight of newCurrent: 'a * newRight: 'a
//     | SplitNode of newCurrent: 'a * newRight: 'a

internal readonly struct InsertResult<T>
{
    internal enum Tag : byte
    {
        SimpleInsertion = 0,
        SlidItemsLeft = 1,
        SlidItemsRight = 2,
        SplitNode = 3
    }

    internal Tag Case { get; }

    private readonly T _left;
    private readonly T _right;

    private InsertResult(Tag @case, T left, T right)
    {
        Case = @case;
        _left = left;
        _right = right;
    }

    internal static InsertResult<T> SimpleInsertion(T newCurrent) =>
        new InsertResult<T>(Tag.SimpleInsertion, newCurrent, default!);

    internal static InsertResult<T> SlidItemsLeft(T newLeft, T newCurrent) =>
        new InsertResult<T>(Tag.SlidItemsLeft, newLeft, newCurrent);

    internal static InsertResult<T> SlidItemsRight(T newCurrent, T newRight) =>
        new InsertResult<T>(Tag.SlidItemsRight, newCurrent, newRight);

    internal static InsertResult<T> SplitNode(T newCurrent, T newRight) =>
        new InsertResult<T>(Tag.SplitNode, newCurrent, newRight);

    internal void Deconstruct(out Tag tag, out T l, out T mid, out T r)
    {
        tag = Case;
        switch (Case)
        {
            case Tag.SimpleInsertion:
                mid = _left;
                l = default!;
                r = default!;
                break;
            case Tag.SlidItemsLeft:
                l = _left;
                mid = _right;
                r = default!;
                break;
            case Tag.SlidItemsRight:
                l = default!;
                mid = _left;
                r = _right;
                break;
            case Tag.SplitNode:
                mid = default!;
                l = _left;
                r = _right;
                break;
            default:
                l = default!;
                mid = default!;
                r = default!;
                break;
        }
    }
}
