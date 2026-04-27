namespace Ficus;

internal interface IRRBInternal<T>
{
    RRBVector<T> InsertIntoTail(int tailIndex, T value);

    RRBVector<T> RemoveFromTailAtTailIdx(int tailIndex);

    /// <summary>
    /// Remove implementation with optional rebalancing behavior.
    /// </summary>
    RRBVector<T> RemoveImpl(bool shouldCheckForRebalancing, int index);

    RRBVector<T> RemoveWithoutRebalance(int index);
}
