using System;
using System.Collections;
using System.Collections.Generic;

namespace Ficus;

public abstract class RRBVector<T> : IEnumerable<T>
{
    // TODO: Consider whether some of these should be properties, e.g. IsEmpty and IterLeaves and such
    // Consider what the IList interface does with similar ones

    /// Creates an empty vector
    public abstract RRBVector<T> Empty();

    /// Tests whether vector is empty. O(1)
    public abstract bool IsEmpty();

    public abstract string StringRepr { get; }

    /// Number of items in vector. O(1)
    public abstract int Length { get; }

    public abstract IEnumerable<T[]> IterLeaves();
    public abstract IEnumerable<T[]> RevIterLeaves();

    public abstract IEnumerable<T> IterItems();
    public abstract IEnumerable<T> RevIterItems();

    public abstract RRBVector<T> Push(T value);
    public abstract T Peek();
    public abstract RRBVector<T> Pop();

    public abstract RRBVector<T> Take(int count);
    public abstract RRBVector<T> Skip(int count);

    /// Split vector into two at index <code>i</code>. The left vector will be length <code>i</code> and
    /// contain items 0 through i-1, while the right vector will be length <code>x.Length - i</code> and
    /// contain items i through x.Length-1. <b>Effectively O(1)</b> (really O(log<sub>32</sub> N))
    public abstract (RRBVector<T> Left, RRBVector<T> Right) Split(int index);

    // Note that this follows C# convention using start and length, *NOT* start and end as in F#. F# code should use the GetSlice method (defined in the RRBVector.fs module) instead, since that one follows F# convention.
    public abstract RRBVector<T> Slice(int start, int length);
    // Note that this follows C# convention: start is *inclusive* and end is *exclusive*. F# code should use the GetSlice method (defined in the RRBVector.fs module) instead, since that one follows F# convention.
    public RRBVector<T> Slice(Index start, Index end) => this[start.GetOffset(Length)..end.GetOffset(Length)];
    public RRBVector<T> Slice(Range slice)
    {
        var (start, len) = slice.GetOffsetAndLength(Length);
        return Slice(start, len);
    }

    /// Concatenate two vectors to create a new one. <b>Effectively O(1)</b> (really O(log<sub>32</sub> N))
    public abstract RRBVector<T> Append(RRBVector<T> other);

    public abstract RRBVector<T> Insert(int index, T value);
    public abstract RRBVector<T> Remove(int index);
    public abstract RRBVector<T> Update(int index, T value);

    public abstract T GetItem(int index);
    public T GetItem(Index index) => GetItem(index.GetOffset(Length));

    public abstract RRBVector<T> Transient();
    public abstract RRBVector<T> Persistent();

    // Indexers
    public T this[int index] => GetItem(index);
    public T this[Index index] => GetItem(index);
    public RRBVector<T> this[Range range] => Slice(range);

    // IEnumerable<T>
    public IEnumerator<T> GetEnumerator()
    {
        return IterItems().GetEnumerator();
    }

    // IEnumerable (non-generic)
    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }
}
