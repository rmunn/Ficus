using System.Collections.Generic;
using System.Linq;
using Ficus.RRBArrayExtensions;

namespace Ficus;

public class RRBExpandedRelaxedNode<T> : RRBRelaxedNode<T>
{
    public int CurrentLength { get; private set; }

    public RRBExpandedRelaxedNode(
        OwnerToken ownerToken,
        RRBNode<T>[] children,
        int[] sizeTable,
        int? realSize = null)
        : base(
            ownerToken,
            children.ExpandToBlockSize(Literals.blockSize),
            sizeTable.ExpandToBlockSize(Literals.blockSize))
    {
        CurrentLength = realSize ?? children.Length;
    }

    public override int NodeSize => CurrentLength;

#if DEBUG
    public override string StringRepr => $"ExpandedRelaxedNode(length={NodeSize})";
#endif

    public override int SlotCount =>
        Children.Take(NodeSize).Sum(c => c.NodeSize);

    public override RRBNode<T>[] SafeChildrenArr => Children.Truncate(NodeSize);

    public override RRBNode<T> Shrink(OwnerToken owner)
    {
        int size = NodeSize;

        if (IsEditableBy(owner) && size == Literals.blockSize)
        {
            // No need to shrink the children list since every child slot has a meaningful value in it
            return new RRBRelaxedNode<T>(owner, Children, SizeTable);
        }

        // Shrink arrays (both children and size table) and/or create new copies for new owner token
        var children =
            size == Literals.blockSize
                ? (RRBNode<T>[])Children.Clone()
                : Children.Truncate(size);
        var sizeTable =
            size == Literals.blockSize
                ? (int[])SizeTable.Clone()
                : SizeTable.Truncate(size);

        return new RRBRelaxedNode<T>(owner, children, sizeTable);
    }

    public override RRBNode<T> Expand(OwnerToken owner) => GetEditableNode(owner);

    public override RRBNode<T> GetEditableNode(OwnerToken owner)
    {
        if (IsEditableBy(owner)) return this;

        return new RRBExpandedRelaxedNode<T>(
            owner,
            (RRBNode<T>[])Children.Clone(),
            (int[])SizeTable.Clone(),
            NodeSize);
    }

    public override void SetNodeSize(int newSize)
    {
        CurrentLength = newSize;
    }

    public override RRBNode<T> ToFullNodeIfNeeded(int shift)
    {
        if (RRBMath.IsSizeTableFullAtShift(shift, SizeTable, NodeSize))
            return new RRBExpandedFullNode<T>(Owner, Children, NodeSize);

        return this;
    }

    // ===== NODE MANIPULATION =====
    // These low-level functions only create new nodes (or modify an expanded node), without doing any bounds checking.

    public override RRBNode<T> AppendChild(OwnerToken owner, int shift, RRBNode<T> newChild)
        => AppendChildS(owner, shift, newChild, newChild.TreeSize(RRBMath.Down(shift)));

    public override RRBNode<T> AppendChildS(OwnerToken owner, int shift, RRBNode<T> newChild, int newChildSize)
    {
        var node = (RRBExpandedRelaxedNode<T>)GetEditableNode(owner);
        int oldSize = node.NodeSize;

        if (oldSize > 0)
        {
            // Expanded nodes always have their rightmost child, and only that child, expanded
            var last = node.LastChild;
            var shrunk = last.ShrinkRightSpine(owner, RRBMath.Down(shift));

            if (!ReferenceEquals(last, shrunk)) node.Children[oldSize - 1] = shrunk;
        }

        node.Children[oldSize] = newChild.Expand(owner);
        node.SizeTable[oldSize] = (oldSize > 0 ? node.SizeTable[oldSize - 1] : 0) + newChildSize;

        node.SetNodeSize(oldSize + 1);

        return node.ToFullNodeIfNeeded(shift);
    }

    public override RRBNode<T> InsertChild(OwnerToken owner, int shift, int localIdx, RRBNode<T> newChild)
        => InsertChildS(owner, shift, localIdx, newChild, newChild.TreeSize(RRBMath.Down(shift)));

    public override RRBNode<T> InsertChildS(OwnerToken owner, int shift, int localIdx, RRBNode<T> newChild, int newChildSize)
    {
        if (localIdx == NodeSize) return AppendChildS(owner, shift, newChild, newChildSize);

        var node = (RRBExpandedRelaxedNode<T>)GetEditableNode(owner);
        int oldSize = node.NodeSize;

        // Move current children out of the way of incoming
        for (int i = oldSize - 1; i >= localIdx; i--)
        {
            node.Children[i + 1] = node.Children[i];
            // Adjust size table at the same time
            node.SizeTable[i + 1] = node.SizeTable[i] + newChildSize;
        }

        node.Children[localIdx] = newChild;
        node.SizeTable[localIdx] = newChildSize + (localIdx <= 0 ? 0 : node.SizeTable[localIdx - 1]);

        node.SetNodeSize(oldSize + 1);

        return node.ToFullNodeIfNeeded(shift);
    }

    public override RRBNode<T> RemoveChild(OwnerToken owner, int shift, int localIdx)
    {
        if (localIdx == NodeSize - 1) return RemoveLastChild(owner, shift);

        var node = (RRBExpandedRelaxedNode<T>)GetEditableNode(owner);
        int newSize = node.NodeSize - 1;

        int oldChildSize = node.SizeTable[localIdx] - (localIdx <= 0 ? 0 : node.SizeTable[localIdx - 1]);

        // Adjust current children to overwrite removed one
        for (int i = localIdx; i < newSize; i++)
        {
            node.Children[i] = node.Children[i + 1];
            // Adjust size table at the same time
            node.SizeTable[i] = node.SizeTable[i + 1] - oldChildSize;
        }

        // Previously-last position gets cleared
        node.Children[newSize] = null!;
        node.SizeTable[newSize] = 0;

        node.SetNodeSize(newSize);

#if DEBuG
        return newSize > 0
            ? node.ToFullNodeIfNeeded(shift)
            : throw new InvalidOperationException("RemoveChild of RRBExpandedRelaxedNode removed only child of singleton node; write test for this scenario");
#else
        return node.ToFullNodeIfNeeded(shift);
#endif
    }

    public override RRBNode<T> RemoveLastChild(OwnerToken owner, int shift)
    {
        // TODO: First make sure that "owner" isn't null at this point, because that's causing a failure in one of my tests
        // [push 1063; rev(); pop 118] --> after the rev(), we're a transient node that has become persistent so our owner is now null -- but we failed to check that.
        // ... I think that's been fixed, but let's make sure we test this scenario.
        var node = (RRBExpandedRelaxedNode<T>)GetEditableNode(owner);

        int newSize = node.NodeSize - 1;

        node.Children[newSize] = null!;
        node.SizeTable[newSize] = 0;
        node.SetNodeSize(newSize);

        if (newSize > 0)
        {
            var last = node.LastChild;
            var expanded = last.Expand(owner);

            if (!ReferenceEquals(last, expanded)) node.Children[newSize - 1] = expanded;

            return node.ToFullNodeIfNeeded(shift);
        }

#if DEBUG
        throw new System.InvalidOperationException("RemoveLastChild of RRBExpandedRelaxedNode removed only child of singleton node; write test for this scenario");
#else
        return node.ToFullNodeIfNeeded(shift);
#endif
    }

    // No need to override UpdateChildSRel and UpdateChildSAbs; versions from compact RRBRelaxedNode will work just fine in expanded nodes

    public override RRBNode<T> KeepNLeft(OwnerToken owner, int shift, int n)
    {
        if (n == NodeSize) return this;

        var node = (RRBExpandedRelaxedNode<T>)GetEditableNode(owner);

        for (int i = n; i < node.NodeSize; i++)
        {
            node.Children[i] = null!;
            node.SizeTable[i] = 0;
        }

        node.SetNodeSize(n);

        if (n > 0)
        {
            var last = node.LastChild;
            var expanded = last.Expand(owner);

            if (!ReferenceEquals(last, expanded))
                node.Children[n - 1] = expanded;

            return node.ToFullNodeIfNeeded(shift);
        }

#if DEBUG
        throw new System.InvalidOperationException("KeepNLeft of RRBExpandedRelaxedNode ended up with empty node; write test for this scenario");
#else
        return node.ToFullNodeIfNeeded(shift);
#endif
    }

    public override RRBNode<T> KeepNRight(OwnerToken owner, int shift, int n)
    {
        int skip = NodeSize - n;

        if (skip == 0) return this;

        var node = (RRBExpandedRelaxedNode<T>)GetEditableNode(owner);

        int lastSizeEntry = node.SizeTable[skip - 1];

        for (int i = 0; i < n; i++)
        {
            node.Children[i] = node.Children[i + skip];
            node.SizeTable[i] = node.SizeTable[i + skip] - lastSizeEntry;
        }

        for (int i = n; i < node.NodeSize; i++)
        {
            node.Children[i] = null!;
            node.SizeTable[i] = 0;
        }

        node.SetNodeSize(n);

#if DEBUG
        if (n > 0) return node.ToFullNodeIfNeeded(shift);
        throw new System.InvalidOperationException("KeepNRight of RRBExpandedRelaxedNode ended up with empty node; write test for this scenario");
#else
        return node.ToFullNodeIfNeeded(shift);
#endif
    }

    public override (RRBNode<T>, RRBNode<T>[]) SplitAndKeepNLeft(OwnerToken owner, int shift, int n)
    {
        var r = Children.Skip(n).Truncate(NodeSize - n);

        if (r.Length > 0)
        {
            var last = r[^1];
            var shrunk = last.Shrink(owner);

            if (!ReferenceEquals(last, shrunk)) r[^1] = shrunk;
        }

        var left = KeepNLeft(owner, shift, n);
        return (left, r);
    }

    public override (RRBNode<T>, (RRBNode<T>[], int[])) SplitAndKeepNLeftS(OwnerToken owner, int shift, int n)
    {
        int size = NodeSize;
        int lastLeft = SizeTable[n - 1];

        var rS = SizeTable.Skip(n).Truncate(size - n);

        for (int i = 0; i < rS.Length; i++) rS[i] -= lastLeft;

        var (node, r) = SplitAndKeepNLeft(owner, shift, n);
        return (node, (r, rS));
    }

    public override (RRBNode<T>[], RRBNode<T>) SplitAndKeepNRight(OwnerToken owner, int shift, int n)
    {
        int skip = NodeSize - n;

        var l = Children.Truncate(skip);
        var node = KeepNRight(owner, shift, n);

        return (l, node);
    }

    public override ((RRBNode<T>[], int[]), RRBNode<T>) SplitAndKeepNRightS(OwnerToken owner, int shift, int n)
    {
        int skip = NodeSize - n;

        var l = Children.Truncate(skip);
        var lS = SizeTable.Truncate(skip);

        var node = KeepNRight(owner, shift, n);

        return ((l, lS), node);
    }

    public override RRBNode<T> AppendNChildren(OwnerToken owner, int shift, int n, IEnumerable<RRBNode<T>> newChildren, bool shouldExpandRightChildIfNeeded)
    {
        var node = (RRBExpandedRelaxedNode<T>)GetEditableNode(owner);
        int size = node.NodeSize;

        // Expanded nodes always have their rightmost child, and only that child, expanded
        var last = node.LastChild;
        var shrunk = last.ShrinkRightSpine(owner, RRBMath.Down(shift));
        if (!ReferenceEquals(last, shrunk)) node.Children[size - 1] = shrunk;

        int newSize = size + n;
        int prev = node.SizeTable[size - 1];

        // int i = size;
        // foreach (var c in newChildren)
        // {
        //     node.Children[i] = c;
        //     prev += c.TreeSize(RRBMath.Down(shift));
        //     node.SizeTable[i] = prev;
        //     i++;
        // }
        // TODO: Test that implementation above compared to below. Below is slightly more robust against incorrect values of n, but do we pay a price penalty because the JIT can optimize the above better?
        using var eC = newChildren.GetEnumerator();
        for (int i = size; i < newSize; i++)
        {
            if (eC.MoveNext())
            {
                var child = eC.Current;
                node.Children[i] = child;
                prev += child.TreeSize(RRBMath.Down(shift));
                node.SizeTable[i] = prev;
            }
        }

        node.SetNodeSize(newSize);

        if (shouldExpandRightChildIfNeeded)
        {
            var lastChild = node.LastChild;
            var expanded = lastChild.Expand(owner);

            if (!ReferenceEquals(lastChild, expanded)) node.Children[newSize - 1] = expanded;
        }

        return node;
    }

    public override RRBNode<T> AppendNChildrenS(OwnerToken owner, int shift, int n, IEnumerable<RRBNode<T>> newChildren, IEnumerable<int> sizes, bool shouldExpandRightChildIfNeeded)
    {
        // Note: "sizes" should be a sequence of size table entries, i.e. cumulative: instead of [3,4,3,2,4] it should be [3,7,10,12,16]
        var node = (RRBExpandedRelaxedNode<T>)GetEditableNode(owner);

        int size = node.NodeSize;

        // Expanded nodes always have their rightmost child, and only that child, expanded
        var last = node.LastChild;
        var shrunk = last.ShrinkRightSpine(owner, RRBMath.Down(shift));
        if (!ReferenceEquals(last, shrunk)) node.Children[size - 1] = shrunk;

        int newSize = size + n;
        int lastSizeTableEntry = node.SizeTable[size - 1];

        using var ec = newChildren.GetEnumerator();
        using var es = sizes.GetEnumerator();

        for (int i = size; i < newSize; i++)
        {
            if (ec.MoveNext() && es.MoveNext())
            {
                node.Children[i] = ec.Current;
                node.SizeTable[i] = lastSizeTableEntry + es.Current;
            }
        }

        node.SetNodeSize(newSize);

        if (shouldExpandRightChildIfNeeded)
        {
            var lastChild = node.LastChild;
            var expanded = lastChild.Expand(owner);

            if (!ReferenceEquals(lastChild, expanded)) node.Children[newSize - 1] = expanded;
        }

        return node;
    }

    public override RRBNode<T> PrependNChildren(OwnerToken owner, int shift, int n, IEnumerable<RRBNode<T>> newChildren)
    {
        var node = (RRBExpandedRelaxedNode<T>)GetEditableNode(owner);

        int size = node.NodeSize;
        int newSize = size + n;

        for (int i = size - 1; i >= 0; i--)
        {
            node.Children[i + n] = node.Children[i];
            node.SizeTable[i + n] = node.SizeTable[i];
        }

        int prev = 0;

        // int i2 = 0;
        // foreach (var c in newChildren)
        // {
        //     node.Children[i2] = c;
        //     prev += c.TreeSize(RRBMath.Down(shift));
        //     node.SizeTable[i2] = prev;
        //     i2++;
        // }
        // TODO: Test that implementation above compared to below. Below is slightly more robust against incorrect values of n, but do we pay a price penalty because the JIT can optimize the above better?
        using var eC = newChildren.GetEnumerator();
        for (int i = 0; i < n; i++)
        {
            if (eC.MoveNext())
            {
                var child = eC.Current;
                node.Children[i] = child;
                prev += child.TreeSize(RRBMath.Down(shift));
                node.SizeTable[i] = prev;
            }
        }

        // Now adjust remaining size table entries
        for (int i = n; i < newSize; i++) node.SizeTable[i] += prev;

        node.SetNodeSize(newSize);
        return node;
    }

    public override RRBNode<T> PrependNChildrenS(OwnerToken owner, int shift, int n, IEnumerable<RRBNode<T>> newChildren, IEnumerable<int> sizes)
    {
        // Note: "sizes" should be a sequence of size table entries, i.e. cumulative: instead of [3,4,3,2,4] it should be [3,7,10,12,16]
        var node = (RRBExpandedRelaxedNode<T>)GetEditableNode(owner);

        int size = node.NodeSize;
        int newSize = size + n;

        for (int i = size - 1; i >= 0; i--)
        {
            node.Children[i + n] = node.Children[i];
            node.SizeTable[i + n] = node.SizeTable[i];
        }

        using var ec = newChildren.GetEnumerator();
        using var es = sizes.GetEnumerator();
        for (int i = 0; i < n; i++)
        {
            if (ec.MoveNext() && es.MoveNext())
            {
                node.Children[i] = ec.Current;
                node.SizeTable[i] = es.Current;
            }
        }

        int last = node.SizeTable[n - 1];

        // Now adjust remaining size table entries
        for (int i = n; i < newSize; i++) node.SizeTable[i] += last;

        node.SetNodeSize(newSize);
        return node;
    }

    public override RRBNode<T> MaybeExpand(OwnerToken owner, int shift)
    {
        if (NodeSize == 0) return this;

        var node = (RRBExpandedRelaxedNode<T>)GetEditableNode(owner);
        var last = node.LastChild;

        var expanded =
            shift <= Literals.shiftSize
                // We're at twig level, so children are leaves and should not be expanded
                ? last.Expand(owner)
                // Above twig level, so child may need to be expanded, possibly recursively
                : ((RRBFullNode<T>)last.Expand(owner)).MaybeExpand(owner, RRBMath.Down(shift));

        if (!ReferenceEquals(last, expanded)) node.Children[node.NodeSize - 1] = expanded;

        return node;
    }

    public override RRBNode<T>[] MkArrayForRebalance(OwnerToken owner, int shift, int length)
        => IsEditableBy(owner) ? Children : new RRBNode<T>[length];

    public override RRBNode<T> MkNodeForRebalance(OwnerToken owner, int shift, RRBNode<T>[] arr, int len)
    {
        if (ReferenceEquals(arr, Children) && IsEditableBy(owner))
        {
            // Zero out any extra child slots, if any (i.e. if new length is shorter than before)
            for (int i = len; i < NodeSize; i++)
            {
                arr[i] = null!;
                SizeTable[i] = 0;
            }

            SetNodeSize(len);
            RRBNode<T>.PopulateSizeTableS(shift, Children, len, SizeTable);
            return ((RRBFullNode<T>)ToFullNodeIfNeeded(shift)).MaybeExpand(owner, shift);
        }

        var node = RRBNode<T>.MkNode(owner, shift, arr).Expand(owner);
        return ((RRBFullNode<T>)node).MaybeExpand(owner, shift);
    }

    public override RRBNode<T> MakeLeftNodeForSplit(OwnerToken owner, int shift, RRBNode<T>[] children, int[] sizes)
        => RRBNode<T>.MkNodeKnownSize(owner, shift, children, sizes).Expand(owner);

    public override RRBNode<T> NewParent(OwnerToken owner, int shift, RRBNode<T>[] siblings)
    {
        var arr = new RRBNode<T>[Literals.blockSize];
        int size = siblings.Length;

        for (int i = 0; i < size - 1; i++)
        {
            arr[i] = ((RRBFullNode<T>)siblings[i])
                .ShrinkRightSpine(owner, shift);
        }

        arr[size - 1] = siblings[size - 1].Expand(owner);

        // Single-child nodes don't need to be relaxed, they count as full
        if (size <= 1) return new RRBExpandedFullNode<T>(owner, arr, size);

        int shiftUp = RRBMath.Up(shift);
        var sizeTable = RRBNode<T>.CreateSizeTableS(shiftUp, arr, size);

        return new RRBExpandedRelaxedNode<T>(owner, arr, sizeTable, size)
            .ToFullNodeIfNeeded(shiftUp);
    }
}
