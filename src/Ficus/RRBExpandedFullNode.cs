using System.Collections.Generic;
using System.Linq;
using Ficus.RRBArrayExtensions;

namespace Ficus;

public sealed class RRBExpandedFullNode<T> : RRBFullNode<T>
{
    public int CurrentLength { get; private set; }

    public RRBExpandedFullNode(
        OwnerToken ownerToken,
        RRBNode<T>[] children,
        int? realSize = null)
        : base(ownerToken, children.ExpandToBlockSize(Literals.blockSize))
    {
        CurrentLength = realSize ?? children.Length;
    }

    public override int NodeSize => CurrentLength;

#if DEBUG
    public override string StringRepr => $"ExpandedFullNode(length={NodeSize})";
#endif

    public override int SlotCount =>
        Children.Take(NodeSize).Sum(child => child.NodeSize);

    public override RRBNode<T>[] SafeChildrenArr =>
        Children.Truncate(NodeSize);

    public override RRBNode<T> Shrink(OwnerToken owner)
    {
        int size = NodeSize;

        if (IsEditableBy(owner) && size == Literals.blockSize)
        {
            // Handy, no need to copy any arrays
            return new RRBFullNode<T>(owner, Children);
        }

        var children =
            size == Literals.blockSize
                ? (RRBNode<T>[])Children.Clone()
                : Children.Truncate(size);

        return new RRBFullNode<T>(owner, children);
    }

    public override RRBNode<T> Expand(OwnerToken owner) => GetEditableNode(owner);

    public override RRBNode<T> GetEditableNode(OwnerToken owner)
    {
        if (IsEditableBy(owner)) return this;

        return new RRBExpandedFullNode<T>(owner, (RRBNode<T>[])Children.Clone(), NodeSize);
    }

    public override void SetNodeSize(int newSize)
    {
        CurrentLength = newSize;
    }

    public override RRBNode<T> ToRelaxedNodeIfNeeded(int shift)
    {
        if (shift <= 0) return this; // Leaf nodes don't have a concept of full/relaxed

        int size = NodeSize;
        var sizeTable = RRBNode<T>.CreateSizeTableS(shift, Children, size);

        if (RRBMath.IsSizeTableFullAtShift(shift, sizeTable, size)) return this;

        return new RRBExpandedRelaxedNode<T>(Owner, Children, sizeTable, NodeSize);
    }

    // ===== NODE MANIPULATION =====
    // These low-level functions only create new nodes (or modify an expanded node), without doing any bounds checking.

    public override RRBNode<T> AppendChild(OwnerToken owner, int shift, RRBNode<T> newChild)
    {
        bool trulyFull = FullNodeIsTrulyFull(shift);
        var node = (RRBExpandedFullNode<T>)GetEditableNode(owner);

        int oldSize = node.NodeSize;

        // Expanded nodes always have their rightmost child, and only that child, expanded
        if (oldSize > 0)
        {
            var last = node.LastChild;
            var shrunk = last.ShrinkRightSpine(owner, RRBMath.Down(shift));

            if (!ReferenceEquals(last, shrunk)) node.Children[oldSize - 1] = shrunk;
        }

        node.Children[oldSize] = newChild.Expand(owner);
        node.SetNodeSize(oldSize + 1);

        // Full nodes are allowed to have their last item be non-full, so we have to check that
        if (trulyFull) return node;

        // Last item was non-full, so now there are TWO non-full items in last two slots and we must become a relaxed, expanded node
        var sizeTable = node.BuildSizeTable(shift, oldSize, oldSize - 1);
        int lastEntry = sizeTable[^1];

        var expanded = sizeTable.ExpandToBlockSize(Literals.blockSize);
        expanded[oldSize] = lastEntry + newChild.TreeSize(RRBMath.Down(shift));

        return new RRBExpandedRelaxedNode<T>(owner, node.Children, expanded, node.NodeSize);
        // TODO: Might be able to optimize this by first checking if we're truly full, and then if we're not, convert to a relaxed node FIRST before calling RelaxedNode.AppendChildS
    }

    public override RRBNode<T> AppendChildS(OwnerToken owner, int shift, RRBNode<T> newChild, int _unusedChildSize)
        => AppendChild(owner, shift, newChild);
    // TODO: Optimize this by making this the implementation and having AppendChild call this one... but only calculating child size on demand
    // Worth it? Not worth it?

    public override RRBNode<T> InsertChild(OwnerToken owner, int shift, int idx, RRBNode<T> child)
        => InsertChildS(owner, shift, idx, child, child.TreeSize(RRBMath.Down(shift)));

    public override RRBNode<T> InsertChildS(
        OwnerToken owner,
        int shift,
        int idx,
        RRBNode<T> newChild,
        int newChildSize)
    {
        if (idx == NodeSize) return AppendChildS(owner, shift, newChild, newChildSize);

        var node = (RRBExpandedFullNode<T>)GetEditableNode(owner);
        int oldSize = node.NodeSize;

        for (int i = oldSize - 1; i >= idx; i--) node.Children[i + 1] = node.Children[i];

        node.Children[idx] = newChild;
        node.SetNodeSize(oldSize + 1);

        // Skip calculating a relaxed size table if we know we don't need it
        if (newChildSize == (1 << shift)) return node;
        // If we get here, though, we need to create a size table
        return node.ToRelaxedNodeIfNeeded(shift);
    }

    public override RRBNode<T> RemoveChild(OwnerToken owner, int shift, int idx)
    {
        if (idx == NodeSize - 1) return RemoveLastChild(owner, shift);

        var node = (RRBExpandedFullNode<T>)GetEditableNode(owner);
        int newSize = node.NodeSize - 1;

        for (int i = idx; i < newSize; i++) node.Children[i] = node.Children[i + 1];

        node.Children[newSize] = null!;
        node.SetNodeSize(newSize);

        // Removing a child from a full node can never make it non-full
        return node;
    }

    public override RRBNode<T> RemoveLastChild(OwnerToken owner, int shift)
    {
        // TODO: First make sure that "owner" isn't null at this point, because that's causing a failure in one of my tests
        // [push 1063; rev(); pop 118] --> after the rev(), we're a transient node that has become persistent so our owner is now null -- but we failed to check that.
        // ... I think that's been fixed, but let's make sure we test this scenario.
        var node = (RRBExpandedFullNode<T>)GetEditableNode(owner);
        int newSize = node.NodeSize - 1;

        node.Children[newSize] = null!;
        node.SetNodeSize(newSize);

        if (newSize > 0)
        {
            var last = node.LastChild;
            var expanded = last.Expand(owner);

            if (!ReferenceEquals(last, expanded)) node.Children[newSize - 1] = expanded;
        }

        // Removing the last child from a full node can never make it non-full
        return node;
    }

    public override RRBNode<T> UpdateChildSRel(
        OwnerToken owner,
        int shift,
        int idx,
        RRBNode<T> newChild,
        int sizeDiff)
    {
        if (idx == NodeSize - 1 || sizeDiff == 0)
        {
            // No need to turn into a relaxed node
            return UpdateChild(owner, shift, idx, newChild);
        }

        int size = NodeSize;
        var sizeTable = BuildSizeTable(shift, size, size - 1);

        var relaxed = new RRBExpandedRelaxedNode<T>(
            Owner,
            Children,
            sizeTable,
            NodeSize);

        return relaxed.UpdateChildSRel(owner, shift, idx, newChild, sizeDiff);
    }

    public override RRBNode<T> UpdateChildSAbs(
        OwnerToken owner,
        int shift,
        int idx,
        RRBNode<T> newChild,
        int childSize)
    {
        if (idx == NodeSize - 1 || childSize == (1 << shift))
        {
            // No need to turn into a relaxed node
            return UpdateChild(owner, shift, idx, newChild);
        }

        int size = NodeSize;
        var sizeTable = BuildSizeTable(shift, size, size - 1);

        var relaxed = new RRBExpandedRelaxedNode<T>(
            Owner,
            Children,
            sizeTable,
            NodeSize);

        return relaxed.UpdateChildSAbs(owner, shift, idx, newChild, childSize);
    }

    public override RRBNode<T> KeepNLeft(OwnerToken owner, int shift, int n)
    {
        if (n == NodeSize) return this;

        var node = (RRBExpandedFullNode<T>)GetEditableNode(owner);

        // Zero out all child slots after the keep point
        for (int i = n; i < node.NodeSize; i++) node.Children[i] = null!;
        node.SetNodeSize(n);

        if (n > 0)
        {
            var last = node.LastChild;
            var expanded = last.Expand(owner);

            if (!ReferenceEquals(last, expanded))
                node.Children[n - 1] = expanded;
        }

        return node;
    }

    public override RRBNode<T> KeepNRight(OwnerToken owner, int shift, int n)
    {
        int skip = NodeSize - n;
        if (skip == 0) return this;

        var node = (RRBExpandedFullNode<T>)GetEditableNode(owner);

        // Move all child nodes down to front
        for (int i = 0; i < n; i++) node.Children[i] = node.Children[i + skip];

        // Zero out rest of child slots
        for (int i = n; i < node.NodeSize; i++) node.Children[i] = null!;

        node.SetNodeSize(n);
        return node;
    }

    public override (RRBNode<T> node, RRBNode<T>[] rightChildren) SplitAndKeepNLeft(OwnerToken owner, int shift, int n)
    {
        var r = Children.Skip(n).Truncate(NodeSize - n);
        // TODO: Write a Skip-and-Truncate extension method to make this not create an intermediate array
        int rLen = r.Length;

        if (rLen > 0)
        {
            var lastChild = r[rLen - 1];
            var shrunkLastChild = lastChild.ShrinkRightSpine(owner, RRBMath.Down(shift));

            if (!ReferenceEquals(lastChild, shrunkLastChild))
            {
                r[rLen - 1] = shrunkLastChild;
            }
        }

        var node = this.KeepNLeft(owner, shift, n);
        return (node, r);
    }

    public override (RRBNode<T> node, (RRBNode<T>[] rightChildren, int[] rightSizeTable)) SplitAndKeepNLeftS(OwnerToken owner, int shift, int n)
    {
        int size = NodeSize;
        var sizeTable = BuildSizeTable(shift, size, size - 1);
        var (lS, rS) = sizeTable.SplitAt(n);
        int lastSizeL = lS.Last();

        for (int i = 0; i < rS.Length; i++)
        {
            rS[i] -= lastSizeL;
        }

        var (node, r) = SplitAndKeepNLeft(owner, shift, n);
        return (node, (r, rS));
    }

    public override (RRBNode<T>[] leftChildren, RRBNode<T> node) SplitAndKeepNRight(OwnerToken owner, int shift, int n)
    {
        var l = Children.Truncate(NodeSize - n);
        var node = KeepNRight(owner, shift, n);
        return (l, node);
    }

    public override ((RRBNode<T>[] leftChildren, int[] leftSizeTable), RRBNode<T> node) SplitAndKeepNRightS(OwnerToken owner, int shift, int n)
    {
        int size = NodeSize;
        int skip = size - n;
        var l = Children.Truncate(skip);
        var sizeTable = BuildSizeTable(shift, size, size - 1);
        var lS = sizeTable.Truncate(skip);
        var node = KeepNRight(owner, shift, n);
        return ((l, lS), node);
    }

    public override RRBNode<T> AppendNChildren(OwnerToken owner, int shift, int n, IEnumerable<RRBNode<T>> newChildren, bool shouldExpandRightChildIfNeeded)
    {
        var node = (RRBExpandedFullNode<T>)this.GetEditableNode(owner);
        int size = node.NodeSize;
        // Expanded nodes always have their rightmost child, and only that child, expanded
        var lastChild = node.LastChild;
        var shrunkLastChild = lastChild.ShrinkRightSpine(owner, RRBMath.Down(shift));

        if (!ReferenceEquals(lastChild, shrunkLastChild))
        {
            node.Children[size - 1] = shrunkLastChild;
        }

        int newSize = size + n;
        // TODO: Double-check this math, make sure we're using the right value here (should it be RRBMath.Down(shift) since we're looking for the full *child* size?)
        int fullChildSize = (1 << shift);
        bool stillFull = node.LastChild.TreeSize(RRBMath.Down(shift)) >= fullChildSize;

        using (var eC = newChildren.GetEnumerator())
        {
            for (int i = size; i < newSize; i++)
            {
                if (eC.MoveNext())
                {
                    var child = eC.Current;
                    node.Children[i] = child;

                    // Last child can be non-full, but we need to check all the others
                    if (stillFull && i < newSize - 1)
                    {
                        int childSize = child.TreeSize(RRBMath.Down(shift));
                        stillFull = childSize >= fullChildSize;
                    }
                }
            }
        }

        node.SetNodeSize(newSize);

        if (shouldExpandRightChildIfNeeded)
        {
            var newLastChild = node.LastChild;
            var expandedNewLastChild = newLastChild.Expand(owner);

            if (!ReferenceEquals(newLastChild, expandedNewLastChild))
            {
                node.Children[newSize - 1] = expandedNewLastChild;
            }
        }

        // If we know we're already full, skip doing another tree-size calculation
        return stillFull ? node : node.ToRelaxedNodeIfNeeded(shift);
    }

    public override RRBNode<T> AppendNChildrenS(OwnerToken owner, int shift, int n, IEnumerable<RRBNode<T>> newChildren, IEnumerable<int> sizes, bool shouldExpandRightChildIfNeeded)
    {
        // Note: "sizes" should be a sequence of size table entries, i.e. cumulative: instead of [3;4;3;2;4] it should be [3;7;10;12;16]
        var node = (RRBExpandedFullNode<T>)this.GetEditableNode(owner);
        int size = node.NodeSize;
        // Expanded nodes always have their rightmost child, and only that child, expanded
        var lastChild = node.LastChild;
        var shrunkLastChild = lastChild.ShrinkRightSpine(owner, RRBMath.Down(shift));

        if (!ReferenceEquals(lastChild, shrunkLastChild))
        {
            node.Children[size - 1] = shrunkLastChild;
        }

        int newSize = size + n;
        var sizeTable = this.BuildSizeTable(shift, size, size - 1);
        var sizeTableCopy = new int[Literals.blockSize];
        sizeTable.CopyTo(sizeTableCopy, 0);
        int lastSizeTableEntry = sizeTableCopy[size - 1];
        // TODO: Double-check this math too, because we shouldn't need different logic here than in AppendNChildren ... not yet anyway
        bool stillFull = node.FullNodeIsTrulyFull(shift);

        using (var eC = newChildren.GetEnumerator())
        using (var eS = sizes.GetEnumerator())
        {
            for (int i = size; i < newSize; i++)
            {
                if (eC.MoveNext() && eS.MoveNext())
                {
                    node.Children[i] = eC.Current;
                    sizeTableCopy[i] = lastSizeTableEntry + eS.Current;

                    int childSize = sizeTableCopy[i] - sizeTableCopy[i - 1];

                    // Last child can be non-full, but we need to check all the others
                    if (stillFull && i < newSize - 1)
                    {
                        stillFull = childSize >= (1 << shift);
                    }
                }
            }
        }

        node.SetNodeSize(newSize);

        if (shouldExpandRightChildIfNeeded)
        {
            var newLastChild = node.LastChild;
            var expandedNewLastChild = newLastChild.Expand(owner);

            if (!ReferenceEquals(newLastChild, expandedNewLastChild))
            {
                node.Children[newSize - 1] = expandedNewLastChild;
            }
        }

        return stillFull ? node : new RRBExpandedRelaxedNode<T>(owner, node.Children, sizeTableCopy, newSize);
    }

    public override RRBNode<T> PrependNChildren(OwnerToken owner, int shift, int n, IEnumerable<RRBNode<T>> newChildren)
    {
        var node = (RRBExpandedFullNode<T>)this.GetEditableNode(owner);
        int size = node.NodeSize;
        int newSize = size + n;
        var sizeTable = node.BuildSizeTable(shift, size, size - 1);
        var sizeTableCopy = new int[Literals.blockSize];
        sizeTable.CopyTo(sizeTableCopy, n);

        // Move current children out of the way of the n incoming ones
        for (int i = size - 1; i >= 0; i--)
        {
            node.Children[i + n] = node.Children[i];
        }

        int prevSizeTableEntry = 0;
        bool stillFull = true;
        int fullChildSize = (1 << shift);

        using (var eC = newChildren.GetEnumerator())
        {
            for (int i = 0; i < n; i++)
            {
                if (eC.MoveNext())
                {
                    node.Children[i] = eC.Current;
                    int childSize = eC.Current.TreeSize(RRBMath.Down(shift));

                    if (childSize < fullChildSize)
                    {
                        stillFull = false;
                    }

                    prevSizeTableEntry += childSize;
                    sizeTableCopy[i] = prevSizeTableEntry;
                }
            }
        }

        for (int i = n; i < newSize; i++)
        {
            sizeTableCopy[i] += prevSizeTableEntry;
        }

        node.SetNodeSize(newSize);

        return stillFull ? node : new RRBExpandedRelaxedNode<T>(owner, node.Children, sizeTableCopy, newSize);
    }

    public override RRBNode<T> PrependNChildrenS(OwnerToken owner, int shift, int n, IEnumerable<RRBNode<T>> newChildren, IEnumerable<int> sizes)
    {
        var node = (RRBExpandedFullNode<T>)this.GetEditableNode(owner);
        int size = node.NodeSize;
        int newSize = size + n;
        var sizeTable = node.BuildSizeTable(shift, size, size - 1);
        var sizeTableCopy = new int[Literals.blockSize];
        sizeTable.CopyTo(sizeTableCopy, n);

        for (int i = size - 1; i >= 0; i--)
        {
            node.Children[i + n] = node.Children[i];
        }

        using (var eC = newChildren.GetEnumerator())
        using (var eS = sizes.GetEnumerator())
        {
            for (int i = 0; i < n; i++)
            {
                if (eC.MoveNext() && eS.MoveNext())
                {
                    node.Children[i] = eC.Current;
                    sizeTableCopy[i] = eS.Current;
                }
            }
        }

        int lastSizeTableEntry = sizeTableCopy[n - 1];
        bool stillFull = lastSizeTableEntry >= (n << shift);

        for (int i = n; i < newSize; i++)
        {
            sizeTableCopy[i] += lastSizeTableEntry;
        }

        node.SetNodeSize(newSize);

        return stillFull ? node : new RRBExpandedRelaxedNode<T>(owner, node.Children, sizeTableCopy, newSize);
    }

    public override RRBNode<T> MaybeExpand(OwnerToken owner, int shift)
    {
        if (this.NodeSize == 0) return this;

        var node = (RRBExpandedFullNode<T>)this.GetEditableNode(owner);
        var lastChild = this.LastChild;
        var expandedLastChild = shift <= Literals.shiftSize
            // We're at twig level, so child is a leaf and should not have its children further expanded
            ? lastChild.Expand(owner)
            // We're above twig level, so last child may need expanding itself
            : ((RRBFullNode<T>)lastChild.Expand(owner)).MaybeExpand(owner, RRBMath.Down(shift));
        // TODO: Can we just call MaybeExpand on the child and trust in inheritance to handle this?

        if (!ReferenceEquals(lastChild, expandedLastChild))
        {
            node.Children[node.NodeSize - 1] = expandedLastChild;
        }

        return node;
    }

    public override RRBNode<T>[] MkArrayForRebalance(OwnerToken owner, int shift, int length)
    {
        if (IsEditableBy(owner)) return Children;

        return new RRBNode<T>[length];
    }

    public override RRBNode<T> MkNodeForRebalance(OwnerToken owner, int shift, RRBNode<T>[] arr, int len)
    {
        if (ReferenceEquals(arr, Children) && IsEditableBy(owner))
        {
            SetNodeSize(len);
            return ((RRBFullNode<T>)ToRelaxedNodeIfNeeded(shift))
                .MaybeExpand(owner, shift);
        }

        var node = RRBNode<T>.MkNode(owner, shift, arr).Expand(owner);
        return ((RRBFullNode<T>)node).MaybeExpand(owner, shift);
    }

    public override RRBNode<T> MakeLeftNodeForSplit(OwnerToken owner, int shift, RRBNode<T>[] children, int[] sizes)
    {
        return RRBNode<T>
            .MkNodeKnownSize(owner, shift, children, sizes)
            .Expand(owner);
    }

    public override RRBNode<T> NewParent(
        OwnerToken owner,
        int shift,
        RRBNode<T>[] children)
    {
        var arr = new RRBNode<T>[Literals.blockSize];
        int size = children.Length;

        // All but rightmost child should be shrunk
        // They may have come from the left side of a merge, so we have to check all of them since we can't know which one(s) were expanded before
        for (int i = 0; i < size - 1; i++)
        {
            arr[i] = ((RRBFullNode<T>)children[i]).ShrinkRightSpine(owner, shift);
        }

        // Last child should be expanded since we are making a new expanded node
        arr[size - 1] = children[size - 1].Expand(owner);

        // Single-child nodes don't need a size table
        if (size <= 1) return new RRBExpandedFullNode<T>(owner, arr, size);

        int shiftUp = RRBMath.Up(shift);
        var sizeTableForParent = RRBNode<T>.CreateSizeTableS(shiftUp, arr, size);

        // Create the relaxed node, then let it decide whether to convert itself to a full node
        return new RRBExpandedRelaxedNode<T>(owner, arr, sizeTableForParent, size).ToFullNodeIfNeeded(shiftUp);
    }
}
