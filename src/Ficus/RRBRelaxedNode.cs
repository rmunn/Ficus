using System.Collections.Generic;
using Ficus.RRBArrayExtensions;

namespace Ficus;

public class RRBRelaxedNode<T> : RRBFullNode<T>
{
    private readonly int[] sizeTable;

    public RRBRelaxedNode(OwnerToken ownerToken, RRBNode<T>[] children, int[] sizeTable)
        : base(ownerToken, children)
    {
        this.sizeTable = sizeTable;
    }

    public static RRBFullNode<T> Create(OwnerToken owner, int shift, RRBNode<T>[] children)
    {
        var sizeTbl = RRBNode<T>.CreateSizeTable(shift, children);
        return CreateWithSizeTable(owner, shift, children, sizeTbl);
    }

    public static RRBFullNode<T> CreateWithSizeTable(
        OwnerToken owner,
        int shift,
        RRBNode<T>[] children,
        int[] sizeTbl)
    {
        if (RRBMath.IsSizeTableFullAtShift(shift, sizeTbl, sizeTbl.Length))
            return new RRBFullNode<T>(owner, children);

        return new RRBRelaxedNode<T>(owner, children, sizeTbl);
    }

    public int[] SizeTable => sizeTable;

    public override int NodeSize => Children.Length;

    public override int TreeSize(int _) => sizeTable[NodeSize - 1];

    // In a relaxed twig node, the last entry in the size table is all we need to look up to find the slot count
    public override int TwigSlotCount => sizeTable[NodeSize - 1];

#if DEBUG
    public override string StringRepr => $"RelaxedNode(length={NodeSize})";
#endif

    public override RRBNode<T> GetEditableNode(OwnerToken owner)
    {
        if (IsEditableBy(owner)) return this;

        return new RRBRelaxedNode<T>(
            owner,
            (RRBNode<T>[])Children.Clone(),
            (int[])sizeTable.Clone()
        );
    }

    public override RRBNode<T> Shrink(OwnerToken owner) => GetEditableNode(owner);

    public override RRBNode<T> Expand(OwnerToken owner)
    {
        var node = (RRBRelaxedNode<T>)GetEditableNode(owner);
        return new RRBExpandedRelaxedNode<T>(owner, node.Children, node.SizeTable);
    }

    public virtual RRBNode<T> ToFullNodeIfNeeded(int shift)
    {
        if (RRBMath.IsSizeTableFullAtShift(shift, sizeTable, sizeTable.Length)) return new RRBFullNode<T>(Owner, Children);
        return this;
    }

    public override (int localIdx, RRBNode<T> child, int nextTreeIdx) IndexesAndChild(int shift, int treeIdx)
    {
        // Starting point is where a full node would have placed that child
        int localIdx = RRBMath.RadixIndex(shift, treeIdx);

        // Relaxed nodes might have to search to find it, but not very far (we aim to guarantee radixSearchErrorMax steps at most)
        while (sizeTable[localIdx] <= treeIdx) localIdx++;

        var child = Children[localIdx];

        int nextTreeIdx = localIdx == 0 ? treeIdx : treeIdx - sizeTable[localIdx - 1];

        return (localIdx, child, nextTreeIdx);
    }

    // ===== NODE MANIPULATION =====
    // These low-level functions only create new nodes (or modify an expanded node), without doing any bounds checking.

    public override RRBNode<T> AppendChild(OwnerToken owner, int shift, RRBNode<T> newChild)
        => AppendChildS(owner, shift, newChild, newChild.TreeSize(RRBMath.Down(shift)));

    public override RRBNode<T> AppendChildS(OwnerToken owner, int shift, RRBNode<T> newChild, int newChildSize)
    {
        var children = Children.CopyAndPush(newChild);
        int last = sizeTable.Length == 0 ? 0 : sizeTable[^1];
        var sizes = sizeTable.CopyAndPush(last + newChildSize);
#if DEBuG
        if (sizes.Length > Literals.blockSize) throw new InvalidOperationException("In AppendChildS, ended up with a too-large size table")
#endif
        return RRBNode<T>.MkNodeKnownSize(owner, shift, children, sizes);
    }

    public override RRBNode<T> InsertChild(OwnerToken owner, int shift, int localIdx, RRBNode<T> newChild)
        => InsertChildS(owner, shift, localIdx, newChild, newChild.TreeSize(RRBMath.Down(shift)));

    public override RRBNode<T> InsertChildS(OwnerToken owner, int shift, int localIdx, RRBNode<T> newChild, int newChildSize)
    {
        if (localIdx == NodeSize) return AppendChildS(owner, shift, newChild, newChildSize);

        var children = Children.CopyAndInsertAt(localIdx, newChild);

        // Now insert new size and update size table after insertion point
        int baseSize = localIdx <= 0 ? 0 : sizeTable[localIdx - 1];
        var sizes = sizeTable.CopyAndInsertAt(localIdx, baseSize + newChildSize);
        for (int i = localIdx + 1; i < sizes.Length; i++) sizes[i] += newChildSize;

        return MkNodeKnownSize(owner, shift, children, sizes);
    }

    public override RRBNode<T> RemoveChild(OwnerToken owner, int shift, int localIdx)
    {
        int left = localIdx <= 0 ? 0 : sizeTable[localIdx - 1];
        int oldSize = sizeTable[localIdx] - left;

        var children = Children.CopyAndRemoveAt(localIdx);
        var sizes = sizeTable.CopyAndRemoveAt(localIdx);

        // Now adjust size table to account for lost child
        for (int i = localIdx; i < sizes.Length; i++) sizes[i] -= oldSize;

        return MkNodeKnownSize(owner, shift, children, sizes);
    }

    public override RRBNode<T> RemoveLastChild(OwnerToken owner, int shift)
    {
        var children = Children.CopyAndPop();
        var sizes = sizeTable.CopyAndPop();

        return MkNodeKnownSize(owner, shift, children, sizes);
    }

    public override RRBNode<T> UpdateChildSRel(OwnerToken owner, int shift, int localIdx, RRBNode<T> newChild, int sizeDiff)
    {
        var node = (RRBRelaxedNode<T>)GetEditableNode(owner);
        node.Children[localIdx] = newChild;
        for (int i = localIdx; i < node.NodeSize; i++) node.sizeTable[i] = sizeTable[i] + sizeDiff;

        return node.ToFullNodeIfNeeded(shift);
    }

    public override RRBNode<T> UpdateChildSAbs(OwnerToken owner, int shift, int localIdx, RRBNode<T> newChild, int childSize)
    {
        int left = localIdx <= 0 ? 0 : sizeTable[localIdx - 1];
        int oldSize = sizeTable[localIdx] - left;

        return UpdateChildSRel(owner, shift, localIdx, newChild, childSize - oldSize);
    }

    public override RRBNode<T> KeepNLeft(OwnerToken owner, int shift, int n)
    {
        if (n == NodeSize) return this;

        var kept = Children.Truncate(n);
        var sizes = sizeTable.Truncate(n);

        return MkNodeKnownSize(owner, shift, kept, sizes);
    }

    public override RRBNode<T> KeepNRight(OwnerToken owner, int shift, int n)
    {
        int skip = NodeSize - n;

        if (skip == 0) return this;

        // TODO: How to ensure that this takes the .Skip from RRBArrayExtensions rather than IEnumerable?
        var kept = Children.Skip(skip);
        var sizes = sizeTable.Skip(skip);

        int offset = sizeTable[skip - 1];

        for (int i = 0; i < sizes.Length; i++) sizes[i] -= offset;

        return MkNodeKnownSize(owner, shift, kept, sizes);
    }

    public override (RRBNode<T>, RRBNode<T>[]) SplitAndKeepNLeft(OwnerToken owner, int shift, int n)
    {
        var (l, r) = Children.SplitAt(n);
        var lS = sizeTable.Truncate(n);

        var node = MkNodeKnownSize(owner, shift, l, lS);
        return (node, r);
    }

    public override (RRBNode<T>, (RRBNode<T>[], int[])) SplitAndKeepNLeftS(OwnerToken owner, int shift, int n)
    {
        var (l, r) = Children.SplitAt(n);
        var (lS, rS) = sizeTable.SplitAt(n);

        int last = lS[^1];

        for (int i = 0; i < rS.Length; i++) rS[i] -= last;

        var node = MkNodeKnownSize(owner, shift, l, lS);
        return (node, (r, rS));
    }

    public override (RRBNode<T>[] leftChildren, RRBNode<T> node) SplitAndKeepNRight(OwnerToken owner, int shift, int n)
    {
        int skip = NodeSize - n;

        var (l, r) = Children.SplitAt(skip);
        var (lS, rS) = sizeTable.SplitAt(skip);

        if (lS.Length > 0)
        {
            int last = lS[^1];
            for (int i = 0; i < rS.Length; i++) rS[i] -= last;
        }

        var node = MkNodeKnownSize(owner, shift, r, rS);
        return (l, node);
    }

    public override ((RRBNode<T>[], int[]), RRBNode<T>) SplitAndKeepNRightS(OwnerToken owner, int shift, int n)
    {
        int skip = NodeSize - n;

        var (l, r) = Children.SplitAt(skip);
        var (lS, rS) = sizeTable.SplitAt(skip);

        if (lS.Length > 0)
        {
            int last = lS[^1];
            for (int i = 0; i < rS.Length; i++) rS[i] -= last;
        }

        var node = MkNodeKnownSize(owner, shift, r, rS);
        return ((l, lS), node);
    }

    public override RRBNode<T> AppendNChildren(OwnerToken owner, int shift, int n, IEnumerable<RRBNode<T>> newChildren, bool shouldExpandRightChildIfNeeded)
    {
        int size = NodeSize;
        int newSize = size + n;

        var children = new RRBNode<T>[newSize];
        System.Array.Copy(Children, children, size);

        var sizeTable = new int[newSize];
        System.Array.Copy(SizeTable, sizeTable, size);

        int prevSizeTableEntry = sizeTable[size - 1];
        using (var eC = newChildren.GetEnumerator())
        {
            for (int i = size; i < newSize; i++)
            {
                if (eC.MoveNext())
                {
                    var child = eC.Current;
                    children[i] = child;
                    int childSize = child.TreeSize(RRBMath.Down(shift));

                    int nextSizeTableEntry = prevSizeTableEntry + childSize;
                    sizeTable[i] = nextSizeTableEntry;
                    prevSizeTableEntry = nextSizeTableEntry;
                }
            }
        }

        return MkNodeKnownSize(owner, shift, children, sizeTable);
    }

    public override RRBNode<T> AppendNChildrenS(OwnerToken owner, int shift, int n, IEnumerable<RRBNode<T>> newChildren, IEnumerable<int> sizes, bool shouldExpandRightChildIfNeeded)
    {
        // Note: "sizes" should be a series of size table entries, i.e. cumulative: instead of [3,4,3,2,4] it should be [3,7,10,12,16]
        int size = this.NodeSize;
        int newSize = size + n;

        var children = new RRBNode<T>[newSize];
        System.Array.Copy(this.Children, children, size);

        var sizeTable = new int[newSize];
        System.Array.Copy(this.SizeTable, sizeTable, size);

        int lastSizeTableEntry = sizeTable[size - 1];
        using (var eC = newChildren.GetEnumerator())
        using (var eS = sizes.GetEnumerator())
        {
            for (int i = size; i < newSize; i++)
            {
                if (eC.MoveNext() && eS.MoveNext())
                {
                    children[i] = eC.Current;
                    sizeTable[i] = lastSizeTableEntry + eS.Current;
                }
            }
        }

        return MkNodeKnownSize(owner, shift, children, sizeTable);
    }

    public override RRBNode<T> PrependNChildren(OwnerToken owner, int shift, int n, IEnumerable<RRBNode<T>> newChildren)
    {
        int size = this.NodeSize;
        int newSize = size + n;

        var children = new RRBNode<T>[newSize];
        System.Array.Copy(this.Children, 0, children, n, size);

        var sizeTable = new int[newSize];
        System.Array.Copy(this.SizeTable, 0, sizeTable, n, size);

        int prevSizeTableEntry = 0;
        using (var eC = newChildren.GetEnumerator())
        {
            for (int i = 0; i < n; i++)
            {
                if (eC.MoveNext())
                {
                    children[i] = eC.Current;
                    int childSize = eC.Current.TreeSize(RRBMath.Down(shift));

                    int nextSizeTableEntry = prevSizeTableEntry + childSize;
                    sizeTable[i] = nextSizeTableEntry;
                    prevSizeTableEntry = nextSizeTableEntry;
                }
            }
        }

        for (int i = n; i < newSize; i++)
        {
            sizeTable[i] += prevSizeTableEntry;
        }

        return RRBNode<T>.MkNodeKnownSize(owner, shift, children, sizeTable);
    }

    public override RRBNode<T> PrependNChildrenS(OwnerToken owner, int shift, int n, IEnumerable<RRBNode<T>> newChildren, IEnumerable<int> sizes)
    {
        // Note: "sizes" should be a series of size table entries, i.e. cumulative: instead of [3,4,3,2,4] it should be [3,7,10,12,16]
        int size = this.NodeSize;
        int newSize = size + n;

        var children = new RRBNode<T>[newSize];
        System.Array.Copy(this.Children, 0, children, n, size);

        var sizeTable = new int[newSize];
        System.Array.Copy(this.SizeTable, 0, sizeTable, n, size);

        using (var eC = newChildren.GetEnumerator())
        using (var eS = sizes.GetEnumerator())
        {
            for (int i = 0; i < n; i++)
            {
                if (eC.MoveNext() && eS.MoveNext())
                {
                    children[i] = eC.Current;
                    sizeTable[i] = eS.Current;
                }
            }
        }

        int lastSizeTableEntry = sizeTable[n - 1];

        for (int i = n; i < newSize; i++)
        {
            sizeTable[i] += lastSizeTableEntry;
        }

        return RRBNode<T>.MkNodeKnownSize(owner, shift, children, sizeTable);
    }
    // TODO: These Append/Prepend variants are *almost* identical to the RRBFullNode versions; see if there's a way to refactor these and combine them
}
