using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using Ficus.RRBArrayExtensions;

namespace Ficus;

public class RRBFullNode<T> : RRBNode<T>
{
    public RRBNode<T>[] Children;

    public RRBFullNode(OwnerToken ownerToken, RRBNode<T>[] children)
        : base(ownerToken)
    {
        Children = children;
    }

    public static RRBFullNode<T> Create(OwnerToken ownerToken, RRBNode<T>[] children)
        => new(ownerToken, children);

    public static readonly RRBFullNode<T> EmptyNode = Create(OwnerTokens.NullOwner, []);

    public override int NodeSize => Children.Length;

#if DEBUG
    public override string StringRepr => $"FullNode(length={NodeSize})";
    public override string ToString()
    {
        if (NodeSize == 0) return "[]";
        // if (FirstChild is RRBLeafNode<T> leaf)
        // {
            return $"[{string.Join(" ", Children.Select(c => c.StringRepr))}]";
        // }
        // else
        // {
        //     var nodes = Children.Cast<RRBFullNode<T>>();
        //     return $"[(string.Join(" ", ))]"
        // }
    }

#endif

    public RRBNode<T> FirstChild => Children[0];

    public RRBNode<T> LastChild => Children[NodeSize - 1];

    public RRBFullNode<T> LeftmostTwig(int shift)
    {
        if (shift > Literals.shiftSize)
            return ((RRBFullNode<T>)FirstChild).LeftmostTwig(RRBMath.Down(shift));
        return this;
    }

    public RRBFullNode<T> RightmostTwig(int shift)
    {
        if (shift > Literals.shiftSize)
            return ((RRBFullNode<T>)LastChild).RightmostTwig(RRBMath.Down(shift));
        return this;
    }

    public override int TreeSize(int shift)
    {
        return ((this.NodeSize - 1) << shift) + LastChild.TreeSize(RRBMath.Down(shift));
    }

    public override int SlotCount => Children.Sum(c => c.NodeSize);

    public override int TwigSlotCount => ((NodeSize - 1) << Literals.shiftSize) + LastChild.NodeSize;

    // No-op; only used in expanded nodes
    public override void SetNodeSize(int size) { }

    public bool FullNodeIsTrulyFull(int shift)
    {
        return NodeSize == 0 || LastChild.TreeSize(RRBMath.Down(shift)) >= (1 << shift);
    }

    public override RRBNode<T> GetEditableNode(OwnerToken owner)
    {
        if (IsEditableBy(owner)) return this;

        return new RRBFullNode<T>(owner, (RRBNode<T>[])Children.Clone());
    }

    // Almost a no-op in this class since a full node is already "shrunk" as far as it will go
    public override RRBNode<T> Shrink(OwnerToken owner) => GetEditableNode(owner);

    public override RRBNode<T> Expand(OwnerToken owner)
    {
        var node = (RRBFullNode<T>)GetEditableNode(owner);
        return new RRBExpandedFullNode<T>(owner, node.Children);
    }

    // TODO: Investigate whether I'm using both of these ShrinkRightSpine variants
    public override RRBNode<T> ShrinkRightSpineOfChild(OwnerToken owner, int shift)
    {
        if (shift <= Literals.shiftSize || NodeSize == 0) return this;

        var shrunkLastChild = ((RRBFullNode<T>)LastChild).ShrinkRightSpine(owner, RRBMath.Down(shift));
        if (ReferenceEquals(shrunkLastChild, LastChild)) return this;

        var node = (RRBFullNode<T>)GetEditableNode(owner);
        node.Children[node.NodeSize - 1] = shrunkLastChild;
        return node;
    }

    public override RRBNode<T> ShrinkRightSpine(OwnerToken owner, int shift)
    {
        if (shift <= Literals.shiftSize || NodeSize == 0) return Shrink(owner);

        var shrunkLastChild = ((RRBFullNode<T>)LastChild).ShrinkRightSpine(owner, RRBMath.Down(shift));

        if (ReferenceEquals(shrunkLastChild, LastChild)) return Shrink(owner);

        var node = (RRBFullNode<T>)Shrink(owner);
        node.Children[node.NodeSize - 1] = shrunkLastChild;
        return node;
    }

    public RRBNode<T> ExpandRightSpine(OwnerToken owner, int shift)
    {
        if (shift <= Literals.shiftSize || NodeSize == 0) return Expand(owner);

        var expandedLastChild = ((RRBFullNode<T>)LastChild).ExpandRightSpine(owner, RRBMath.Down(shift));

        if (ReferenceEquals(expandedLastChild, LastChild)) return Expand(owner);

        var node = (RRBFullNode<T>)Expand(owner);
        node.Children[node.NodeSize - 1] = expandedLastChild;
        return node;
    }

    public RRBNode<T> ReplaceLastLeaf(OwnerToken owner, int shift, RRBLeafNode<T> newLeaf, int sizeDiff)
    {
        if (shift <= Literals.shiftSize || NodeSize == 0) return UpdateChildSRel(owner, shift, NodeSize - 1, newLeaf, sizeDiff);

        var newLastChild = ((RRBFullNode<T>)LastChild).ReplaceLastLeaf(owner, RRBMath.Down(shift), newLeaf, sizeDiff);
        return UpdateChildSRel(owner, shift, NodeSize - 1, newLastChild, sizeDiff);
    }

    public virtual RRBNode<T> ToRelaxedNodeIfNeeded(int shift)
    {
        if (shift <= 0) return this;

        int size = NodeSize;
        var sizeTable = RRBNode<T>.CreateSizeTableS(shift, Children, size);
        if (RRBMath.IsSizeTableFullAtShift(shift, sizeTable, size)) return this;

        return new RRBExpandedRelaxedNode<T>(Owner, Children, sizeTable, NodeSize);
    }

    public virtual (int localIdx, RRBNode<T> child, int nextTreeIdx) IndexesAndChild(int shift, int treeIdx)
    {
        int localIdx = RRBMath.RadixIndex(shift, treeIdx);
        var child = Children[localIdx];
        int antimask = ~(Literals.blockIndexMask << shift);
        int nextTreeIdx = treeIdx & antimask;
        return (localIdx, child, nextTreeIdx);
    }

    public IEnumerable<RRBNode<T>> ChildrenSeq => Children.Take(NodeSize);

    public IEnumerable<RRBLeafNode<T>> LeavesSeq(int shift)
    {
        if (shift <= Literals.shiftSize) return ChildrenSeq.Cast<RRBLeafNode<T>>();
        return ChildrenSeq.SelectMany(child => ((RRBFullNode<T>)child).LeavesSeq(RRBMath.Down(shift)));
    }

    public IEnumerable<RRBLeafNode<T>> RevLeavesSeq(int shift)
    {
        if (shift <= Literals.shiftSize) return ChildrenSeq.Reverse().Cast<RRBLeafNode<T>>();
        return ChildrenSeq.Reverse().SelectMany(child => ((RRBFullNode<T>)child).RevLeavesSeq(RRBMath.Down(shift)));
    }

    // Overridden in expanded nodes which will use the equivalent of ChildrenSeq instead
    public virtual RRBNode<T>[] SafeChildrenArr => Children;

    // ===== NODE MANIPULATION =====
    // These low-level methods only create new nodes (or modify an expanded node), without doing any bounds checking.

    public RRBNode<T> UpdateChild(OwnerToken owner, int shift, int localIdx, RRBNode<T> newChild)
    {
        var node = (RRBFullNode<T>)GetEditableNode(owner);
        node.Children[localIdx] = newChild;
        return node;
    }

    public virtual RRBNode<T> UpdateChildSRel(OwnerToken owner, int shift, int idx, RRBNode<T> newChild, int sizeDiff)
    {
        // TODO: Why doesn't sizeDiff get used in here?
        int size = NodeSize;
        var sizeTable = BuildSizeTable(shift, size, size - 1);

        var node = new RRBRelaxedNode<T>(Owner, Children, sizeTable);
        return node.UpdateChildSRel(owner, shift, idx, newChild, sizeDiff);
    }

    public virtual RRBNode<T> UpdateChildSAbs(OwnerToken owner, int shift, int idx, RRBNode<T> newChild, int childSize)
    {
        int size = NodeSize;
        var sizeTable = BuildSizeTable(shift, size, size - 1);

        var node = new RRBRelaxedNode<T>(Owner, Children, sizeTable);
        return node.UpdateChildSAbs(owner, shift, idx, newChild, childSize);
    }

    public virtual RRBNode<T> AppendChild(OwnerToken owner, int shift, RRBNode<T> newChild)
    {
        var newChildren = Children.CopyAndPush(newChild);

        // Full nodes are allowed to have their last item be non-full, so we have to check that
        return FullNodeIsTrulyFull(shift)
            // Last item was also full, so this remains a full node
            ? RRBNode<T>.MkFullNode(owner, newChildren)
            // Last item wasn't full, so this is going to become a relaxed node
            : RRBNode<T>.MkNode(owner, shift, newChildren);
    }

    // Basically no-op in this class, overridden in relaxed nodes
    // TODO: Verify that that summary is correct
    public virtual RRBNode<T> AppendChildS(OwnerToken owner, int shift, RRBNode<T> newChild, int newChildSize)
    {
        return this.AppendChild(owner, shift, newChild);
    }

    // TODO: Combine the S and non-S variants using C# overloading
    public virtual RRBNode<T> InsertChild(OwnerToken owner, int shift, int idx, RRBNode<T> newChild)
    {
        if (idx == NodeSize) return this.AppendChild(owner, shift, newChild);

        var newChildren = Children.CopyAndInsertAt(idx, newChild);
        int newChildSize = newChild.TreeSize(RRBMath.Down(shift));

        return newChildSize >= (1 << shift)
            // New child is full, so this is going to become/remain a full node
            ? RRBNode<T>.MkFullNode(owner, newChildren)
            // New child wasn't full, so this is going to become a relaxed node
            : RRBNode<T>.MkNode(owner, shift, newChildren);
    }

    public virtual RRBNode<T> InsertChildS(OwnerToken owner, int shift, int idx, RRBNode<T> newChild, int newChildSize)
    {
        if (idx == NodeSize) return this.AppendChildS(owner, shift, newChild, newChildSize);

        var newChildren = Children.CopyAndInsertAt(idx, newChild);
        return newChildSize >= (1 << shift)
            // New child is full, so this is going to become/remain a full node
            ? RRBNode<T>.MkFullNode(owner, newChildren)
                // New child wasn't full, so this is going to become a relaxed node
            : RRBNode<T>.MkNode(owner, shift, newChildren);
    }

    public virtual RRBNode<T> RemoveChild(OwnerToken owner, int shift, int idx)
    {
        var newChildren = Children.CopyAndRemoveAt(idx);
        return RRBNode<T>.MkFullNode(owner, newChildren);
    }

    public virtual RRBNode<T> RemoveLastChild(OwnerToken owner, int shift)
    {
        // Expanded nodes can do this in a transient way, but "normal" nodes can't
        var arr = Children.CopyAndPop();
        return RRBNode<T>.MkFullNode(owner, arr);
    }
    // TODO: Would RemoveChildS ever be useful? Doubt it, but think about it

    public virtual RRBNode<T> KeepNLeft(OwnerToken owner, int shift, int n)
    {
        if (n == NodeSize) return this;

        var arr = new RRBNode<T>[n];
        Array.Copy(Children, 0, arr, 0, n);

        // Cannot become a relaxed node, since full nodes have all but their last child full (and the last child may be full)
        return new RRBFullNode<T>(owner, arr);
    }

    public virtual RRBNode<T> KeepNRight(OwnerToken owner, int shift, int n)
    {
        int skip = NodeSize - n;
        if (skip == 0) return this;

        var arr = new RRBNode<T>[n];
        Array.Copy(Children, skip, arr, 0, n);

        // Cannot become a relaxed node, since full nodes have all but their last child full (and the last child may be full)
        return new RRBFullNode<T>(owner, arr);
    }

    public virtual (RRBNode<T> node, RRBNode<T>[] rightChildren) SplitAndKeepNLeft(OwnerToken owner, int shift, int n)
    {
        var left = new RRBNode<T>[n];
        var right = new RRBNode<T>[NodeSize - n];

        Array.Copy(Children, 0, left, 0, n);
        Array.Copy(Children, n, right, 0, NodeSize - n);

        var node = new RRBFullNode<T>(owner, left);
        return (node, right);
    }

    public virtual (RRBNode<T> node, (RRBNode<T>[] rightChildren, int[] rightSizeTable)) SplitAndKeepNLeftS(OwnerToken owner, int shift, int n)
    {
        int size = NodeSize;
        var sizeTable = BuildSizeTable(shift, size, size - 1);

        var left = new RRBNode<T>[n];
        var right = new RRBNode<T>[size - n];
        Array.Copy(Children, 0, left, 0, n);
        Array.Copy(Children, n, right, 0, size - n);

        var lS = new int[n];
        var rS = new int[size - n];
        Array.Copy(sizeTable, 0, lS, 0, n);
        Array.Copy(sizeTable, n, rS, 0, size - n);

        int lastSize = lS[lS.Length - 1];

        for (int i = 0; i < rS.Length; i++)
            rS[i] -= lastSize;

        var node = new RRBFullNode<T>(owner, left);
        return (node, (right, rS));
    }

    public virtual (RRBNode<T>[] leftChildren, RRBNode<T> node) SplitAndKeepNRight(OwnerToken owner, int shift, int n)
    {
        int skip = NodeSize - n;

        var left = new RRBNode<T>[skip];
        var right = new RRBNode<T>[n];

        Array.Copy(Children, 0, left, 0, skip);
        Array.Copy(Children, skip, right, 0, n);

        var node = new RRBFullNode<T>(owner, right);
        return (left, node);
    }

    public virtual ((RRBNode<T>[] leftChildren, int[] leftSizeTable), RRBNode<T> node) SplitAndKeepNRightS(OwnerToken owner, int shift, int n)
    {
        int size = NodeSize;
        int skip = size - n;

        var sizeTableL = BuildSizeTable(shift, skip, skip - 1);

        var left = new RRBNode<T>[skip];
        var right = new RRBNode<T>[n];
        Array.Copy(Children, 0, left, 0, skip);
        Array.Copy(Children, skip, right, 0, n);

        // No need to adjust rS here since we don't use it
        var node = new RRBFullNode<T>(owner, right);
        return ((left, sizeTableL), node);
    }

    public virtual RRBNode<T> AppendNChildren(OwnerToken owner, int shift, int n, IEnumerable<RRBNode<T>> newChildren, bool shouldExpandRightChildIfNeeded)
    {
        int size = NodeSize;
        int newSize = size + n;

        var children2 = new RRBNode<T>[newSize];
        Array.Copy(Children, children2, size);

        var sizeTable = BuildSizeTable(shift, size, size - 1);
        var sizeTable2 = new int[newSize];
        Array.Copy(sizeTable, sizeTable2, size);

        int lastSizeTableEntry = sizeTable2[size - 1];

        int i = size;
        foreach (var child in newChildren)
        {
            if (i >= newSize) break;

            children2[i] = child;
            int childSize = child.TreeSize(RRBMath.Down(shift));

            int nextSizeTableEntry = lastSizeTableEntry + childSize;
            sizeTable2[i] = nextSizeTableEntry;
            lastSizeTableEntry = nextSizeTableEntry;

            i++;
        }

        return MkNodeKnownSize(owner, shift, children2, sizeTable2);
    }

    public virtual RRBNode<T> AppendNChildrenS(OwnerToken owner, int shift, int n, IEnumerable<RRBNode<T>> newChildren, IEnumerable<int> sizes, bool shouldExpandRightChildIfNeeded)
    {
        // Note: "sizes" should be a series of size table entries, i.e. cumulative: instead of [3,4,3,2,4] it should be [3,7,10,12,16]
        int size = NodeSize;
        int newSize = size + n;

        var children2 = new RRBNode<T>[newSize];
        Array.Copy(Children, children2, size);

        var sizeTable = BuildSizeTable(shift, size, size - 1);
        var sizeTable2 = new int[newSize];
        Array.Copy(sizeTable, sizeTable2, size);

        int last = sizeTable2[size - 1];

        using var eC = newChildren.GetEnumerator();
        using var eS = sizes.GetEnumerator();

        for (int i = size; i < newSize; i++)
        {
            if (eC.MoveNext() && eS.MoveNext())
            {
                children2[i] = eC.Current;
                sizeTable2[i] = last + eS.Current;
            }
        }

        return MkNodeKnownSize(owner, shift, children2, sizeTable2);
    }

    public virtual RRBNode<T> PrependNChildren(OwnerToken owner, int shift, int n, IEnumerable<RRBNode<T>> newChildren)
    {
        int size = NodeSize;
        int newSize = size + n;

        var children2 = new RRBNode<T>[newSize];
        Array.Copy(Children, 0, children2, n, size);

        var sizeTable = BuildSizeTable(shift, size, size - 1);
        var sizeTable2 = new int[newSize];
        Array.Copy(sizeTable, 0, sizeTable2, n, size);

        int lastSizeTableEntry = 0;
        int i = 0;

        using var eC = newChildren.GetEnumerator();
        for (; i < n; i++)
        {
            if (!eC.MoveNext()) break;
            var child = eC.Current;
            children2[i] = child;
            int childSize = child.TreeSize(RRBMath.Down(shift));

            int nextSizeTableEntry = lastSizeTableEntry + childSize;
            sizeTable2[i] = nextSizeTableEntry;
            lastSizeTableEntry = nextSizeTableEntry;
        }

        for (; i < newSize; i++)
        {
            sizeTable2[i] += lastSizeTableEntry;
        }

        return MkNodeKnownSize(owner, shift, children2, sizeTable2);
    }

    public virtual RRBNode<T> PrependNChildrenS(OwnerToken owner, int shift, int n, IEnumerable<RRBNode<T>> newChildren, IEnumerable<int> sizes)
    {
        // Note: "sizes" should be a series of size table entries, i.e. cumulative: instead of [3,4,3,2,4] it should be [3,7,10,12,16]
        int size = NodeSize;
        int newSize = size + n;

        var children2 = new RRBNode<T>[newSize];
        Array.Copy(Children, 0, children2, n, size);

        var sizeTable = BuildSizeTable(shift, size, size - 1);
        var sizeTable2 = new int[newSize];
        Array.Copy(sizeTable, 0, sizeTable2, n, size);

        using var eC = newChildren.GetEnumerator();
        using var eS = sizes.GetEnumerator();

        int i = 0;
        for (; i < n; i++)
        {
            if (eC.MoveNext() && eS.MoveNext())
            {
                children2[i] = eC.Current;
                sizeTable2[i] = eS.Current;
            }
        }

        int lastSizeTableEntry = sizeTable2[n - 1];

        for (; i < newSize; i++)
        {
            sizeTable2[i] += lastSizeTableEntry;
        }

        return MkNodeKnownSize(owner, shift, children2, sizeTable2);
    }

    public virtual RRBNode<T> MaybeExpand(OwnerToken owner, int shift)
    {
        return this;
    }

    public int[] BuildSizeTable(int shift, int count, int lastIdx)
    {
        int fullSize = 1 << shift;

        var result = new int[count];

        for (int i = 0; i < count; i++)
        {
            if (i == lastIdx)
                result[i] = fullSize * i + Children[i].TreeSize(RRBMath.Down(shift));
            else
                result[i] = fullSize * (i + 1);
        }

        return result;
    }

    public RRBNode<T> NewPathL(OwnerToken owner, int endShift, RRBLeafNode<T> leaf)
    {
        // NOTE: this does NOT expand any nodes, because we're building a left path

        int shift = Literals.shiftSize;
        RRBNode<T> node = RRBNode<T>.MkNode(owner, shift, new RRBNode<T>[] { leaf });

        while (shift < endShift)
        {
            shift = RRBMath.Up(shift);
            node = RRBNode<T>.MkNode(owner, shift, new RRBNode<T>[] { node });
        }

        return node;
    }

    public RRBNode<T> NewPathR(OwnerToken owner, int endShift, RRBLeafNode<T> leaf)
    {
        // NOTE: this DOES expand nodes, because we're building a right path -- but only if we are an expanded node ourselves
        // (i.e., this calls MaybeExpand, which is overridden in descendants)

        int shift = Literals.shiftSize;

        // TODO: Figure out if these casts to RRBFullNode are actually needed in C# (F#'s type system was subtly different)
        var node = (RRBFullNode<T>)this.NewParent(
            owner,
            shift,
            new RRBNode<T>[] { leaf }
        );

        while (shift < endShift)
        {
            var nextShift = RRBMath.Up(shift);
            node = (RRBFullNode<T>)this.NewParent(
                owner,
                nextShift,
                new RRBNode<T>[] { node.MaybeExpand(owner, shift) }
            );
            shift = nextShift;
        }

        return node.MaybeExpand(owner, shift);
    }

    // Will append leaf if node has room to expand without growing up to a new parent
    // Returns null if node is completely full and needs a new parent in order to append the leaf
    public RRBNode<T>? TryAppendLeaf(OwnerToken owner, int shift, RRBLeafNode<T> newLeaf, int leafLen)
    {
        if (shift <= Literals.shiftSize)
        {
            if (this.NodeSize >= Literals.blockSize) return null;

            return this.AppendChildS(owner, shift, newLeaf, leafLen);
        }

        var lastChild = (RRBFullNode<T>)this.LastChild;
        var newChild = lastChild.TryAppendLeaf(owner, RRBMath.Down(shift), newLeaf, leafLen);

        if (newChild != null) return this.UpdateChildSRel(owner, shift, this.NodeSize - 1, newChild, leafLen);

        // Child subtree had no room to append leaf
        if (this.NodeSize >= Literals.blockSize) return null; // And neither did we, new parent needed

        // We have room for a new child, so create a new path to leaf level as the new rightmost child
        return this.AppendChildS( owner, shift, this.NewPathR(owner, RRBMath.Down(shift), newLeaf), leafLen);
        // Note that if we're an expanded node, AppendChildS will take care of shrinking old last child and expanding new one
    }

    public (RRBNode<T> node, int shift) AppendLeaf(OwnerToken owner, int shift, RRBLeafNode<T> newLeaf)
    {
        var result = this.TryAppendLeaf(owner, shift, newLeaf, newLeaf.NodeSize);

        if (result != null) return (result, shift);

        // Remember that NewPathR calls MaybeExpand, so we don't have to call it here
        var newRight = (RRBFullNode<T>)this.NewPathR(owner, shift, newLeaf);

        // And if we're an expanded node, then this.NewParent will create a new expanded node
        var newParent = this.NewParent(
            owner,
            shift,
            new RRBNode<T>[] { this.ShrinkRightSpine(owner, shift), newRight }
        );

        return (newParent, RRBMath.Up(shift));
    }

    // Will prepend leaf if node has room to expand without growing up to a new parent
    // Returns null if node is completely full and needs a new parent in order to prepend the leaf
    public RRBNode<T>? TryPrependLeaf(OwnerToken owner, int shift, RRBLeafNode<T> newLeaf, int leafLen)
    {
        if (shift <= Literals.shiftSize)
        {
            if (this.NodeSize >= Literals.blockSize) return null;

            return this.InsertChildS(owner, shift, 0, newLeaf, leafLen);
        }

        var firstChild = (RRBFullNode<T>)this.FirstChild;
        var newChild = firstChild.TryPrependLeaf(owner, RRBMath.Down(shift), newLeaf, leafLen);

        if (newChild != null) return this.UpdateChildSRel(owner, shift, 0, newChild, leafLen);

        // Child subtree had no room to prepend leaf
        if (this.NodeSize >= Literals.blockSize) return null;

        return this.InsertChildS(owner, shift, 0, this.NewPathL(owner, RRBMath.Down(shift), newLeaf), leafLen);
        // Note that if we're an expanded node, InsertChildS will take care of shrinking old last child and expanding new one
        // TODO: Wait, is that true? It's true for Append, but why would it need to be true for InsertChildS? Check on that.
    }

    public (RRBNode<T> node, int shift) PrependLeaf(OwnerToken owner, int shift, RRBLeafNode<T> newLeaf)
    {
        var result = this.TryPrependLeaf(owner, shift, newLeaf, newLeaf.NodeSize);

        if (result != null) return (result, shift);

        var newLeft = (RRBFullNode<T>)this.NewPathL(owner, shift, newLeaf);

        var parent = this.NewParent(
            owner,
            shift,
            new RRBNode<T>[] { newLeft, this }
        );

        return (parent, RRBMath.Up(shift));
    }

    public (RRBNode<T>, RRBNode<T>) SlideChildrenLeft(
        OwnerToken owner, int shift, int n, RRBFullNode<T> leftSibling)
    {
        if (n == 0)
            return (leftSibling, this);

        int keep = NodeSize - n;

        var ((l, lS), rightNode) = SplitAndKeepNRightS(owner, shift, keep);
        var leftNode = leftSibling.AppendNChildrenS(owner, shift, n, l, lS, true);

        return (leftNode, rightNode);
    }

    public (RRBNode<T>, RRBNode<T>) SlideChildrenRight(
        OwnerToken owner, int shift, int n, RRBFullNode<T> rightSibling)
    {
        if (n == 0)
            return (this, rightSibling);

        int keep = NodeSize - n;

        var (leftNode, (r, rS)) = SplitAndKeepNLeftS(owner, shift, keep);
        var rightNode = rightSibling.PrependNChildrenS(owner, shift, n, r, rS);

        return (leftNode, rightNode);
    }

    public (RRBNode<T> l, RRBNode<T> r) InsertAndSlideChildrenLeft(OwnerToken owner, int shift, int localIdx, RRBNode<T> newChild, RRBFullNode<T> leftSibling)
    {
        // GOAL: we make left and right balanced, with any extra going on the left.
        // Method: calculate the average size, and make each one fit that average. Extra node (if total is odd) can go on either side; it doesn't matter much.
        int thisSize = this.NodeSize;
        int sibSize = leftSibling.NodeSize;

        int totalSize = thisSize + sibSize;
        int resultSizeL = totalSize >> 1;
        int resultSizeR = totalSize - resultSizeL;

        int n = Math.Max(thisSize - resultSizeR, 1);

        var (l, r) = SlideChildrenLeft(owner, shift, n, leftSibling);

        int idxAfterSlide = localIdx - n;

        if (idxAfterSlide < 0)
        {
            // Inserting into left sibling
            int insertIdx = idxAfterSlide + l.NodeSize;

            var l2 = ((RRBFullNode<T>)l).InsertChild(owner, shift, insertIdx, newChild);
            return (l2, r);
        }
        else
        {
            // Inserting into right sibling
            var r2 = ((RRBFullNode<T>)r).InsertChild(owner, shift, idxAfterSlide, newChild);
            return (l, r2);
        }
    }

    public (RRBNode<T> l, RRBNode<T> r) InsertAndSlideChildrenRight(OwnerToken owner, int shift, int localIdx, RRBNode<T> newChild, RRBFullNode<T> rightSibling)
    {
        int thisSize = this.NodeSize;
        int sibSize = rightSibling.NodeSize;

        if (thisSize == Literals.blockSize && localIdx == Literals.blockSize && sibSize == Literals.blockSize - 1)
        {
            // Special case because the algorithm below would fail on this corner case: we'd first shift one item to
            // the right sibling and then try to insert at position 1 in the now-full right sibling. So we skip all
            // the shifting and just insert into position 0 of the right sibling, which is the right thing in this case.
            var newRightSibling = rightSibling.InsertChild(owner, shift, 0, newChild);
            return (this, newRightSibling);
        }
        else
        {
            int totalSize = thisSize + sibSize;
            int resultSizeL = totalSize >> 1;

            int n = thisSize - resultSizeL;

            var (l, r) = SlideChildrenRight(owner, shift, n, rightSibling);

            if (localIdx <= resultSizeL)
            {
                // Inserting into left sibling
                var l2 = ((RRBFullNode<T>)l).InsertChild(owner, shift, localIdx, newChild);
                return (l2, r);
            }
            else
            {
                // Inserting into right sibling
                var r2 = ((RRBFullNode<T>)r).InsertChild(owner, shift, localIdx - resultSizeL, newChild);
                return (l, r2);
            }
        }
    }

    public (RRBNode<T> l, RRBNode<T> r) InsertAndSplitNode(OwnerToken owner, int shift, int localIdx, RRBNode<T> newChild)
    {
        int size = this.NodeSize;
        int half = size >> 1;

        bool shouldInsertIntoLeft = localIdx < half;

        var (lArr, rNode) = SplitAndKeepNRight(owner, shift, size - half);

        if (shouldInsertIntoLeft)
        {
            // We have an array that hasn't been made a node yet, so insert into the array before nmaking it a node
            var lArr2 = lArr.CopyAndInsertAt(localIdx, newChild);
            var lNode = RRBNode<T>.MkNode(owner, shift, lArr2);
            return (lNode, rNode);
        }
        else
        {
            // Right is a node, not an array, so use the node's InsertChild since it might have an efficiency gain
            var lNode = RRBNode<T>.MkNode(owner, shift, lArr);
            var rNode2 = ((RRBFullNode<T>)rNode).InsertChild(owner, shift, localIdx - half, newChild);
            return (lNode, rNode2);
        }
    }

    public override RRBNode<T> UpdatedTree(OwnerToken owner, int shift, int treeIdx, T newItem)
    {
        var (localIdx, child, nextIdx) = IndexesAndChild(shift, treeIdx);
        var newNode = child.UpdatedTree(owner, RRBMath.Down(shift), nextIdx, newItem);
        return UpdateChild(owner, shift, localIdx, newNode);
    }

    internal override SlideResult<RRBNode<T>> InsertedTree(OwnerToken owner, int shift, int treeIdx, T item, RRBFullNode<T>? parentOpt, int idxOfNodeInParent)
    {
        var (localIdx, child, nextIdx) = IndexesAndChild(shift, treeIdx);

        var (insertResult, newLeftChild, newChild, newRightChild) = child.InsertedTree(owner, RRBMath.Down(shift), nextIdx, item, this, localIdx);
        RRBFullNode<T> node;

        switch (insertResult)
        {
            case SlideResult<RRBNode<T>>.Tag.SimpleInsertion:
                return SlideResult<RRBNode<T>>.SimpleInsertion(
                    UpdateChildSRel(owner, shift, localIdx, newChild, 1));

            case SlideResult<RRBNode<T>>.Tag.SlidItemsLeft:
                // TODO: Do we need an "Update two child items at once" function? What about the size table? We should be able to manage the size table more cleverly in RelaxedNodes.
                node = (RRBFullNode<T>)UpdateChildSAbs(
                    owner, shift, localIdx - 1,
                    newLeftChild,
                    newLeftChild.TreeSize(RRBMath.Down(shift)));

                return SlideResult<RRBNode<T>>.SimpleInsertion(
                    node.UpdateChildSAbs(
                        owner, shift, localIdx,
                        newChild,
                        newChild.TreeSize(RRBMath.Down(shift))));

            case SlideResult<RRBNode<T>>.Tag.SlidItemsRight:
                // TODO: Do we need an "Update two child items at once" function? What about the size table? We should be able to manage the size table more cleverly in RelaxedNodes.
                node = (RRBFullNode<T>)UpdateChildSAbs(
                    owner, shift, localIdx,
                    newChild,
                    newChild.TreeSize(RRBMath.Down(shift)));

                return SlideResult<RRBNode<T>>.SimpleInsertion(
                    node.UpdateChildSAbs(
                        owner, shift, localIdx + 1,
                        newRightChild,
                        newRightChild.TreeSize(RRBMath.Down(shift))));

            case SlideResult<RRBNode<T>>.Tag.SplitNode:
                if (NodeSize < Literals.blockSize)
                {
                    // We have room to grow the current node
                    node = (RRBFullNode<T>)UpdateChildSAbs(
                        owner, shift, localIdx,
                        newLeftChild,
                        newLeftChild.TreeSize(RRBMath.Down(shift)));

                    return SlideResult<RRBNode<T>>.SimpleInsertion(
                        node.InsertChild(owner, shift, localIdx + 1, newRightChild));
                }

                if (parentOpt != null &&
                    idxOfNodeInParent > 0 &&
                    parentOpt.Children[idxOfNodeInParent - 1].NodeSize < Literals.blockSize)
                {
                    // Current node is full but left sibling has some room
                    var leftSib = (RRBFullNode<T>)parentOpt.Children[idxOfNodeInParent - 1];
                    // NOTE: Important that we're updating the left child at localIdx, and inserting the right child at localIdx+1.
                    // If we inserted the left child at localIdx, InsertAndSlideChildrenLeft could fail when localIdx = 0 and the sibling sizes were (M-1, M).
                    // But updating the left child and inserting the right child avoids that corner case.

                    node = (RRBFullNode<T>)UpdateChildSAbs(
                        owner, shift, localIdx,
                        newLeftChild,
                        newLeftChild.TreeSize(RRBMath.Down(shift)));

                    var (l, r) = node.InsertAndSlideChildrenLeft(owner, shift, localIdx + 1, newRightChild, leftSib);

                    return SlideResult<RRBNode<T>>.SlidItemsLeft(l, r);
                }

                if (parentOpt != null &&
                    idxOfNodeInParent < parentOpt.NodeSize - 1 &&
                    parentOpt.Children[idxOfNodeInParent + 1].NodeSize < Literals.blockSize)
                {
                    // Current node is full but right sibling has some room
                    var rightSib = (RRBFullNode<T>)parentOpt.Children[idxOfNodeInParent + 1];

                    node = (RRBFullNode<T>)UpdateChildSAbs(
                        owner, shift, localIdx,
                        newLeftChild,
                        newLeftChild.TreeSize(RRBMath.Down(shift)));

                    var (l2, r2) = node.InsertAndSlideChildrenRight(
                        owner, shift, localIdx + 1, newRightChild, rightSib);

                    return SlideResult<RRBNode<T>>.SlidItemsRight(l2, r2);
                }

                // No room in immediate left or right siblings, so split current node
                // (We do not try to slide items to siblings two or more steps removed, not worth the cost)
                node = (RRBFullNode<T>)UpdateChildSAbs(
                    owner, shift, localIdx,
                    newLeftChild,
                    newLeftChild.TreeSize(RRBMath.Down(shift)));

                var (l3, r3) = node.InsertAndSplitNode(
                    owner, shift, localIdx + 1, newRightChild);

                return SlideResult<RRBNode<T>>.SplitNode(l3, r3);

#if DEBUG
            default:
                throw new InvalidOperationException("Unhandled insert scenario");
#endif
        }
    }

    public override T GetTreeItem(int shift, int treeIdx)
    {
        var (_, child, nextIdx) = IndexesAndChild(shift, treeIdx);
        return child.GetTreeItem(RRBMath.Down(shift), nextIdx);
    }

    public override RRBNode<T> RemovedItem(
        OwnerToken owner,
        int shift,
        bool shouldCheckForRebalancing,
        int treeIdx)
    {
        var (localIdx, child, nextIdx) = IndexesAndChild(shift, treeIdx);

        var newChild = child.RemovedItem(owner, RRBMath.Down(shift), shouldCheckForRebalancing, nextIdx);

        RRBNode<T> node = newChild.NodeSize <= 0
            ? RemoveChild(owner, shift, localIdx)
            : UpdateChildSRel(owner, shift, localIdx, newChild, -1);

        if (shouldCheckForRebalancing &&
            node.NodeSize > 0 &&
            node.NeedsRebalance(shift))
        {
            return ((RRBFullNode<T>)node).Rebalance(owner, shift);
        }

        return node;
    }

    public override RRBNode<T> KeepNTreeItems(OwnerToken owner, int shift, int treeIdx)
    {
        var (localIdx, child, nextIdx) = IndexesAndChild(shift, treeIdx);

        if (nextIdx == 0)
        {
            // Splitting exactly between two subtrees, no need for further recursion
#if DEBUG
            if (localIdx == 0)
            {
                throw new InvalidOperationException("In KeepNTreeItems, localIdx should never be 0 when nextTreeIdx = 0, because that means we should have stopped at a level further up");
            }
            if (localIdx < 0)
            {
                throw new InvalidOperationException("In KeepNTreeItems, localIdx should never be < 0");
            }
#endif
            return KeepNLeft(owner, shift, localIdx);
        }

        var node = (RRBFullNode<T>)KeepNLeft(owner, shift, localIdx + 1);
        var newChild = child.KeepNTreeItems(owner, RRBMath.Down(shift), nextIdx);

        return node.UpdateChildSAbs(
            owner, shift, localIdx,
            newChild,
            newChild.TreeSize(RRBMath.Down(shift)));
    }

    public override RRBNode<T> SkipNTreeItems(OwnerToken owner, int shift, int treeIdx)
    {
        var (localIdx, child, nextIdx) = IndexesAndChild(shift, treeIdx);

        int keep = NodeSize - localIdx;

        if (nextIdx == 0)
        {
            // Splitting exactly between two subtrees, no need for further recursion
#if DEBUG
            if (localIdx == 0)
            {
                throw new InvalidOperationException("In SkipNTreeItems, localIdx should never be 0 when nextTreeIdx = 0, because that means we should have stopped at a level further up");
            }
            if (localIdx < 0)
            {
                throw new InvalidOperationException("In SkipNTreeItems, localIdx should never be < 0");
            }
#endif
            return KeepNRight(owner, shift, keep);
        }

        var node = (RRBFullNode<T>)KeepNRight(owner, shift, keep);
        var newChild = child.SkipNTreeItems(owner, RRBMath.Down(shift), nextIdx);

        return node.UpdateChildSAbs(
            owner, shift, 0,
            newChild,
            newChild.TreeSize(RRBMath.Down(shift)));
    }

    public override (RRBNode<T>, RRBNode<T>) SplitTree(OwnerToken owner, int shift, int treeIdx)
    {
        // treeIdx is first index of right-hand tree, i.e. after split it will be item 0 in the right-hand tree
        var (localIdx, child, nextIdx) = IndexesAndChild(shift, treeIdx);

        int keep = NodeSize - localIdx;

        if (nextIdx == 0)
        {
            // Splitting exactly between two subtrees, no need to recurse further down
#if DEBUG
            if (localIdx == 0)
            {
                throw new InvalidOperationException("In SplitTree, localIdx should never be 0 when nextTreeIdx = 0, because that means we should have stopped at a level further up");
            }

            if (localIdx < 0)
            {
                throw new InvalidOperationException("In SplitTree, localIdx should never be < 0");
            }
#endif
            // Sigh... it's dumb that C# makes me use different names here and six lines down, there's no
            // possible way for these names to overlap in lexical scope. Just ignore the "2" suffix here.
            var ((lChildren2, lSizes2), rNode2) = SplitAndKeepNRightS(owner, shift, keep);
            var lNode2 = MakeLeftNodeForSplit(owner, shift, lChildren2, lSizes2);
            return (lNode2, rNode2);
        }

        var ((lChildren, lSizes), rNode) = SplitAndKeepNRightS(owner, shift, keep);
        var lNode = MakeLeftNodeForSplit(owner, shift, lChildren, lSizes);

        var (childL, childR) = child.SplitTree(owner, RRBMath.Down(shift), nextIdx);

        var newLeft = ((RRBFullNode<T>)lNode).AppendChild(owner, shift, childL);
        var newRight = ((RRBFullNode<T>)rNode).UpdateChildSAbs(
            owner, shift, 0,
            childR,
            childR.TreeSize(RRBMath.Down(shift)));

        return (newLeft, newRight);
    }

    public virtual RRBNode<T> MakeLeftNodeForSplit(
        OwnerToken owner,
        int shift,
        RRBNode<T>[] children,
        int[] sizes)
    {
        return RRBNode<T>.MkNodeKnownSize(owner, shift, children, sizes);
    }

    public (RRBLeafNode<T> leaf, RRBNode<T> newNode) PopLastLeaf(OwnerToken owner, int shift)
    {
        if (shift <= Literals.shiftSize)
        {
            // Children are leaves
            var leaf = (RRBLeafNode<T>)this.LastChild;
            var newNode = this.RemoveLastChild(owner, shift);
            // Popping the last entry from a FullNode can't ever turn it into an RRBNode.
            return (leaf, newNode);
        }
        else
        {
            // Children are nodes
            var (leaf, newLastChild) = ((RRBFullNode<T>)this.LastChild).RemoveLastLeaf(owner, RRBMath.Down(shift));

            RRBNode<T> newNode;
            if (newLastChild.NodeSize == 0)
            {
                newNode = this.RemoveLastChild(owner, shift);
                // Child had just one child of its own and is now empty, so remove it
            }
            else
            {
                newNode = this.UpdateChildSAbs(
                    owner,
                    shift,
                    this.NodeSize - 1,
                    newLastChild,
                    newLastChild.TreeSize(RRBMath.Down(shift))
                );
            }

            return (leaf, ((RRBFullNode<T>)newNode).MaybeExpand(owner, shift));
        }
    }

    public (RRBLeafNode<T> leaf, RRBNode<T> newNode) RemoveLastLeaf(OwnerToken owner, int shift)
    {
        return this.PopLastLeaf(owner, shift);
        // TODO: Replace all occurrences of "RemoveLastLeaf" in RRBVector code with "PopLastLeaf"
    }

    // TODO: Also need methods to add a new leaf at the end of the tree, and to prepend a leaf at the *beginning* of the tree.
    // Prepending a leaf is used in appending two vectors; if the left vector is leaf-only, or a root+leaf sapling, then we'll just prepend one or two leaves, then rebalance the leftmost twig.
    // Appending a leaf is used when the tail is already full while appending an item to the vector. And the reason for the invariant (the last leaf is full if its parent is full) is so that
    // when you append a full leaf, which happens all the time, you can count on not having to convert the full node above it to a relaxed node. (If the last leaf had NOT been full, then
    // you could have had M;M;M;M-2 and had a full node, but add a full leaf and you get M;M;M;M-2;M which must be a relaxed node).

    // ===== REBALANCING =====

    // TODO: Use MaybeExpand where appropriate in here, which is a no-op in compact nodes but does something in expanded nodes.
    // DESIGN: Two approaches possible. One would use SlideChildrenLeft/Right to move things around... but that doesn't (currently) exist for leaves.
    // Another approach would be to always build arrays and then rebuild size tables. Which is better? We'll just have to try it.

    public RRBNode<T> Rebalance(OwnerToken owner, int shift)
    {
        int len = this.NodeSize;

        return this.RebalanceImpl(owner, shift, len, this.Children.Take(len)).Item1;
    }

    public (RRBNode<T> left, RRBNode<T>? right) Rebalance2(OwnerToken owner, int shift, RRBFullNode<T> right)
    {
        return this.Rebalance2Plus1(owner, shift, null, right);
    }

    // TODO: Consolidate the names of these variants into method overloads in C#
    public (RRBNode<T> left, RRBNode<T>? right) Rebalance2Plus1(OwnerToken owner, int shift, RRBLeafNode<T>? middle, RRBFullNode<T> right)
    {
        // The left node, but not the right, needs to have its right spine shrunk before merging
        // Note, though, that we do *not* want to shrink *this* node yet; that will be done in MkNodeForRebalance
        var left = (RRBFullNode<T>)this.ShrinkRightSpineOfChild(owner, shift);
        // TODO: Comment out the ShrinkRightSpineOfChild and see which tests fail, then write a simple regression test for this scenario

        int totalLength;
        IEnumerable<RRBNode<T>> childrenSeq;

        if (middle == null)
        {
            totalLength = left.NodeSize + right.NodeSize;
            childrenSeq =
                left.Children.Take(left.NodeSize)
                    .Concat(right.Children.Take(right.NodeSize));
        }
        else
        {
            totalLength = left.NodeSize + 1 + right.NodeSize;
            childrenSeq =
                left.Children.Take(left.NodeSize)
                    .Concat(new[] { middle })
                    .Concat(right.Children.Take(right.NodeSize));
        }

        return left.RebalanceImpl(owner, shift, totalLength, childrenSeq);
    }

    public (RRBNode<T> left, RRBNode<T>? right) RebalanceImpl(OwnerToken owner, int shift, int totalLength, IEnumerable<RRBNode<T>> childrenSeq)
    {
        var sizes = childrenSeq.Select(node => node.NodeSize);
        // TODO: Try just FindMergeCandidates here and benchmark what happens to split-join scenarios. Are they faster, or slower, with single-pass version?
        // BENCHMARK - search for this uppercase text to find benchmarking opportunities
        var (idx, mergeLen, sizeReduction) = RRBMath.FindMergeCandidatesTwoPasses(sizes, totalLength);

        int newLen = totalLength - sizeReduction;

        using (var items = childrenSeq.GetEnumerator())
        {
            if (newLen <= Literals.blockSize)
            {
                // Everything can fit into a single node: the overall tree will shrink in width
                var arr = this.MkArrayForRebalance(owner, shift, newLen);

                // Items before merge index are untouched
                RRBArrayExtensions.RRBArrayExtensions.FillFromEnumerator(arr, items, 0, idx);

                // Items in range (merge index + merge length) get shuffled around
                var mergedChildrenSeq = ApplyRebalancePlan(owner, shift, sizes, (idx, mergeLen, sizeReduction), items);
                RRBArrayExtensions.RRBArrayExtensions.FillFromSeq(arr, mergedChildrenSeq, idx, mergeLen - sizeReduction);

                // Items after merge index + length are also untouched
                RRBArrayExtensions.RRBArrayExtensions.FillFromEnumerator(arr, items, idx + mergeLen - sizeReduction, newLen);

                return (MkNodeForRebalance(owner, shift, arr, newLen), null);
            }
            else
            {
                // We will still have two nodes after this rebalancing: a full node on the left, and a likely-relaxed node on the right
                int lenL = Literals.blockSize;
                int lenR = newLen - lenL;

                var arrL = this.MkArrayForRebalance(owner, shift, lenL);
                var arrR = new RRBNode<T>[lenR];

                RRBArrayExtensions.RRBArrayExtensions.Fill2FromEnumerator(arrL, arrR, items, 0, idx);
                var mergedChildrenSeq = ApplyRebalancePlan(owner, shift, sizes, (idx, mergeLen, sizeReduction), items);
                RRBArrayExtensions.RRBArrayExtensions.Fill2FromSeq(arrL, arrR, mergedChildrenSeq, idx, mergeLen - sizeReduction);
                // RRBArrayExtensions.RRBArrayExtensions.Fill2FromEnumerator(arrL, arrR, items, idx + mergeLen - sizeReduction, lenL); // buggy
                RRBArrayExtensions.RRBArrayExtensions.Fill2FromEnumerator(arrL, arrR, items, idx + mergeLen - sizeReduction, newLen); // bugfix

                // TODO: Take advantage of the fact that we know the left node will be full, so no size-checking needed
                // TODO: But also test. Maybe the lower nodes weren't full so this node still needed a size table?
                // TODO: Benchmark what speedup we get by taking advantage of that fact... if it's even possible to get a speedup here
                // BENCHMARK - search for this uppercase text to find benchmarking opportunities
                return (MkNodeForRebalance(owner, shift, arrL, lenL), MkNodeForRebalance(owner, shift, arrR, lenR));
            }
        }
    }

    public virtual RRBNode<T>[] MkArrayForRebalance(OwnerToken owner, int shift, int length)
    {
        // In expanded nodes, this will return "this.Children" -- and MkNodeForRebalance will "zero out" its remaining children if its size shrank (which it should)
        return new RRBNode<T>[length];
    }

    public virtual RRBNode<T> MkNodeForRebalance(OwnerToken owner, int shift, RRBNode<T>[] arr, int len)
    {
        // In expanded nodes, this will return "this" -- but first setting the length and zeroing out any remaining children since the length probably shrank
        return RRBNode<T>.MkNode(owner, shift, arr);
    }

    internal IEnumerable<C> GetGrandchildren<C>(IEnumerator<RRBNode<T>> childrenEnum, Func<RRBNode<T>, C[]> getGrandChildren)
    {
        while (childrenEnum.MoveNext())
        {
            var child = childrenEnum.Current;
            foreach (var c in getGrandChildren(child)) yield return c;
        }
    }

    public IEnumerable<RRBNode<T>> ApplyRebalancePlan(
        OwnerToken owner,
        int shift,
        IEnumerable<int> sizes,
        (int, int, int) mergePlan,
        IEnumerator<RRBNode<T>> childrenEnum)
    {
        var totalSlots = sizes.Skip(mergePlan.Item1).Take(mergePlan.Item2).Sum();
        if (shift > Literals.shiftSize)
        {
            // We are above twig level, children are nodes
            var grandChildrenSeq = GetGrandchildren(childrenEnum, (node) => ((RRBFullNode<T>)node).SafeChildrenArr);
            var arraysSeq = RRBArrayExtensions.RRBArrayExtensions.CreateManyFromSeq(grandChildrenSeq, totalSlots, Literals.blockSize);
            return arraysSeq.Select((arr) => RRBNode<T>.MkNode(owner, RRBMath.Down(shift), arr));
        }
        else
        {
            // We are at twig level, children are leaves
            var grandChildrenSeq = GetGrandchildren(childrenEnum, (leaf) => ((RRBLeafNode<T>)leaf).Items);
            var arraysSeq = RRBArrayExtensions.RRBArrayExtensions.CreateManyFromSeq(grandChildrenSeq, totalSlots, Literals.blockSize);
            return arraysSeq.Select((arr) => RRBNode<T>.MkLeaf(owner, arr));
        }
    }

    public (RRBNode<T> left, RRBNode<T>? right) ConcatNodes(OwnerToken owner, int shift, RRBFullNode<T> right)
    {
        // This will form part of the MergeTrees logic, which will be used in concatenating vectors.
        if (this.NeedsRebalance2(shift, right))
        {
            return this.Rebalance2(owner, shift, right);
        }
        else if (this.NodeSize + right.NodeSize > Literals.blockSize)
        {
            // Can't fit into a single node, so save time by not rewriting
            return (this, right);
        }
        else
        {
            RRBNode<T> left =
                // Efficiency: if right already has a size table, reuse it
                right is RRBRelaxedNode<T>
                    ? this.AppendNChildrenS(owner, shift, right.NodeSize, right.Children, ((RRBRelaxedNode<T>)right).SizeTable, false)
                    : this.AppendNChildren(owner, shift, right.NodeSize, right.Children, false);
            return (left, null);
            // TODO: Benchmark what speedup we get by reusing that size table
            // BENCHMARK - search for this uppercase text to find benchmarking opportunities
        }
    }

    public (RRBNode<T> left, RRBNode<T>? right) ConcatTwigsPlusLeaf(
        OwnerToken owner,
        int shift,
        RRBLeafNode<T> middle,
        RRBFullNode<T> right)
    {
        // Usually used when concatenating two vectors, where `middle` will be the tail of the left vector
        // Will ONLY be called when there's room to merge in the middle leaf, whether directly or by rebalancing
        if (this.NeedsRebalance2PlusLeaf(shift, middle.NodeSize, right))
        {
            return this.Rebalance2Plus1(owner, shift, middle, right);
        }
        else if (this.NodeSize + 1 + right.NodeSize <= Literals.blockSize)
        {
            // If we can compress into a single node, then the time spent rewriting the node is worth it
            int newLen = this.NodeSize + 1 + right.NodeSize;
            var arr = this.MkArrayForRebalance(owner, shift, newLen);

            var items = this.Children.Take(this.NodeSize)
                                    .Concat(new[] { middle })
                                    .Concat(right.Children.Take(right.NodeSize));

            // TODO: Might just write the Array.CopyTo methods here, honestly
            RRBArrayExtensions.RRBArrayExtensions.FillFromSeq(arr, items, 0, newLen);
            // TODO: Benchmark whether that's faster than building that items enumerable
            // BENCHMARK - search for this uppercase text to find benchmarking opportunities

            return (this.MkNodeForRebalance(owner, shift, arr, newLen), null);
        }
        else if (this.NodeSize < Literals.blockSize)
        {
            // Save time by not rewriting right node
            return (this.AppendChild(owner, shift, middle), right);
        }
        else if (right.NodeSize < Literals.blockSize)
        {
            // Save time by not rewriting left node
            return (this, right.InsertChild(owner, shift, 0, middle));
        }
        else
        {
#if DEBUG
            throw new InvalidOperationException($"ConcatTwigsPlusLeaf should only be called when there is room to merge the tail, and HasRoomToMergeTheTail reported true. Since left and right were both full, that should mean that the tail was mergeable because a rebalance was possible... but we didn't rebalance. This resulted in a tail merge that wasn't actually possible. Must find out why. Left was {this.StringRepr}, middle was {middle.StringRepr}, and right was {right.StringRepr}");
#else
            throw new InvalidOperationException("Merge impossible due to size limits.");
#endif
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool HasRoomToMergeTheTail(int shift, RRBLeafNode<T> tail, RRBFullNode<T> right)
    {
        return this.NodeSize < Literals.blockSize
            || right.NodeSize < Literals.blockSize
            || this.NeedsRebalance2PlusLeaf(shift, tail.NodeSize, right);
    }

    public (RRBNode<T> left, RRBNode<T>? right) MergeTree(
        OwnerToken owner,
        int shift,
        RRBLeafNode<T>? tailOpt,
        int rightShift,
        RRBFullNode<T> right,
        bool shouldKeepExpandedLeftNode)
    {
        // Helper function to shrink the left node if required (encapsulates logic that we use at every final branch of this method)
        Func<(RRBNode<T>, RRBNode<T>?), (RRBNode<T>, RRBNode<T>?)> shrinkLeftNode = (pair) =>
        {
            var (leftNode, rightNode) = pair;
            if (rightNode == null && shouldKeepExpandedLeftNode) return (leftNode, null);
            return (leftNode.ShrinkRightSpine(owner, shift), rightNode);
        };

        // Merge steps very much depend on what height the left and right trees are at
        if (shift == Literals.shiftSize && rightShift == Literals.shiftSize)
        {
            // Both left and right are at twig level, so this is where we merge in the tail (if any) as a new leaf
            var combined = (tailOpt == null)
                ? this.ConcatNodes(owner, shift, right)
                : this.ConcatTwigsPlusLeaf(owner, shift, tailOpt, right);
            return shrinkLeftNode(combined);
        }
        else if (shift == rightShift)
        {
            // We are at equal height, but above twig level, so recurse to merge the tree, then update this node with updated children (one or two)
            var childL = (RRBFullNode<T>)this.LastChild;
            var childR = (RRBFullNode<T>)right.FirstChild;

            var (newLeft, newRight) = childL.MergeTree(owner, RRBMath.Down(shift), tailOpt, RRBMath.Down(rightShift), childR, shouldKeepExpandedLeftNode);
            if (newRight == null)
            {
                // Merge resulted in a single node, so we won't grow
                var parentL = (RRBFullNode<T>)this.UpdateChildSAbs(owner, shift, this.NodeSize - 1, newLeft, newLeft.TreeSize(RRBMath.Down(shift)));

                if (right.NodeSize > 1)
                {
                    // Still need to move remainder of right-hand nodes into left (which we know is big enough to fit them)
                    var parentR = (RRBFullNode<T>)right.RemoveChild(owner, rightShift, 0);
                    return shrinkLeftNode(parentL.ConcatNodes(owner, shift, parentR));
                }
                else
                {
                    // Right was single-node, so no need to call ConcatNodes on a will-become-empty right side
                    return shrinkLeftNode((parentL, null));
                }
            }
            else
            {
                // Merge resulted in two nodes still
                var parentL = (RRBFullNode<T>)this.UpdateChildSAbs(owner, shift, this.NodeSize - 1, newLeft, newLeft.TreeSize(RRBMath.Down(shift)));
                var newChildR = (RRBFullNode<T>)newRight;

                // TODO: Prove that this "if (right.NodeSize > 1) then shrink right spine" step is needed, by commenting it out and seeing what fails
                // Then create a targeted regression test with a minimal example for that scenario
                if (right.NodeSize > 1)
                {
                    newChildR = (RRBFullNode<T>)newChildR.ShrinkRightSpine(owner, RRBMath.Down(rightShift));
                }

                var parentR = (RRBFullNode<T>)right.UpdateChildSAbs(owner, rightShift, 0, newChildR, newChildR.TreeSize(RRBMath.Down(rightShift)));
                return shrinkLeftNode(parentL.ConcatNodes(owner, shift, parentR));
            }
        }
        else if (shift < rightShift)
        {
            // We are at different heights, and right tree is taller than left tree: descend one level in right tree and recurse
            var firstChildR = (RRBFullNode<T>)right.FirstChild;
            var (newLeft, newRight) = this.MergeTree(owner, shift, tailOpt, RRBMath.Down(rightShift), firstChildR, shouldKeepExpandedLeftNode);

            if (newRight == null)
            {
                // Left tree merged entirely into right, so we will not grow
                var newChild = (RRBFullNode<T>)newLeft;
                // TODO: Same as above, delete this and see what tests fail, then create targeted regression test for specific scenario
                if (right.NodeSize > 1)
                {
                    newChild = (RRBFullNode<T>)newChild.ShrinkRightSpine(owner, RRBMath.Down(rightShift));
                }

                var parentR = right.UpdateChildSAbs(owner, rightShift, 0, newChild, newChild.TreeSize(RRBMath.Down(rightShift)));
                // TODO: Likewise test whether shrinking parentR is needed, by seeing what fails if we don't shrink here
                return shrinkLeftNode((parentR, null));
            }
            else
            {
                // Right was too full to entirely accept left, new left parent will be needed at right's height
                var parentL = (RRBFullNode<T>)this.NewParent(owner, RRBMath.Down(rightShift), new[] { newLeft });
                var childR = (RRBFullNode<T>)newRight;

                // TODO: Same as above, delete this and see what tests fail, then create targeted regression test for specific scenario
                // NOTE: I've already proven that at some point, but I forget which test fails when this step isn't done. Make a note of it in the test itself.
                if (right.NodeSize > 1)
                {
                    childR = (RRBFullNode<T>)childR.ShrinkRightSpine(owner, RRBMath.Down(rightShift));
                }

                var parentR = (RRBFullNode<T>)right.UpdateChildSAbs(owner, rightShift, 0, childR, childR.TreeSize(RRBMath.Down(rightShift)));
                return shrinkLeftNode(parentL.ConcatNodes(owner, rightShift, parentR));
            }
        }
        else // If we get here then shift > rightShift
        {
            // We are at different heights, and left tree is taller than right tree: descend one level in left tree and recurse
            var childL = (RRBFullNode<T>)this.LastChild;
            var (newLeft, newRight) = childL.MergeTree(owner, RRBMath.Down(shift), tailOpt, rightShift, right, shouldKeepExpandedLeftNode);

            if (newRight == null)
            {
                // Right tree merged entirely into left, so we will not grow
                var parentL = this.UpdateChildSAbs(owner, shift, this.NodeSize - 1, newLeft, newLeft.TreeSize(RRBMath.Down(shift)));
                return shrinkLeftNode((parentL, null));
            }
            else
            {
                var parentL = (RRBFullNode<T>)this.UpdateChildSAbs(owner, shift, this.NodeSize - 1, newLeft, newLeft.TreeSize(RRBMath.Down(shift)));
                var parentR = (RRBFullNode<T>)this.NewParent(owner, RRBMath.Down(shift), new[] { newRight });
                return shrinkLeftNode(parentL.ConcatNodes(owner, shift, parentR));
            }
        }
    }

    public virtual RRBNode<T> NewParent(OwnerToken owner, int shift, RRBNode<T>[] siblings)
    {
        int size = siblings.Length;

        // All but last item of siblings should be shrunk if it used to be an expanded node
        for (int i = 0; i < size - 1; i++)
        {
            siblings[i] = (RRBFullNode<T>)siblings[i].ShrinkRightSpine(owner, shift);
        }

        return RRBNode<T>.MkNode(owner, RRBMath.Up(shift), siblings);
    }
}
