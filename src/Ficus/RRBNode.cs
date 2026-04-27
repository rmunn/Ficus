using System;
using System.Runtime.CompilerServices;

namespace Ficus;

// Summary of type hierarchy of nodes
//
// type RRBNode<'T> =
//     | RRBFullNode of children : Node<'T>[]
//         with subclass RRBExpandedFullNode of Node<'T>[] * realSize : int
//     | RRBRelaxedNode of children : Node<'T>[] * sizeTable : int[]
//         with subclass RRBExpandedRelaxedNode of Node<'T>[] * sizeTable : int[] * realSize : int
//     | RRBLeafNode of items : 'T[]
//         with subclass RRBExpandedLeafNode of items : 'T[] * realSize : int
//
// The file is organized with the "compact" nodes together, and the "expanded" nodes together in a lower section.

// Descendants:
// - RRBFullNode (and subclass RRBExpandedFullNode)
// - RRBRelaxedNode (and subclass RRBExpandedRelaxedNode)
// - RRBLeafNode (and subclass RRBExpandedLeafNode)

// The "expanded" variants have a children array of maxiumum size, with a RealSize property to let us know how many we really have
// The "relaxed" variants have a size table (the "full" variants are a completely full subtree so no size table needed)

public abstract class RRBNode<T>
{
    // TODO: Verify OwnerToken logic via existing unit tests
    internal OwnerToken ownerToken;

    public OwnerToken Owner
    {
        get => ownerToken;
        set => ownerToken = value;
    }

    internal RRBNode(OwnerToken ownerToken)
    {
        this.ownerToken = ownerToken;
    }

    public abstract RRBNode<T> Shrink(OwnerToken owner);
    public abstract RRBNode<T> Expand(OwnerToken owner);
    public abstract RRBNode<T> ShrinkRightSpine(OwnerToken owner, int shift);
    public abstract RRBNode<T> ShrinkRightSpineOfChild(OwnerToken owner, int shift);

#if DEBUG
    public abstract string StringRepr { get; }
#endif

    public abstract int NodeSize { get; } // How many children does this single node have?
    public abstract int TreeSize(int shift); // How many total items are found in this node's entire descendant tree?
    public abstract int SlotCount { get; } // Used in rebalancing; the "slot count" is the total of the node sizes of this node's children
    public abstract int TwigSlotCount { get; } // Like SlotCount, but used when we *know* this node is a twig and its children are leaves, which allows some optimizations

    public abstract void SetNodeSize(int size);

    public abstract RRBNode<T> GetEditableNode(OwnerToken owner);

    public bool IsEditableBy(OwnerToken owner)
    {
        return ReferenceEquals(owner, ownerToken)
               && owner.Value != null;
    }

    // ---- Node operations ----

    public static void PopulateSizeTableS(
        int shift,
        RRBNode<T>[] array,
        int len,
        int[] sizeTable)
    {
        int total = 0;

        for (int i = 0; i < len; i++)
        {
            total += array[i].TreeSize(RRBMath.Down(shift));
            sizeTable[i] = total;
        }
    }

    public static int[] CreateSizeTableS(int shift, RRBNode<T>[] array, int len)
    {
        var sizeTable = new int[len];
        PopulateSizeTableS(shift, array, len, sizeTable);
        return sizeTable;
    }

    public static int[] CreateSizeTable(int shift, RRBNode<T>[] array)
    {
        return CreateSizeTableS(shift, array, array.Length);
    }

    // TODO: Have this one return an RRBLeafNode rather than an RRBNode, we can upcast it to the parent if we need to but having a child type is useful to avoid a bunch of downcasts
    // If the downcast would ever fail, the compiler can tell us about it
    public static RRBNode<T> MkLeaf(OwnerToken owner, T[] items)
    {
        return new RRBLeafNode<T>(owner, items);
    }

    public static RRBNode<T> MkNode(OwnerToken owner, int shift, RRBNode<T>[] children)
    {
        return RRBRelaxedNode<T>.Create(owner, shift, children);
    }

    public static RRBNode<T> MkNodeKnownSize(
        OwnerToken owner,
        int shift,
        RRBNode<T>[] children,
        int[] sizeTable)
    {
        return RRBRelaxedNode<T>.CreateWithSizeTable(owner, shift, children, sizeTable);
    }

    public static RRBNode<T> MkFullNode(OwnerToken owner, RRBNode<T>[] children)
    {
        return RRBFullNode<T>.Create(owner, children);
    }

    // ---- Tree operations ----

    public abstract RRBNode<T> UpdatedTree(
        OwnerToken owner,
        int shift,
        int treeIdx,
        T newItem);

    internal abstract InsertResult<RRBNode<T>> InsertedTree(
        OwnerToken owner,
        int shift,
        int treeIdx,
        T item,
        RRBFullNode<T>? parentOpt,
        int idxOfNodeInParent);

    // TODO: Rename to RemovedTree for consistency??
    public abstract RRBNode<T> RemovedItem(
        OwnerToken owner,
        int shift,
        bool isTopLevel,
        int treeIdx);

    public abstract T GetTreeItem(int shift, int treeIdx);

    public abstract RRBNode<T> KeepNTreeItems(
        OwnerToken owner,
        int shift,
        int n);

    public abstract RRBNode<T> SkipNTreeItems(
        OwnerToken owner,
        int shift,
        int n);

    public abstract (RRBNode<T> Left, RRBNode<T> Right) SplitTree(
        OwnerToken owner,
        int shift,
        int idx);

    // TODO: Combine into one overload since we're in C# world now
    public bool NeedsRebalance(int shift)
    {
        int slots = shift > Literals.shiftSize ? SlotCount : TwigSlotCount;

        return slots <= ((NodeSize - Literals.eMaxPlusOne) << Literals.shiftSize);
    }

    public bool NeedsRebalance2(int shift, RRBNode<T> right)
    {
        int slots = shift > Literals.shiftSize
            ? SlotCount + right.SlotCount
            : TwigSlotCount + right.TwigSlotCount;

        return slots <= ((NodeSize + right.NodeSize - Literals.eMaxPlusOne) << Literals.shiftSize);
    }

    public bool NeedsRebalance2PlusLeaf(int shift, int leafLen, RRBNode<T> right)
    {
        int slots = TwigSlotCount + leafLen + right.TwigSlotCount;

        return slots <= ((NodeSize + 1 + right.NodeSize - Literals.eMaxPlusOne) << Literals.shiftSize);
    }
}
