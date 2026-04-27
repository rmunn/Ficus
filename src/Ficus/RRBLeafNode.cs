using System;
using System.Linq;
using Ficus.RRBArrayExtensions;

namespace Ficus;

public sealed class RRBLeafNode<T> : RRBNode<T>
{
    private T[] items;

    public RRBLeafNode(OwnerToken ownerToken, T[] items)
        : base(ownerToken)
    {
        this.items = items;
    }

    public T[] Items => items;

    public override int NodeSize => items.Length;
    public override int TreeSize(int _shift) => items.Length;
    public override int SlotCount => NodeSize;
    public override int TwigSlotCount => NodeSize;

#if DEBUG
    public override string StringRepr => $"L{NodeSize}";
#endif

    public override void SetNodeSize(int _) { }

    // Shrink/expand/etc are no-ops for leaf nodes
    public override RRBNode<T> Shrink(OwnerToken owner) => GetEditableNode(owner);
    public override RRBNode<T> Expand(OwnerToken owner) => GetEditableNode(owner);
    public override RRBNode<T> ShrinkRightSpine(OwnerToken owner, int _shift) => GetEditableNode(owner);
    public override RRBNode<T> ShrinkRightSpineOfChild(OwnerToken owner, int _shift) => GetEditableNode(owner);

    public override RRBNode<T> GetEditableNode(OwnerToken owner)
    {
        if (IsEditableBy(owner)) return this;

        return new RRBLeafNode<T>(owner, (T[])items.Clone());
    }

    public T[] GetEditableArrayOfBlockSizeLength(OwnerToken owner)
    {
        if (IsEditableBy(owner)) return items.ExpandToBlockSize(Literals.blockSize);

        // If not editable, we need to guarantee a copy even if we were already blockSize in length
        var fresh = new T[Literals.blockSize];
        Array.Copy(items, fresh, items.Length);
        return fresh;
    }

    public RRBNode<T> LeafNodeWithItems(OwnerToken owner, T[] newItems)
    {
        if (NodeSize == newItems.Length)
        {
            // Let's see if we can skip creating a new leaf object, and just overwrite the existing item array
            var node = (RRBLeafNode<T>)GetEditableNode(owner);
            Array.Copy(newItems, node.items, newItems.Length);
            return node;
        }

        return RRBNode<T>.MkLeaf(owner, newItems);
    }

    public RRBNode<T> UpdatedItem(OwnerToken owner, int localIdx, T newItem)
    {
        var node = (RRBLeafNode<T>)GetEditableNode(owner);
        node.items[localIdx] = newItem;
        return node;
    }

    public override RRBNode<T> UpdatedTree(OwnerToken owner, int shift, int treeIdx, T newItem)
        => UpdatedItem(owner, treeIdx, newItem);

    public RRBNode<T> InsertedItem(OwnerToken owner, int localIdx, T item)
    {
        // Leaf nodes are never expanded and cannot insert in place so there's no point in checking the owner.
        var newItems = RRBArrayExtensions.RRBArrayExtensions.CopyAndInsertAt(items, localIdx, item);
        return new RRBLeafNode<T>(owner, newItems);
    }

    // TODO: Remove this method entirely since we apparently never use it nor test it
    public RRBNode<T> AppendedItem(OwnerToken owner, T item)
    {
        // Leaf nodes are never expanded and cannot insert in place so there's no point in checking the owner.
        var newItems = RRBArrayExtensions.RRBArrayExtensions.CopyAndPush(items, item);
        return new RRBLeafNode<T>(owner, newItems);
    }

    internal override InsertResult<RRBNode<T>> InsertedTree(OwnerToken owner, int shift, int treeIdx, T item, RRBFullNode<T>? parentOpt, int idxOfNodeInParent)
    {
        if (NodeSize < Literals.blockSize)
        {
            // Easiest case
            return InsertResult<RRBNode<T>>.SimpleInsertion(InsertedItem(owner, treeIdx, item));
        }

        int localIdx = treeIdx;

        if (parentOpt is RRBFullNode<T> parent)
        {
            if (idxOfNodeInParent > 0 &&
                parent.Children[idxOfNodeInParent - 1].NodeSize < Literals.blockSize)
            {
                // Room in the left sibling
                var left = (RRBLeafNode<T>)parent.Children[idxOfNodeInParent - 1];

                var (newLeftItems, newRightItems) =
                    RRBArrayExtensions.RRBArrayExtensions.AppendAndInsertAndSplitEvenly(left.Items, items, localIdx + left.NodeSize, item);
                // TODO: Get rid of that ugly double namespace

                var newLeft = RRBNode<T>.MkLeaf(owner, newLeftItems);
                var newRight = LeafNodeWithItems(owner, newRightItems);

                return InsertResult<RRBNode<T>>.SlidItemsLeft(newLeft, newRight);
            }

            if (idxOfNodeInParent < parent.NodeSize - 1 &&
                parent.Children[idxOfNodeInParent + 1].NodeSize < Literals.blockSize)
            {
                // Room in the right sibling
                var right = (RRBLeafNode<T>)parent.Children[idxOfNodeInParent + 1];

                var (newLeftItems, newRightItems) =
                    RRBArrayExtensions.RRBArrayExtensions.AppendAndInsertAndSplitEvenly(items, right.Items, localIdx, item);

                // var newLeft = RRBNode<T>.MkLeaf(owner, newLeftItems);
                var newLeft = LeafNodeWithItems(owner, newLeftItems); // TODO: Verify that this doesn't cause bugs
                var newRight = RRBNode<T>.MkLeaf(owner, newRightItems);

                return InsertResult<RRBNode<T>>.SlidItemsRight(newLeft, newRight);
            }
        }

        // No parent, so we have to make one
        var (l, r) = RRBArrayExtensions.RRBArrayExtensions.InsertAndSplitEvenly(items, localIdx, item);

        return InsertResult<RRBNode<T>>.SplitNode(
            RRBNode<T>.MkLeaf(owner, l),
            LeafNodeWithItems(owner, r)
        );
    }

    // TODO: Check whether we actually use this at all, then remove it if it turns out we don't
    public (T item, RRBNode<T> node) PopLastItem(OwnerToken owner)
    {
        var item = items[^1];
        var newItems = RRBArrayExtensions.RRBArrayExtensions.CopyAndPop(items);
        return (item, RRBNode<T>.MkLeaf(owner, newItems));
    }

    public override T GetTreeItem(int _shift, int localIdx) => items[localIdx];

    // TODO: Consider whether this.RemovedItem should mirror PopLastItem (i.e. return the removed item) or not.
    // There's not nearly as much demand for RemoveAndReturn from the middle of a list as there is to pop the last item.
    public override RRBNode<T> RemovedItem(OwnerToken owner, int shift, bool shouldCheckForRebalancing, int localIdx)
    {
        var newItems = RRBArrayExtensions.RRBArrayExtensions.CopyAndRemoveAt(items, localIdx);
        return RRBNode<T>.MkLeaf(owner, newItems);
    }

    public override RRBNode<T> KeepNTreeItems(OwnerToken owner, int shift, int treeIdx)
    {
        var newItems = RRBArrayExtensions.RRBArrayExtensions.Truncate(items, treeIdx);
        return RRBNode<T>.MkLeaf(owner, newItems);
    }

    public override RRBNode<T> SkipNTreeItems(OwnerToken owner, int shift, int treeIdx)
    {
        var newItems = RRBArrayExtensions.RRBArrayExtensions.Skip(items, treeIdx);
        return RRBNode<T>.MkLeaf(owner, newItems);
    }
    // TODO: Might be an efficiency gain to be had by special-casing those to check if we already have the right number of items,
    // and just returning self if we do. But that check is probably already done further up.
    // TODO: So put in an #if DEBUG section here that will catch that possibility and throw if it happens. Then run the tests and see
    // if it ever comes up. We might not have to worry about it at all.

    public override (RRBNode<T> Left, RRBNode<T> Right) SplitTree(OwnerToken owner, int shift, int treeIdx)
    {
        var leftArr = RRBArrayExtensions.RRBArrayExtensions.Truncate(items, treeIdx);
        var rightArr = RRBArrayExtensions.RRBArrayExtensions.Skip(items, treeIdx);

        return (
            RRBNode<T>.MkLeaf(owner, leftArr),
            RRBNode<T>.MkLeaf(owner, rightArr)
        );
    }
}

// TODO: Decide where to keep the emptyNode constant
