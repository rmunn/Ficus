module Ficus.Tests.RRBVectorGen

open Ficus
open Ficus.RRBVectorNodes
open Ficus.RRBVector
open FsCheck
open Expecto.Logging
open Expecto.Logging.Message

// module Literals = Ficus.Literals

let logger = Log.create "Expecto"

let mkLeaf items = RRBNode<'T>.MkLeaf nullOwner items
let mkNode<'T> level items =
    if level = 0
        then failwith "Don't call RRBVectorGen.mkNode at level 0" // Leaves have 'T children, whereas nodes have RRBNode<'T> children
        else RRBNode<'T>.MkNode nullOwner (level * Literals.blockSizeShift) items

let mkEmptyNode<'T>() =
    RRBNode<'T>.MkFullNode nullOwner Array.empty

let minSlots childCount = (childCount - Literals.eMaxPlusOne) * Literals.blockSize + 1 |> max childCount
let maxSlots childCount = childCount * Literals.blockSize

let genSlotCount childCount = Gen.choose(minSlots childCount, maxSlots childCount)

let distribute slots childCount = gen {
    let buckets = Array.create childCount 1
    let mutable remaining = slots - childCount
    let mutable availableBucketIndices = [ 0..childCount - 1 ]
    while remaining > 0 do
        let! chosenIdx = Gen.elements availableBucketIndices
        buckets.[chosenIdx] <- buckets.[chosenIdx] + 1
        remaining <- remaining - 1
        if buckets.[chosenIdx] >= Literals.blockSize then
            availableBucketIndices <- availableBucketIndices |> List.filter ((<>) chosenIdx)
    return buckets
}

open TreeParser // Pull in TreeRepresentation and ListItem types

let treeReprHeight treeRepr =
    let rec loop h = function
        | Int _ -> h
        | List [] -> h+1  // Empty roots are considered height 1
        | List (x::xs) -> loop (h+1) x
    loop 0 treeRepr.Root

// Generate trees of arbitrary data types for unit testing (currently a little underused)
let rec genSpecificRRBNode (g : Gen<'T>) level (childSizes:ListItem) = gen {
    let childrenGenerators =
        match childSizes with
        | Int n -> g |> Gen.arrayOfLength n |> Gen.map mkLeaf |> List.singleton
        | List items -> items |> List.map (genSpecificRRBNode g (level-1))
    let! children = Gen.sequence childrenGenerators  // TODO: Determine why Gen.sequenceToArr was marked "Not intended for use from F#" -- because it would be an optimization here
    let node = children |> Array.ofList |> mkNode<'T> level
    return node
}

// Generate int-based trees for unit testing
let mkCounter() =
    let mutable count = 0
    let incBy n =
        count <- count + n
        count
    incBy

let mkArray n counter =
    let endVal = counter n
    let array = [| (endVal - n) .. (endVal - 1) |]
    array

let next counter = counter 1  // Get next value and update the counter
let peek counter = counter 0  // Returns what the next value would be, but doesn't change the counter

let mkSpecificRRBTwig itemSrc (childSizes:ListItem) =
    match childSizes with
    | List items when items |> List.forall isInt -> // This also succeeds on empty root node, which is good
        items
        |> Array.ofList
        |> Array.map (function | (Int n) -> itemSrc |> mkArray n |> mkLeaf
                               | (List _) -> failwith "Can't happen")
        |> mkNode<int> 1
    | _ -> failwith "Incorrect twig spec"

let rec mkSpecificRRBNode itemSrc level (childSizes:ListItem) =
    if level = 1 then
        match childSizes with
        | Int n -> failwith "Individual ints should have been handled as part of their component lists"
        | List items when items |> List.forall isInt -> // This also succeeds on empty root node, which is good
            mkSpecificRRBTwig itemSrc childSizes
        | List _ ->
            failwith "Shouldn't be at level 1 here"
    else
        match childSizes with
        | Int _ -> failwith "Ints in tree reprs should only be found at level 0"
        | List items ->
            items
            |> Array.ofList
            |> Array.map (mkSpecificRRBNode itemSrc (level-1))
            |> mkNode<int> level

let mkSpecificTree treeRepr =
    let height = treeReprHeight treeRepr
    let items = mkCounter()
    let tailSize = defaultArg treeRepr.Tail 1  // Tail required to be present, so default to 1 if not specified
    // if height = 0 then
    //     // Empty root (tail-only tree) is represented as being of height 1 so that the root is never a leaf
    //     let tail = items |> mkArray tailSize
    //     RRBPersistentVector(tailSize, Literals.blockSizeShift, mkEmptyNode(), tail, 0) :> RRBVector<_>
    // else
    let rootNode = mkSpecificRRBNode items height treeRepr.Root
    let tailItems = items |> mkArray tailSize
    let vecLen = peek items
    let tail = tailItems
    RRBPersistentVector(vecLen, height * Literals.blockSizeShift, rootNode, tail, vecLen - tailSize) :> RRBVector<_>

let treeReprStrToVec s =
    let treeRepr =
        match FParsec.CharParsers.run pTreeRepr s with
        | FParsec.CharParsers.ParserResult.Success(treeRepr, _, _) -> treeRepr
        | FParsec.CharParsers.ParserResult.Failure(message, _, _) -> failwith message
    let vec = mkSpecificTree treeRepr
    vec

let looserTreeReprStrToVec =
    let regex = System.Text.RegularExpressions.Regex(@"\s+")
    fun (s : string) ->
        regex.Replace(s.Trim().Replace("\n", " "), " ") |> treeReprStrToVec

let strJoin (between:string) (parts:#seq<string>) =
    System.String.Join(between,parts)

let sizeToTreeReprStr = function
    | i when i >= Literals.blockSize -> "M"
    | i when i  = Literals.blockSize - 1 -> "M-1"
    // | i when i  = Literals.blockSize - 2 -> "M-2"
    // | i when i  = Literals.blockSize - 3 -> "M-3"
    | i -> i.ToString()

let leafToTreeReprStr (leaf : RRBNode<'T>) =
    leaf.NodeSize |> sizeToTreeReprStr

let rec nodeToTreeReprStr<'T> level (node : RRBNode<'T>) =
    if level > 1 then
        let node = node :?> RRBFullNode<'T>
        let children = node.Children |> Seq.truncate node.NodeSize |> Seq.map (fun node -> nodeToTreeReprStr (level - 1) node) |> List.ofSeq
        if children |> List.distinct |> List.length = 1 then
            // All these children are the same
            let mult = children |> List.length |> sizeToTreeReprStr
            let childRepr = children |> List.head
            sprintf "[%s*%s]" childRepr mult
            // TODO: Improve output by detecting "runs" of at least three identical children
        else
            children |> strJoin " " |> sprintf "[%s]"
    elif level = 1 then
        let node = node :?> RRBFullNode<'T>
        let leaves = node.Children |> Seq.truncate node.NodeSize |> List.ofSeq
        if leaves |> List.distinctBy (fun leaf -> leaf.NodeSize) |> List.length = 1 then
            // All these leaves are the same
            let mult = leaves |> List.length |> sizeToTreeReprStr
            let leafSize = (leaves |> List.head).NodeSize |> sizeToTreeReprStr
            sprintf "[%s*%s]" leafSize mult
        else
            leaves |> Seq.map leafToTreeReprStr |> strJoin " " |> sprintf "[%s]"
    else
        leafToTreeReprStr node

let vecToTreeReprStr (vec : RRBVector<'T>) =
    if vec.Length <= 0 then "<emptyVec>" else
    let root, shift, tailLen =
        match vec with
        | :? RRBPersistentVector<'T> as v -> v.Root, v.Shift, (v.Length - v.TailOffset)
        | :? RRBTransientVector<'T>  as v -> v.Root, v.Shift, (v.Length - v.TailOffset)
        | _ -> failwith "Unknown vector type"
    let rootRepr =
        root
        |> nodeToTreeReprStr<'T> (shift / Literals.blockSizeShift)
        |> fun s -> s.[1..s.Length-2] // Strip brackets from outermost list
    (sprintf "%s T%d" rootRepr tailLen).Trim()

let genSpecificTree g treeRepr =
    failwith "Not implemented yet"  // TODO: Write if desired

let rec genRRBNode (g : Gen<'a>) level childCount = gen {
    let! slotCount = genSlotCount childCount
    let! childSizes = distribute slotCount childCount
    let childrenGenerators =
        if level > 1 then
            childSizes |> Array.map (genRRBNode g (level - 1))
        else
            childSizes |> Array.map (fun n -> g |> Gen.arrayOfLength n |> Gen.map mkLeaf)
    let! children = Gen.sequence childrenGenerators
    let node = children |> Array.ofList |> mkNode<'a> level
    return node
}

let rec genFullNode (g : Gen<'a>) level childCount = gen {
    let childSizes = Array.create childCount Literals.blockSize
    let childrenGenerators =
        if level > 1 then
            childSizes |> Array.map (genFullNode g (level - 1))
        else
            childSizes |> Array.map (fun n -> g |> Gen.arrayOfLength n |> Gen.map mkLeaf)
    let! children = Gen.sequence childrenGenerators
    let node = children |> Array.ofList |> mkNode<'a> level
    return node
}

// TODO: Those two are practically identical. Refactor with "genSpecificNode g level childSizes" as the common factor between them

let genNode (g : Gen<'a>) level childCount =
    Gen.frequency [3, genRRBNode g level childCount
                   1, genFullNode g level childCount]
    // Ratio is 3:1 because when you generate a full node, it's full all the way down to the leaves.

let genTinyVec<'a> size =
    // For sizes from 0 to M*2, call this function
    // No randomness here
    Arb.generate<'a> |> Gen.arrayOfLength size |> Gen.map RRBVector.ofArray

// Invariant that the append algorithm needs: rightmost leaf is full if its parent is full
let rec getRightTwig shift (node : RRBFullNode<'T>) =
    if shift <= Literals.blockSizeShift then node
    else (node.LastChild :?> RRBFullNode<'T>) |> getRightTwig (RRBMath.down shift)

let fleshOutRightmostLeafIfNecessary g level (root : RRBFullNode<'a>) = gen {
    let twig = root |> getRightTwig (level * Literals.blockSizeShift)
    let leaf = twig.LastChild
    let missingItemCount = Literals.blockSize - leaf.NodeSize
    if missingItemCount <= 0 || twig :? RRBRelaxedNode<'a> then
        return root
    else
        let! newLeaf = gen {
            let! newItems = Gen.arrayOfLength missingItemCount g
            return (Array.append (leaf :?> RRBLeafNode<'a>).Items newItems) |> mkLeaf
        }
        return (root.ReplaceLastLeaf nullOwner (level * Literals.blockSizeShift) (newLeaf :?> RRBLeafNode<'a>) missingItemCount) :?> RRBFullNode<'a>
}

let genVec<'a> level childCount =
    if level <= 0 then failwith "genVec needs to have level 1+. For level 0, use genTinyVec"
    if childCount < 2 then failwith "genVec needs to have childCount 2+. For level 0, use genTinyVec"
    gen {
        let g = Arb.generate<'a>
        let! root = genNode g level childCount
        let! root' = (root :?> RRBFullNode<'a>) |> fleshOutRightmostLeafIfNecessary g level
        let! tailSize = Gen.choose(1,Literals.blockSize)
        let! tail = Gen.arrayOfLength tailSize g
        let shift = level * Literals.blockSizeShift
        let rootSize = root'.TreeSize shift
        let totalSize = rootSize + Array.length tail
        let vec = RRBPersistentVector<'a>(totalSize, shift, root', tail, rootSize) :> RRBVector<'a>
        return vec
    }

let genVecOfLength<'a> len =
    if len <= Literals.blockSize * 2 then
        len |> genTinyVec<'a>
    else
        let len' = len - Literals.blockSize * 2
        // childCount must be in range 2..Literals.blockSize
        // So we take (Literals.blockSize - 1) and that's the divisor for the mod operation
        let denom = Literals.blockSize - 1
        let level = len' / denom + 1
        let childCount = len' % denom + 2
        genVec<'a> level childCount

let sizedGenVec<'a> = Gen.sized <| fun s -> gen {
    let! len = Gen.choose(0, s)
    let len = (max len 0) % (Literals.blockSize * 5)  // Ensure length is reasonable
    let! vec = genVecOfLength<'a> len
    // let! coinFlip = Arb.generate<bool>
    // return (if coinFlip then vec else (vec :?> RRBPersistentVector<_>).Transient() :> RRBVector<_>)
    return vec
}

// TODO: Write functions that will generate a vector *representation* (in structured form), and use that in our tests
// Then we can FAR more easily write shrinkers that don't depend on the code under test (e.g., to shrink a vector would need to call .Remove)
// And the shrinkers can start by shrinking entire top-level nodes at a time, and if that doesn't work, go on to shrink lower-level nodes

(*
type ListItem =
    | Int of int
    | List of ListItem list

type TreeRepresentation =
    { Root: ListItem
      Tail: int option }
*)

// let genLeafRepr maxSize = Gen.choose(1, maxSize) |> Gen.map ListItem.Int
// let rec genNodeRepr height maxSizes =
//     let maxSize = List.head maxSizes
//     if height <= 0 then genLeafRepr maxSize
//     else
//         gen {
//             let! size = Gen.choose(1, maxSize)
//             let! lst = Gen.listOfLength size (genNodeRepr (height - 1) (List.tail maxSizes))
//             return ListItem.List lst
//         }

let genLeafRepr size = Gen.constant (size |> ListItem.Int)
let rec genNodeReprImpl level childSizes g = gen {
    let childrenGenerators =
        if level > 1 then
            childSizes |> Array.map (g (level - 1))
        else
            childSizes |> Array.map genLeafRepr
    let! children = Gen.sequence childrenGenerators
    let node = children |> ListItem.List
    return node
}

let rec genRelaxedNodeRepr level childCount = gen {
    let! slotCount = genSlotCount childCount
    let! childSizes = distribute slotCount childCount
    return! genNodeReprImpl level childSizes genRelaxedNodeRepr
}  // TODO: This should be able to be turned into a non-CE function

let rec genFullNodeRepr level childCount = gen {
    let childSizes = Array.create childCount Literals.blockSize
    return! genNodeReprImpl level childSizes genFullNodeRepr
}  // TODO: This should be able to be turned into a non-CE function

let genNodeRepr level childCount =
    Gen.frequency [3, genRelaxedNodeRepr level childCount
                   1, genFullNodeRepr level childCount]

let shrinkNodeRepr node =
    match node with
    | Int n -> Arb.shrink n |> Seq.filter (fun n -> n > 0 (* Empty leaves are not allowed *)) |> Seq.map Int
    | List l -> Arb.shrink l |> Seq.filter (not << List.isEmpty) (* Empty nodes are not allowed *) |> Seq.map List

let fleshOutRightmostLeafOfRepr (root : ListItem) =
    let rec getRightmostLeaves root =
        match root with
        | Int n -> None
        | List [] -> None
        | List l when l |> List.forall isInt -> Some l
        | List l -> List.last l |> getRightmostLeaves
    match getRightmostLeaves root with
    | None -> root
    | Some leafList ->
        let leaves = leafList |> Array.ofList
        let len = Array.length leaves
        if len <= 0 || leaves.[len - 1] = Int Literals.blockSize then
            root
        else
            let isFull = leaves |> Seq.take (len - 1) |> Seq.forall ((=) (Int Literals.blockSize))
            if not isFull then
                root
            else
                // Recursively build up a new tree up to the root, with a different leaf count in the end
                // Note that we don't bother adjusting the tail, for simplicity.
                let rec buildNewTree root =
                    match root with
                    | Int _ -> Int Literals.blockSize
                    | List [] -> root // Empty roots shouldn't be touched
                    | List l ->
                        let a = Array.ofList l
                        let i = a.Length - 1
                        a.[i] <- a.[i] |> buildNewTree
                        List (List.ofArray a)
                buildNewTree root

let genTreeRepr level childCount =
    if level <= 0 then failwith "genVec needs to have level 1+. For level 0, use genTinyVec"
    if childCount < 2 then failwith "genVec needs to have childCount 2+. For level 0, use genTinyVec"
    gen {
        let! root = genNodeRepr level childCount
        let root' = root |> fleshOutRightmostLeafOfRepr
        let! tailSize = Gen.choose(1,Literals.blockSize)
        let tree = { Root = root' ; Tail = Some tailSize }
        return tree
    }

let shrinkTreeRoot root =
    shrinkNodeRepr root |> Seq.map fleshOutRightmostLeafOfRepr

let shrinkTreeRepr (tree : TreeRepresentation) =
    let roots = shrinkTreeRoot tree.Root |> Seq.map (fun root -> { tree with Root = root })
    let tailSizes = match tree.Tail with
                    | None -> Seq.empty
                    | Some tailSize -> Arb.shrinkNumber tailSize
    let tails = tailSizes |> Seq.filter (fun n -> n > 0) |> Seq.map (fun size -> { tree with Tail = Some size })
    Seq.append roots tails

let genTreeReprOfLength len =
    if len <= Literals.blockSize then
        Gen.constant { Root = List []; Tail = Some len }
    elif len <= Literals.blockSize * 2 then
        Gen.constant { Root = List [Int Literals.blockSize]; Tail = Some (len - Literals.blockSize) }
    else
        let len' = len - Literals.blockSize * 2
        // childCount must be in range 2..Literals.blockSize
        // So we take (Literals.blockSize - 1) and that's the divisor for the mod operation
        let denom = Literals.blockSize - 1
        let level = len' / denom + 1
        let childCount = len' % denom + 2
        genTreeRepr level childCount

let arbTreeRepr =
    { new Arbitrary<TreeRepresentation>() with
        override x.Generator = Gen.sized genTreeReprOfLength
        override x.Shrinker t = shrinkTreeRepr t }
