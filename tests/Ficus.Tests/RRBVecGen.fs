module ExpectoTemplate.RRBVecGen

open Ficus
open Ficus.RRBVector
open Ficus.RRBVector.RRBHelpers
open FsCheck
open Expecto.Logging
open Expecto.Logging.Message

// module Literals = Ficus.Literals

let logger = Log.create "Expecto"

let mkNode<'T> level items =
    if level = 0
        then failwith "Don't call RRBVecGen.mkNode at level 0" // RRBHelpers.mkNode items // Leaves never need to be RRBNodes
        else NodeCreation.mkRRBNode<'T> (ref null) (level * Literals.blockSizeShift) items
let mkLeaf items = mkNode 0 items  // Nope. Do something else now. TODO: Determine what.
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
        | List [] -> h
        | List (x::xs) -> loop (h+1) x
    match treeRepr.Root with
    | List [Int _] -> 0  // Special case: a tree consisting of just one leaf will fit that leaf in the root
    | _ -> loop 0 treeRepr.Root

let arrayToObjArray (arr : 'T[]) = arr |> Array.map box // Works everywhere

// Generate trees of arbitrary data types for unit testing (currently a little underused)
let rec genSpecificRRBNode (g : Gen<'T>) level (childSizes:ListItem) = gen {
    let childrenGenerators =
        match childSizes with
        | Int n -> g |> Gen.arrayOfLength n |> Gen.map (arrayToObjArray >> mkLeaf) |> List.singleton
        | List items -> items |> List.map (genSpecificRRBNode g (level-1))
    let! children = Gen.sequence childrenGenerators  // TODO: Determine why Gen.sequenceToArr was marked "Not intended for use from F#" -- because it would be an optimization here
    let node = children |> Array.ofList |> arrayToObjArray |> mkNode<'T> level
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
    | List items when items |> List.forall (function Int _ -> true | _ -> false) -> // Singleton root node at level 0 gets treated as a leaf
        items
        |> Array.ofList
        |> Array.map (fun (Int n) ->
            itemSrc |> mkArray n |> box)
        |> mkNode<int> 1
    | _ -> failwith "Incorrect twig spec"
let rec mkSpecificRRBNode itemSrc level (childSizes:ListItem) =
    if level = 1 then
        match childSizes with
        | Int n -> failwith "Individual ints should have been handled as part of their component lists"
        | List items when items |> List.forall (function Int _ -> true | _ -> false) -> // Singleton root node at level 0 gets treated as a leaf
            mkSpecificRRBTwig itemSrc childSizes
        | List _ ->
            failwith "Shouldn't be at level 0 here"
    else
        match childSizes with
        | Int _ -> failwith "Ints in tree reprs should only be found at level 0"
        | List items ->
            items
            |> Array.ofList
            |> Array.map (mkSpecificRRBNode itemSrc (level-1) >> box)
            |> mkNode<int> level

let mkSpecificTree treeRepr =
    let height = treeReprHeight treeRepr
    let items = mkCounter()
    let tailSize = defaultArg treeRepr.Tail 1  // Tail required to be present, so default to 1 if not specified
    if height = 0 then
        let rootSize = match treeRepr.Root with
                       | Int n -> n
                       | List [Int n] -> n
                       | _ -> 0
        let root = items |> mkArray rootSize
        let tail = items |> mkArray tailSize
        RRBSapling(rootSize + tailSize, 0, root, tail, rootSize) :> RRBVector<int>
    else
        let rootNode = mkSpecificRRBNode items height treeRepr.Root
        let tailItems = items |> mkArray tailSize
        let vecLen = peek items
        let tail = tailItems
        RRBTree(vecLen, height * Literals.blockSizeShift, rootNode, tail, vecLen - tailSize) :> RRBVector<int>

let treeReprStrToVec s =
    let treeRepr =
        match FParsec.CharParsers.run pTreeRepr s with
        | FParsec.CharParsers.ParserResult.Success(treeRepr, _, _) -> treeRepr
        | FParsec.CharParsers.ParserResult.Failure(message, _, _) -> failwith message
    let vec = mkSpecificTree treeRepr
    vec

let strJoin (between:string) (parts:#seq<string>) =
    System.String.Join(between,parts)

let leafToTreeReprStr (leaf : 'T []) =
    match leaf.Length with
    | x when x >= Literals.blockSize -> "M"
    | x when x  = Literals.blockSize - 1 -> "M-1"
    // | x when x  = Literals.blockSize - 2 -> "M-2"
    // | x when x  = Literals.blockSize - 3 -> "M-3"
    | x -> x.ToString()

let rec nodeToTreeReprStr<'T> level (node : obj []) =
    if level > 1 then
        node |> Seq.cast<Node> |> Seq.map (fun node -> nodeToTreeReprStr (level - 1) node.Array) |> strJoin " " |> sprintf "[%s]"
    elif level = 1 then
        node |> Seq.cast<'T []> |> Seq.map leafToTreeReprStr |> strJoin " " |> sprintf "[%s]"
    else
        leafToTreeReprStr node

let vecToTreeReprStr (vec : RRBVector<'T>) =
    if vec.Length <= 0 then "<emptyVec>" else
    let rootRepr, tail =
        match vec with
        | :? RRBSapling<'T> as sapling -> sapling.Root |> Array.map box |> nodeToTreeReprStr<'T> 0, sapling.Tail
        | :? RRBTree<'T> as tree ->
            let rootRepr =
                tree.Root.Array
                |> nodeToTreeReprStr<'T> (tree.Shift / Literals.blockSizeShift)
                |> fun s -> s.[1..s.Length-2] // Strip brackets from outermost list
            rootRepr, tree.Tail
    (sprintf "%s T%d" rootRepr tail.Length).Trim()

let genSpecificTree g treeRepr =
    failwith "Not implemented yet"  // TODO: Write if desired

let rec genRRBNode (g : Gen<'a>) level childCount = gen {
    let! slotCount = genSlotCount childCount
    let! childSizes = distribute slotCount childCount
    let childrenGenerators =
        if level <= 1 then
            childSizes |> Array.map (fun n -> g |> Gen.arrayOfLength n |> Gen.map box)
        else
            childSizes |> Array.map (genRRBNode g (level - 1) >> Gen.map box)
    let! children = Gen.sequence childrenGenerators
    let node = children |> Array.ofList |> mkNode<'a> level
    return node
}

let rec genFullNode (g : Gen<'a>) level childCount = gen {
    let childSizes = Array.create childCount Literals.blockSize
    let children =
        if level <= 1 then
            childSizes |> Array.map (fun n -> g |> Gen.arrayOfLength n |> Gen.map box)
        else
            childSizes |> Array.map (genFullNode g (level - 1) >> Gen.map box)
    let! node = children |> Gen.sequence |> Gen.map (Array.ofList >> mkNode<'a> level)
    return node
}

let genNode (g : Gen<'a>) level childCount =
    Gen.frequency [3, genRRBNode<'a> g level childCount
                   1, genFullNode<'a> g level childCount]
    // Ratio is 3:1 because when you generate a full node, it's full all the way down to the leaves.

let genTinyVec<'a> size =
    // For sizes from 0 to M*2, call this function
    // No randomness here
    Arb.generate<'a> |> Gen.arrayOfLength size |> Gen.map RRBVector.ofArray

// Invariant that the append algorithm needs: rightmost leaf is full if its parent is leftwise dense
// NOTE: We're fulfilling a strictly greater invariant here.
// TODO: See if we can relax this invariant just a *little* bit (check if the parent is leftwise dense), in order to test more scenarios
let fleshOutRightmostLeafIfNecessary<'a> g level root = gen {
    let twig = root |> getRightmostTwig (level * Literals.blockSizeShift)
    let leaf = (Array.last twig.Array) :?> 'a []
    let missingItemCount = Literals.blockSize - leaf.Length
    if missingItemCount <= 0 then
        return root
    else
        let! newLeaf = gen {
            let! newItems = Gen.arrayOfLength missingItemCount g
            return (Array.append leaf newItems)
        }
        return root |> replaceLastLeaf (level * Literals.blockSizeShift) newLeaf
}

let genVec<'a> level childCount =
    if level <= 0 then failwith "genVec needs to have level 1+. For level 0, use genTinyVec"
    if childCount < 2 then failwith "genVec needs to have childCount 2+. For level 0, use genTinyVec"
    gen {
        let g = Arb.generate<'a>
        let! root = genNode g level childCount
        let! root' = root |> fleshOutRightmostLeafIfNecessary g level
        let! tailSize = Gen.choose(1,Literals.blockSize)
        let! tail = Gen.arrayOfLength tailSize g
        let shift = level * Literals.blockSizeShift
        let rootSize = NodeCreation.treeSize<'a> shift root'
        let totalSize = rootSize + Array.length tail
        let vec = RRBTree<'a>(totalSize, shift, root', tail, rootSize) :> RRBVector<'a>
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
    return vec
}
