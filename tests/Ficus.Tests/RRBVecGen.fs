module ExpectoTemplate.RRBVecGen

open Ficus
open Ficus.RRBVector
open Ficus.RRBVector.RRBHelpers
open FsCheck
open Expecto.Logging
open Expecto.Logging.Message

// module Literals = Ficus.Literals

let logger = Log.create "Expecto"

let mkNode level items =
    if level = 0
        then RRBHelpers.mkNode items // Leaves never need to be RRBNodes
        else RRBHelpers.mkRRBNode (level * Literals.blockSizeShift) items
let mkLeaf items = mkNode 0 items
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

// TODO: Determine which of these two functions (gen or mk) we'll want to use in the future
// TODO: If it's mk, then we can probably separate this part out into its own module (TreeRepr, perhaps)
let rec genSpecificRRBNode g level (childSizes:ListItem) = gen {
    let childrenGenerators =
        match childSizes with
        | Int n -> g |> Gen.arrayOfLength n |> Gen.map (arrayToObjArray >> mkLeaf) |> List.singleton
        | List items -> items |> List.map (genSpecificRRBNode g (level-1))
    let! children = Gen.sequence childrenGenerators  // TODO: Determine why Gen.sequenceToArr was marked "Not intended for use from F#" -- because it would be an optimization here
    let node = children |> Array.ofList |> arrayToObjArray |> mkNode level
    return node
}

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
let rec mkSpecificRRBNode itemSrc level (childSizes:ListItem) =
    match childSizes with
    | Int n ->
        itemSrc
        |> mkArray n
        |> arrayToObjArray
        |> mkLeaf
    | List [Int n] when level = 0 -> // Singleton root node at level 0 gets treated as a leaf
        itemSrc
        |> mkArray n
        |> arrayToObjArray
        |> mkLeaf
    | List items ->
        items
        |> Array.ofList
        |> Array.map (mkSpecificRRBNode itemSrc (level-1))
        |> arrayToObjArray
        |> mkNode level

let mkSpecificTree treeRepr =
    let height = treeReprHeight treeRepr
    let items = mkCounter()
    let rootNode = mkSpecificRRBNode items height treeRepr.Root
    let tailSize = defaultArg treeRepr.Tail 1  // Tail required to be present, so default to 1 if not specified
    let tailItems = items |> mkArray tailSize
    let vecLen = peek items
    let tail = tailItems |> arrayToObjArray
    RRBVector(vecLen, height * Literals.blockSizeShift, rootNode, tail)

let treeReprStrToVec s =
    let treeRepr =
        match FParsec.CharParsers.run pTreeRepr s with
        | FParsec.CharParsers.ParserResult.Success(treeRepr, _, _) -> treeRepr
        | FParsec.CharParsers.ParserResult.Failure(message, _, _) -> failwith message
    let vec = mkSpecificTree treeRepr
    vec

let strJoin (between:string) (parts:#seq<string>) =
    System.String.Join(between,parts)

let rec nodeToTreeReprStr level (node : Node) =
    if level > 0 then
        node.Array |> Seq.cast |> Seq.map (nodeToTreeReprStr (level - 1)) |> strJoin " " |> sprintf "[%s]"
    else
        match node.Array.Length with
        | x when x >= Literals.blockSize -> "M"
        | x when x  = Literals.blockSize - 1 -> "M-1"
        // | x when x  = Literals.blockSize - 2 -> "M-2"
        // | x when x  = Literals.blockSize - 3 -> "M-3"
        | x -> x.ToString()

let vecToTreeReprStr (vec : RRBVector<'T>) =
    if vec.Length <= 0 then "<emptyVec>" else
    let rootRepr =
        vec.Root
        |> nodeToTreeReprStr (vec.Shift / Literals.blockSizeShift)
        |> (if vec.Shift <= 0 then id else fun s -> s.[1..s.Length-2]) // Strip brackets from outermost list... if there *is* an outermost list
    (sprintf "%s T%d" rootRepr vec.Tail.Length).Trim()
let genSpecificTree g treeRepr =
    failwith "Not implemented yet"  // TODO: Write if desired

let rec genRRBNode g level childCount = gen {
    let! slotCount = genSlotCount childCount
    let! childSizes = distribute slotCount childCount
    logger.verbose(
        eventX "genRRBNode called for level {level} with childCount {childCount}. Calculated slotCount {slotCount} and childSizes {childSizes}"
        >> setField "level" level
        >> setField "childCount" childCount
        >> setField "slotCount" slotCount
        >> setField "childSizes" (sprintf "%A" childSizes)
    )
    let childrenGenerators =
        if level <= 1 then
            childSizes |> Array.map (fun n -> g |> Gen.arrayOfLength n |> Gen.map (arrayToObjArray >> mkLeaf))
        else
            childSizes |> Array.map (genRRBNode g (level - 1))
    let! children = Gen.sequence childrenGenerators
    logger.verbose(
        eventX "genRRBNode called for level {level} with childCount {childCount}. Calculated slotCount {slotCount} and children:\n{children}"
        >> setField "level" level
        >> setField "childCount" childCount
        >> setField "slotCount" slotCount
        >> setField "children" (sprintf "%A" children)
    )
    let node = children |> Array.ofList |> arrayToObjArray |> mkNode level
    logger.verbose (
        eventX "genRRBNode at level {level} with childCount {childCount} generated node:\n{node}"
        >> setField "level" level
        >> setField "childCount" childCount
        >> setField "node" (sprintf "%A" node)
    )
    return node
}

let rec genFullNode g level childCount = gen {
    let childSizes = Array.create childCount Literals.blockSize
    let children =
        if level <= 1 then
            childSizes |> Array.map (fun n -> g |> Gen.arrayOfLength n |> Gen.map (arrayToObjArray >> mkLeaf))
        else
            childSizes |> Array.map (genFullNode g (level - 1))
    return! children |> Gen.sequence |> Gen.map (Array.ofList >> arrayToObjArray >> mkNode level)
}

let genNode g level childCount =
    Gen.frequency [3, genRRBNode g level childCount
                   1, genFullNode g level childCount]
    // Ratio is 3:1 because when you generate a full node, it's full all the way down to the leaves.

let genTinyVec<'a> size =
    // For sizes from 0 to M*2, call this function
    // No randomness here
    Arb.generate<'a> |> Gen.arrayOfLength size |> Gen.map RRBVector.ofArray

// Invariant that the append algorithm needs: rightmost leaf is full if its parent is leftwise dense
// NOTE: We're fulfilling a strictly greater invariant here.
// TODO: See if we can relax this invariant just a *little* bit (check if the parent is leftwise dense), in order to test more scenarios
let fleshOutRightmostLeafIfNecessary g level root = gen {
    let leaf = if level <= 0 then root else root |> getRightmostTwig (level * Literals.blockSizeShift) |> getLastChildNode
    let missingItemCount = Literals.blockSize - leaf.Array.Length
    if missingItemCount <= 0 then
        return root
    else
        let! newLeaf = gen {
            let! newItems = Gen.arrayOfLength missingItemCount g
            return mkLeaf (Array.append leaf.Array (arrayToObjArray newItems))
        }
        return root |> replaceLastLeaf (level * Literals.blockSizeShift) newLeaf
}

let genVec<'a> level childCount =
    if level <= 0 then failwith "genVec needs to have level 1+. For level 0, use genTinyVec"
    if childCount < 2 then failwith "genVec needs to have childCount 2+. For level 0, use genTinyVec"
    gen {
        let g = Arb.generate<'a>
        let! root = genNode g level childCount
        logger.verbose (
            eventX "genVec called at level {level} with childCount {childCount} generated root:\n{root}"
            >> setField "level" level
            >> setField "childCount" childCount
            >> setField "root" (sprintf "%A" root)
        )
        let! root' = root |> fleshOutRightmostLeafIfNecessary g level
        logger.verbose (
            eventX "And then root':\n{root}"
            >> setField "root" (sprintf "%A" root')
        )
        let! tailSize = Gen.choose(1,Literals.blockSize)
        let! tail = Gen.arrayOfLength tailSize g
        logger.verbose (
            eventX "And then tail:\n{tail}"
            >> setField "tail" (sprintf "%A" tail)
        )
        let shift = level * Literals.blockSizeShift
        let rootSize = treeSize shift root'
        let totalSize = rootSize + Array.length tail
        let vec = RRBVector<'a>(totalSize, shift, root', tail |> arrayToObjArray, rootSize)
        logger.verbose (
            eventX "And then vec:\n{vec}"
            >> setField "vec" (sprintf "%A" vec)
        )
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
