module Ficus.Tests.RRBVectorExpectoTest

open Expecto
open Ficus
open Ficus.RRBArrayExtensions
open Ficus.RRBVectorNodes
open Ficus.RRBVector
open FsCheck
open Expecto.Logging
open Expecto.Logging.Message

let logger = Log.create "Expecto"

// For various tests, we'll want to generate a list and an index of an item within that list.
// Note that if the list is empty, the index will be 0, which is not a valid index of an
// empty list. So for some tests, we'll want to use arbNonEmptyArrayAndIdx instead.
type ArrayAndIdx = ArrayAndIdx of arr:int[] * idx:int
type NonEmptyArrayAndIdx = NonEmptyArrayAndIdx of arr:int[] * idx:int
type VecPlusArrAndIdx = VecPlusArrAndIdx of vec:RRBVector<int> * arr:int[] * idx:int
type NonEmptyVecPlusArrAndIdx = NonEmptyVecPlusArrAndIdx of vec:RRBVector<int> * arr:int[] * idx:int
type VecAndIdx = VecAndIdx of vec:RRBVector<int> * idx:int
type NonEmptyVecAndIdx = NonEmptyVecAndIdx of vec:RRBVector<int> * idx:int

// TODO: Determine if we can use Arb.from<PositiveInt> here, or Gen.nonEmptyListOf, or something
let genArray = Gen.sized <| fun s -> gen {
    let! arr = Gen.arrayOfLength s (Gen.choose (1,100))
    let! idx = Gen.choose (0, Operators.max 0 (Array.length arr - 1))
    return ArrayAndIdx (arr,idx)
}
let genArraySimpler = gen {
    let! arr = Gen.arrayOf (Gen.choose (1,100))
    let! idx = Gen.choose (0, Operators.max 0 (Array.length arr - 1))
    return ArrayAndIdx (arr,idx)
}
let genNonEmptyArray = Gen.sized <| fun s -> gen {
    let! arr = Gen.arrayOfLength (Operators.max s 1) (Gen.choose (1,100))
    let! idx = Gen.choose (0, Operators.max 0 (Array.length arr - 1))
    return NonEmptyArrayAndIdx (arr,idx)
}
let genNonEmptyArraySimpler = gen {
    let! lst = Gen.nonEmptyListOf (Gen.choose (1,100))
    let arr = Array.ofList lst
    let! idx = Gen.choose (0, Operators.max 0 (Array.length arr - 1))
    return NonEmptyArrayAndIdx (arr,idx)
}
let mapArrAndIdxToVec (ArrayAndIdx (arr,idx)) = VecPlusArrAndIdx (RRBVector.ofArray arr,arr,idx)
let mapNEArrAndIdxToNEVec (NonEmptyArrayAndIdx (arr,idx)) = NonEmptyVecPlusArrAndIdx (RRBVector.ofArray arr,arr,idx)

let mapArrAndIdxToVecAndIdx (ArrayAndIdx (arr,idx)) = VecAndIdx (RRBVector.ofArray arr,idx)
let mapNEArrAndIdxToNEVecAndIdx (NonEmptyArrayAndIdx (arr,idx)) = NonEmptyVecAndIdx (RRBVector.ofArray arr,idx)

let genVecPlusArrAndIdx = genArraySimpler |> Gen.map mapArrAndIdxToVec
let genNonEmptyVecPlusArrAndIdx = genNonEmptyArraySimpler |> Gen.map mapNEArrAndIdxToNEVec
let genVecAndIdx = genArraySimpler |> Gen.map mapArrAndIdxToVecAndIdx
let genNonEmptyVecAndIdx = genNonEmptyArraySimpler |> Gen.map mapNEArrAndIdxToNEVecAndIdx

let rec shrink (ArrayAndIdx (arr:int[],idx:int)) = seq {
    if arr.Length <= 0 then yield! Seq.empty else
    let rest = arr |> Array.skip 1
    let i = Operators.max 0 (Operators.min idx rest.Length - 1)
    yield ArrayAndIdx (rest,i)
    yield! shrink (ArrayAndIdx (rest,i))
}
let rec shrinkNonEmpty (NonEmptyArrayAndIdx (arr:int[],idx:int)) = seq {
    if arr.Length <= 1 then yield! Seq.empty else
    let rest = arr |> Array.skip 1
    let i = Operators.max 0 (Operators.min idx rest.Length - 1)
    yield NonEmptyArrayAndIdx (rest,i)
    yield! shrinkNonEmpty (NonEmptyArrayAndIdx (rest,i))
}
let shrinkVec (VecPlusArrAndIdx (vec,arr,idx)) =
    shrink (ArrayAndIdx (arr,idx)) |> Seq.map mapArrAndIdxToVec
let shrinkNonEmptyVec (NonEmptyVecPlusArrAndIdx (vec,arr,idx)) =
    shrinkNonEmpty (NonEmptyArrayAndIdx (arr,idx)) |> Seq.map mapNEArrAndIdxToNEVec
let shrinkVecAndIdx (VecPlusArrAndIdx (vec,arr,idx)) =
    shrink (ArrayAndIdx (arr,idx)) |> Seq.map mapArrAndIdxToVecAndIdx
let shrinkNonEmptyVecAndIdx (NonEmptyVecPlusArrAndIdx (vec,arr,idx)) =
    shrinkNonEmpty (NonEmptyArrayAndIdx (arr,idx)) |> Seq.map mapNEArrAndIdxToNEVecAndIdx

type MyGenerators =
    static member arbArrayAndIdx() =
        { new Arbitrary<ArrayAndIdx>() with
            override x.Generator = genArraySimpler
            override x.Shrinker t = shrink t }
    static member arbNonEmptyArrayAndIdx() =
        { new Arbitrary<NonEmptyArrayAndIdx>() with
            override x.Generator = genNonEmptyArraySimpler
            override x.Shrinker t = shrinkNonEmpty t }
    static member arbVec() =
        { new Arbitrary<VecPlusArrAndIdx>() with
            override x.Generator = genVecPlusArrAndIdx
            override x.Shrinker t = shrinkVec t }
    static member arbNonEmptyVec() =
        { new Arbitrary<NonEmptyVecPlusArrAndIdx>() with
            override x.Generator = genNonEmptyVecPlusArrAndIdx
            override x.Shrinker t = shrinkNonEmptyVec t }
    static member arbSizedRRBVec() =
        { new Arbitrary<RRBVector<'a>>() with
            override x.Generator = RRBVectorGen.sizedGenVec<'a>
            override x.Shrinker _ = Seq.empty }
    static member arbSplitTest() =
        { new Arbitrary<RRBVectorTransientCommands.SplitTestInput>() with
            // override x.Generator = RRBVectorTransientCommands.genBasicOperations RRBVectorTransientCommands.cmdsExtraLarge
            override x.Generator = RRBVectorTransientCommands.genComplexOperations
            override x.Shrinker _ = Seq.empty }

Arb.register<MyGenerators>() |> ignore
let testProp  name fn =  testPropertyWithConfig { FsCheckConfig.defaultConfig with arbitrary = [typeof<MyGenerators>] ; startSize = 120 ; endSize = 180 } name fn
let ptestProp name fn = ptestPropertyWithConfig { FsCheckConfig.defaultConfig with arbitrary = [typeof<MyGenerators>] ; startSize = 120 ; endSize = 180 } name fn
let ftestProp name fn = ftestPropertyWithConfig { FsCheckConfig.defaultConfig with arbitrary = [typeof<MyGenerators>] ; startSize = 120 ; endSize = 180 } name fn
let etestProp replay name fn = etestPropertyWithConfig replay { FsCheckConfig.defaultConfig with arbitrary = [typeof<MyGenerators>] ; startSize = 120 ; endSize = 180 } name fn

let testPropMed  name fn =  testPropertyWithConfig { FsCheckConfig.defaultConfig with arbitrary = [typeof<MyGenerators>] ; startSize = 60 ; endSize = 120 } name fn
let ptestPropMed name fn = ptestPropertyWithConfig { FsCheckConfig.defaultConfig with arbitrary = [typeof<MyGenerators>] ; startSize = 60 ; endSize = 120 } name fn
let ftestPropMed name fn = ftestPropertyWithConfig { FsCheckConfig.defaultConfig with arbitrary = [typeof<MyGenerators>] ; startSize = 60 ; endSize = 120 } name fn
let etestPropMed replay name fn = etestPropertyWithConfig replay { FsCheckConfig.defaultConfig with arbitrary = [typeof<MyGenerators>] ; startSize = 60 ; endSize = 120 } name fn

let testPropSm  name fn =  testPropertyWithConfig { FsCheckConfig.defaultConfig with arbitrary = [typeof<MyGenerators>] ; startSize = 30 ; endSize = 90 } name fn
let ptestPropSm name fn = ptestPropertyWithConfig { FsCheckConfig.defaultConfig with arbitrary = [typeof<MyGenerators>] ; startSize = 30 ; endSize = 90 } name fn
let ftestPropSm name fn = ftestPropertyWithConfig { FsCheckConfig.defaultConfig with arbitrary = [typeof<MyGenerators>] ; startSize = 30 ; endSize = 90 } name fn
let etestPropSm replay name fn = etestPropertyWithConfig replay { FsCheckConfig.defaultConfig with arbitrary = [typeof<MyGenerators>] ; startSize = 30 ; endSize = 90 } name fn

// *** TEST DATA ***

// A few vectors either generated by hand, or generated by the RRBVectorGen code, which have proven useful in testing certain scenarios

let ridiculouslyBigVectorAtBlockSize8 : RRBVector<int> =  // TODO: Construct a similar test for M=32?
    RRBVectorGen.treeReprStrToVec
        "[[[6 M-1 M-1 5 6 6 5 M-1] [M*M] [M M M 6 M M-1 6 M] [6 M-1 5 M M 6 5 5] [4 6 5 M-1 M-1 5 6 5] [4 5 6 5 6 6 5 5] [6 M M-1 M-1 M-1 M-1 M M-1]] [[5 5 5 6 5 5] [5 5 5 5 6 5 5 6] [6 M-1 6 M-1 M-1] [M M-1 M-1 5 5 6 6] [4 3 5 3 4]] [[5 6 5 6 6 6] [4 3 6 3 5] [6 M-1 M M-1] [M-1 5 M-1 6 M M 6] [4 4 3 3]] [[5 4 3 M-1 3] [1 3 3] [4 4] [6 5 5 5] [6 M-1 M-1]] [[6 M-1 6 5 5 5 6] [M-1 M M M M M M] [6 6 M-1 M-1 M M M] [6 6 6 5 M 6 5 M-1] [5 5 6 5 5 5 5]] [[5 5 5 5 M-1] [5 5 4 4 4 4] [M M M 6 M M M-1] [M-1 6 5 5 6 M-1 M-1] [4 4 5 4 5 4] [M M-1 M M M M 6] [5 6 5 5 M-1]] [[6 5 5 6 5 6] [1 3 2] [6 6 M M-1 M] [5 5 M-1 5 M] [M M-1 M]] [[5 6 5 5 5 5 6] [M*M] [6 M-1 6 M-1 M-1 6] [M-1 M-1 6 6 6 6 6 6] [6 5 5 5 5 5 5 5]]] [[[6 6 M 6 6 6] [M 5 5 M 6 6 6 M-1] [M*M] [6 M M M M M M M] [M M M M M-1 6 6] [M M M M M M]]] [[[5 3] [2 2 5] [3 4 4 3 M] [M M-1 6 5 6]] [[M M M M M] [2 3 3 1] [M-1 M] [5 3 5]] [[5 4 5 4 6] [M M-1 M M M M M] [M-1 M-1 M-1 M-1 M-1 M-1] [5 5 5 M-1 M-1 M-1 5] [6 5 5 4 5 5 6] [6 M M M M 6 M-1] [M-1 6 M-1 M-1 5 6]] [[4 4 3 3] [4 4 5 4 4 4] [6 M-1 6 5] [5 6 5 5 6] [3 4 3 3 5]] [[5 5 6 5 6 6] [4 5 5 4 5 5] [6 M M-1 M M-1 M-1 M]] [[4 4 5 5 5 6] [M M M M M M M] [M 6 M-1 6 M-1 M-1 6 M] [5 6 6 5 6 6] [4 5 6 5 5 4 4] [M M M 5 M-1 M M M]]] [[[M-1 4 M-1 4 5] [5 6 M-1 6] [5 4 4] [3 5 3 4 3] [M M-1 M 6 M 6 M-1 M-1] [M M-1 M-1 6 5 5 6] [4 4 4 6 6 6 5]] [[6 M M-1 M-1 6 6 5 5] [5 4 5 5 5 5] [M M-1 M-1 6 M M M] [6 M-1 6 M-1 M-1 6] [5 3 4 M-1 3] [5 5 5 5 5 6 5 5] [M-1 M 6 M-1 M-1 M-1 M-1 M-1]] [[3 3 4] [M-1 M 6 M-1 6 6 5] [M-1 M M 5 M] [M-1 5 6 6 6 5] [4 1 M-1 4 3] [4 4 3 5]] [[6 5 M-1 6 5 M-1 5] [6 3 3 M-1 6 3] [M M-1 M-1 M 5] [5 M 6 5 M 6] [6 4 3 6 6 4] [M M M M M M] [M-1 5 6 M-1 6] [5 M-1 5 4 M-1 6]] [[5 5 4 5] [3 2 2 3] [M M-1 M 6] [4 5 5 6 M] [5 3 3 4] [M 6 M M M]] [[5 M-1 M-1 5 6 4 5] [M-1 6 5 5 4 5 4] [M M M 6 5 M M] [6 M 4 M-1 6 M M-1] [5 4 M-1 3 4 6] [M M M M M M M]] [[6 5 5 M-1 6 5] [3 3 5 5 4 5] [M-1 M-1 M M-1 M M M-1 M] [6 6 M-1 M-1 6 6 5] [3 4 6 M-1 4 6] [M 6 M M M-1 M M] [5 M-1 M-1 4 M-1 5] [5 4 5 6 6 4]] [[6 6 4 4 4] [6 M-1 5 3 4 3 M-1] [M M 6 M 6 M-1 M-1] [5 M 5 6 5] [3 6 3 3 5] [M-1 M M M M] [5 6 5 6] [5 4 6 5 M]]] T6"

let compareVecToArray v arr =
    Expect.equal (RRBVector.length v) (Array.length arr) (sprintf "Length of vector %A did not equal length of array %A" (RRBVector.toArray v) arr)
    for i in 0..(RRBVector.length v - 1) do
        Expect.equal (v |> RRBVector.nth i) (arr |> Array.item i)
                     (sprintf "Item at position %d of vector (%A) did not equal item at position %d of array (%A)" i (RRBVector.nth i v) i (Array.item i arr))
// TODO: Improve error messages here, such as allowing us to name the vector (e.g. "left half" and "right half"). Also, *print* the vector and array, not just the item that failed to compare.

let doJoinTest v1 v2 =
    RRBVectorProps.checkProperties v1 "v1 in join test"
    RRBVectorProps.checkProperties v2 "v2 in join test"
    let s1 = RRBVector.toSeq v1
    let s2 = RRBVector.toSeq v2
    let joined = RRBVector.append v1 v2
    let joined' = RRBVector.append v2 v1
    RRBVectorProps.checkProperties joined "Joined vector"
    RRBVectorProps.checkProperties joined' "Opposite-joined vector"
    Expect.sequenceEqual joined (Seq.append s1 s2) "Joined vectors did not equal equivalent appended arrays"
    Expect.sequenceEqual joined' (Seq.append s2 s1) "Opposite-joined vectors did not equal equivalent appended arrays"

let doSplitTest vec i =
    let repr = RRBVectorGen.vecToTreeReprStr vec
    RRBVectorProps.checkProperties vec "Original vector"
    let vL, vR = vec |> RRBVector.split i
    RRBVectorProps.checkProperties vL (sprintf "Original vector was %A, split at %d\nRepr: %s\nLeft half of split" vec i repr)
    RRBVectorProps.checkProperties vR (sprintf "Original vector was %A, split at %d\nRepr: %s\nRight half of split" vec i repr)
    // doJoinTest vL vR  // Nope; if we want to do a join test as well, we'll do it explicitly in the unit test
    vL, vR

let splitTest vec i =
    let vL, vR = doSplitTest vec i
    let aL, aR = RRBVector.toArray vL, RRBVector.toArray vR
    Expect.equal (Array.append aL aR) (RRBVector.toArray vec) "Vector halves after split, when put back together, did not equal original array"

let splitFullVecTest (len,i) =
    let vec = seq { 0..len-1 } |> RRBVector.ofSeq
    splitTest vec i

let splitConstructedVecTest (repr,i) =
    let vec = RRBVectorGen.treeReprStrToVec repr
    splitTest vec i

module Expect =
    let vecEqual (v1 : RRBVector<'T>) (v2 : RRBVector<'T>) msg =
        Expect.equal (v1.Length) (v2.Length) <| sprintf "Vectors should be equal but had different lengths: expected %d and got %d\n%s" v1.Length v2.Length msg
        for i = 0 to v1.Length - 1 do
            Expect.equal (v1.Item i) (v2.Item i) <| sprintf "Not equal at idx %d: expected %A and got %A\n%s" i (v1.Item i) (v2.Item i) msg

    let vecEqualArr (v : RRBVector<'T>) (a : 'T []) msg =
        Expect.equal v.Length a.Length <| sprintf "Vector and array should be equal but had different lengths: expected %d and got %d\n%s" v.Length a.Length msg
        for i = 0 to v.Length - 1 do
            Expect.equal (v.Item i) a.[i] <| sprintf "Not equal at idx %d: expected %A and got %A\n%s" i a.[i] (v.Item i) msg

let vectorTests =
  testList "Basic vector operations" [
    testCase "empty vector has 0 items" (fun _ ->
        let v = RRBVector.empty<int>
        RRBVectorProps.checkProperties v |> ignore
        Expect.equal (RRBVector.length v) 0 "empty vector should have 0 items"
    )
    testCase "splitHugeVec" <| fun _ ->
        let vec = ridiculouslyBigVectorAtBlockSize8
        RRBVectorProps.checkProperties vec "Original huge vector"
        let vL, vR = vec |> RRBVector.split 9  // Past first leaf, but not very far past
        RRBVectorProps.checkProperties vL "Left side of split"
        RRBVectorProps.checkProperties vR "Right side of split"
        let arrL = vL |> RRBVector.toArray
        let arrR = vR |> RRBVector.toArray
        let arrTotal = vec |> RRBVector.toArray
        Expect.equal (Array.append arrL arrR) arrTotal "Items in vectors after split did not add up to original vector's items"

    testProp "vecLen" (fun (VecPlusArrAndIdx (v,a,_)) -> RRBVectorProps.checkPropertiesSimple v ; RRBVector.length v = Array.length a)
    testProp "vecItems" (fun (VecPlusArrAndIdx (v,a,_)) ->
        // Original name: "Indexing into vectors works"
        // NOTE: This found errors with vectors of length 25, 37, 57, and 69 when M=4. NONE of those are tests I would have thought to write myself.
        // But once I looked at them, it was immediately clear that my code had built the wrong data structure for those cases.
        // Thanks to FsCheck, I found these four bugs *immediately* without spending a day or two in head-scratching!
        RRBVectorProps.checkPropertiesSimple v
        seq {0..a.Length-1}
        |> Seq.forall (fun i -> a.[i] = RRBVector.nth i v)
    )
    testProp "vecPop" (fun (VecPlusArrAndIdx (v,a,_)) ->
        // Original name: "Popping from vectors works"
        RRBVectorProps.checkProperties v "Original vector"
        let folder expected (valid,acc) =
            RRBVectorProps.checkProperties acc "Partially-popped vector"
            let actual = RRBVector.peek acc
            let acc' = RRBVector.pop acc
            (valid && (expected = actual), acc')
        Array.foldBack folder a (true,v) |> fst
    )
    testProp "vecPush" (fun (VecPlusArrAndIdx (v,a,_)) ->
        let mutable vec = v
        RRBVectorProps.checkProperties vec "Original vector"
        for i = 1 to Literals.blockSize + 1 do
            vec <- vec |> RRBVector.push i
            RRBVectorProps.checkProperties vec (sprintf "Vector after %d push%s" i (if i = 1 then "" else "es"))
    )
    testProp "vecUpdate" <| fun (VecPlusArrAndIdx (v,a,i)) ->
        if i >= v.Length then () else  // Can't run this test if we've picked an index equal to length, since that's not a legal update index (it *is* a legal take or skip index)
        RRBVectorProps.checkProperties v "Original vector"
        let vec' = v.Update i 512
        let arr' = a |> Array.copyAndSet i 512
        RRBVectorProps.checkProperties vec' (sprintf "Vector after updating at %d" i)
        Expect.vecEqualArr vec' arr' "Vector update produced wrong results"

    testProp "vecSlice" (fun (VecPlusArrAndIdx (v,a,idx)) (PositiveInt endIdx) ->
        let endIdx = if v.Length = 0 then 0 else endIdx % v.Length
        let idx, endIdx = if idx <= endIdx then idx,endIdx else endIdx,idx
        RRBVectorProps.checkProperties v "Original vector"
        let v' = v.Slice (idx, endIdx)
        RRBVectorProps.checkProperties v' <| sprintf "Vector after slicing from %d to %d" idx endIdx
        if a.Length > 0 then
            let a' = a.[idx..endIdx]
            Expect.vecEqualArr v' a' "Sliced vector should equal equivalent slice from array"
    )

    testProp "vecSlice with vec.[a..b] notation" <| fun (vec : RRBVector<int>) (startIdx : NonNegativeInt option) (endIdx : NonNegativeInt option) ->
        let endIdx = endIdx |> Option.map (fun (NonNegativeInt endIdx) -> if vec.Length = 0 then 0 else endIdx % vec.Length)
        let startIdx = startIdx |> Option.map (fun (NonNegativeInt startIdx) -> if vec.Length = 0 then 0 else startIdx % vec.Length)
        // Ensure start <= end, but not necessary if either is None
        let startIdx, endIdx =
            match startIdx, endIdx with
            | _,      Some _ when vec.Length = 0 -> startIdx, None  // If vector is empty, only a "slice to end" is valid, and index 0 is off the end
            | Some s, Some e when s > e -> endIdx, startIdx
            | _ -> startIdx, endIdx
        let arr = vec |> RRBVector.toArray
        let slicedArr =
            match startIdx, endIdx with
            | Some s, Some e -> arr.[s..e]
            | Some s, None -> arr.[s..]
            | None, Some e -> arr.[..e]
            | None, None -> arr
        RRBVectorProps.checkProperties vec "Original vector"
        let slicedVec =
            match startIdx, endIdx with
            | Some s, Some e -> vec.[s..e]
            | Some s, None -> vec.[s..]
            | None, Some e -> vec.[..e]
            | None, None -> vec
        RRBVectorProps.checkProperties slicedVec <| sprintf "Vector after slicing from %A to %A" startIdx endIdx
        Expect.equal slicedVec.Length slicedArr.Length "Vector slicing should be equivalent to array slicing in length"
        Expect.equal (slicedVec |> RRBVector.toArray) slicedArr "Vector slicing should be equivalent to array slicing"

    testCase "Slicing only in tail, case 1" <| fun _ ->
        let vec = { 1..39 } |> RRBVector.ofSeq
        let t = (vec :?> RRBPersistentVector<_>).Transient()
        let x = t.[32..36]
        // logger.warn (eventX "vec is {vec}" >> setField "vec" (RRBVectorGen.vecToTreeReprStr x))
        RRBVectorProps.checkProperties x "foo"

    testCase "Slicing only in tail, case 2" <| fun _ ->
        let vec = { 1..39 } |> RRBVector.ofSeq
        let t = (vec :?> RRBPersistentVector<_>).Transient()
        let x = t.[34..36]
        // logger.warn (eventX "vec is {vec}" >> setField "vec" (RRBVectorGen.vecToTreeReprStr x))
        RRBVectorProps.checkProperties x "foo"

    testCase "Test appending a new child to an empty transient with relaxed root" <| fun _ ->
        let vec = { 1..52 } |> RRBVector.ofSeq
        let mutable t = (vec :?> RRBPersistentVector<_>).Transient()
        // Split the node with an insert
        t <- t.Insert 18 -512 :?> RRBTransientVector<_>
        // Empty the tree with a slice so the root remains an ExpandedRelaxedNode
        t <- t.[54..] :?> RRBTransientVector<_>
        // Push a full tail
        for i = 1 to Literals.blockSize do
            t <- t.Push i :?> RRBTransientVector<_>
            RRBVectorProps.checkProperties t <| sprintf "After pushing %d" i
        // Push tail down to form new root - was "Index outside the bounds of the array" before bug in AppendChildS was fixed
        t <- t.Push (Literals.blockSize + 1) :?> RRBTransientVector<_>
        RRBVectorProps.checkProperties t <| sprintf "After pushing %d" (Literals.blockSize + 1)

    testCase "push M+1 items onto an empty vector" <| fun _ ->
        let mutable vec = RRBVector.empty<int>
        for i = 1 to Literals.blockSize + 1 do
            vec <- vec |> RRBVector.push i
            RRBVectorProps.checkProperties vec (sprintf "Vector after %d push%s" i (if i = 1 then "" else "es"))
    testCase "push M+2 items onto a vector that starts with a nearly-full tail" <| fun _ ->
        let mutable vec = RRBVectorGen.treeReprStrToVec "TM-1"
        for i = 1 to Literals.blockSize + 2 do
            vec <- vec |> RRBVector.push i
            RRBVectorProps.checkProperties vec (sprintf "Vector after %d push%s" i (if i = 1 then "" else "es"))
    testCase "push 2 items onto a vector that starts with a full root and a nearly-full tail" <| fun _ ->
        let mutable vec = RRBVectorGen.treeReprStrToVec "M TM-1"
        for i = 1 to 2 do
            vec <- vec |> RRBVector.push i
            RRBVectorProps.checkProperties vec (sprintf "Vector after %d push%s" i (if i = 1 then "" else "es"))
    testCase "insert into two full nodes" <| fun _ ->
        let vec = RRBVectorGen.treeReprStrToVec "M M T1"
        RRBVectorProps.checkProperties vec "Original vector"
        let vec = vec |> RRBVector.insert 21 512
        RRBVectorProps.checkProperties vec "Vector after insert at idx 21"
        Expect.equal vec.Length (Literals.blockSize * 2 + 2) "Vector length is wrong"
    testProp "split" (fun (VecPlusArrAndIdx (v,a,i)) ->
        let aL, aR = Array.truncate i a, Array.skip i a
        let vL, vR = v |> RRBVector.split i
        RRBVectorProps.checkProperties vL "Left half of split"
        RRBVectorProps.checkProperties vR "Right half of split"
        compareVecToArray vL aL
        compareVecToArray vR aR
        Expect.equal (Array.append (RRBVector.toArray vL) (RRBVector.toArray vR)) a "Vector halves after split, when put back together, did not equal original array"
    )
    testProp "splitWithPushes" (fun (VecPlusArrAndIdx (v,a,i)) (PositiveInt pushCnt) ->
        let aL, aR = Array.truncate i a, Array.skip i a
        let vL, vR = v |> RRBVector.split i
        RRBVectorProps.checkProperties vL "Left half of split"
        RRBVectorProps.checkProperties vR "Right half of split"
        compareVecToArray vL aL
        compareVecToArray vR aR
        Expect.equal (Array.append (RRBVector.toArray vL) (RRBVector.toArray vR)) a "Vector halves after split, when put back together, did not equal original array"
        let mutable vR' = vR
        let mutable cnt = pushCnt % (Literals.blockSize * Literals.blockSize) // Sanity bound
        while cnt > 0 do
            vR' <- vR' |> RRBVector.push cnt
            RRBVectorProps.checkProperties vR' "Right half after some pushes"
            cnt <- cnt - 1
        Expect.equal cnt 0 "Did not successfully complete all pushes after the split"
    )
    testProp "splitVec" (fun (vec:RRBVector<int>,i:int) ->
        let i = (abs i) % (RRBVector.length vec + 1)
        splitTest vec i
    )
    testCase "splitInto with not enough input leaves empty trailing vectors" (fun _ ->
        let vec = [1;2;3] |> RRBVector.ofList
        let split = vec |> RRBVector.splitInto 4
        let result = split |> RRBVector.map RRBVector.toList |> RRBVector.toList
        Expect.equal result [[1];[2];[3];[]] "splitInto should leave empty vectors at the end"
    )
    testCase "splitInto with not uneven input leaves smaller last vector" (fun _ ->
        let vec = [1..7] |> RRBVector.ofList
        let split = vec |> RRBVector.splitInto 4
        let result = split |> RRBVector.map RRBVector.toList |> RRBVector.toList
        Expect.equal result [[1;2];[3;4];[5;6];[7]] "splitInto should leave smaller vectors at the end"
    )
    testProp "splitVecWithPushes" (fun (vec:RRBVector<int>,i:int) (PositiveInt pushCnt) ->
        let i = (abs i) % (RRBVector.length vec + 1) // Make sure it's a valid split point, between 0 and vecLen
        splitTest vec i
        let vL, vR = vec |> RRBVector.split i
        let mutable vR' = vR
        let mutable cnt = pushCnt % (Literals.blockSize * Literals.blockSize) // Sanity bound
        while cnt > 0 do
            vR' <- vR' |> RRBVector.push cnt
            RRBVectorProps.checkProperties vR' "Right half after some pushes"
            cnt <- cnt - 1
        Expect.equal cnt 0 "Did not successfully complete all pushes after the split"
    )
    testCase "a full vector can be popped down to empty" <| fun _ ->
        let mutable vec = RRBVectorGen.treeReprStrToVec "M M TM"
        RRBVectorProps.checkProperties vec "Original vector"
        for i = 1 to Literals.blockSize * 3 do
            vec <- vec |> RRBVector.pop
            RRBVectorProps.checkProperties vec (sprintf "Vector after %d pop%s" i (if i = 1 then "" else "s"))
        Expect.equal vec.Length 0 "Vector should be empty at end of test"

    testPropSm "any random vector can be popped down to empty" <| fun (vec:RRBVector<int>) ->
        let mutable v = vec
        RRBVectorProps.checkProperties v "Original vector"
        for i = 1 to vec.Length do
            v <- v |> RRBVector.pop
            RRBVectorProps.checkProperties v (sprintf "Vector after %d pop%s" i (if i = 1 then "" else "s"))
        Expect.equal v.Length 0 "Vector should be empty at end of test"
  ]

let regressionTests =
  let doActionListTest (actions : RRBVectorMoreCommands.Cmd list) vec postPopCount postPushCount =
    // logger.debug (eventX "Starting with {vec}" >> setField "vec" (RRBVectorGen.vecToTreeReprStr vec))
    let mutable current = vec
    let logVec action vec =
        // logger.debug (
        //     eventX "After {cmd}, vec was {vec} with actual structure {structure}"
        //     >> setField "cmd" action
        //     >> setField "vec" (RRBVectorGen.vecToTreeReprStr vec)
        //     >> setField "structure" (sprintf "%A" vec))
        ()
    for action in actions do
        current <- current |> action.RunActual
        logVec (action.ToString()) current
        RRBVectorProps.checkProperties current <| sprintf "Vector after %s" (action.ToString())
    for i = 1 to postPopCount do
        current <- current.Pop()
        logVec (sprintf "After popping %d" i) current
        RRBVectorProps.checkProperties current <| sprintf "Vector after popping %d" i
    for i = 1 to postPushCount do
        current <- current.Push i
        logVec (sprintf "After pushing %d" i) current
        RRBVectorProps.checkProperties current <| sprintf "Vector after pushing %d" i

  testList "Regression tests" [
    testCase "Shorten trees after split" <| fun _ ->
        let v = RRBVectorGen.treeReprStrToVec "M M TM/2"
        let i = 11
        let a = RRBVector.toArray v
        let aL, aR = Array.truncate i a, Array.skip i a
        let vL, vR = v |> RRBVector.split i
        RRBVectorProps.checkProperties vL "Left half of split"
        RRBVectorProps.checkProperties vR "Right half of split"
        compareVecToArray vL aL
        compareVecToArray vR aR
        Expect.equal (Array.append (RRBVector.toArray vL) (RRBVector.toArray vR)) a "Vector halves after split, when put back together, did not equal original array"
        let mutable vR' = vR
        let mutable cnt = Literals.blockSize * 2 + 1
        while cnt > 0 do
            vR' <- vR' |> RRBVector.push cnt
            RRBVectorProps.checkProperties vR' "Right half after some pushes"
            cnt <- cnt - 1
        Expect.equal cnt 0 "Did not successfully complete all pushes after the split"

    testCase "Shifting into tail can continue recursively" <| fun _ ->
        let vec = RRBVectorGen.treeReprStrToVec "[[M*M-1 5]] [[5 5]] T1"
        RRBVectorProps.checkProperties vec "Original vector"
        let vec' = vec |> RRBVector.pop
        RRBVectorProps.checkProperties vec' "Popped vector"

    testCase "Slicing near tail can shift into tail" <| fun _ ->
        // This test proves that we need .adjustTree() instead of .shortenTree() in the final (main) branch of RRBVector.Skip()
        let vec = RRBVectorGen.treeReprStrToVec "[[M*M-1 5]] [[5 5]] T1"
        RRBVectorProps.checkProperties vec "Original vector"
        // TODO: Write this test in such a way that the original vector *passes* properties, so that it's a fair test.
        let mutable vec' = vec.Skip (Literals.blockSize * (Literals.blockSize - 1) + 10)
        RRBVectorProps.checkProperties vec' "Popped vector"
        for n = 1 to Literals.blockSize + 1 do
            vec' <- vec' |> RRBVector.push n
            RRBVectorProps.checkProperties vec' <| sprintf "Right half after %d pushes" n

    testCase "Join regression test" <| fun _ ->
        let reprL = "M*11 T17"
        let reprR = "[M*M] [M*3] T3"
        let vL = RRBVectorGen.treeReprStrToVec reprL
        let vR = RRBVectorGen.treeReprStrToVec reprR
        doJoinTest vL vR

    testCase "Join regression test 2" <| fun _ ->
        let reprL = "M 27 M 29 M 30 M 22 M 16 M 25 M 27 M 28 M M 17 M 26 M T3"
        let reprR = "[M 16 M 20 M 19 M 28 M 22 19 M 21 M 22 M 24 M 25 M 17 M 28 M 20 M 22 M 23 M M 30] [M 31 M 16 M] T3"
        let vL = RRBVectorGen.treeReprStrToVec reprL
        let vR = RRBVectorGen.treeReprStrToVec reprR
        doJoinTest vL vR

    testCase "Join regression test 3" <| fun _ ->
        let reprL = "28 M 29 M 31 M M M 17 M 26 30 M 17 M 16 M 20 T3"
        let reprR = "19 M 18 M 22 17 M 18 M 20 T3"
        let vL = RRBVectorGen.treeReprStrToVec reprL
        let vR = RRBVectorGen.treeReprStrToVec reprR
        doJoinTest vL vR

    testCase "Join regression test 4" <| fun _ ->
        let reprL = "M*M T3"
        let reprR = "19 M 18 M 22 17 M 18 M 20 T3"
        let vL = RRBVectorGen.treeReprStrToVec reprL
        let vR = RRBVectorGen.treeReprStrToVec reprR
        doJoinTest vL vR

    testCase "Join regression test 5" <| fun _ ->
        let reprL = """
            [25 32 30 29 32 32 23 32 32 28 32 28 24 32 32 27 30 32 32 28 32 32 29 32 30 31 32 31]
            [28 31 26 32 26 32 32 32 32 26 25 28 26 32 30 32 28 32 32 25 26 31]
            [32 32 29 32 32 32 32 29 31 32 32 32 32 30 28 32 32 32 32 26 29 32 32]
            [29 23 31 32 32 32 32 32 32 30 30 32 32 32 32 32 27 32 31 30 32 32 32 32 32]
            [32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 27 25 30 25 32 29 31 32 23 24 32]
            [29 32 26 32 32 32 29 32 27 29 30 32 29 31 32 29 31 25 32 32 30 30 30 29 28 32 32 27 32 32 29 30]
            [32 30 24 26 32 30 32 32 32 32 19 25 30 32 31 32 32 32 25 19 24 29 32 32 32]
            [32 32 29 32 32 28 32 32 31 32 32 31 32 27 32 32 32 32 32 32 32 32 32 32 31 32]
            T24"""
        let reprR = """
            [31 28 32 32 32 32 30 24 30 32 32 32 32 32 32 28 29 31 32 32 31 27 32 25 27]
            [32 32 29 29 32 26 32 32 29 29 32 32 32 30 24 32 30 30 25 31 31 32 29 28]
            [27 29 30 26 32 32 29 32 32 28 32 32 32 32 30 32 32 31 32 32 32 32 32 32 29 28 32]
            [32 32 32 30 32 32 32 29 32 27 28 30 27 28 32 30 31 32 32 32 32 32 31 32 32 32 32 28 27]
            [32 32 32 32 31 32 31 32 32 32 26 32 32 32 32 32 24 32 32 30 32 32 27]
            [32 32 32 31 32 28 32 32 32 32 31 31 32 26 32 32 32 32 32 32 32]
            [32 32 32 32 32 32 32 32 23 32 32 28 32 32 31 32 32 29 32 32 32 31 32 32 32 32]
            [32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 29 32 32 32 32 32 32 32 32 32 32 32]
            [32 27 32 32 32 31 32 32 32 32 31 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 31 32]
            [25 24 20 30 30 31 32 32 25 25 21 29 32 32 32 28 32 24 23 25 32 32 32 31 32]
            [26 27 31 27 27 30 29 22 31 29 28 28 29 27 22 27 32 32 27 26 32 27 31 27]
            [31 32 25 25 31 22 32 29 30 24 31 32 25 26 31 26 29 32 26 30 32 31 26 32]
            T23"""
        let vL = reprL |> RRBVectorGen.looserTreeReprStrToVec
        let vR = reprR |> RRBVectorGen.looserTreeReprStrToVec
        doJoinTest vL vR

    testCase "Split+push+join regression test" <| fun _ ->
        let bigVecRepr = """
            [M M M M 30 M M M 29 M M M M M M M-1 M M 27 M M-1 M 30 M-1 29 M M M M M 30 M]
            [M 28 M M M-1 M-1 M M 29 M M M M 29 M M 28 M M M M M M 30 M M M M]
            [M 29 M 29 29 M-1 27 25 M M-1 M-1 26 M M M-1 M M-1 30 M M M M 30 26 25 M 30 22]
            [28 M 29 M M M 29 M 30 28 26 30 24 M 28 M M M-1 M M M 23]
            [M-1 M 26 25 M-1 M 29 30 M-1 M 28 M 30 M M M-1 M M-1 30 28 28 27 M M M-1 29 29]
            [M M 27 M M M M 30 M M M M M 30 M M 30 27 M M M M M 28 26 27 28 M M M-1 M]
            [28 29 M M-1 26 M-1 27 25 30 23 M 29 M M M M M M M M M M M M M M M]
            [M M 28 M M M 30 M M M M M M-1 M M 30 24 M M M M 28 M-1 M M M M 29 M-1 M M]
            [M M 30 M 30 27 29 M M-1 30 27 28 M M 29 M 29 M M-1 M M M M M]
            [M M M M M-1 30 M M M M M M M 28 30 M-1 M M 30 28 M M]
            [M M M M M M M M M M M M M-1 M M M M 23 30 25 M M M M M M M 30 M]
            [M M M M M M M M M M M M 30 M M M-1 M M M M M-1 M M M M M M 30 M M]
            [M M M M M M M M 30 28 28 M M M M M M M M M M M M M M M M M-1 M M M M]
            T31"""
        let i = 32
        let vec = RRBVectorGen.looserTreeReprStrToVec bigVecRepr
        let vec' = vec |> RRBVector.insert i 512
        RRBVectorProps.checkProperties vec' (sprintf "Vector after inserting 512 at idx %d" i)
        let vL, vR = doSplitTest vec i
        let vL' = vL |> RRBVector.push 512
        let joined = RRBVector.append vL' vR
        RRBVectorProps.checkProperties joined "Joined vector"
        Expect.equal joined.Length vec'.Length "Joined vector length plus one should be same as equivalent vector length plus one"
        Expect.sequenceEqual (joined |> RRBVector.toSeq) (vec' |> RRBVector.toSeq) "Split + push left + joined vectors did not equal insertion into original vector"

    testCase "Split+remove+join regression test" <| fun _ ->
        let bigVecLRepr = "[M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M]*13 T31"
        let bigVecRRepr = "[M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M]*8 T23"
        let vL = RRBVectorGen.treeReprStrToVec bigVecLRepr
        let vR = RRBVectorGen.treeReprStrToVec bigVecRRepr
        let vL' = RRBVector.remove 0 vL
        let joinedOrig = RRBVector.append vL vR
        let joined = RRBVector.append vL' vR
        RRBVectorProps.checkProperties joinedOrig "Joined vector without removal"
        RRBVectorProps.checkProperties joined "Joined vector after removal"
        Expect.equal joined.Length (joinedOrig.Length - 1) "Joined vector length minus one should be same as equivalent vector length minus one"
        Expect.sequenceEqual (joined |> RRBVector.toSeq) (RRBVector.remove 0 joinedOrig |> RRBVector.toSeq) "remove idx 0 of left + join did not equal join + remove idx 0"

    testCase "Slice regression test" <| fun _ ->
        let vec = RRBVectorGen.treeReprStrToVec "M-1 M T1"
        let vec' = vec.Take (Literals.blockSize * 2 - 1)
        RRBVectorProps.checkProperties vec' "Sliced vector"
        let tailOffset, tailLen =
            if vec' |> isTransient then
                let v = vec' :?> RRBTransientVector<_>
                v.TailOffset, v.Length - v.TailOffset
            else
                let v = vec' :?> RRBPersistentVector<_>
                v.TailOffset, v.Tail.Length
        Expect.equal tailOffset (Literals.blockSize) "Wrong tail offset"
        Expect.equal tailLen (Literals.blockSize - 1) "Wrong tail length"

    testCase "Split regression test" <| fun _ ->
        let vec = RRBVectorGen.treeReprStrToVec "M 18 T1"
        for i = 0 to vec.Length do
            let vL, vR = doSplitTest vec i
            doJoinTest vL vR

    testCase "Split regression test 2" <| fun _ ->
        let vecReprs = [
            "[30 M M M 26 28 M 30 29 M M M M M M 25 M 27 M 30 M M-1 25 M 30 M 26 30 M 30 M 7] [M-1] T1"
            "M-1 M-1 M M M 26 28 M 30 29 M M M M M M 25 M 27 M 30 M M-1 25 M 30 M 26 30 M 30 M T7"
            "5 M*M-1 T7"
        ]
        for vecRepr in vecReprs do
            let vec = RRBVectorGen.treeReprStrToVec vecRepr
            let vL, vR = doSplitTest vec 32
            let vL' = RRBVector.remove 0 vL
            doJoinTest vL' vR
            let vec' = RRBVector.remove 0 vec
            let joined = RRBVector.append vL' vR
            Expect.equal vec'.Length joined.Length "Joined vector minus one should be same length as original vector minus one"
            Expect.sequenceEqual joined vec' "Joined vector minus one should be same as original vector minus one"

    testCase "Pushing into full tail regression test" <| fun _ ->
        let mutable vec = RRBVectorGen.treeReprStrToVec "17 16 M M M M M M M M M M M M M M M M M M 17 16 M 17 16 M M M M M M M TM"   // A nearly-full vector containing a few insertion splits
        for i = 1 to Literals.blockSize + 1 do
            vec <- vec.Push i
        RRBVectorProps.checkProperties vec "Vector after pushing a full leaf plus one"

    testCase "Command regression test on really big vector" <| fun _ ->
        let bigReprStr = """
            [M 27 M 29 28 M M-1 22 25 M-1 M-1 M 27 M 25 26 28 M 26 M M 28 M-1 30 M 25 M-1 M 25 M-1 M-1 24]
            [M-1 28 29 M M-1 29 M 26 30 26 M M 27 M M 27 29 29 28 M 28 M 29 28 26 M-1 30 28 28 M-1 M-1 M-1]
            [30 28 26 M-1 M-1 M 29 M-1 27 M M M 30 M M 26 29 26 29 M M M M 26 29 26 29 29 26 29 29 26]
            [M 27 24 M-1 M-1 M-1 28 25 30 28 29 M 28 M M-1 M-1 25 30 M M 28 M 27 M M M M M-1 M-1 28 M M-1]
            [26 27 29 M-1 30 26 M-1 30 M 29 25 25 27 30 28 24 M-1 26 26 30 28 M-1 30 M-1 M-1 M M M 30 28 30 M]
            [29 M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M]
            [30 M-1 M 29 29 29 30 M M-1 M 30 M-1 M 23 M 28 26 M-1 29 29 M-1 27 M 29 M-1 29 26 25 28 M 26 28]
            [27 30 30 25 27 29 M-1 M M 29 29 29 M 30 29 29 27 M M 26 M 28 M 29 27 25 28 30 M 29 M-1 25]
            [27 29 29 24 M 30 28 27 27 28 30 30 29 M-1 30 30 M-1 M-1 M 28 M M M-1 30 27 30 29 M M-1 M 29 M]
            [28 M 28 30 M M-1 27 M 27 25 29 30 M M 28 27 26 M 28 26 M 28 M-1 30 M 28 M M M-1 27 28 M]
            [M M M-1 M M 29 30 M-1 30 26 M M M M 28 M 27 30 M 25 M M-1 M-1 27 24 M 28 30 M 25 29 M]
            [25 30 26 M M-1 26 M M 29 27 M M 30 28 30 M M 27 M 29 M M 29 M 27 29 29 M 30 27 M 30]
            [M M 28 30 M M M 26 M-1 M M-1 M 28 M-1 M-1 M M 25 29 M-1 M-1 M 28 M-1 29 27 M M M 28 27 M]
            [M 29 M 30 M-1 M 27 M 30 M 29 27 28 M M-1 29 28 M-1 M-1 M M-1 M 27 25 M M-1 M 30 30 M 27 M-1]
            [M 29 M M M-1 M 28 M M 30 M 27 30 M M M 29 M 28 30 29 M 28 M M M M 27 27 M M M-1]
            [27 M M-1 M M-1 M 27 M M M M 30 M M M 27 M-1 29 M 29 M M 26 27 29 M 30 M 29 M 28 M]
            [M 29 M M M M-1 30 M M M 27 M M M 30 M M-1 M 29 M M M 30 M M M-1 26 M 27 M M-1 30]
            [M M 28 M 22 M-1 M M M 23 M M-1 M M M 30 M M M M M-1 M 30 M 29 M 29 M M M-1 30 M]
            [M M 30 M 29 29 M M M 30 M M 29 M M 28 M M M 30 M-1 M 30 M M M M-1 M M M M M-1]
            [30 M 27 M 30 M M M M M M-1 M M-1 30 M M-1 M M M 29 28 M-1 M M 30 M M 30 M 29 M M]
            [M M M M M M M-1 M 29 M M-1 M M M M M-1 M M M-1 M-1 M M M M M 28 M 30 M M M M]
            [M 29 M-1 M-1 M M M 29 M M M M M M M M M M-1 28 M 29 M M M M M M M M 29 M M]
            [M M M M M M M-1 M M M M M M M-1 M M M 30 M M M M M M M M M M M M M M]
            [M M M M M M M M M M M-1 M M M-1 M M M 30 M-1 M M M M M-1 M M M 29 M M M M]
            [M M M M M M M M-1 29 29 M 28 28 M M M 29 M M M M 28 27 28 M M 27 M-1 27 M-1 M M]
            [M M M M 30 M 26 M M M M M M M M M 29 M M 28 30 M M M M M 29 29 M 27 M M]
            [30 M M M 30 27 25 M M M M 28 M M-1 30 M 28 M M M M M-1 M M 30 M M-1 30 M M M M]
            [M-1 M M 28 M M M M M M M-1 M 29 M M-1 M M M M M M M 27 M M M M M M M 29 M]
            [M M M M-1 M M M 27 M M M M M M M M M 27 M 30 30 M 28 M M-1 M M M M M 29 M]
            [M-1 M M M 30 M M M M M M M 30 M M M M M-1 M M 30 M M M M M M M M M M M]
            T26"""
        let vec = bigReprStr |> RRBVectorGen.looserTreeReprStrToVec
        let mergeL = RRBVectorGen.treeReprStrToVec >> RRBVectorMoreCommands.ParameterizedVecCommands.mergeL
        let mergeR = RRBVectorGen.treeReprStrToVec >> RRBVectorMoreCommands.ParameterizedVecCommands.mergeR
        let push = RRBVectorMoreCommands.ParameterizedVecCommands.push
        let remove = RRBVectorMoreCommands.ParameterizedVecCommands.remove
        let insert = RRBVectorMoreCommands.ParameterizedVecCommands.insert
        let actions = [mergeL "M T19"; push 99; mergeL "0 T28"; push 80; push 127; push 17; push 30;
                       push 138; remove -109; mergeR "M T9"; push 91; push 72; insert (-90,126); push 64;
                       push 52; insert (-138,1); push 130]
        doActionListTest actions vec 0 0

    testCase "Command regression test 2 on several vectors with several action chains" <| fun _ ->
        let scanf a _ = a + 1  // So that scans will produce increasing sequences
        let mapf a = a  // So that map won't change the numbers
        let mergeL = RRBVectorGen.treeReprStrToVec >> RRBVectorMoreCommands.ParameterizedVecCommands.mergeL
        let mergeR = RRBVectorGen.treeReprStrToVec >> RRBVectorMoreCommands.ParameterizedVecCommands.mergeR
        let push = RRBVectorMoreCommands.ParameterizedVecCommands.push
        let remove = RRBVectorMoreCommands.ParameterizedVecCommands.remove
        let insert = RRBVectorMoreCommands.ParameterizedVecCommands.insert
        let scan = RRBVectorMoreCommands.ParameterizedVecCommands.scan
        let map = RRBVectorMoreCommands.ParameterizedVecCommands.map
        let actionsLong = [
          push 124; mergeR "M T5"; push 35; push 58; push 94; push 24; push 54;
          mergeR "0 T27"; mergeL "M T6"; push 64; push 12; mergeL "0 T22"; mergeR "M T2";
          mergeR "0 T7"; push 34; push 85; push 73; push 32; push 126; push 125;
          mergeR "0 T27"; insert (-127,-101); insert (-58,21); mergeR "M T11";
          insert (-109,-118); push 116; push 16; insert (78,40); mergeR "0 T19";
          insert (27,-59); insert (110,-80); insert (-69,95); insert (79,38);
          scan scanf 83; insert (117,73); scan scanf -31; push 54;
          mergeR "0 T3"; mergeR "0 T6"; push 114; insert (-42,-78); insert (60,-45); push 14;
          scan scanf -70; insert (71,-14); push 93; insert (29,-69); push 51;
          insert (-114,-117); insert (101,34); push 50; mergeL "0 T24"; insert (32,-67);
          insert (-105,111); push 16; insert (115,64); insert (-109,110); mergeR "M T22";
          push 117; insert (14,-93); insert (20,-93); insert (88,-87); insert (-85,-19);
          map mapf; mergeR "M T3"; insert (-42,114); push 66 ]
        let actionsShort = [ map mapf; mergeR "M T3"; insert (-42,114) ]
        let actionsEvenShorter = [ insert (-42,114) ]
        let fullRepr = "[26 18 25 24 17 26 24 M M M M M M M M M M M M M M M M M M M] [M M M M M M M M M M M M M M M M M M M M M M M M M 17 16 M 17 25 24 M] [M M M M 17 16 M] T24"
        let start = RRBVector.empty
        let startFull = fullRepr |> RRBVectorGen.treeReprStrToVec
        let startEvenShorter = { 0..1982 } |> RRBVector.ofSeq
        for vec in [startFull; startEvenShorter] do
            for actions in [actionsLong; actionsShort; actionsEvenShorter] do
                doActionListTest actions vec 0 (Literals.blockSize * 2 + 2)

    testCase "Command regression test 3 on empty vector with two action chains" <| fun _ ->
        let mergeL = RRBVectorGen.treeReprStrToVec >> RRBVectorMoreCommands.ParameterizedVecCommands.mergeL
        let mergeR = RRBVectorGen.treeReprStrToVec >> RRBVectorMoreCommands.ParameterizedVecCommands.mergeR
        let push = RRBVectorMoreCommands.ParameterizedVecCommands.push
        let pop = RRBVectorMoreCommands.ParameterizedVecCommands.pop
        let rev = RRBVectorMoreCommands.ParameterizedVecCommands.rev
        let actionsLong = [push 84; mergeL "M T4"; push 21; push 95; mergeL "M T13"; push 27; push 73; mergeR "M T2"; mergeR "0 T17"; push 110; push 25; push 23; push 112; push 88; push 41; push 96; push 41; push 96; rev(); pop 118]
        let actionsMedium = [push 84; mergeL "M T4"; push 115; mergeL "M T13"; push 100; mergeR "M T2"; mergeR "0 T17"; push 632; rev(); pop 118]
        let actionsShort = [push 1063; rev(); pop 118]
        let vec = RRBVector.empty
        for actions in [actionsLong; actionsMedium; actionsShort] do
            doActionListTest actions vec 0 0

    testCase "Command regression test 4 on several vectors" <| fun _ ->
        let scanf a _ = a + 1  // So that scans will produce increasing sequences
        let mapf a = a  // So that map won't change the numbers
        let mergeL = RRBVectorGen.treeReprStrToVec >> RRBVectorMoreCommands.ParameterizedVecCommands.mergeL
        let mergeR = RRBVectorGen.treeReprStrToVec >> RRBVectorMoreCommands.ParameterizedVecCommands.mergeR
        let push = RRBVectorMoreCommands.ParameterizedVecCommands.push
        let insert = RRBVectorMoreCommands.ParameterizedVecCommands.insert
        let map = RRBVectorMoreCommands.ParameterizedVecCommands.map
        let actionsLong = [
                push 501;
                mergeR "0 T14"; push 155; mergeR "26 30 M 28 28 23 29 30 30 26 M T24";
                mergeL "M T18"; map mapf ]
        let actionsShort = [ map mapf; mergeR "M T3"; insert (-42,114) ]
        let actionsEvenShorter = [ insert (-42,114) ]
        let start = RRBVector.empty
        let startFull = RRBVectorGen.treeReprStrToVec "[26 18 25 24 17 26 24 M M M M M M M M M M M M M M M M M M M] [M M M M M M M M M M M M M M M M M M M M M M M M M 17 16 M 17 25 24 M] [M M M M 17 16 M] T24"
        let startEvenShorter = { 0..1982 } |> RRBVector.ofSeq
        let logVec action vec = printfn "After %O, vec was %s" action (RRBVectorGen.vecToTreeReprStr vec)
        for vec in [startFull; startEvenShorter] do
            for actions in [actionsLong; actionsShort; actionsEvenShorter] do
                doActionListTest actions vec (Literals.blockSize * 2 + 7) 0

    // TODO: Those four command regression tests have common structure and could be collapsed into one data-driven test

    testCase "Pushing into tail can create new path to root even when it's more than one level high" <| fun _ ->
        let mutable current = RRBVectorGen.treeReprStrToVec "[M*M]*M TM-3"
        for i = 1 to Literals.blockSize + 6 do
          current <- current.Push(i)
          RRBVectorProps.checkProperties current <| sprintf "Vector after pushing %d times" i
        current <- current |> RRBVector.rev
        RRBVectorProps.checkProperties current <| sprintf "Vector after rev()"

    testCase "Big full vector has right properties" <| fun _ ->
        let vec = { 1 .. 98321 } |> RRBVector.ofSeq
        RRBVectorProps.checkProperties vec "Full three-level vector"

    testCase "Smallish transient vector has right properties" <| fun _ ->
        let mutable vec = RRBTransientVector()
        for i = 1 to Literals.blockSize * Literals.blockSize + Literals.blockSize do
            vec <- vec.Push i :?> RRBTransientVector<_>
        RRBVectorProps.checkProperties vec "Transient vector"
        let pvec = vec.Persistent()
        RRBVectorProps.checkProperties pvec "Persistent vector"

    testCase "Really big joined vector has right properties" <| fun _ ->
        let bigNum = 1 <<< (Literals.blockSizeShift * 3)
        let vL = seq {0..bigNum+2} |> RRBVector.ofSeq
        let vR = RRBVectorGen.treeReprStrToVec "M M-1 M*M-5 M-1*2 M T4"
        RRBVectorProps.checkProperties vL "vL"
        RRBVectorProps.checkProperties vR "vR"
        doJoinTest vL vR

    testCase "Split, reverse, and join" <| fun _ ->
        let vecRepr = """
            [M M M-1 M M 29 M M M M M M M M M M M M-1 M M M M M M M-1 M M 29]
            [M M 28 M M M 29 M M M M M M 27 M M M-1 27 M M M 28 M M M M-1 M 28]
            [29 29 M 28 28 26 30 30 M M M-1 30 M 27 M 27 26 26 24 26 27 27 M-1 30 M M 27 26 30]
            [M M M M M-1 M M M M M 29 M M M M 28 29 M 29 M M M M M M M]
            [M M M 29 M M 30 30 M M M-1 M M M-1 M M M 29 M M 29 27 M M]
            [29 26 M M M 30 M M M 26 M M M 29 28 M M M M M M M M 27 25 M M 25 M]
            [M 30 30 M 29 M-1 30 30 27 28 30 M M-1 M M M M 26 M 30 M M M 22 27 M-1 M M M]
            [M M-1 30 M 26 M M-1 M M M M M-1 M 30 27 M M M M M 28 28 M-1 M M M-1 30 M M-1 M 30]
            [27 M 27 28 29 28 M 30 30 M 27 M M 29 27 23 30 29 M M M 30 26 M M M 29 30 M 30 M]
            [24 M 24 29 M M 30 M M 27 M M 25 30 M 24 M M 22 27 M 28 M M 24 30 M M]
            [30 30 M-1 30 25 29 M-1 M M 29 M M-1 M 29 28 M M M M-1 29 M M M 28 28 27 28 M-1 30 M-1 29 M]
            [M 30 30 30 29 M-1 M-1 M-1 30 26 28 M M-1 23 28 30 M-1 M 30 28 27 27 M M 27 M]
            [M M 30 30 M M 29 25 M M-1 23 M M-1 M 30 M M 27 M M M M M M-1 29 28 M-1 M 30 M 28 M]
            [M M 29 M M 26 29 M 30 26 M M-1 30 M 30 M-1 M 28 26 M M 26 M M 27 M M 27]
            [29 M M-1 M 30 M-1 M M-1 M-1 29 M 28 30 M 29 M M-1 M M M 28 30 M 30 M M-1 M-1 28 M-1]
            [M 29 M M 29 30 M 28 27 M M M M 30 M M 24 M-1 29 M-1 M M M-1 M M-1 24]
            [M M M 29 M 28 28 M M 27 M M 30 M M 30 M M M M M 29 M M 28 M M M]
            [M-1 M-1 M-1 M-1 M M M M 30 28 30 24 M M M M M-1 M-1 29 30 30 M M M M M M-1 M 30]
            [M M M M 30 M M M M M 27 M M M M M M M 27 29 30 M M M M M M M]
            [M M 27 M M M 27 M-1 M M M M M M M 28 M M M M M-1 M M M M M 28 30 M M M]
            [M M 30 M M M M M M M M M M M M M 30 M 30 M M M M M M-1 M M M M M]
            [M-1 M M M M M M M M 28 M-1 M M M M M M M M M-1 M M M 30 M M M 29 M]
            [M 26 20 26 26 29 M M M M 23 17 23 M-1 M M M M 27 22 26 27 M M M]
            [M M M M M M M M M M M M M M M M M M M M M M M M 30]
            [M 28 27 25 27 29 M 29 M 27 26 29 28 29 27 29 30 27 M 29 27 29 29 29 M M 29]
            [26 29 M-1 24 M 30 M-1 27 28 23 M M M-1 M 28 28 30 30 24 27 M 26 30 29 24 M M M 28]
            [28 M 27 M M-1 28 M 28 27 27 M M-1 24 M 30 30 M 30 30 29 M 27 29 29 M 29 M 26 M 29 30 M]
            T23"""
        let vec = RRBVectorGen.looserTreeReprStrToVec vecRepr
        RRBVectorProps.checkProperties vec "Original vector"
        let arr = vec |> RRBVector.toArray
        let idx = 1
        let vL, vR = doSplitTest vec idx
        doJoinTest vL vR
        let revL = RRBVector.rev vL
        let revR = RRBVector.rev vR
        let joined = RRBVector.append revR revL
        RRBVectorProps.checkProperties joined "Joined vector"
        Expect.equal joined.Length arr.Length "Vector length should be same as equivalent array length"
        Expect.sequenceEqual (joined |> RRBVector.toSeq) (arr |> Array.rev |> Array.toSeq) "Vector differs from equivalent array"

    testCase "Split just before, exactly at, and just after end of first node, full vector" <| fun _ ->
        let vec = seq { 1..14338 } |> RRBVector.ofSeq
        RRBVectorProps.checkProperties vec "Original vector"
        let l, r = doSplitTest vec 31
        doJoinTest l r
        let l, r = doSplitTest vec 32
        doJoinTest l r
        let l, r = doSplitTest vec 33
        doJoinTest l r

    testCase "Split just before, exactly at, and just after end of first node, non-full vector" <| fun _ ->
        let vecRepr = """
            [32 32 32 32 32 32 30 32 32 32 32 32 32 32 32 32 32 32 32 27 32 32 32 32 28 32 32 32 32 32 28 32]
            [32 32 32 28 30 32 32 32 32 30 28 32 32 32 32 30 32 27 32 32 32 32 31 32 32 32]
            [32 30 32 31 32 31 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 31 32 32 32 32 32 32 32 32 32]
            [32 30 32 32 32 32 32 32 32 32 32 32 32 32 32 31 32 32 29 32 32 32 32 31 29 32 32]
            [29 28 30 28 30 32 25 30 27 32 32 31 26 32 28 30 23 26 31 30 29 27 29 28 32 31 27 29 32 28 32 29]
            [32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32]
            [32 32 29 30 28 32 31 29 32 29 29 26 26 24 26 23 27 31 30 26 32 32 24 29 30 32 32]
            [29 27 32 31 25 32 30 30 32 23 28 32 31 30 27 29 29 32 30 30 32 27 30 30 30 32 24 31 32 28 30 23]
            [31 31 31 29 29 32 31 32 32 30 31 25 22 28 32 32 32 32 31 22 27 29 32 32 32 32 24 26 28 32]
            [32 32 32 30 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32]
            [32 32 32 32 31 32 32 32 32 32 32 32 32 27 32 32 32 32 32 32 32 31 32 32 32 32 32 32 32 32 32 32]
            [32 28 28 26 32 32 32 32 28 27 27 30 26 29 27 31 32 27 24 21 23 24 32 32 30 32 32]
            [32 23 20 21 30 31 30 27 27 30 29 32 31 28 25 31 32 32 32 32 32 28 27 28 27 32 27 27 32 32 32]
            [32 32 32 32 25 25 27 32 30 32 32 32 21 23 28 32 32 32 32 24 28 28 29 32 31 32 32 29 26 28]
            [26 29 32 28 28 32 28 31 32 26 27 29 30 32 32 30 26 28 30 32 30 24 32 31 30 27 26 29 30 31 31 32]
            [27 30 27 31 30 26 31 32 32 31 32 30 27 32 31 28 32 30 31 27 30 32 32 28 28 29 28 30 32 26 32 32]
            [28 29 32 29 31 30 26 27 31 32 32 32 32 29 22 30 32 32 30 26 32 26 28 32 29 32 32 25 27]
            [27 30 30 32 32 29 29 32 28 32 27 30 32 32 30 27 29 31 32 32 27 30 32 30 32 27 32 27 30 32 32 32]
            [30 31 26 25 32 31 32 23 26 31 32 25 22 30 31 32 24 23 30 31 32 23 24 30]
            [32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 31 32 32 32 32 32 32 32 32 32]
            [32 32 19 26 32 32 32 32 32 23 28 24 32 32 32 32 32 22 27 32 30 32 32 29 27 21 26 32 32 32]
            [32 26 28 28 32 31 29 30 31 29 32 27 27 31 29 26 28 31 31 32 32 31 30 29 27 30 26 30 30 27 27 28]
            [28 31 32 32 28 32 27 32 32 27 26 32 29 28 32 32 24 30 29 26 32 31 32 22 28 26 32 32]
            [32 27 25 32 32 32 31 28 28 32 32 32 32 31 24 23 26 31 32 30 30 28 31 28 32 32 24 25 29 32 32]
            [30 30 30 32 30 26 31 32 31 32 27 31 31 30 28 29 32 28 32 32 32 32 30 26 32 32 32 26 27 30 30 29]
            [28 32 19 32 32 28 25 32 29 32 32 32 20 27 28 28 32 32 27 26 32 32 32]
            [31 32 27 32 29 32 32 32 32 30 30 30 27 32 29 31 32 30 32 25 32 30 26 32 28 32 32 25 32 32 32 32]
            [29 32 32 32 32 25 31 30 31 32 26 32 31 32 27 28 32 31 32 26 28 29 31 32 31 32 32 31 32 29 25 32]
            [30 32 31 30 31 26 32 28 32 32 30 32 26 32 31 32 30 27 32 22 32 32]
            [32 28 26 32 32 32 32 25 32 32 32 30 31 32 28 32 30 32 32 27 32 31 32 32 25 32 28 32 32 28 32 32]
            [32 32 32 32 32 32 32 32 32 29 26 32 32 32 32 32 31 29 32 32 31 32 30 32 32 32 32 32 21 27 32]
            [32 32 27 32 32 22 32 29 30 32 32 31 32 32 27 32 32 30 32 32 29 32 32 28 32 32 32 30]
            T26"""
        // let vec = RRBVectorGen.treeReprStrToVec (vecRepr.Trim().Replace("\n", " ").Replace("             ", " "))
        let vec = RRBVectorGen.looserTreeReprStrToVec vecRepr
        RRBVectorProps.checkProperties vec "Original vector"
        let l, r = doSplitTest vec 31
        doJoinTest l r
        let l, r = doSplitTest vec 32
        doJoinTest l r
        let l, r = doSplitTest vec 33
        doJoinTest l r

    testCase "Slicing with vec.[a..b] syntax" <| fun _ ->
        // TODO: Move to slicing tests?
        let vec = RRBVectorGen.treeReprStrToVec "T18"
        let arr = vec |> RRBVector.toArray
        let slicedV = vec.[..15]
        let slicedA = arr.[..15]
        Expect.equal slicedV.Length slicedA.Length "Vector slice should be equivalent to array slice for transients as well"
        let t = (vec :?> RRBPersistentVector<_>).Transient()
        let slicedT = t.[..15]
        let slicedA = arr.[..15]
        Expect.equal slicedT.Length slicedA.Length "Vector slice should be equivalent to array slice for transients as well"

    testCase "Splitting (via an insert) the root of a transient will cause an expanded node to be the new root" <| fun _ ->
        let repr = "30 32 29 29 30 24 30 30 30 32 29 31 25 27 25 28 30 28 T32"
        let vec = RRBVectorGen.treeReprStrToVec repr
        let push = RRBVectorTransientCommands.VecCommands.push
        let insert = RRBVectorTransientCommands.VecCommands.insert
        let cmds = [push 215; insert (-39,46); push 113; insert (24,49); insert (30,5); insert (-28,81); insert (27,73); insert (-76,34)]
        // let doActionListTest (actions : RRBVectorMoreCommands.Cmd list) vec postPopCount postPushCount =
        // logger.debug (eventX "Starting with {vec}" >> setField "vec" (RRBVectorGen.vecToTreeReprStr vec))
        let mutable current = (vec :?> RRBPersistentVector<_>).Transient()
        let logVec action vec =
            // logger.warn (
            //     eventX "After {cmd}, vec was {vec} with actual structure {structure}"
            //     >> setField "cmd" action
            //     >> setField "vec" (RRBVectorGen.vecToTreeReprStr vec)
            //     >> setField "structure" (sprintf "%A" vec))
            ()
        for action in cmds do
            if action.ToString() = "insert (-76,34)" then
                () // Breakpoint
            current <- current |> action.RunActual
            logVec (action.ToString()) current
            RRBVectorProps.checkProperties current <| sprintf "Vector after %s" (action.ToString())

        ()

    testCase "Splitting a transient will expand nodes appropriately" <| fun _ ->
        let repr = """
            [18 27 24 27 M M M M M 29 29 29 M-1 M M M M M M 28 26 28 M M M 30]
            [M-1 M M M M M M M 30 M 30 M 29 M M M M M M-1 M M 30 M-1 M 27 28]
            [M-1 M 30 M 27 29 26 M 27 M M-1 M M-1 30 M 30 M 26 28 28 M-1 M M M-1]
            [M M 30 M 26 30 27 M M M 29 M M M M-1 29 28 M 27 30 26 30 M-1 M M 27 M-1 M M-1]
            [M M M M M 23 M M 28 M M 25 M 28 M M M M 28 M-1 M 29 M M 30 M M M]
            [M M 30 M M 25 29 M M M M-1 28 M M 29 M 29 28 30 M-1 M M 30 28 M 29]
            [M 29 M-1 M 28 26 M M M M-1 M M M 28 M M M M M M M-1 30 30 M M M]
            [26 M M M 28 M M M M-1 M M M-1 29 28 M M M M M M-1 M 29 M-1 27]
            [M M M M M 26 28 M-1 M M M M M M M M M M M-1 M M 30]
            [M M-1 M M-1 M M M 28 M-1 30 28 M M M M M M 29 M M 29 M M M M]
            [M M M M M M M M 30 M M 30 M M M M 17 18 M]
            T32"""
        let vec = repr |> RRBVectorGen.looserTreeReprStrToVec
        let slicedVec = vec.[..77]
        RRBVectorProps.checkProperties slicedVec "Sliced persistent vector"
        let t = (vec :?> RRBPersistentVector<_>).Transient()
        let slicedT = t.[..77]
        RRBVectorProps.checkProperties slicedT "Sliced transient vector"


(* Disabling this test since we no longer apply this invariant, and instead we allow saplings to have a mix of non-full root and tail
    // Note that by allowing saplings to have a mix of non-full root and tail, it allows us to reuse the node arrays and go faster in this particular scenario
    testCase "Slicing at tail can promote new tail and adjust tree so last leaf is full" <| fun _ ->
        let vec = RRBVectorGen.treeReprStrToVec "M-1 M T1"
        let vec' = vec.Take (Literals.blockSize * 2 - 1)
        RRBVectorProps.checkProperties vec' "Sliced vector"
        match vec' with
        | :? RRBSapling<int> as sapling ->
            Expect.equal sapling.TailOffset (Literals.blockSize) "Wrong tail offset"
            Expect.equal sapling.Tail.Length (Literals.blockSize - 1) "Wrong tail length"
        | :? RRBTree<int> as tree -> // Or perhaps just: failwithf "Vector after slice should be sapling, instead is %A" tree
            Expect.equal tree.TailOffset (Literals.blockSize) "Wrong tail offset"
            Expect.equal tree.Tail.Length (Literals.blockSize - 1) "Wrong tail length"
        | _ -> failwith "Unknown RRBVector subclass: this test needs to be taught about this variant"
*)
    testCase "Merging specific scenarios" <| fun _ ->
        // A ends with [M*20] TM.
        // B is [M M M M M 5] TM-3.
        // Try it.
        let a = RRBVectorGen.treeReprStrToVec "[M*M] [M*M] [M*M] [M*20] TM"
        let b = RRBVectorGen.treeReprStrToVec "M M M M 5 TM-3"
        RRBVectorProps.checkProperties a "Original A"
        // RRBVectorProps.checkProperties b "Original B"
        // TODO: Write this test in such a way that original B *passes* properties, so that it's a fair test.
        let joined = RRBVector.append a b
        RRBVectorProps.checkProperties joined "Joined"

    testCase "Pairwise should not throw for 0 or 1-length vectors" <| fun _ ->
        let empty = RRBVector.empty
        let singleton = empty |> RRBVector.push 42
        let pe = RRBVector.pairwise empty
        let ps = RRBVector.pairwise singleton
        Expect.equal (pe |> RRBVector.length) 0 "Pairwise with 0 input items should return empty vector"
        Expect.equal (ps |> RRBVector.length) 0 "Pairwise with 1 input item should return empty vector"

    testCase "Testing skipWhile" <| fun _ ->  // TODO: Move this to its appropriate place. And the pairwise test above
        let vec = [|-5..5|] |> RRBVector.ofArray
        RRBVectorProps.checkProperties vec "Original vector"
        let vec' = vec |> RRBVector.skipWhile (fun x -> x <= 0)
        RRBVectorProps.checkProperties vec' "Vector after skipWhile"
        Expect.equal vec'.Length 5 "The skipWhile function should have returned 5 positive items"
        let vec'' = vec |> RRBVector.skipWhile (fun x -> x < 10)
        RRBVectorProps.checkProperties vec'' "Vector after skipWhile"
        Expect.equal vec''.Length 0 "The second skipWhile invocation should have returned 0 items"
        let vec''' = vec |> RRBVector.skipWhile (fun x -> x > 10)
        RRBVectorProps.checkProperties vec''' "Vector after skipWhile"
        Expect.equal vec'''.Length 11 "The third skipWhile invocation should have returned 11 items"

    testCase "Testing takeWhile" <| fun _ ->  // TODO: Move this to its appropriate place. And the skipWhile test above
        let vec = [|-5..5|] |> RRBVector.ofArray
        RRBVectorProps.checkProperties vec "Original vector"
        let vec' = vec |> RRBVector.takeWhile (fun x -> x < 0)
        RRBVectorProps.checkProperties vec' "Vector after takeWhile"
        Expect.equal vec'.Length 5 "The takeWhile function should have returned 5 negative items"
        let vec'' = vec |> RRBVector.takeWhile (fun x -> x < 10)
        RRBVectorProps.checkProperties vec'' "Vector after takeWhile"
        Expect.equal vec''.Length 11 "The second takeWhile invocation should have returned 11 items"
        let vec''' = vec |> RRBVector.takeWhile (fun x -> x > 10)
        RRBVectorProps.checkProperties vec''' "Vector after takeWhile"
        Expect.equal vec'''.Length 0 "The third takeWhile invocation should have returned 0 items"

    testCase "Merging short right tree will shift into tail as needed" <| fun _ ->
        let a = RRBVectorGen.treeReprStrToVec "M M T1"
        let b = RRBVectorGen.treeReprStrToVec "M T1"
        RRBVectorProps.checkProperties a "Original A"
        RRBVectorProps.checkProperties b "Original B"
        let joined = RRBVector.append a b
        RRBVectorProps.checkProperties joined "Joined"

    testCase "Merging short left tree will have correct length" <| fun _ ->
        let a = RRBVectorGen.treeReprStrToVec "TM/2"
        let b = RRBVectorGen.treeReprStrToVec "[M M M-2] [M-1 M-3 M] T6"
        doJoinTest a b

    testCase "Merging two VERY short vectors will have correct length" <| fun _ ->
        let vL = RRBVectorGen.treeReprStrToVec "T1"
        let vR = RRBVectorGen.treeReprStrToVec "M T1"
        doJoinTest vL vR

(* Disabling test since we no longer apply this invariant, and instead we allow saplings to have a mix of non-full root and tail
    testCase "Splitting root+tail vector to two tail-only vectors has empty root on both vectors" <| fun _ ->
        let vec = RRBVectorGen.treeReprStrToVec "M T1"
        let a,b = vec |> RRBVector.split 1
        RRBVectorProps.checkProperties a "Original A"
        RRBVectorProps.checkProperties b "Original B"
        let joined = RRBVector.append a b
        RRBVectorProps.checkProperties joined "Joined"
        Expect.equal a.Length 1 "Wrong length for A"
        Expect.equal b.Length Literals.blockSize "Wrong length for B"
        match a with
        | :? RRBTree<int> as tree -> failwithf "Left tree after split should be sapling, instead is %A" tree
        | :? RRBSapling<int> as sapling ->
            Expect.equal sapling.Root.Length 0 "Root node of A should be empty"
            Expect.equal sapling.TailOffset 0 "Tail offset of A should be zero"
        | _ -> failwith "Unknown RRBVector subclass: this test needs to be taught about this variant"
        match b with
        | :? RRBTree<int> as treeR -> failwithf "Right tree after split should be sapling, instead is %A" treeR
        | :? RRBSapling<int> as sapling ->
            Expect.equal sapling.Root.Length 0 "Root node of B should be empty"
            Expect.equal sapling.TailOffset 0 "Tail offset of B should be zero"
        | _ -> failwith "Unknown RRBVector subclass: this test needs to be taught about this variant"
*)
    testCase "Splitting root+tail vector to one tail-only vector and one root+tail vector has empty root on only one vector" <| fun _ ->
        let vec = RRBVectorGen.treeReprStrToVec "M M TM/2"
        let a,b = vec |> RRBVector.split 1
        let a = a :?> RRBPersistentVector<int>
        let b = b :?> RRBPersistentVector<int>
        RRBVectorProps.checkProperties a "Original A"
        RRBVectorProps.checkProperties b "Original B"
        let joined = RRBVector.append a b
        RRBVectorProps.checkProperties joined "Joined"
        Expect.equal a.Length 1 "Wrong length for A"
        Expect.equal b.Length (Literals.blockSize * 2 + (Literals.blockSize / 2) - 1) "Wrong length for B"
        Expect.equal a.Root.NodeSize 0 "Root node of A should be empty"
        Expect.equal a.TailOffset 0 "Tail offset of A should be zero"
        Expect.equal b.Root.NodeSize 2 "Root node of B should be size 2"
        Expect.isGreaterThan b.TailOffset 0 "Tail offset of B should be non-zero"

    testCase "Joining two vectors that push up a new root should produce the right root length, shift, and tail offset" <| fun _ ->
        let a = RRBVectorGen.treeReprStrToVec "M M T1"
        let b = RRBVectorGen.treeReprStrToVec "M*M T1"
        let bShift = (b :?> RRBPersistentVector<int>).Shift
        let joined = RRBVector.append a b :?> RRBPersistentVector<int>
        RRBVectorProps.checkProperties joined "Joined"
        Expect.equal joined.Length (a.Length + b.Length) "Wrong length for joined vector"
        Expect.equal joined.Shift (bShift + Literals.blockSizeShift) "Joined vector should have pushed up a new root"
        Expect.equal joined.Root.NodeSize 2 "Joined vector should have pushed up a new root of size 2"
        Expect.equal joined.TailOffset (joined.Length - 1) "Joined vector should have just one item in its tail"

    testCase "Removing an item from the last leaf maintains the invariant" <| fun _ ->
        let vec = RRBVectorGen.treeReprStrToVec "M M T2"
        let vec' = vec.Remove (Literals.blockSize + 1) :?> RRBPersistentVector<int>
        RRBVectorProps.checkProperties vec' "Vector after removal from last leaf"
        Expect.equal vec'.Tail.Length 1 "Vector adjustment should have removed one item from the tail to maintain the invariant"

    testCase "Removing an item from the last leaf can promote new leaf to maintain the invariant" <| fun _ ->
        let vec = RRBVectorGen.treeReprStrToVec "M M T1"
        let oldShift = (vec :?> RRBPersistentVector<int>).Shift
        let vec' = vec.Remove (Literals.blockSize + 1) :?> RRBPersistentVector<int>
        RRBVectorProps.checkProperties vec' "Vector after removal from last leaf"
        Expect.equal vec'.Tail.Length Literals.blockSize "Vector adjustment should have promoted new leaf"
        Expect.equal vec'.Shift Literals.blockSizeShift "Vector adjustment should have left tree with height 1"
        Expect.equal vec'.Root.NodeSize 1 "Vector adjustment should have left tree with single-node root"

    testCase "Inserting into last leaf can still maintain invariant when height is not affected" <| fun _ ->
        let vec = RRBVectorGen.treeReprStrToVec "M/2-1 M T2"
        let vec' = vec.Insert (Literals.blockSize / 2) 512
        RRBVectorProps.checkProperties vec "Original vector"
        RRBVectorProps.checkProperties vec' "Vector after insertion"

    testCase "Inserting into last leaf can still maintain invariant when rebalance causes lower height" <| fun _ ->
        let vec = RRBVectorGen.treeReprStrToVec "M-2 M T1"
        let vec' = vec.Insert (vec.Length - 2) 512
        // You'd think this would turn into "M M-1 T1", but that would break the invariant. So instead a node is shifted out of the tail to make M TM, which has height 0.
        RRBVectorProps.checkProperties vec "Original vector"
        RRBVectorProps.checkProperties vec' "Vector after insertion"

    testCase "Pushing tail down in root+tail tree, when root size is equal to blockSizeShift, creates a correct tree" <| fun _ ->
        // This was a particularly subtle bug, since it only triggered in very specific cases.
        let vec = RRBVectorGen.treeReprStrToVec "5 TM"
        let step1 = vec |> RRBVector.insert (vec.Length - 2) -512
        RRBVectorProps.checkProperties step1 "Vector after first step"
        let step2 = step1 |> RRBVector.remove 5
        RRBVectorProps.checkProperties step2 "Vector after second step"

    testCase "An insert that splits the last leaf will not cause later pushes to break the invariant" <| fun _ ->
        let vec = RRBVector.ofSeq { 0..1982 }
        let vec = RRBVectorGen.treeReprStrToVec "[M*M] [M*M-3] T31"
        let mutable vec' = vec.Insert (vec.Length - 42) -42
        RRBVectorProps.checkProperties vec "Original vector"
        RRBVectorProps.checkProperties vec' "Vector after insertion"
        for i = 1 to Literals.blockSize * 2 + 1 do
            vec' <- vec'.Push i
        RRBVectorProps.checkProperties vec' "Vector after 65 pushes"
        // Up to this point, the vector looks like "[M*M] [M*M-4 M/2+1 M/2 M M] TM", and the root node can still be a full node
        vec' <- vec'.Push (Literals.blockSize * 2 + 2)
        // Now the vector looks like "[M*M] [M*M-4 M/2+1 M/2 M M] [M] T1", and the root node should have been converted to a relaxed node
        RRBVectorProps.checkProperties vec' "Vector after 66th push"

    testCase "A join that pushes down a short tail will still maintain the invariant" <| fun _ ->
        // The join ends up with [M*M-1 4] [M] T1 at the end of the vector. Then the pop promotes the M to the tail,
        // and ends up with [M*M-1 4] -- which was a full node, so now it needs to shift items from the tail to maintain the invariant.
        let vL = RRBVectorGen.treeReprStrToVec "[M 6 7 M 6 7] [6 6 7 5 6 M 7] [5 6 6 5 4 4 6] [M*M-1] T4"
        let vR = RRBVectorGen.treeReprStrToVec "M T1"
        RRBVectorProps.checkProperties vL "Orig left vector"
        RRBVectorProps.checkProperties vR "Orig right vector"
        let vR' = RRBVector.pop vR
        let joinedOrig = RRBVector.append vL vR
        RRBVectorProps.checkProperties joinedOrig "Joined orig vector"
        let joinedOrigThenPop = RRBVector.pop joinedOrig
        RRBVectorProps.checkProperties joinedOrigThenPop "Joined orig vector, then popped"
        let joined = RRBVector.append vL vR'
        RRBVectorProps.checkProperties joined "Joined vector"
        Expect.vecEqual joined (RRBVector.pop joinedOrig) "pop right + join did not equal join + pop"
  ]

let mergeTests =
  testList "Merge tests" [
    testCase "adjustTree is needed in merge algorithm when right tree is root+tail" <| fun _ ->
        let vL = RRBVectorGen.treeReprStrToVec <| sprintf "M*M T1"
        let vR = RRBVectorGen.treeReprStrToVec <| sprintf "M T1"
        RRBVectorProps.checkProperties vL "Left half of merge"
        RRBVectorProps.checkProperties vR "Right half of merge"
        let joined = RRBVector.append vL vR
        RRBVectorProps.checkProperties joined <| sprintf "Joined vector"

    testCase "adjustTree is needed in merge algorithm when tree height increases" <| fun _ ->
        let vL = RRBVectorGen.treeReprStrToVec <| sprintf "M*M-2 TM/2"
        let vR = RRBVectorGen.treeReprStrToVec <| sprintf "M-1 M M-4 T1"
        RRBVectorProps.checkProperties vL "Left half of merge"
        RRBVectorProps.checkProperties vR "Right half of merge"
        let joined = RRBVector.append vL vR
        RRBVectorProps.checkProperties joined <| sprintf "Joined vector"

    testCase "adjustTree is needed in merge algorithm when tree height does not increase" <| fun _ ->
        let vL = RRBVectorGen.treeReprStrToVec <| sprintf "M*M/2 TM"
        // let vR = RRBVectorGen.treeReprStrToVec <| sprintf "M M M-1 T2" // This makes an invalid tree
        let fullLeaf = [|1..Literals.blockSize|] |> RRBNode<int>.MkLeaf nullOwner
        let fullLeafMinusOne = [|1..Literals.blockSize-1|] |> RRBNode<int>.MkLeaf nullOwner
        let vR_root = RRBRelaxedNode<int>(ref null, [|fullLeaf; fullLeaf; fullLeafMinusOne|], [|Literals.blockSize; Literals.blockSize*2; Literals.blockSize*3-1|])
        let vR = RRBPersistentVector<int>(Literals.blockSize * 3 + 1, Literals.blockSizeShift, vR_root, [|1;2|], Literals.blockSize * 3 - 1) :> RRBVector<int>
        RRBVectorProps.checkProperties vL "Left half of merge"
        // vR does not pass property checks, because we have a property that verifies that no could-have-been-full RRBRelaxedNodes are created.
        // So we disable the property checks for vR for this test only, because we *do* want its root to be an RRBRelaxedNode.
        // RRBVectorProps.checkProperties vR "Right half of merge"
        let joined = RRBVector.append vL vR
        RRBVectorProps.checkProperties joined <| sprintf "Joined vector"

    testCase "Joining vectors where the left tree is taller than the right produces valid results" <| fun _ ->
        let vL = RRBVectorGen.treeReprStrToVec "[M*M]*3 TM/4"
        let vR = RRBVectorGen.treeReprStrToVec "M*M/2 TM"
        doJoinTest vL vR

    testCase "Joining vectors where the left tree is taller than the right produces valid results, with larger vectors" <| fun _ ->
        let vL = RRBVectorGen.treeReprStrToVec "[M*M] [M*M] [M*M] [M*20] TM"
        let vR = RRBVectorGen.treeReprStrToVec "M M M M 5 TM-3"
        doJoinTest vL vR

    testCase "Joining vectors will rebalance properly at heights above leaf level" <| fun _ ->
        // There used to be a subtle bug in the rebalancing code, where rebalancing at the twig (or higher) levels would make incorrectly-sized nodes.
        let vL = RRBVectorGen.treeReprStrToVec "[28 M-1 26 M-1 M 21 M 26 M M 28 M M-1 29 26 29 28 23 M M M M 26 M 29 29 M 23 M-1 M M M] [M M M M M] T30"
        let vR = RRBVectorGen.treeReprStrToVec "[25 M-1 19 25 M-1 M M M M M M 29 24 28 30 M M M M 27 30 27 M 27 29 25 27] [M M M M M M M M M M M M M M M M M 28 M M M M-1 M] [M M M M M M M 28 M M M M M M M 28 M-1 M M M M M M M-1 M] [22 22 26 30 M 29 M M 27 27 M M M M M M M 26 29 M 28 27 24 27 26 22 29] [M M M M M M M M M M M M M M M M M M M M M M] [27 28 24 M M 29 28 M-1 30 30 23 29 25 24 25 M M 25 M-1 M 26 28] [26 23 M 27 M-1 29 23 M 27 26 26 29 M M M M 30 24 M-1 27 28 29 27 27] [M M 21 M M 30 19 M M 27 M M 26 M 27 24 M M 29 30 25 M 23] [30 M-1 M 26 M 26 M-1 M-1 M-1 28 M 24 30 30 27 27 29 27 27 M 25 M-1 28 M-1 28 30] [27 M 22 28 M 27 M 28 M 29 M M M-1 27 29 27 M-1 26 M 23 M M] [M-1 26 M M 25 M 26 24 M 29 M-1 30 29 M M 28 26 28 26 27 M 28] [29 26 M 28 M 30 M 30 M M 23 M M M 30 M 29 M 28 27 M 21 M M M M] T27"
        RRBVectorProps.checkProperties vL "Left half of merge"
        RRBVectorProps.checkProperties vR "Right half of merge"
        let joined = RRBVector.append vL vR
        RRBVectorProps.checkProperties joined <| sprintf "Joined vector"

    testCase "Joining any number of small vectors on the left (from tail-only to root+tail) with a large vector on the right will produce correct results" <| fun _ ->
        // Regression test for the "left vector is short" logic in the vector-join algorithm
        let vL = RRBVectorGen.treeReprStrToVec "T8"
        let vR = RRBVectorGen.treeReprStrToVec "[M 29 M M M M M 29 M M 29 M M M 29 27 M] [M M-1 M-1 M M M 30 M-1 29 29] [23 M M M 23 21 M 24 27 M-1 30 29 21 23 28] [M M M M-1 30 28 28 M M M-1 M M 28 M] [24 24 23 27 24 17 16 20 M] T25"
        RRBVectorProps.checkProperties vL "Left half of merge"
        RRBVectorProps.checkProperties vR "Right half of merge"
        for i in 1..Literals.blockSize do
            let mutable vL' = vL
            for x in 1..i do
                vL' <- vL' |> RRBVector.push x
            RRBVectorProps.checkProperties vL' "Left half of merge"
            let joined = RRBVector.append vL' vR
            RRBVectorProps.checkProperties joined <| sprintf "Joined vector (with left tail %d)\nvL was %s and vR was %s" i (RRBVectorGen.vecToTreeReprStr vL') (RRBVectorGen.vecToTreeReprStr vR)
            Expect.equal joined.Length (vL'.Length + vR.Length) "Joined vector length should be sum of original vectors' lengths"

    testCase "Joining two large vectors can correctly trigger a rebalance" <| fun _ ->
        // Without a rebalance, the joined vector's root would end up with six nodes, but with the rebalance, the left vector can be squeezed into the right vector's leftmost twig.
        let vL = RRBVectorGen.treeReprStrToVec "M*15 T24" :?> RRBPersistentVector<int>
        let vR = RRBVectorGen.treeReprStrToVec "[M 29 M M M M M 29 M M 29 M M M 29 27 M] [M M-1 M-1 M M M 30 M-1 29 29] [23 M M M 23 21 M 24 27 M-1 30 29 21 23 28] [M M M M-1 30 28 28 M M M-1 M M 28 M] [24 24 23 27 24 17 16 20 M] T25" :?> RRBPersistentVector<int>
        RRBVectorProps.checkProperties vL "Left half of merge"
        RRBVectorProps.checkProperties vR "Right half of merge"
        let joined = RRBVector.append vL vR :?> RRBPersistentVector<int>
        RRBVectorProps.checkProperties joined <| sprintf "Joined vector\nvL was %s and vR was %s" (RRBVectorGen.vecToTreeReprStr vL) (RRBVectorGen.vecToTreeReprStr vR)
        Expect.equal joined.Root.NodeSize vR.Root.NodeSize "Rebalance should have left the joined vector's root the same size as the original right vector's root"
  ]

let doSplitTransientTest (RRBVectorTransientCommands.SplitTestInput (vec, cmds)) =
    let vec = if vec |> isTransient then (vec :?> RRBTransientVector<_>).Persistent() else vec :?> RRBPersistentVector<_>
    let mailbox = RRBVectorTransientMailboxTest.startSplitTesting vec cmds
    let result = mailbox.PostAndReply RRBVectorTransientMailboxTest.AllThreadsResult.Go
    match result with
    | RRBVectorTransientMailboxTest.AllThreadsResult.Go _ -> failtest "Shouldn't happen"
    | RRBVectorTransientMailboxTest.AllThreadsResult.OneFailed (position, cmdsDone, vec, arr, cmd, errorMsg) ->
        failtestf "Split vector number %d failed on %A after %d commands, with message %A; vector was %A and corresponding array was %A" position cmd cmdsDone errorMsg vec arr
    | RRBVectorTransientMailboxTest.AllThreadsResult.AllCompleted _ -> ()

let splitTransientTests =
  testList "MailboxProcessor + Transient tests" [
    testPropSm "small vectors (up to root+tail in size)" doSplitTransientTest
    testPropMed "medium vectors (up to about 1-2 levels high)" doSplitTransientTest
    testProp "large vectors (up to about 3-4 levels high)" doSplitTransientTest
    testPropSm "small vectors into thing" <| fun (vec : RRBVector<int>) ->
        RRBVectorTransientCommands.doTestXL vec
    testPropSm "small commands" <| fun (vec : RRBVector<int>) ->
        RRBVectorTransientCommands.doComplexTest vec
    testPropMed "medium commands" <| fun (vec : RRBVector<int>) ->
        RRBVectorTransientCommands.doComplexTest vec
    testProp "large commands" <| fun (vec : RRBVector<int>) ->
        // logger.warn (eventX "{vec}" >> setField "vec" (RRBVectorGen.vecToTreeReprStr vec))
        RRBVectorTransientCommands.doComplexTest vec

    testCase "Transients can be split apart and re-appended again" <| fun _ ->
        let vec = (RRBVectorGen.treeReprStrToVec "M T5" :?> RRBPersistentVector<_>).Transient()
        let vL, vR = vec.Split (Literals.blockSize + 2)
        vL.Append vR |> ignore
        RRBVectorProps.checkProperties vL "Vector after appending"

    // Test cases to move into regressionTests once they're done
    ptestCase "template for regression tests on split commands that failed" <| fun _ ->
        let push = RRBVectorTransientCommands.VecCommands.push
        let pop = RRBVectorTransientCommands.VecCommands.pop
        let insert = RRBVectorTransientCommands.VecCommands.insert
        let remove = RRBVectorTransientCommands.VecCommands.remove
        let slice = RRBVectorTransientCommands.VecCommands.slice
        // Edit cmdsL and cmdsR below
        let cmds = []
        let cmdsL = [insert (-16,57); slice (None,Some -79); remove -15; push 80; remove 46; push 73; remove -7; insert (22,84); insert (59,53); push 80; push 14; insert (27,65)]
        let cmdsR = [insert (50,59); insert (87,93); insert (-48,71); slice (Some 58,Some -61); remove -85; remove 98; insert (85,85); insert (28,24); insert (-78,76); insert (-64,83); insert (6,72); insert (-90,78); push 40]

        let logVec cmd vec =
            // logger.debug (
            //     eventX "After {cmd}, vec was {vec} with actual structure {structure}"
            //     >> setField "cmd" cmd
            //     >> setField "vec" (RRBVectorGen.vecToTreeReprStr vec)
            //     >> setField "structure" (sprintf "%A" vec))
            ()

        // Edit vec below
        let vec = RRBVectorGen.looserTreeReprStrToVec "[M M M 29 M M M M M 27 M M 28 M-1 M-1 30 M M M M M-1 M M M M-1 M M M M] [28 27 M 30 30 M-1 30 M M M M M 29 M M M 27 M M M M 28 M M M M-1 30 30 29 M-1 27 M] [M M 29 28 M M M M 28 29 29 M M M M M 27 26 29 30 M M 29 30 28 28 28 M 29] [M M-1 M M M M M 24 28 28 29 30 M M-1 25 26 29 M M M M-1 M M M 28 M M] [M M M M M M M M M M M M M M M M M M M M M M 30 M M M M-1 M M M] [M M 27 M 30 M M M-1 25 M 27 M M 30 M 30 28 M M M M-1 M 25 28 29 29] [M M M M-1 M M 27 28 M 26 M M 29 30 M M 29 M M-1 M M-1 M 29 M M 30 M M] [M-1 29 24 29 M M M 30 M 28 28 M M M M-1 M 30 30 M M M-1 M M 30 29 M M M M M] [M M M 30 M M M 29 M M M M 26 M M M M M M M M-1 M 23 M] [M M M M M M-1 29 29 M M M-1 M M M M M M M M M M 26 M-1 M M M 28 M-1 M-1 27] [M M M M M M M M M-1 30 M M M M M M M M M M 26 M M M M M M-1 M-1 M M M] [M M M M 30 M M M M M M M M M M M 27 M M M M M M M M M M M M 23 M] [M M M M M M M M M M M M M M M-1 M M M M M M M M M M] [M-1 M M M M M M-1 M M M M M M-1 M M M M M-1 M M M M M 30 M M] [30 M 29 M 25 30 M 24 27 M 29 M 28 M M-1 28 M 27 M 30 27 24 25 28 29 30 M 30 M 26 M 28] [26 24 M M 28 28 26 28 M M-1 M M-1 M M 28 27 24 M 30 27 M M 24 27 24 25 26] [30 27 M M-1 M 28 M 25 28 M-1 M M 29 28 M 28 25 M M-1 M 29 29 28 27 M-1 28 30 M 30 29 M-1 26] [M M M 29 26 29 26 M 29 M M 22 28 M 28 28 M 28 M 24 M M 29 27 M M 24 26 27 M] [26 M M M M 23 27 M M-1 30 M 30 30 29 28 M M-1 M M 27 23 29 M 23 M M M 26 M M] [26 27 M 27 28 M 30 M-1 M-1 29 M 30 30 M 27 23 26 M M M 20 26 30 M-1 M M] [26 M-1 26 M M-1 30 28 M M 28 M M M M M M 28 29 M 26 M M M 27 28 M M M 26 M M-1 28] [M M M M 27 30 M M M M M-1 29 M 25 21 29 28 28 M M-1 M 28 25 27 M M-1 28] [M 26 M 28 27 25 30 M M 29 M M M-1 M M M M M M M 26 M-1 27 M M M 25] [M M M M M M M 27 M-1 M 29 28 27 M-1 M M M 29 29 29 24 26 29 29 30 M 30] T14"
        // Original was *28, but *2 is more than enough
        let mutable current = (vec :?> RRBPersistentVector<_>).Transient()
        // for cmd in cmds do
        //     current <- current |> cmd.RunActual
        //     logVec (cmd.ToString()) current
        //     RRBVectorProps.checkProperties current <| sprintf "Pre-split vector after %s" (cmd.ToString())

        let vL, vR = current.Split 74

        current <- vL :?> RRBTransientVector<_>
        for cmd in cmdsL do
            current <- current |> cmd.RunActual
            logVec (cmd.ToString()) current
            RRBVectorProps.checkProperties current <| sprintf "Left vector after %s" (cmd.ToString())

        current <- vR :?> RRBTransientVector<_>
        for cmd in cmdsR do
            current <- current |> cmd.RunActual
            logVec (cmd.ToString()) current
            RRBVectorProps.checkProperties current <| sprintf "Right vector after %s" (cmd.ToString())

        logger.warn (
            eventX "After all commands, vL was {vec} with actual structure {structure}"
            >> setField "vec" (RRBVectorGen.vecToTreeReprStr vL))
            // >> setField "structure" (sprintf "%A" vL))
        logger.warn (
            eventX "After all commands, vR was {vec} with actual structure {structure}"
            >> setField "vec" (RRBVectorGen.vecToTreeReprStr vR))
            // >> setField "structure" (sprintf "%A" vR))
        let joined = vL.Append vR
        logger.warn (
            eventX "After all commands, joined vector was {vec} with actual structure {structure}"
            >> setField "vec" (RRBVectorGen.vecToTreeReprStr joined))
            // >> setField "structure" (sprintf "%A" joined))
        RRBVectorProps.checkProperties joined "Joined vector after all commands run"

    testCase "Join transients where they... do something else, left smaller" <| fun _ ->
        let vL = RRBVectorGen.treeReprStrToVec "17 26 24 M M M M M T21"
        let vR = RRBVectorGen.treeReprStrToVec "[27 18 25 24 M-1 M 27 M M 28 M-1 M-1 30 M M M M M-1 M M M M-1 M M M M] [28 27 M 30 30 M-1 30 M M M M M 29 M M M 27 M M M M 28 M M M M-1 30 30 29 M-1 27 M] [M M 29 28 M M M M 28 29 29 M M M M M 27 26 29 30 M M 29 30 28 28 28 M 29] [M M-1 M M M M M 24 28 28 29 30 M M-1 25 26 29 M M M M-1 M M M 28 M M] [M M M M M M M M M M M M M M M M M M M M M M 30 M M M M-1 M M M] [M M 27 M 30 M M M-1 25 M 27 M M 30 M 30 28 M M M M-1 M 25 28 29 29] [M M M M-1 M M 27 28 M 26 M M 29 30 M M 29 M M-1 M M-1 M 29 M M 30 M M] [M-1 29 24 29 M M M 30 M 28 28 M M M M-1 M 30 30 M M M-1 M M 30 29 M M M M M] [M M M 30 M M M 29 M M M M 26 M M M M M M M M-1 M 23 M] [M M M M M M-1 29 29 M M M-1 M M M M M M M M M M 26 M-1 M M M 28 M-1 M-1 27] [M M M M M M M M M-1 30 M M M M M M M M M M 26 M M M M M M-1 M-1 M M M] [M M M M 30 M M M M M M M M M M M 27 M M M M M M M M M M M M 23 M] [M M M M M M M M M M M M M M M-1 M M M M M M M M M M] [M-1 M M M M M M-1 M M M M M M-1 M M M M M-1 M M M M M 30 M M] [30 M 29 M 25 30 M 24 27 M 29 M 28 M M-1 28 M 27 M 30 27 24 25 28 29 30 M 30 M 26 M 28] [26 24 M M 28 28 26 28 M M-1 M M-1 M M 28 27 24 M 30 27 M M 24 27 24 25 26] [30 27 M M-1 M 28 M 25 28 M-1 M M 29 28 M 28 25 M M-1 M 29 29 28 27 M-1 28 30 M 30 29 M-1 26] [M M M 29 26 29 26 M 29 M M 22 28 M 28 28 M 28 M 24 M M 29 27 M M 24 26 27 M] [26 M M M M 23 27 M M-1 30 M 30 30 29 28 M M-1 M M 27 23 29 M 23 M M M 26 M M] [26 27 M 27 28 M 30 M-1 M-1 29 M 30 30 M 27 23 26 M M M 20 26 30 M-1 M M] [26 M-1 26 M M-1 30 28 M M 28 M M M M M M 28 29 M 26 M M M 27 28 M M M 26 M M-1 28] [M M M M 27 30 M M M M M-1 29 M 25 21 29 28 28 M M-1 M 28 25 27 M M-1 28] [M 26 M 28 27 25 30 M M 29 M M M-1 M M M M M M M 26 M-1 27 M M M 25] [M M M M M M M 27 M-1 M 29 28 27 M-1 M M M 29 29 29 24 26 30 30 M M] T23"
        let tL = (vL :?> RRBPersistentVector<_>).Transient()
        let tR = (vR :?> RRBPersistentVector<_>).Transient()
        tR.Owner <- tL.Owner  // So they can be joined still as transients. Not a good idea outside of unit tests.
        tL.Append tR |> ignore
        RRBVectorProps.checkProperties tL "Joined vector after all commands run"

    testCase "Join transients where they... do something else, right smaller" <| fun _ ->
        let vL = RRBVectorGen.treeReprStrToVec "[27 18 25 24 M-1 M 27 M M 28 M-1 M-1 30 M M M M M-1 M M M M-1 M M M M] [28 27 M 30 30 M-1 30 M M M M M 29 M M M 27 M M M M 28 M M M M-1 30 30 29 M-1 27 M] [M M 29 28 M M M M 28 29 29 M M M M M 27 26 29 30 M M 29 30 28 28 28 M 29] [M M-1 M M M M M 24 28 28 29 30 M M-1 25 26 29 M M M M-1 M M M 28 M M] [M M M M M M M M M M M M M M M M M M M M M M 30 M M M M-1 M M M] [M M 27 M 30 M M M-1 25 M 27 M M 30 M 30 28 M M M M-1 M 25 28 29 29] [M M M M-1 M M 27 28 M 26 M M 29 30 M M 29 M M-1 M M-1 M 29 M M 30 M M] [M-1 29 24 29 M M M 30 M 28 28 M M M M-1 M 30 30 M M M-1 M M 30 29 M M M M M] [M M M 30 M M M 29 M M M M 26 M M M M M M M M-1 M 23 M] [M M M M M M-1 29 29 M M M-1 M M M M M M M M M M 26 M-1 M M M 28 M-1 M-1 27] [M M M M M M M M M-1 30 M M M M M M M M M M 26 M M M M M M-1 M-1 M M M] [M M M M 30 M M M M M M M M M M M 27 M M M M M M M M M M M M 23 M] [M M M M M M M M M M M M M M M-1 M M M M M M M M M M] [M-1 M M M M M M-1 M M M M M M-1 M M M M M-1 M M M M M 30 M M] [30 M 29 M 25 30 M 24 27 M 29 M 28 M M-1 28 M 27 M 30 27 24 25 28 29 30 M 30 M 26 M 28] [26 24 M M 28 28 26 28 M M-1 M M-1 M M 28 27 24 M 30 27 M M 24 27 24 25 26] [30 27 M M-1 M 28 M 25 28 M-1 M M 29 28 M 28 25 M M-1 M 29 29 28 27 M-1 28 30 M 30 29 M-1 26] [M M M 29 26 29 26 M 29 M M 22 28 M 28 28 M 28 M 24 M M 29 27 M M 24 26 27 M] [26 M M M M 23 27 M M-1 30 M 30 30 29 28 M M-1 M M 27 23 29 M 23 M M M 26 M M] [26 27 M 27 28 M 30 M-1 M-1 29 M 30 30 M 27 23 26 M M M 20 26 30 M-1 M M] [26 M-1 26 M M-1 30 28 M M 28 M M M M M M 28 29 M 26 M M M 27 28 M M M 26 M M-1 28] [M M M M 27 30 M M M M M-1 29 M 25 21 29 28 28 M M-1 M 28 25 27 M M-1 28] [M 26 M 28 27 25 30 M M 29 M M M-1 M M M M M M M 26 M-1 27 M M M 25] [M M M M M M M 27 M-1 M 29 28 27 M-1 M M M 29 29 29 24 26 30 30 M M] T23"
        let vR = RRBVectorGen.treeReprStrToVec "17 26 24 M M M M M T21"
        let tL = (vL :?> RRBPersistentVector<_>).Transient()
        let tR = (vR :?> RRBPersistentVector<_>).Transient()
        tR.Owner <- tL.Owner  // So they can be joined still as transients. Not a good idea outside of unit tests.
        tL.Append tR |> ignore
        RRBVectorProps.checkProperties tL "Joined vector after all commands run"

    testCase "Join transients where they... do something, left smaller" <| fun _ ->
        let vL = RRBVectorGen.treeReprStrToVec "17 27 M M M M T15"
        let vR = RRBVectorGen.treeReprStrToVec "[[M M 18 16 M*28] [M*M]*M-1] [[M*M]*M] [[17 16 M M 17 16]*1] T31"
        let tL = (vL :?> RRBPersistentVector<_>).Transient()
        let tR = (vR :?> RRBPersistentVector<_>).Transient()
        tR.Owner <- tL.Owner  // So they can be joined still as transients. Not a good idea outside of unit tests.
        tL.Append tR |> ignore
        RRBVectorProps.checkProperties tL "Joined vector after all commands run"

    testCase "Join transients where they... do something, right smaller" <| fun _ ->
        // This one passes, BTW
        let vL = RRBVectorGen.treeReprStrToVec "[[M M 18 16 M*28] [M*M]*M-1] [[M*M]*M] [[17 16 M M 17 16]*1] T31"
        let vR = RRBVectorGen.treeReprStrToVec "17 27 M M M M T15"
        let tL = (vL :?> RRBPersistentVector<_>).Transient()
        let tR = (vR :?> RRBPersistentVector<_>).Transient()
        tR.Owner <- tL.Owner  // So they can be joined still as transients. Not a good idea outside of unit tests.
        tL.Append tR |> ignore
        RRBVectorProps.checkProperties tL "Joined vector after all commands run"

    testCase "Join transients where they trigger a rebalance above the twig level, left smaller" <| fun _ ->
        let vL = RRBVectorGen.treeReprStrToVec "14 17 T25"
        let vR = RRBVectorGen.treeReprStrToVec "[[27 29 28 M-1 27 27 M-1 M 28 29 27 26 M 24 M 30 29 M-1 30 28 27 30 M M M 28 30 27 29 M-1 25] [30 M-1 27 M M-1 26 29 25 29 26 28 M 27 M 28 28 30 28 M M-1 27 30 M-1 M 27 M 29 28 30 26 28 29] [28 M M 27 30 24 28 M M 29 M 26 M-1 30 M 29 27 30 M M 24 M-1 M M-1 28 25 28 M 30 M-1 28 M-1] [29 M-1 29 28 27 26 M 29 M-1 M-1 M 29 29 26 25 30 30 M M 29 26 M 27 29 24 24] [M-1 M 28 M 24 M M M-1 26 24 M M 30 M 30 M M 30 M 27 M 24 30 M 25 29 30 M-1 M M-1 29 30] [28 30 28 30 M 29 M-1 M 20 30 30 M M 30 M 25 26 25 M M M M M M M 23 M M 30 M 25 M-1] [27 29 M M-1 M-1 26 29 M M 23 28 M-1 M-1 M-1 24 30 30 M 28 26 M-1 M M M] [30 20 23 M M M M M 28 28 21 28 M 27 M 30 M M 28 26 M M M M M] [29 M M-1 M M M 28 M M 30 M-1 M-1 25 M 30 M-1 M M M M 28 M-1 29 M 28 M M 26 M 26 M 30] [M-1 29 M 28 M 26 M M-1 29 M M M-1 M M-1 M 29 28 M M-1 M 26 M-1 28 27 M M M 28 30 M 29 M] [M M 27 29 M M M 26 M 30 M M M 29 M M 27 M-1 28 M M 29 27 M M-1 M M M M M] [28 M M-1 26 M M M M M 28 M M M M M-1 M M 25 M M-1 27 M M 26 30 M 29 M 29] [28 23 29 28 M M 26 26 M M-1 29 27 27 29 M M-1 27 30 M M-1 30 28 26 M] [M M 29 30 M M 25 M 30 26 M M M M M 26 M 28 M-1 M 26 27 M 29 29 30 27 M M] [20 M M 25 M M 28 M 28 30 M 30 27 M 30 29 M M M M 29 29 29 M 27 M-1 M 28] [M 28 M M M M M-1 M 27 26 29 27 M M M 28 M-1 29 24 29 M M M M-1 M-1] [M 28 M-1 M 28 M 30 M M M 26 28 M 30 23 M 30 M 27 28 25 M M M] [M-1 M M M M-1 29 M M-1 26 M M M 29 M 27 M M M M 30 M 28 30 M M M M 26 M M-1] [M-1 M 28 M M-1 M 29 M M 29 M M 27 M M M M M M 27 M 29 27 28 M M M-1 M M 29 M 30] [M M 30 M M M M 26 M-1 M M M 26 M M M-1 M M M M M M 29 M M M M 28 30 M M M] [M M M 29 M-1 M 28 25 M M M M M 26 M-1 27 M M M M M 29 M M M M] [M M M M-1 28 M M M M M M M-1 M-1 M-1 M-1 M M M M 30 M M M M M M M M M 28] [M M M M M M M M 29 30 M M 30 M M 30 M M M M M M-1 M-1 30 M M 25 M M] [M M M M M M M M M M M 30 M M M M M M M M M M M 29 M M M M] [28 M M M M M M M M M M M M M-1 M M M M M M-1 30 M M M M M 30 M M M M M] [27 29 M M-1 M M-1 M 24 M M M M M M M-1 M M 30 M 26 M 28 M M 29 30 M 29 M 28 M M] [M M M M-1 M M-1 M M M M M M M 28 M M M M M M M M M 30 M M 30 M M M-1 M M] [M 29 27 30 23 M 29 29 M M M 24 M-1 M 29 M 30 M M M 29 M-1 29 M 30 M 30 M M 29 22 M] [M M 30 26 M M 27 M 28 26 25 24 29 25 M M M M-1 M M M 27 27 M M-1 M-1 28 27 M] [29 M 29 M M 30 M M 30 28 M-1 M 21 29 M M-1 26 M-1 30 M M 28 M M 27 M M M M M] [28 M M 27 29 22 30 M M M 27 29 26 27 M 27 30 28 M M M-1 30 M 29 M 29 27 26 28 M 27 M]] [[25 M M-1 28 M 30 M 28 M 28 M-1 M 28 30 M 29 27 28 M 27 M M 27 M M M 26 M M M M M-1] [28 M-1 M M 27 25 30 28 M 27 27 M 23 30 M M-1 30 M-1 30 30 25 M 25 M 29 30 29 M-1 29 M-1 M-1 27] [M 29 M 29 M M 29 27 M M M M M M 20 M M M M 30 28 29 29 M M-1 M M-1 M M 28 M M] [M-1 22 M 21 M M M 26 M M-1 27 M-1 29 M 30 M 23 M M-1 28 27 M-1 21 30 M 30 26 M] [28 M-1 25 30 M 28 M M 25 29 30 25 23 M M-1 M M-1 M 26 26 M M 29 M 28 M-1 M 29 29 27 27 29] [29 M 26 25 M M 28 28 30 M 27 28 M M 28 30 M 26 30 M 26 26 M 27 27 M-1 27 26 M-1 M-1 29 M] [29 M M-1 29 30 28 M-1 M 27 M M-1 M 29 30 M M 28 M-1 M 29 M 29 M-1 M M-1 M 29 30 M M 27 28] [M M M M M M 29 M M M M M M M M M M M M M 27 M M M M 30 M M M-1 M-1 25 M] [30 M 28 M 25 M M 26 30 M-1 M M 23 M 26 29 M 27 26 M M M M-1 M M M 29 25 M 27 M-1 M] [M M M 28 26 M 29 M M 25 M-1 M 29 M M M 30 M 29 M-1 M M 27 M M M M M-1 26 28 28] [25 28 M 29 28 27 M M M M-1 30 28 27 25 M-1 M M 27 29 M 30 M 29 M M 29 M 26 M-1 28 25 M] [M-1 26 M M-1 M M 25 M-1 30 M M 28 27 M M 26 29 M M-1 27 M-1 M M 29 M M M M 27 M M-1 M] [M M M M M M M M M M 28 M M M M M M M M M M M M M M M M M M M M M] [M M M M M M M M M 26 M M M M M M M M M M M M M M M 28 M M M M M M] [M M M 29 M M M 28 29 M 30 M M M 25 30 M M 30 M M M-1 M M-1 M M M M M 22] [28 29 M M M 28 25 30 M-1 28 28 28 M 30 28 26 M M-1 28 30 M M 27 29 26 M-1 M M 29 27 30 27] [M M 23 M M M M M 30 30 M 28 28 M M M M 29 M 27 M M-1 M M M M-1 27 M 27 M M M] [M M 28 29 30 30 M-1 M 26 M M 27 28 M-1 M M 29 29 30 M 27 M-1 M 30 26 27 M-1 M 24 28 26 M] [M M-1 M M 28 M M M M-1 M M M M M 30 30 29 M M M M M M M M 26 30 M M M M M] [M-1 26 28 M M-1 M M M 28 M-1 26 29 29 28 27 M 30 29 29 25 27 M M-1 27 22 25 M] [M-1 30 29 M 30 M M M M M 22 30 23 M-1 M M 30 M 26 20 27 24 28 M M 29 M] [M-1 M M M M-1 M M M M-1 M M M-1 M M 30 M M M M 28 M M M 27 M M M M M 30 M M] [M-1 M M M M M M M M M M M M M M M M M M M M M M M M M M M M-1 M M M] [M M M M M M 30 M M M M M 28 M M M M M-1 M M M M M M M M M M M] [26 M M 27 27 29 M M M-1 26 25 M 26 M M 29 25 25 27 M M M-1 30 27 30 M-1 M-1 M 27 27 27] [29 26 29 29 30 M 30 29 28 25 M-1 27 30 28 M-1 M 24 M 30 28 28 28 27 M-1 28 M 24 M-1 M-1 30 30 M-1] [29 M 29 M M-1 29 M M 30 26 26 28 M M-1 M 25 27 29 M-1 28 30 M 30 M 25 M-1 M 28 30 27 26 M] [M 29 30 25 M 28 M M M M 26 27 M 28 M 27 28 29 25 M M M M-1 M 25 22 28 26 30 M-1 30] [M M M-1 27 26 M-1 28 30 29 28 M M M 29 30 M-1 M 27 M 25 M M M M 27 M-1 29 M M 30 24 27]] [[30 M M M M M M M M M M-1 M M M M M 28 M M M-1 M M M M M M-1 M M 29] [24 M M 29 M 29 M M M M M M M M M M M-1 M 30 M M M M M M M M 30 M M M M] [30 M M M M M 30 30 29 27 30 M M 29 M M 28 M-1 29 24 30 29 28 28 M 30 M-1 M-1 29 30 29 M-1] [26 27 M-1 M-1 M-1 M-1 29 26 28 M 29 29 30 28 M 27 M-1 26 M M 26 28 M M 28 28 26 26 M 29 29 28] [25 M M-1 29 M 21 M 28 29 M-1 M M M 29 M-1 30 M 25 29 M-1 28 M-1 M M M M-1 M 26 M M 27 27] [28 29 M 30 M 28 29 29 M 29 M 30 23 28 M M 25 28 28 M M-1 M-1 29 M 27 M] [M 30 29 M 30 30 M 27 28 M 26 29 M M M M 29 M M-1 28 M 28 M 26 M 28 M 26 27] [30 M M M 29 25 M 27 30 M-1 22 M M M M 30 M M M 30 M 30 27 M-1 M M M 30 M-1 M 30 M] [26 M M M 27 27 29 M M M 29 26 M M-1 M 29 M 27 M M M M M-1 M] [25 27 M 27 25 29 29 M M M 25 M M 25 M 26 30 M 25 27 M M 25 28 M 29 M 27 M 30 M-1 M] [M M-1 M-1 M M M M M M 28 M M M M 29 30 M M M M M 29 M M M 29 M M M M 28] [M-1 M M M M M M 29 25 29 30 M M 29 M M M M-1 M 29 28 M 28 M M 28 27 M M M 28 M] [28 M 27 M M M 29 M 25 26 M-1 28 M 27 29 M-1 30 27 29] [28 M M M-1 M M M-1 M M 25 30 M 30 M M M M M-1 M 29 M M M M 29 M 30 28 M 29 M M-1] [25 M M 30 M M 26 M 27 M M 27 M M M M M 29 M M 30 M M 27 28 M] [M M M M M M M M M M 30 M M 30 M 30 28 30 M M M M 30 30 M M M 26 M M 30 M] [M M M M 29 28 M-1 29 M M M M M M-1 30 M M 26 M 25 30 M M M M M M M M] [M M M 30 M 29 M M M M M M M M M 29 M M M-1 29 M 30 M M M M M M] [M M M 30 M M M-1 M M 30 M M M M M M 25 M M 28 M M M M M M 29 M M M M] [M 29 M M M M M M M M M M M M M-1 M M M M M M M M M M M M M M M M M] [M 30 M M M M M M M 29 30 25 M M M-1 M-1 29 30 M M M M M M 22 M M 29 M-1 M M 29] [30 M M M M-1 30 M 25 24 30 M M M M M-1 28 24 30 M M M M M 26 M] [M M 29 M M M M 29 M 30 M 30 30 M M-1 29 28 M M M M 30 M M M 30 29 M M M M M-1]] [[M M M 26 27 26 M M M 28 28 M M 29 M 28 30 M M M-1 M M-1 28 M-1 M M-1 M-1] [29 M 25 28 30 20 M M 26 M M M 28 30 M 29 M-1 28 29 M 21 M 22 M M 25 M 27 30 M M M] [M 30 M M 29 27 24 30 27 30 M-1 28 M-1 28 M 29 28 25 26 M M-1 M M-1 M-1 M 26 M-1 29 M-1 M M] [30 M 30 M-1 28 30 28 M 29 M M M 29 21 24 M M 29 30 M M-1 25 28 M 28 M 30 30 24 29] [M 27 M M 28 29 M 23 M M M 27 M M M 28 M M M M-1 22 25 M-1 M M M M 26 26] [28 25 M 29 M M-1 30 M 27 M 26 28 M 27 M 28 29 M 27 30 M M M 26 M-1 M M 29 M M-1 29 M-1] [M 30 M 30 29 29 30 26 28 29 M M 26 29 29 26 M M M M M M M-1 M-1 M M 26] [M 29 M-1 30 M 29 M 23 M 26 29 M M 28 M-1 M 30 30 M 27 M M 25 30 30 28 M M] [M-1 M M 29 M-1 28 M M M M M 28 23 30 28 M M M M M M-1 21 M-1 M M] [M M M 25 30 M-1 M M 29 M 27 29 28 M M M M 30 M-1 30 30 30 M M M M 26 29 29] [30 29 30 M M M 24 M M 28 M M M M M 29 M M M M 30 28 29 M M M M M M] [M M 28 27 M M 30 M 30 M M M M M M 28 M M 28 M M-1 M 24 M 28 M 30 M M M M] [M M M M 26 M M M M M M M-1 M M M M 28 M 30 M M M 29 M M M M M 27 M M] [M M M M 26 M M-1 M 27 M M 29 M M M M M M M M M 29 M 30 M M M M M M M 26] [M M M M 30 M M M M M 30 25 M M M M M M M M M M M M M M M M M M] [30 M M M 30 M M 28 M M M M-1 M M M-1 M M M M 29 M M M 29 M M M M M M] [M M M M M M M M M M M M M M-1 M M M M M M M M M M M M M M M M M] [M M M M M M M M M M M M M M 26 M M M M M M M M M M M M M M M M M] [28 M-1 28 26 25 23 M M 30 28 M-1 M M 28 M M M-1 26 M 30 26 28 26 M 25 M 30 M 27 26 M] [30 22 M M M-1 26 27 26 M 28 26 27 M-1 30 30 M 28 M-1 28 M M-1 M 28 M 26 23 M-1 M-1 M-1 29 25] [28 27 M M M M 23 M 23 M M 29 M 23 M 27 27 M 30 M 29 24 M-1 26 M 27 M M 29 M M M] [M M M M M M M M M M M M M M M M 28 M M M-1 M M M M M M M M M M M 29] [30 M M-1 M-1 M 29 M M M M M M 27 M M M M M M M M M M M M M M M-1 M M M M] [23 26 23 M M M 28 24 28 25 29 26 M M M M-1 23 28 26 M 28 M M-1 27 24] [M 29 M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M] [M M 25 27 M M 29 30 M-1 25 M 28 25 M M M M 27 27 26 M 25 28 M 25 M 28 28 M 26 M M] [M-1 M M M 28 M 30 29 30 29 23 28 24 29 M M 23 25 27 29 29 M 25 26 23 M-1] [27 28 M 30 M 27 27 M 23 M 26 M M 25 M 29 M M-1 M M M M 30 23 M 24 M M-1 29 M 28 M] [28 28 28 M-1 27 M M M 26 M-1 M 27 29 23 M M] [25 M 29 M M 28 M 21 M-1 M 30 30 M-1 17 16]] T20"
        let tL = (vL :?> RRBPersistentVector<_>).Transient()
        let tR = (vR :?> RRBPersistentVector<_>).Transient()
        tR.Owner <- tL.Owner  // So they can be joined still as transients. Not a good idea outside of unit tests.
        tL.Append tR |> ignore
        RRBVectorProps.checkProperties tL "Joined vector after all commands run"

    testCase "Join transients where they trigger a rebalance above the twig level, right smaller" <| fun _ ->
        let vL = RRBVectorGen.treeReprStrToVec "[[27 29 28 M-1 27 27 M-1 M 28 29 27 26 M 24 M 30 29 M-1 30 28 27 30 M M M 28 30 27 29 M-1 25] [30 M-1 27 M M-1 26 29 25 29 26 28 M 27 M 28 28 30 28 M M-1 27 30 M-1 M 27 M 29 28 30 26 28 29] [28 M M 27 30 24 28 M M 29 M 26 M-1 30 M 29 27 30 M M 24 M-1 M M-1 28 25 28 M 30 M-1 28 M-1] [29 M-1 29 28 27 26 M 29 M-1 M-1 M 29 29 26 25 30 30 M M 29 26 M 27 29 24 24] [M-1 M 28 M 24 M M M-1 26 24 M M 30 M 30 M M 30 M 27 M 24 30 M 25 29 30 M-1 M M-1 29 30] [28 30 28 30 M 29 M-1 M 20 30 30 M M 30 M 25 26 25 M M M M M M M 23 M M 30 M 25 M-1] [27 29 M M-1 M-1 26 29 M M 23 28 M-1 M-1 M-1 24 30 30 M 28 26 M-1 M M M] [30 20 23 M M M M M 28 28 21 28 M 27 M 30 M M 28 26 M M M M M] [29 M M-1 M M M 28 M M 30 M-1 M-1 25 M 30 M-1 M M M M 28 M-1 29 M 28 M M 26 M 26 M 30] [M-1 29 M 28 M 26 M M-1 29 M M M-1 M M-1 M 29 28 M M-1 M 26 M-1 28 27 M M M 28 30 M 29 M] [M M 27 29 M M M 26 M 30 M M M 29 M M 27 M-1 28 M M 29 27 M M-1 M M M M M] [28 M M-1 26 M M M M M 28 M M M M M-1 M M 25 M M-1 27 M M 26 30 M 29 M 29] [28 23 29 28 M M 26 26 M M-1 29 27 27 29 M M-1 27 30 M M-1 30 28 26 M] [M M 29 30 M M 25 M 30 26 M M M M M 26 M 28 M-1 M 26 27 M 29 29 30 27 M M] [20 M M 25 M M 28 M 28 30 M 30 27 M 30 29 M M M M 29 29 29 M 27 M-1 M 28] [M 28 M M M M M-1 M 27 26 29 27 M M M 28 M-1 29 24 29 M M M M-1 M-1] [M 28 M-1 M 28 M 30 M M M 26 28 M 30 23 M 30 M 27 28 25 M M M] [M-1 M M M M-1 29 M M-1 26 M M M 29 M 27 M M M M 30 M 28 30 M M M M 26 M M-1] [M-1 M 28 M M-1 M 29 M M 29 M M 27 M M M M M M 27 M 29 27 28 M M M-1 M M 29 M 30] [M M 30 M M M M 26 M-1 M M M 26 M M M-1 M M M M M M 29 M M M M 28 30 M M M] [M M M 29 M-1 M 28 25 M M M M M 26 M-1 27 M M M M M 29 M M M M] [M M M M-1 28 M M M M M M M-1 M-1 M-1 M-1 M M M M 30 M M M M M M M M M 28] [M M M M M M M M 29 30 M M 30 M M 30 M M M M M M-1 M-1 30 M M 25 M M] [M M M M M M M M M M M 30 M M M M M M M M M M M 29 M M M M] [28 M M M M M M M M M M M M M-1 M M M M M M-1 30 M M M M M 30 M M M M M] [27 29 M M-1 M M-1 M 24 M M M M M M M-1 M M 30 M 26 M 28 M M 29 30 M 29 M 28 M M] [M M M M-1 M M-1 M M M M M M M 28 M M M M M M M M M 30 M M 30 M M M-1 M M] [M 29 27 30 23 M 29 29 M M M 24 M-1 M 29 M 30 M M M 29 M-1 29 M 30 M 30 M M 29 22 M] [M M 30 26 M M 27 M 28 26 25 24 29 25 M M M M-1 M M M 27 27 M M-1 M-1 28 27 M] [29 M 29 M M 30 M M 30 28 M-1 M 21 29 M M-1 26 M-1 30 M M 28 M M 27 M M M M M] [28 M M 27 29 22 30 M M M 27 29 26 27 M 27 30 28 M M M-1 30 M 29 M 29 27 26 28 M 27 M]] [[25 M M-1 28 M 30 M 28 M 28 M-1 M 28 30 M 29 27 28 M 27 M M 27 M M M 26 M M M M M-1] [28 M-1 M M 27 25 30 28 M 27 27 M 23 30 M M-1 30 M-1 30 30 25 M 25 M 29 30 29 M-1 29 M-1 M-1 27] [M 29 M 29 M M 29 27 M M M M M M 20 M M M M 30 28 29 29 M M-1 M M-1 M M 28 M M] [M-1 22 M 21 M M M 26 M M-1 27 M-1 29 M 30 M 23 M M-1 28 27 M-1 21 30 M 30 26 M] [28 M-1 25 30 M 28 M M 25 29 30 25 23 M M-1 M M-1 M 26 26 M M 29 M 28 M-1 M 29 29 27 27 29] [29 M 26 25 M M 28 28 30 M 27 28 M M 28 30 M 26 30 M 26 26 M 27 27 M-1 27 26 M-1 M-1 29 M] [29 M M-1 29 30 28 M-1 M 27 M M-1 M 29 30 M M 28 M-1 M 29 M 29 M-1 M M-1 M 29 30 M M 27 28] [M M M M M M 29 M M M M M M M M M M M M M 27 M M M M 30 M M M-1 M-1 25 M] [30 M 28 M 25 M M 26 30 M-1 M M 23 M 26 29 M 27 26 M M M M-1 M M M 29 25 M 27 M-1 M] [M M M 28 26 M 29 M M 25 M-1 M 29 M M M 30 M 29 M-1 M M 27 M M M M M-1 26 28 28] [25 28 M 29 28 27 M M M M-1 30 28 27 25 M-1 M M 27 29 M 30 M 29 M M 29 M 26 M-1 28 25 M] [M-1 26 M M-1 M M 25 M-1 30 M M 28 27 M M 26 29 M M-1 27 M-1 M M 29 M M M M 27 M M-1 M] [M M M M M M M M M M 28 M M M M M M M M M M M M M M M M M M M M M] [M M M M M M M M M 26 M M M M M M M M M M M M M M M 28 M M M M M M] [M M M 29 M M M 28 29 M 30 M M M 25 30 M M 30 M M M-1 M M-1 M M M M M 22] [28 29 M M M 28 25 30 M-1 28 28 28 M 30 28 26 M M-1 28 30 M M 27 29 26 M-1 M M 29 27 30 27] [M M 23 M M M M M 30 30 M 28 28 M M M M 29 M 27 M M-1 M M M M-1 27 M 27 M M M] [M M 28 29 30 30 M-1 M 26 M M 27 28 M-1 M M 29 29 30 M 27 M-1 M 30 26 27 M-1 M 24 28 26 M] [M M-1 M M 28 M M M M-1 M M M M M 30 30 29 M M M M M M M M 26 30 M M M M M] [M-1 26 28 M M-1 M M M 28 M-1 26 29 29 28 27 M 30 29 29 25 27 M M-1 27 22 25 M] [M-1 30 29 M 30 M M M M M 22 30 23 M-1 M M 30 M 26 20 27 24 28 M M 29 M] [M-1 M M M M-1 M M M M-1 M M M-1 M M 30 M M M M 28 M M M 27 M M M M M 30 M M] [M-1 M M M M M M M M M M M M M M M M M M M M M M M M M M M M-1 M M M] [M M M M M M 30 M M M M M 28 M M M M M-1 M M M M M M M M M M M] [26 M M 27 27 29 M M M-1 26 25 M 26 M M 29 25 25 27 M M M-1 30 27 30 M-1 M-1 M 27 27 27] [29 26 29 29 30 M 30 29 28 25 M-1 27 30 28 M-1 M 24 M 30 28 28 28 27 M-1 28 M 24 M-1 M-1 30 30 M-1] [29 M 29 M M-1 29 M M 30 26 26 28 M M-1 M 25 27 29 M-1 28 30 M 30 M 25 M-1 M 28 30 27 26 M] [M 29 30 25 M 28 M M M M 26 27 M 28 M 27 28 29 25 M M M M-1 M 25 22 28 26 30 M-1 30] [M M M-1 27 26 M-1 28 30 29 28 M M M 29 30 M-1 M 27 M 25 M M M M 27 M-1 29 M M 30 24 27]] [[30 M M M M M M M M M M-1 M M M M M 28 M M M-1 M M M M M M-1 M M 29] [24 M M 29 M 29 M M M M M M M M M M M-1 M 30 M M M M M M M M 30 M M M M] [30 M M M M M 30 30 29 27 30 M M 29 M M 28 M-1 29 24 30 29 28 28 M 30 M-1 M-1 29 30 29 M-1] [26 27 M-1 M-1 M-1 M-1 29 26 28 M 29 29 30 28 M 27 M-1 26 M M 26 28 M M 28 28 26 26 M 29 29 28] [25 M M-1 29 M 21 M 28 29 M-1 M M M 29 M-1 30 M 25 29 M-1 28 M-1 M M M M-1 M 26 M M 27 27] [28 29 M 30 M 28 29 29 M 29 M 30 23 28 M M 25 28 28 M M-1 M-1 29 M 27 M] [M 30 29 M 30 30 M 27 28 M 26 29 M M M M 29 M M-1 28 M 28 M 26 M 28 M 26 27] [30 M M M 29 25 M 27 30 M-1 22 M M M M 30 M M M 30 M 30 27 M-1 M M M 30 M-1 M 30 M] [26 M M M 27 27 29 M M M 29 26 M M-1 M 29 M 27 M M M M M-1 M] [25 27 M 27 25 29 29 M M M 25 M M 25 M 26 30 M 25 27 M M 25 28 M 29 M 27 M 30 M-1 M] [M M-1 M-1 M M M M M M 28 M M M M 29 30 M M M M M 29 M M M 29 M M M M 28] [M-1 M M M M M M 29 25 29 30 M M 29 M M M M-1 M 29 28 M 28 M M 28 27 M M M 28 M] [28 M 27 M M M 29 M 25 26 M-1 28 M 27 29 M-1 30 27 29] [28 M M M-1 M M M-1 M M 25 30 M 30 M M M M M-1 M 29 M M M M 29 M 30 28 M 29 M M-1] [25 M M 30 M M 26 M 27 M M 27 M M M M M 29 M M 30 M M 27 28 M] [M M M M M M M M M M 30 M M 30 M 30 28 30 M M M M 30 30 M M M 26 M M 30 M] [M M M M 29 28 M-1 29 M M M M M M-1 30 M M 26 M 25 30 M M M M M M M M] [M M M 30 M 29 M M M M M M M M M 29 M M M-1 29 M 30 M M M M M M] [M M M 30 M M M-1 M M 30 M M M M M M 25 M M 28 M M M M M M 29 M M M M] [M 29 M M M M M M M M M M M M M-1 M M M M M M M M M M M M M M M M M] [M 30 M M M M M M M 29 30 25 M M M-1 M-1 29 30 M M M M M M 22 M M 29 M-1 M M 29] [30 M M M M-1 30 M 25 24 30 M M M M M-1 28 24 30 M M M M M 26 M] [M M 29 M M M M 29 M 30 M 30 30 M M-1 29 28 M M M M 30 M M M 30 29 M M M M M-1]] [[M M M 26 27 26 M M M 28 28 M M 29 M 28 30 M M M-1 M M-1 28 M-1 M M-1 M-1] [29 M 25 28 30 20 M M 26 M M M 28 30 M 29 M-1 28 29 M 21 M 22 M M 25 M 27 30 M M M] [M 30 M M 29 27 24 30 27 30 M-1 28 M-1 28 M 29 28 25 26 M M-1 M M-1 M-1 M 26 M-1 29 M-1 M M] [30 M 30 M-1 28 30 28 M 29 M M M 29 21 24 M M 29 30 M M-1 25 28 M 28 M 30 30 24 29] [M 27 M M 28 29 M 23 M M M 27 M M M 28 M M M M-1 22 25 M-1 M M M M 26 26] [28 25 M 29 M M-1 30 M 27 M 26 28 M 27 M 28 29 M 27 30 M M M 26 M-1 M M 29 M M-1 29 M-1] [M 30 M 30 29 29 30 26 28 29 M M 26 29 29 26 M M M M M M M-1 M-1 M M 26] [M 29 M-1 30 M 29 M 23 M 26 29 M M 28 M-1 M 30 30 M 27 M M 25 30 30 28 M M] [M-1 M M 29 M-1 28 M M M M M 28 23 30 28 M M M M M M-1 21 M-1 M M] [M M M 25 30 M-1 M M 29 M 27 29 28 M M M M 30 M-1 30 30 30 M M M M 26 29 29] [30 29 30 M M M 24 M M 28 M M M M M 29 M M M M 30 28 29 M M M M M M] [M M 28 27 M M 30 M 30 M M M M M M 28 M M 28 M M-1 M 24 M 28 M 30 M M M M] [M M M M 26 M M M M M M M-1 M M M M 28 M 30 M M M 29 M M M M M 27 M M] [M M M M 26 M M-1 M 27 M M 29 M M M M M M M M M 29 M 30 M M M M M M M 26] [M M M M 30 M M M M M 30 25 M M M M M M M M M M M M M M M M M M] [30 M M M 30 M M 28 M M M M-1 M M M-1 M M M M 29 M M M 29 M M M M M M] [M M M M M M M M M M M M M M-1 M M M M M M M M M M M M M M M M M] [M M M M M M M M M M M M M M 26 M M M M M M M M M M M M M M M M M] [28 M-1 28 26 25 23 M M 30 28 M-1 M M 28 M M M-1 26 M 30 26 28 26 M 25 M 30 M 27 26 M] [30 22 M M M-1 26 27 26 M 28 26 27 M-1 30 30 M 28 M-1 28 M M-1 M 28 M 26 23 M-1 M-1 M-1 29 25] [28 27 M M M M 23 M 23 M M 29 M 23 M 27 27 M 30 M 29 24 M-1 26 M 27 M M 29 M M M] [M M M M M M M M M M M M M M M M 28 M M M-1 M M M M M M M M M M M 29] [30 M M-1 M-1 M 29 M M M M M M 27 M M M M M M M M M M M M M M M-1 M M M M] [23 26 23 M M M 28 24 28 25 29 26 M M M M-1 23 28 26 M 28 M M-1 27 24] [M 29 M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M] [M M 25 27 M M 29 30 M-1 25 M 28 25 M M M M 27 27 26 M 25 28 M 25 M 28 28 M 26 M M] [M-1 M M M 28 M 30 29 30 29 23 28 24 29 M M 23 25 27 29 29 M 25 26 23 M-1] [27 28 M 30 M 27 27 M 23 M 26 M M 25 M 29 M M-1 M M M M 30 23 M 24 M M-1 29 M 28 M] [28 28 28 M-1 27 M M M 26 M-1 M 27 29 23 M M] [25 M 29 M M 28 M 21 M-1 M 30 30 M-1 17 16]] T20"
        let vR = RRBVectorGen.treeReprStrToVec "14 17 T25"
        let tL = (vL :?> RRBPersistentVector<_>).Transient()
        let tR = (vR :?> RRBPersistentVector<_>).Transient()
        tR.Owner <- tL.Owner  // So they can be joined still as transients. Not a good idea outside of unit tests.
        tL.Append tR |> ignore
        RRBVectorProps.checkProperties tL "Joined vector after all commands run"

    testCase "Join transients where both are twigs and they trigger a rebalance, left smaller" <| fun _ ->
        let vL = RRBVectorGen.treeReprStrToVec "17 25 24 17 16 T31"
        let vR = RRBVectorGen.treeReprStrToVec "5 M 30 M M M 17 16 M M M M M M M M M-1 M M M 25 M-1 M 17 16 M M M T2"
        let tL = (vL :?> RRBPersistentVector<_>).Transient()
        let tR = (vR :?> RRBPersistentVector<_>).Transient()
        tR.Owner <- tL.Owner  // So they can be joined still as transients. Not a good idea outside of unit tests.
        tL.Append tR |> ignore
        RRBVectorProps.checkProperties tL "Joined vector after all commands run"

    testCase "Join transients where both are twigs and they trigger a rebalance, right smaller" <| fun _ ->
        let vL = RRBVectorGen.treeReprStrToVec "5 M 30 M M M 17 16 M M M M M M M M M-1 M M M 25 M-1 M 17 16 M M M T2"
        let vR = RRBVectorGen.treeReprStrToVec "17 25 24 17 16 T31"
        let tL = (vL :?> RRBPersistentVector<_>).Transient()
        let tR = (vR :?> RRBPersistentVector<_>).Transient()
        tR.Owner <- tL.Owner  // So they can be joined still as transients. Not a good idea outside of unit tests.
        tL.Append tR |> ignore
        RRBVectorProps.checkProperties tL "Joined vector after all commands run"

    testCase "Join transients where left root fits neatly into leftmost trig of right tree" <| fun _ ->
        let vL = RRBVectorGen.treeReprStrToVec "M-1 17 16 T22"
        let vR = RRBVectorGen.treeReprStrToVec "[13 M*26] [M*M]*10 [M*16] [M*14 17 16 M M M-1] T13"
        let tL = (vL :?> RRBPersistentVector<_>).Transient()
        let tR = (vR :?> RRBPersistentVector<_>).Transient()
        tR.Owner <- tL.Owner  // So they can be joined still as transients. Not a good idea outside of unit tests.
        tL.Append tR |> ignore
        RRBVectorProps.checkProperties tL "Joined vector after all commands run"

    testCase "Join transients where left root fits neatly into leftmost trig of right tree, but reversed" <| fun _ ->
        let vL = RRBVectorGen.treeReprStrToVec "[13 M*26] [M*M]*10 [M*16] [M*14 17 16 M M M-1] T13"
        let vR = RRBVectorGen.treeReprStrToVec "M-1 17 16 T22"
        let tL = (vL :?> RRBPersistentVector<_>).Transient()
        let tR = (vR :?> RRBPersistentVector<_>).Transient()
        tR.Owner <- tL.Owner  // So they can be joined still as transients. Not a good idea outside of unit tests.
        tL.Append tR |> ignore
        RRBVectorProps.checkProperties tL "Joined vector after all commands run"

    testCase "Join transients where left root doesn't quite fit into leftmost trig of right tree, causing a rebalance" <| fun _ ->
        let vL = RRBVectorGen.treeReprStrToVec "10 25 M-1 T3"
        let vR = RRBVectorGen.treeReprStrToVec "[21 M 25 25 16 M*3 29 M*7 29 29 26 M*3 29 M M 28 M*6] [17 16 M-1] T13"
        let tL = (vL :?> RRBPersistentVector<_>).Transient()
        let tR = (vR :?> RRBPersistentVector<_>).Transient()
        tR.Owner <- tL.Owner  // So they can be joined still as transients. Not a good idea outside of unit tests.
        tL.Append tR |> ignore
        RRBVectorProps.checkProperties tL "Joined vector after all commands run"

    testCase "Join transients where left root doesn't quite fit into leftmost trig of right tree, causing a rebalance, reversed" <| fun _ ->
        let vL = RRBVectorGen.treeReprStrToVec "[21 M 25 25 16 M*3 29 M*7 29 29 26 M*3 29 M M 28 M*6] [17 16 M-1] T13"
        let vR = RRBVectorGen.treeReprStrToVec "10 25 M-1 T3"
        let tL = (vL :?> RRBPersistentVector<_>).Transient()
        let tR = (vR :?> RRBPersistentVector<_>).Transient()
        tR.Owner <- tL.Owner  // So they can be joined still as transients. Not a good idea outside of unit tests.
        tL.Append tR |> ignore
        RRBVectorProps.checkProperties tL "Joined vector after all commands run"

    testCase "Join transients where left root doesn't quite fit into leftmost trig of right tree, causing a rebalance, second version" <| fun _ ->
        let vL = RRBVectorGen.treeReprStrToVec "M 25 24 25 24 T13"
        let vR = RRBVectorGen.treeReprStrToVec "[11 25 25 M-1 M M-1 M 25 M-1 M 26 M-1 M M 29 M 27 30 M 30 25 29 29 28 M M M-1 M] [27 27 30 21 20 25 24 26 29 29 24 22 24 28 M] T17"
        let tL = (vL :?> RRBPersistentVector<_>).Transient()
        let tR = (vR :?> RRBPersistentVector<_>).Transient()
        tR.Owner <- tL.Owner  // So they can be joined still as transients. Not a good idea outside of unit tests.
        tL.Append tR |> ignore
        RRBVectorProps.checkProperties tL "Joined vector after all commands run"

    testCase "Join transients where left root doesn't quite fit into leftmost trig of right tree, causing a rebalance, second version, reversed" <| fun _ ->
        let vL = RRBVectorGen.treeReprStrToVec "[11 25 25 M-1 M M-1 M 25 M-1 M 26 M-1 M M 29 M 27 30 M 30 25 29 29 28 M M M-1 M] [27 27 30 21 20 25 24 26 29 29 24 22 24 28 M] T17"
        let vR = RRBVectorGen.treeReprStrToVec "M 25 24 25 24 T13"
        let tL = (vL :?> RRBPersistentVector<_>).Transient()
        let tR = (vR :?> RRBPersistentVector<_>).Transient()
        tR.Owner <- tL.Owner  // So they can be joined still as transients. Not a good idea outside of unit tests.
        tL.Append tR |> ignore
        RRBVectorProps.checkProperties tL "Joined vector after all commands run"

    testCase "split commands that failed, medium, simpler" <| fun _ ->
        let vL = RRBVectorGen.treeReprStrToVec "17 16 M-1 17 17 M M 17 16 M T15"
        let vR = RRBVectorGen.treeReprStrToVec "5 24 30 28 M 28 26 M M-1 M 26 29 M-1 24 M 27 28 26 30 M M 17 25 24 M M M T3"
        let tL = (vL :?> RRBPersistentVector<_>).Transient()
        let tR = (vR :?> RRBPersistentVector<_>).Transient()
        tR.Owner <- tL.Owner  // So they can be joined still as transients. Not a good idea outside of unit tests.
        tL.Append tR |> ignore
        RRBVectorProps.checkProperties tL "Joined vector after all commands run"
// 	RRBPersistentVector<length=648,shift=5,tailOffset=636,root=RelaxedNode(length=22, sizetable=[|30; 60; 86; 115; 142; 174; 202; 228; 260; 291; 323; 349; 378; 409; 433; 465;
//   492; 520; 546; 576; 607; 636|], children=[|L30; L30; L26; L29; L27; L32; L28; L26; L32; L31; L32; L26; L29; L31; L24; L32;
//   L27; L28; L26; L30; L31; L29|]),tail=[|88; 59; 63; -50; 84; 55; 59; 30; 80; 51; 55; 26|]>
// [split (55,
//   [push 98; remove -11; insert (-18,83); insert (19,52); insert (85,58); push 85; remove 36; insert (89,33); insert (-75,39); insert (-9,73)],
//   [remove 9; insert (70,30); insert (-65,36); pop 29; push 98; remove 27; insert (-60,17); insert (-23,89); insert (43,58); push 85]
// )]

    testCase "split commands that failed, large" <| fun _ ->
        ()
// 	RRBPersistentVector<length=913,shift=5,tailOffset=900,root=RelaxedNode(length=29, sizetable=[|30; 59; 90; 122; 154; 186; 218; 250; 279; 311; 343; 375; 407; 439; 471; 503;
//   532; 561; 587; 619; 651; 683; 712; 744; 776; 804; 836; 868; 900|], children=[|L30; L29; L31; L32; L32; L32; L32; L32; L29; L32; L32; L32; L32; L32; L32; L32;
//   L29; L29; L26; L32; L32; L32; L29; L32; L32; L28; L32; L32; L32|]),tail=[|108; 32; 96; -119; -88; 87; 99; -35; -112; -12; 62; 51; 41|]> [split (-63,[pop 16; remove -64; remove 11; insert (-61,32); insert (5,58); push 85;
//  insert (65,49); insert (-67,10); insert (39,36); push 29; remove 53;
//  insert (7,11)],[push 46; remove 46; insert (54,28); insert (-81,62); insert (-44,1); push 66;
//  insert (38,26); insert (2,6); insert (73,44); push 40; remove 47; remove 63])]


    testCase "Figure out the name" <| fun _ ->
        let vec = RRBVectorGen.looserTreeReprStrToVec TestData.ridiculouslyLargeVector
        RRBVectorProps.checkProperties vec "Original persistent vector"
        let mutable t = (vec :?> RRBPersistentVector<_>).Transient()
        RRBVectorProps.checkProperties t "Original transient vector"
        for i = 1 to 8 do
            t <- t.Push i :?> RRBTransientVector<_>
            // logger.warn (eventX "Transient after push {i}: {vec}" >> setField "i" i >> setField "vec" (RRBVectorGen.vecToTreeReprStr t))
            RRBVectorProps.checkProperties t <| sprintf "Transient after push %d" i
        // After push 7, we're fine. THe eight push builds a new path to the root, and apparently fails to clean up the old path
        // so that we get "If a tree's root is an expanded Node variant, its right spine should contain expanded nodes but nothing else should"

    testCase "Figure out the name 2" <| fun _ ->
        let vec = RRBVectorGen.looserTreeReprStrToVec TestData.ridiculouslyLargeVector
        RRBVectorProps.checkProperties vec "Original persistent vector"
        let mutable t = (vec :?> RRBPersistentVector<_>).Transient()
        RRBVectorProps.checkProperties t "Original transient vector"
        for i = 1 to 66 do
            t <- t.Push i :?> RRBTransientVector<_>
            // logger.warn (eventX "Transient after push {i}: {vec}" >> setField "i" i >> setField "vec" (RRBVectorGen.vecToTreeReprStr t))
            RRBVectorProps.checkProperties t <| sprintf "Transient after push %d" i
        for i = 1 to 58 do
            t <- t.Pop() :?> RRBTransientVector<_>
            // logger.warn (eventX "Transient after pop {i}: {vec}" >> setField "i" i >> setField "vec" (RRBVectorGen.vecToTreeReprStr t))
            RRBVectorProps.checkProperties t <| sprintf "Transient after pop %d" i
        // logger.warn (eventX "Transient after pop 58: {vec}" >> setField "vec" (RRBVectorGen.vecToTreeReprStr t))
        t <- t.Pop() :?> RRBTransientVector<_>
        // logger.warn (eventX "Transient after pop 59: {vec}" >> setField "vec" (RRBVectorGen.vecToTreeReprStr t))
        RRBVectorProps.checkProperties t <| sprintf "Transient after pop 59"

    testCase "A push that grows the height of a transient vector will leave it with a properly relaxed and expanded root" <| fun _ ->
        let vec = RRBVectorGen.treeReprStrToVec "M 17 16 M 29 30 24 30 30 30 M 29 M-1 25 27 25 28 30 28 M M M M M 17 16 M 17 16 M M M T32"
        let vec' = vec.Push 512
        RRBVectorProps.checkProperties vec' "Persistent after push"
        let mutable t = (vec :?> RRBPersistentVector<_>).Transient()
        t <- t.Push 512 :?> RRBTransientVector<_>
        RRBVectorProps.checkProperties t "Transient after push"

    testCase "A remove that triggers a rebalance in a transient tree will still leave it properly expanded" <| fun _ ->
        let repr = """
            [M M M M M M M M M M-1 M M M M M M M M M M M-1 M M M 28 M M M M M M M]
            [30 26 M M 30 25 M 27 26 M 28 30 M-1 M M-1 28 M 24 27 M-1 27 30 M-1 M 27 29 M 28 28 M 30 25]
            [30 27 29 M 29 27 M 29 27 30 27 M M M 27 26 M-1 28 M 29 27 28 29 27 29 27 29 29 27 30 M 30]
            [30 26 26 26 25 M 30 M M 30 27 M-1 28 26 30 M-1 28 M 29 29 28 M-1 29 27 28 M M]
            [28 M-1 M-1 30 29 M 28 26 27 28 M M-1 M-1 28 29 23 28 26 27 27 27 M M 28 27 M M]
            [26 M-1 M-1 M M M M 30 M M 25 29 29 M-1 30 30 30 M 30 M 25 24 M 29 30 27 M M 29 29 30 30]
            [M 27 28 M 30 29 29 27 24 27 M M-1 24 M M-1 29 27 M 27 M-1 M-1 M-1 M 29 29 M M 27 26 29 26 25]
            [M M M M M M M M M M M M M 29 M M M M M M M M M M M M]
            [M-1 M-1 19 M M 29 30 25 M M-1 M 29 28 29 26 M-1 29 24 26 M-1 M M 28 M 25 28 M-1 M-1 M 30 M 30]
            [23 28 M-1 23 M M-1 20 M 29 29 M M 25 M-1 29 25 M-1 28 26 M 26 29 M 27 28 M]
            [29 30 27 M M 26 29 M M-1 26 30 M 27 M-1 29 25 M M 25 30 30 M-1 29 M 29 M-1 M 25 27 M 28 M]
            [M M-1 M M M M 30 M M M-1 M M M 28 M M M M M M M-1 M M 29 M M M-1 M M-1 M M M]
            [M M M M M-1 24 28 M 26 M M M 30 28 M-1 28 25 M M M M 26 26 23 30]
            [M-1 M M M M M M M M M M M M 28 M M M M M M M M M 30 M M M 30 M M 29 M]
            [M 30 M M M M M 30 M 29 M 28 M-1 M 29 30 M M M 30 M-1 30 M-1 M 30 M-1 M M M 29 M-1 26]
            [M M M 27 M M-1 M M M M M 29 M M M-1 28 M-1 M-1 30 27 M M M-1 29 M 24 M M 28 M]
            [M-1 M-1 29 M M M M M M 27 M 27 M 30 M 28 29 29 27 M]
            [M 29 M M M M 29 M M 28 M 29 M M M M 24 M-1 M 29 M 28 M M-1 28 M M-1 M 23 M-1 M 28]
            [M M M M M M M M M 30 27 M-1 M M M M M-1 M 30 M M 30 M 28 M M M M-1 M-1 M M-1 30]
            [28 M-1 M M 23 M 26 28 27 M M M M-1 23 M M 26 28 M-1 25 M M M 29 27 27 M 27 30 30 30 27]
            [M M-1 M 30 M M 26 M M 26 M M-1 26 29 29 M M M M 24 26 M M 28 M M 27 M M M-1 M M]
            [30 M M 26 29 28 23 M-1 29 28 27 M M M M 30 23 M-1 30 M M M-1 29 M 29 M-1 28 29 24 M M]
            [M M M M M M M M M M M-1 M M M M M M M M M]
            [M 30 M M M M M M 30 M M M M M M M M M M M M M M 30 M M M M M M M M]
            [M M M M-1 25 M-1 24 25 M 29 M-1 30 M-1 30 27 M 28 M-1 30 M 28 26 27 27 M M-1 28 M-1 27 25 M 28]
            [M 28 26 M-1 25 28 28 30 30 M M-1 M-1 27 23 28 30 27 27 27 M 27 28 22 26]
            [28 M M 29 28 M 27 29 M M 29 27 24 26 M M M-1 M-1 M M 28 M-1 28 27 M-1 28 27 M 29 M 25 M]
            [28 30 M 26 26 M 29 29 29 30 M-1 M M-1 25 27 M 29 26 M M M-1 29 M 29 28 M 28 M 28 M-1 27 27]
            T22"""
        let vec = RRBVectorGen.looserTreeReprStrToVec repr
        let push = RRBVectorTransientCommands.VecCommands.push
        let remove = RRBVectorTransientCommands.VecCommands.remove
        let insert = RRBVectorTransientCommands.VecCommands.insert
        let cmds = [push 45; insert (58,50); remove -23]
        // The remove will trigger a rebalance, which will end up NOT having an expanded node at the end of the rebalance!
        let mutable current = (vec :?> RRBPersistentVector<_>).Transient()
        let logVec cmd vec =
            // logger.debug (
            //     eventX "After {cmd}, vec was {vec} with actual structure {structure}"
            //     >> setField "cmd" cmd
            //     >> setField "vec" (RRBVectorGen.vecToTreeReprStr vec)
            //     >> setField "structure" (sprintf "%A" vec))
            ()
        for cmd in cmds do
            current <- current |> cmd.RunActual
            logVec (cmd.ToString()) current
            RRBVectorProps.checkProperties current <| sprintf "Vector after %s" (cmd.ToString())

    // Individual test cases that were once failures of the above properties

    testCase "Removing one item from full-sized root of transient preserves tail" <| fun _ ->
        let arr = [| 1 .. Literals.blockSize + 6 |]
        let vec = RRBVector.ofArray arr
        let newVec = vec.Remove 3
        let newArr = arr |> Array.copyAndRemoveAt 3
        RRBVectorProps.checkProperties newVec "New vector"
        Expect.equal (newVec |> RRBVector.toArray) newArr "New vector did not match array"
        let tvec = (vec :?> RRBPersistentVector<_>).Transient()
        let newTVec = tvec.Remove 3
        RRBVectorProps.checkProperties newTVec "New transient vector"
        Expect.equal (newTVec |> RRBVector.toArray) newArr "New transient vector did not match array"

    testCase "Removing one item from root of transient of length M+1 moves entire new M-sized root into tail" <| fun _ ->
        let arr = [| 1 .. Literals.blockSize + 1 |]
        let vec = RRBVector.ofArray arr
        let newVec = vec.Remove 3
        let newArr = arr |> Array.copyAndRemoveAt 3
        RRBVectorProps.checkProperties newVec "New vector"
        Expect.equal (newVec |> RRBVector.toArray) newArr "New vector did not match array"
        let tvec = (vec :?> RRBPersistentVector<_>).Transient()
        let newTVec = tvec.Remove 3
        RRBVectorProps.checkProperties newTVec "New transient vector"
        Expect.equal (newTVec |> RRBVector.toArray) newArr "New transient vector did not match array"

    testCase "Inserting one item at start of full-sized tail of transient with empty root preserves tail size" <| fun _ ->
        let arr = [| 1 .. Literals.blockSize |]
        let vec = RRBVector.ofArray arr
        let newVec = vec.Insert 0 3
        let newArr = arr |> Array.copyAndInsertAt 0 3
        RRBVectorProps.checkProperties newVec "New vector"
        Expect.equal (newVec |> RRBVector.toArray) newArr "New vector did not match array"
        let tvec = (vec :?> RRBPersistentVector<_>).Transient()
        let newTVec = tvec.Insert 0 3
        RRBVectorProps.checkProperties newTVec "New transient vector"
        Expect.equal (newTVec |> RRBVector.toArray) newArr "New transient vector did not match array"

    testCase "Inserting one item at start of not-quite-full-size tail of transient with empty root leaves full tail and empty root" <| fun _ ->
        let arr = [| 1 .. Literals.blockSize - 1 |]
        let vec = RRBVector.ofArray arr
        Expect.equal (vec :?> RRBPersistentVector<_>).Tail.[vec.Length - 1] (Literals.blockSize - 1) <| sprintf "Persistent vector's tail should initially end in %d" (Literals.blockSize - 1)
        let newVec = vec.Insert 0 3
        let newArr = arr |> Array.copyAndInsertAt 0 3
        RRBVectorProps.checkProperties newVec "New vector"
        Expect.equal (newVec |> RRBVector.toArray) newArr "New vector did not match array"
        Expect.equal (newVec :?> RRBPersistentVector<_>).Root.NodeSize 0 "New vector's root should still be empty"
        Expect.equal (newVec :?> RRBPersistentVector<_>).Tail.Length Literals.blockSize "New vector's tail should still be full"
        Expect.notEqual (newVec :?> RRBPersistentVector<_>).Tail.[Literals.blockSize - 1] 0 "New vector's tail should not end in 0"
        let tvec = (vec :?> RRBPersistentVector<_>).Transient()
        Expect.equal (tvec :?> RRBTransientVector<_>).Tail.[Literals.blockSize - 1] 0 "Transient vector's tail should initially end in 0"
        let newTVec = tvec.Insert 0 3
        RRBVectorProps.checkProperties newTVec "New transient vector"
        Expect.equal (newTVec |> RRBVector.toArray) newArr "New transient vector did not match array"
        Expect.equal (newTVec :?> RRBTransientVector<_>).Root.NodeSize 0 "New transient vector's root should still be empty"
        Expect.equal (newTVec :?> RRBTransientVector<_>).Tail.Length Literals.blockSize "New transient vector's tail should still be full"
        Expect.notEqual (newTVec :?> RRBTransientVector<_>).Tail.[Literals.blockSize - 1] 0 "New transient vector's tail should not end in 0"

    testCase "Shifting nodes into tail twice, leaving empty root, preserves tail correctly" <| fun _ ->
        let vec = RRBVectorGen.treeReprStrToVec "M/2-3 M/2-1 T1"
        let arr = vec |> RRBVector.toArray
        RRBVectorProps.checkProperties vec "Original vector"
        let v = vec :?> RRBPersistentVector<_>
        Expect.equal v.Root.NodeSize 2 "Root should have 2 nodes"
        let root = v.Root :?> RRBFullNode<_>
        Expect.equal root.FirstChild.NodeSize (Literals.blockSize / 2 - 3) "First child should have M/2-3 items"
        Expect.equal root.LastChild.NodeSize (Literals.blockSize / 2 - 1) "Last child should have M/2-1 items"
        let tvec = (vec :?> RRBPersistentVector<_>).Transient()
        let v = tvec :?> RRBTransientVector<_>
        Expect.equal v.Root.NodeSize 2 "Root of transient should have 2 nodes"
        let root = v.Root :?> RRBFullNode<_>
        Expect.equal root.FirstChild.NodeSize (Literals.blockSize / 2 - 3) "First child of transient should have M/2-3 items"
        Expect.equal root.LastChild.NodeSize (Literals.blockSize / 2 - 1) "Last child of transient should have M/2-1 items"
        let newVec = vec.Pop()
        let newArr = arr |> Array.copyAndPop
        RRBVectorProps.checkProperties newVec "New vector"
        Expect.equal (newVec :?> RRBPersistentVector<_>).Root.NodeSize 0 "New vector's root should be empty now"
        Expect.equal (newVec :?> RRBPersistentVector<_>).Tail.Length (Literals.blockSize - 4) "New vector's tail should contain whole remaining tree"
        Expect.notEqual (newVec :?> RRBPersistentVector<_>).Tail.[Literals.blockSize - 5] 0 "New vector's tail should NOT end in 0"
        Expect.equal (newVec |> RRBVector.toArray) newArr "New vector did not match array"
        let newTVec = tvec.Pop()
        RRBVectorProps.checkProperties newTVec "New transient vector"
        Expect.equal (newTVec :?> RRBTransientVector<_>).Root.NodeSize 0 "New transient vector's root should be empty now"
        Expect.equal (newTVec :?> RRBTransientVector<_>).Tail.Length Literals.blockSize "New transient vector's tail should still be full"
        Expect.equal (newTVec :?> RRBTransientVector<_>).Tail.[Literals.blockSize - 1] 0 "New transient vector's tail should end in 0"
        Expect.equal (newTVec |> RRBVector.toArray) newArr "New transient vector did not match array"

    testCase "Removing one item from root of transient of length M*2+4 turns full root into relaxed root" <| fun _ ->
        let arr = [| 1 .. Literals.blockSize * 2 + 4 |]
        let vec = RRBVector.ofArray arr
        let newVec = vec.Remove 0
        let newArr = arr |> Array.copyAndRemoveAt 0
        RRBVectorProps.checkProperties newVec "New vector"
        Expect.equal (newVec |> RRBVector.toArray) newArr "New vector did not match array"
        let tvec = (vec :?> RRBPersistentVector<_>).Transient()
        Expect.equal (tvec |> RRBVector.toArray) arr "Orig transient vector did not match orig array"
        let newTVec = tvec.Remove 0
        RRBVectorProps.checkProperties newTVec "New transient vector"
        Expect.equal (newTVec |> RRBVector.toArray) newArr "New transient vector did not match array"
  ]

let simpleVectorTests =
    [
        0,0
        1,0
        1,1
        5,1
        10,6
        6,4
        9,6
        17,5
        10,4
        12,5
        13,7
        26,13
        24,4
        33,17
        39,20
        19,12
        68,36
        10,7
        21,18
        27,21
        48,33
        10,5
        15,7
    ] |> List.mapi (fun i data -> testCase (sprintf "Test %d with data %A" (i+1) data) (fun _ -> splitFullVecTest data))
      |> testList "Simple vector tests"

let manualVectorTests =
  testList "Manual vector tests" [
    testCase "empty vector has 0 items" (fun _ ->
        RRBVectorProps.checkProperties (RRBVector.empty<int>) "Empty vector"
        Expect.equal (RRBVector.length (RRBVector.empty<int>)) 0 "Empty vector should have length 0"
    )
    testCase "Appending 1 item to empty vector should produce vector of length 1" (fun _ ->
        let vec = RRBVector.empty<int> |> RRBVector.push 42
        RRBVectorProps.checkProperties vec "1-item vector"
        Expect.equal (RRBVector.length vec) 1 "1-item vector should have length 1"
    )
    testCase "Appending 2 items to empty vector should produce vector of length 2" (fun _ ->
        let vec = RRBVector.empty<int> |> RRBVector.push 4 |> RRBVector.push 2
        RRBVectorProps.checkProperties vec "2-item vector"
        Expect.equal (RRBVector.length vec) 2 "2-item vector should have length 2"
    )
    testCase "Can create vector from list" (fun _ ->
        let vec = RRBVector.ofList [1;3;5;7;9;11;13;15;17]
        RRBVectorProps.checkProperties vec "9-item vector"
        Expect.equal (RRBVector.length vec) 9 "9-item vector should have length 9"
    )
    testCase "Can create vector from array" (fun _ ->
        let vec = RRBVector.ofArray [|1;3;5;7;9;11;13;15;17|]
        RRBVectorProps.checkProperties vec "9-item vector"
        Expect.equal (RRBVector.length vec) 9 "9-item vector should have length 9"
    )
    testCase "Can create vector from seq" (fun _ ->
        let vec = RRBVector.ofSeq (seq { 1..2..17 })
        RRBVectorProps.checkProperties vec "9-item vector"
        Expect.equal (RRBVector.length vec) 9 "9-item vector should have length 9"
    )
    testCase "Inserting into tail of short-leaf, full-tail sapling will shift nodes from tail to make full leaf" <| fun _ ->
        let vec = RRBVector.ofArray [|1..(Literals.blockSize * 2)|]
        RRBVectorProps.checkProperties vec "Full sapling"
        let vec2 = vec.Remove 0
        RRBVectorProps.checkProperties vec2 "Sapling with short leaf"
        let vec3 = vec2.Insert (vec2.Length - 2) 65 :?> RRBPersistentVector<int>
        RRBVectorProps.checkProperties vec3 "Sapling with formerly short leaf, after a tail insert, which should be a full sapling"
        Expect.equal (vec3.Root :?> RRBFullNode<_>).LastChild.NodeSize Literals.blockSize "Leaf length should be BlockSize"
        Expect.equal vec3.Tail.Length Literals.blockSize "Tail length should be BlockSize"
  ]

let constructedVectorSplitTests =
    [
        "M/2 TM", Literals.blockSize / 2 - 1
        "M/2 TM", Literals.blockSize / 2
        "M/2 TM", Literals.blockSize / 2 + 1
        "M T5", 0
        "M T5", 1
        "1 2 M T1", 4
        "1 6 TM-1", 6
        "1 6 TM-1", 7
        "1 6 TM-1", 8
        "1 M-2 TM-1", Literals.blockSize - 1
        "1 M-2 TM-1", Literals.blockSize
        "1 M-1 TM-1", Literals.blockSize
        "[2 4 3 1] [M M] [3 6 2 3 4] [2 2 4 3] [3 3 3 3] [M M M-1 M*M-3] [6 6 4 M-1 M*M-5] T2", 7
        "[M-2] [2 M] T5", Literals.blockSize + 1
        "[[M-1] [2 2] [1]] [[2 2 2 3] [5 4 5] [4 5 6 4]] [[3 4 3 2] [3 4 2] [5 M-1 M 6 6 M 6]] [[M*M-3] [6 4 M-1 M*M-5 6 M] [6*M-5 M-1 M M-1 5 M]] T7", Literals.blockSize + 2
    ] |> List.mapi (fun i data -> testCase (sprintf "Test %d with data %A" (i+1) data) (fun _ -> splitConstructedVecTest data))
      |> testList "Constructed vector tests"

let splitJoinTests =
  testList "Split + join tests" [
    testCase "Pushing the tail down in a root+tail node should not cause it to break the invariant, whether or not the root was full" <| fun _ ->
        let vec = RRBVectorGen.treeReprStrToVec "M TM"
        RRBVectorProps.checkProperties vec "Original vector, a full root+tail"
        let vec1 = vec.Remove 0
        RRBVectorProps.checkProperties vec1 "Full root+tail vector after removing first item"
        let vec2 = vec1 |> RRBVector.push 512
        RRBVectorProps.checkProperties vec2 "Full root+tail vector after removing first item, then pushing one item"
        let vec3 = vec2 |> RRBVector.push 512
        RRBVectorProps.checkProperties vec3 "Full root+tail vector after removing first item, then pushing two items"

(*    testProp "multiple splits+joins recreate same vector each time" <| fun (vec : RRBVector<int>) (idxs : int list) ->
        // Might comment this one out sometimes, as it's QUITE slow. Not surprising, considering how much work you end up doing per test run.
        let vecResult =
            idxs
            |> List.truncate 10 // If we allow too many split-join cycles per test, it gets ridiculously slow
            |> List.map (fun i -> (abs i) % (RRBVector.length vec + 1))
            |> List.fold (fun v i ->
                let vL, vR = doSplitTest v i
                let vec' = RRBVector.append vL vR
                let reprL = RRBVectorGen.vecToTreeReprStr vL
                let reprR = RRBVectorGen.vecToTreeReprStr vR
                RRBVectorProps.checkProperties vec' (sprintf "Joined vector from reprL %A and reprR %A" reprL reprR)
                Expect.vecEqual vec' vec "Vector halves after split, when put back together, did not equal original vector"
                vec'
            ) vec
        Expect.vecEqual vecResult vec "After all split+join operations, resulting vector did not equal original vector"
*)
    testCase "Splitting a vector will adjust it to maintain the invariant, and so will joining it together again" <| fun _ ->
        // (Shrunk: RRBVector<length=21,shift=3,tailOffset=18,root=RRBNode(sizeTable=[|5; 10; 18|],children=[|FullNode([|-16; -6; 4; -7; 14|]); FullNode([|15; -17; 14; 5; -1|]);
        // FullNode([|4; 1; -14; 14; 1; -5; -14; 15|])|]),tail=[|7; -7; 17|]> [1])
        let vec = RRBVectorGen.treeReprStrToVec "M/2+1 M/2+1 M T3"
        let vL, vR = doSplitTest vec 1
        RRBVectorProps.checkProperties vL "Left half of split"
        RRBVectorProps.checkProperties vR "Right half of split"
        let vec' = RRBVector.append vL vR
        let reprL = RRBVectorGen.vecToTreeReprStr vL
        let reprR = RRBVectorGen.vecToTreeReprStr vR
        RRBVectorProps.checkProperties vec' (sprintf "Joined vector from reprL %A and reprR %A" reprL reprR)
        Expect.vecEqual vec' vec "Vector halves after split, when put back together, did not equal original vector"

    testCase "Manual test for one scenario that failed the \"split + remove idx 0 of left + join = remove idx 0 of entire\" property" <| fun _ ->
        let vecRepr = "5 M*M-1 T7"
        let vec = RRBVectorGen.treeReprStrToVec vecRepr
        let i = 32
        let vL, vR = doSplitTest vec i
        let vL', vR' =
            if vL.Length > 0 then
                RRBVector.remove 0 vL, vR
            else
                // Can't remove from an empty vector -- but in this case, we know the right vector is non-empty
                vL, RRBVector.remove 0 vR
        let joined = RRBVector.append vL' vR'
        RRBVectorProps.checkProperties joined "Joined vector"
        Expect.vecEqual joined (RRBVector.remove 0 vec) "Split + remove idx 0 of left + joined vectors did not equal original vector with its idx 0 removed"

    // TODO: Make custom test that joins "T0" (emptyVec) with "M T2"
    testCase "split+reverse+join on a sapling" <| fun _ ->
        let vec = RRBVector.ofSeq {1..40}
        let vL, vR = doSplitTest vec 0
        let revL = RRBVector.rev vL
        let revR = RRBVector.rev vR
        RRBVectorProps.checkProperties revL "Reversed left vector"
        RRBVectorProps.checkProperties revR "Reversed right vector"
        let vec' = RRBVector.append revR revL
        RRBVectorProps.checkProperties vec' "Joined vector"
        Expect.vecEqual vec' (RRBVector.rev vec) "Vector halves after split+reverse, when put back together, did not equal reversed vector"

    testCase "Can join two tail-only vectors" <| fun _ ->
        // TODO: Figure out if this is a duplicate of an existing regression test
        let v1 = RRBVectorGen.treeReprStrToVec "T1"
        let v2 = RRBVectorGen.treeReprStrToVec "T1"
        let joined = RRBVector.append v1 v2
        RRBVectorProps.checkProperties joined (sprintf "Joined vector from %A and %A" v1 v2)
        Expect.equal joined.Length 2 "Joined vector should be length 2"
        Expect.equal (joined |> RRBVector.item 0) (v1 |> RRBVector.item 0) "First item of joined vector should be first item of vector 1"
        Expect.equal (joined |> RRBVector.item 1) (v2 |> RRBVector.item 0) "Second item of joined vector should be first item of vector 2"

    testCase "joining two unbalanced vectors will trigger a rebalance" <| fun _ ->
        // NOTE: This test will only work when blockSize = 32. It will have to be rewritten with completely different input if blockSize is ever changed.
        let vL = RRBVectorGen.treeReprStrToVec "[28 31 26 31 32 21 32 26 32 32 28 32 31 29 26 29 28 23 32 32 32 32 26 32 29 29 32 23 31 32 32 32] [32 32 32 32 32] T30"
        let vR = RRBVectorGen.treeReprStrToVec "[25 31 19 25 31 32 32 32 32 32 32 29 24 28 30 32 32 32 32 27 30 27 32 27 29 25 27] [32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 28 32 32 32 31 32] [32 32 32 32 32 32 32 28 32 32 32 32 32 32 32 28 31 32 32 32 32 32 32 31 32] [22 22 26 30 32 29 32 32 27 27 32 32 32 32 32 32 32 26 29 32 28 27 24 27 26 22 29] [32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32] [27 28 24 32 32 29 28 31 30 30 23 29 25 24 25 32 32 25 31 32 26 28] [26 23 32 27 31 29 23 32 27 26 26 29 32 32 32 32 30 24 31 27 28 29 27 27] [32 32 21 32 32 30 19 32 32 27 32 32 26 32 27 24 32 32 29 30 25 32 23] [30 31 32 26 32 26 31 31 31 28 32 24 30 30 27 27 29 27 27 32 25 31 28 31 28 30] [27 32 22 28 32 27 32 28 32 29 32 32 31 27 29 27 31 26 32 23 32 32] [31 26 32 32 25 32 26 24 32 29 31 30 29 32 32 28 26 28 26 27 32 28] [29 26 32 28 32 30 32 30 32 32 23 32 32 32 30 32 29 32 28 27 32 21 32 32 32 32] T27"
        let vLShift = (vL :?> RRBPersistentVector<int>).Shift
        let vRShift = (vR :?> RRBPersistentVector<int>).Shift
        let vLRoot = (vL :?> RRBPersistentVector<int>).Root
        let vRRoot = (vR :?> RRBPersistentVector<int>).Root
        RRBVectorProps.checkProperties vL "Left half of merge"
        RRBVectorProps.checkProperties vR "Right half of merge"
        let joined = RRBVector.append vL vR :?> RRBPersistentVector<int>
        RRBVectorProps.checkProperties joined <| sprintf "Joined vector"
        Expect.equal (joined.Shift) (Operators.max vLShift vRShift) "Joined vector should not have increased in height"
        Expect.equal (joined.Root.NodeSize) (vLRoot.NodeSize + vRRoot.NodeSize - 1) "Joined vector should have merged two nodes in the process of rebalancing"

    // TODO: Duplicate the above test for 5-6 different custom-built vectors

    testCase "remove can shorten trees even when it doesn't rebalance" <| fun _ ->
        let vec = RRBVectorGen.treeReprStrToVec "[M/2 M/2+1] T1" :?> RRBPersistentVector<int>
        // RRBVectorProps.checkProperties vec "Original vector"  // Original vector is *not* compliant with the "vectors shouldn't be too tall" property
        Expect.equal vec.Shift (Literals.blockSizeShift * 2) "Original vector should have height of 2"
        let vec' = vec |> RRBVector.remove 0 :?> RRBPersistentVector<int>
        RRBVectorProps.checkProperties vec' "Vector after one item removed at idx 0"
        Expect.equal vec'.Shift Literals.blockSizeShift "After removal, vector should have height of 1"
        Expect.equal vec'.Root.NodeSize 2 "Removal should not rebalance this tree"

    testCase "pop will slide nodes into tail if it needs to" <| fun _ ->
        let vec = RRBVectorGen.treeReprStrToVec "[M*M-1 M-1] [M] T1" :?> RRBPersistentVector<int>
        RRBVectorProps.checkProperties vec "Original vector"
        Expect.equal vec.Shift (Literals.blockSizeShift * 2) "Original vector should have height of 2"
        let vec' = vec |> RRBVector.pop :?> RRBPersistentVector<int>
        RRBVectorProps.checkProperties vec' "Vector after one item popped"
        Expect.equal vec'.Tail.Length (Literals.blockSize - 1) "After pop, 1 item should have been slid back into the vector"
        Expect.equal vec'.Shift (Literals.blockSizeShift) "After pop, vector should have height of 1"

    testCase "remove can rebalance trees when they become too unbalanced" <| fun _ ->
        let vec = RRBVectorGen.treeReprStrToVec "M/4 M/4+1 M/4+1 M/4 T1" :?> RRBPersistentVector<int>
        RRBVectorProps.checkProperties vec "Original vector"
        Expect.equal vec.Shift (Literals.blockSizeShift) "Original vector should have height of 1"
        let vec' = vec |> RRBVector.remove 0 :?> RRBPersistentVector<int>
        RRBVectorProps.checkProperties vec' "Vector after one item removed at idx 0"
        Expect.equal vec'.Shift Literals.blockSizeShift "After first removal, vector should have height of 1"
        Expect.equal vec'.Root.NodeSize 4 "First removal should not rebalance this tree"
        let vec'' = vec' |> RRBVector.remove 0 :?> RRBPersistentVector<int>
        RRBVectorProps.checkProperties vec'' "Vector after second item removed at idx 0"
        Expect.equal vec''.Shift Literals.blockSizeShift "After second removal, vector should have height of 1"
        Expect.isLessThan vec''.Root.NodeSize 4 "Second removal should rebalance this tree"

    testCase "removeWithoutRebalance can remove without rebalancing trees that the normal remove would have rebalanced" <| fun _ ->
        let vec = RRBVectorGen.treeReprStrToVec "M/4 M/4+1 M/4+1 M/4 T1" :?> RRBPersistentVector<int>
        RRBVectorProps.checkProperties vec "Original vector"
        Expect.equal vec.Shift (Literals.blockSizeShift) "Original vector should have height of 1"
        let vec' = vec.RemoveWithoutRebalance 0 :?> RRBPersistentVector<int>
        RRBVectorProps.checkProperties vec' "Vector after one item removed at idx 0"
        Expect.equal vec'.Shift Literals.blockSizeShift "After first removal, vector should have height of 1"
        Expect.equal vec'.Root.NodeSize 4 "First removal should not rebalance this tree"
        let vec'' = vec'.RemoveWithoutRebalance 0 :?> RRBPersistentVector<int>
        RRBVectorProps.checkProperties vec'' "Vector after second item removed at idx 0"
        Expect.equal vec''.Shift Literals.blockSizeShift "After second removal, vector should have height of 1"
        Expect.equal vec''.Root.NodeSize 4 "Second removal should not rebalance this tree"

    testCase "manual test for split + remove idx 0 of left + join" <| fun _ ->
        let orig = RRBVectorGen.treeReprStrToVec "[M-3 M-3] [2 1 3] [M] TM-1"
        RRBVectorProps.checkProperties orig "Original vector"
        let left, right = orig |> RRBVector.split 17
        RRBVectorProps.checkProperties left "Left vector before remove"
        RRBVectorProps.checkProperties right "Right vector"
        let left = left |> RRBVector.remove 0
        RRBVectorProps.checkProperties left "Left vector after remove"
        let joined = RRBVector.append left right
        RRBVectorProps.checkProperties joined "Joined vector"

    testCase "Manual test for pop right + join" <| fun _ ->
        let vL = RRBVectorGen.treeReprStrToVec "M TM"
        let vR = RRBVectorGen.treeReprStrToVec "[[M-3 M-3]] [[M]] T1"
        RRBVectorProps.checkProperties vL "Orig left vector"
        RRBVectorProps.checkProperties vR "Orig right vector"
        let vR' = RRBVector.pop vR
        let joinedOrig = RRBVector.append vL vR
        RRBVectorProps.checkProperties joinedOrig "Joined orig vector"
        let joinedOrigThenPop = RRBVector.pop joinedOrig
        RRBVectorProps.checkProperties joinedOrigThenPop "Joined orig vector, then popped"
        let joined = RRBVector.append vL vR'
        RRBVectorProps.checkProperties joined "Joined vector"
        Expect.vecEqual joined (RRBVector.pop joinedOrig) "pop right + join did not equal join + pop"

    testCase "one join that could break the \"last leaf is full if parent is full\" invariant" (fun _ ->
        let vL = RRBVectorGen.treeReprStrToVec "M*M-1 TM-2"
        let vR = RRBVectorGen.treeReprStrToVec "T2"
        doJoinTest vL vR
    )
    testCase "another join that could break the \"last leaf is full if parent is full\" invariant" (fun _ ->
        let vL = RRBVectorGen.treeReprStrToVec "M*M-1 TM/2"
        let vR = RRBVectorGen.treeReprStrToVec "TM-1"
        doJoinTest vL vR
    )

    testProp "vecTake" <| fun (VecPlusArrAndIdx (v,a,i)) ->
        let sliced = v.Take i
        RRBVectorProps.checkProperties sliced "Sliced vector"
        Expect.equal (RRBVector.toArray sliced) (Array.truncate i a) "Sliced things didn't match"

    testProp "vecSkip" <| fun (VecPlusArrAndIdx (v,a,i)) ->
        let sliced = v.Skip i
        RRBVectorProps.checkProperties sliced "Sliced vector"
        Expect.sequenceEqual sliced (Array.skip i a) "Sliced things didn't match"
  ]

let manualInsertTestWithVec idx item vec =
    let a = vec |> RRBVector.toArray
    let expected = a |> Array.copyAndInsertAt idx item
    let actual = vec |> RRBVector.insert idx item
    RRBVectorProps.checkProperties actual "Vector after insertion"
    Expect.equal (actual |> RRBVector.toArray) expected (sprintf "Inserting %A at %d should have produced identical results" item idx)

let manualInsertTest idx item treeRepr =
    RRBVectorGen.treeReprStrToVec treeRepr |> manualInsertTestWithVec idx item

let insertTests =
  testList "Insert tests" [
    testProp "insert into full vectors" (fun (VecPlusArrAndIdx (v,a,i)) ->
        let expected = a |> Array.copyAndInsertAt i 512
        let v' = v |> RRBVector.insert i 512
        RRBVectorProps.checkProperties v' (sprintf "Vector with insertion at %d" i)
        Expect.equal (v' |> RRBVector.toArray) expected "insert did not insert the right value"
    )
    testPropMed "insert into random vectors" (fun (vec : RRBVector<int>) (idx : int) ->
        let i = (abs idx) % (RRBVector.length vec + 1)
        let expected = vec |> RRBVector.toArray |> Array.copyAndInsertAt i 512
        let vec' = vec |> RRBVector.insert i 512
        RRBVectorProps.checkProperties vec' (sprintf "Vector with insertion at %d" i)
        Expect.equal (vec' |> RRBVector.toArray) expected "insert did not insert the right value"
    )

    // Specifically-constructed tests
    testCase "Insert into root+tail vector at head" <| fun _ -> manualInsertTest 0 512 "M T1"
    testCase "Insert into root+tail vector somewhere inside first leaf" <| fun _ -> manualInsertTest 1 512 "M T1"

    testCase "insert into tail-only vector with full tail, at start of tail"        <| fun _ -> manualInsertTest 0 512 "TM"
    testCase "insert into tail-only vector with full tail, almost at start of tail" <| fun _ -> manualInsertTest 1 512 "TM"
    testCase "insert into tail-only vector with full tail, at middle of tail"       <| fun _ -> manualInsertTest (Literals.blockSize >>> 1) 512 "TM"
    testCase "insert into tail-only vector with full tail, almost at end of tail"   <| fun _ -> manualInsertTest (Literals.blockSize - 1)   512 "TM"
    testCase "insert into tail-only vector with full tail, at end of tail"          <| fun _ -> manualInsertTest Literals.blockSize         512 "TM"

    testCase "insert that splits a full final leaf" <| fun _ -> manualInsertTest (Literals.blockSize * (Literals.blockSize - 1) - 1) 512 "[M*M-1] T1"
    testCase "insert that splits a full first leaf" <| fun _ -> manualInsertTest 0 512 "[M*M] [M] T1"

    testCase "insert into full vector of size 1025" <| fun _ -> seq { 0..1024 } |> RRBVector.ofSeq |> manualInsertTestWithVec 0 512
    testCase "insert into full vector of size 264"  <| fun _ -> seq { 0..263 }  |> RRBVector.ofSeq |> manualInsertTestWithVec 0 512

    testCase "insert that requires a shift left"  <| fun _ -> manualInsertTest (Literals.blockSize + 3) 512 "[[2 2]] [[M-1 M] [6 6 M]] T2"
    testCase "insert that requires a shift right" <| fun _ -> manualInsertTest 5                        512 "[[2 2]] [[M M-1] [6 6 M]] T2"
  ]

// let bigTestVec = [0..1092] |> ofList
// bigTestVec.shift
// let a,b = (bigTestVec.root, bigTestVec.root) |> findNodePairAtIdx (191,192) 6 bigTestVec.shift
// (bigTestVec.root, bigTestVec.root) |> findNodePairAtIdx (206,207) 4 bigTestVec.shift
// isSameNode (fst a) (fst b)

// TODO: Also implement some operational tests, aka model-based testing
// The idea is you have a bunch of operations, like "pop", "push 42", "rev",
// "skip 3", "take 5", "truncate 8" and so on. Can't have random parameters
// on them: the parameters MUST be fixed, so that the operations can be of
// type ('a -> 'a). Then you apply the same operations to the vector, and to
// another object that serves as a model (we'll use a list), and make sure
// that the model continues to be in sync with the real object through all
// sequences of operations. This will help find cases where order of operations
// matters (i.e., a bug only happens if you do A,B,A,A in that specific order).

let fixedSpecTestFromData (spec : RRBVectorFsCheckCommands.Cmd list) (data : RRBVector<int>) () =
    // A test I wrote to focus on one specific test scenario that was causing a property failure.
    // If you want to uncomment the printfn statements here, first focus this test so it runs alone.
    let mutable vec = data
    for cmd in spec do
        logger.debug (
            eventX "Before {cmd}, vec was {vec}"
            >> setField "cmd" (sprintf "%A" cmd)
            >> setField "vec" (RRBVectorGen.vecToTreeReprStr vec)
        )
        vec <- cmd.RunActual vec
        logger.debug (
            eventX "After  {cmd}, vec was {vec}"
            >> setField "cmd" (sprintf "%A" cmd)
            >> setField "vec" (RRBVectorGen.vecToTreeReprStr vec)
        )
        RRBVectorProps.checkPropertiesSimple vec

let fixedSpecTest (spec : RRBVectorFsCheckCommands.Cmd list) =
    fixedSpecTestFromData spec RRBVector.empty<int>

let operationTests =
  testList "Operational transform tests" [
    testProp "Extra-small lists from empty" (Command.toProperty RRBVectorFsCheckCommands.specExtraSmallFromEmpty)
    testProp "Small lists from empty" (Command.toProperty RRBVectorFsCheckCommands.specSmallFromEmpty)
    testProp "Medium lists from empty" (Command.toProperty RRBVectorFsCheckCommands.specMediumFromEmpty)
    testProp "Large lists from empty" (Command.toProperty RRBVectorFsCheckCommands.specLargeFromEmpty)
    testProp "Extra-large lists from empty" (Command.toProperty RRBVectorFsCheckCommands.specExtraLargeFromEmpty)

    testProp "Extra-small lists from almost-full sapling" (Command.toProperty RRBVectorFsCheckCommands.specExtraSmallFromAlmostFullSapling)
    testProp "Small lists from almost-full sapling" (Command.toProperty RRBVectorFsCheckCommands.specSmallFromAlmostFullSapling)
    testProp "Medium lists from almost-full sapling" (Command.toProperty RRBVectorFsCheckCommands.specMediumFromAlmostFullSapling)
    testProp "Large lists from almost-full sapling" (Command.toProperty RRBVectorFsCheckCommands.specLargeFromAlmostFullSapling)
    testProp "Extra-large lists from almost-full sapling" (Command.toProperty RRBVectorFsCheckCommands.specExtraLargeFromAlmostFullSapling)

    testCase "SingletonNodes in right spine don't cause problems while pushing" <| fun _ ->
        let mutable vec = RRBVectorGen.treeReprStrToVec "17 16 M M M M M M M M M M M M M M M M M M 17 16 M 17 16 M M M M M M M T32"   // A nearly-full vector containing a few insertion splits
        for i = 1 to 33 do
            vec <- vec.Push i
        RRBVectorProps.checkPropertiesSimple vec

    testCase "Inserting into a full tail with empty root will not cause an invariant break" <| fun _ ->
        let vec = RRBVectorGen.treeReprStrToVec "TM"
        let step1 = vec |> RRBVector.insert 7 1
        RRBVectorProps.checkProperties step1 "After one insert"
        let step2 = step1 |> RRBVector.pop
        RRBVectorProps.checkProperties step2 "After one insert then one pop"
        let step3 = step2.Remove 3
        RRBVectorProps.checkProperties step3 "After one insert then one pop and one remove from first leaf"

    testCase "Removing from the root of a root+tail vector maintains the invariant" <| fun _ ->
        let vec = RRBVectorGen.treeReprStrToVec "M T5"
        RRBVectorProps.checkProperties vec "Original vector"
        let step1 = vec.Insert 3 -512
        RRBVectorProps.checkProperties step1 "Vector after one insert"
        let step2 = step1.Remove 0
        RRBVectorProps.checkProperties step2 "Vector after one insert and one remove"
        // Wait... this does *not*, in fact, maintain the invariant! And yet my property checks are skipping it, and
        // it's not causing trouble with the push function! There are just two possible reasons and solutions:
        // 1. This is a bug that my property checks aren't catching, and I have to fix my property checks to catch it so this test fails, or
        // 2. The invariant *isn't* necessary for shift=0 trees, because of the "elif root.NodeSize = Literals.blockSize" check in appendLeafWithGrowth.
        // I think it's #2, which means I'll be able to relax the invariant checking somewhat. Nice!
        // TODO: Either tweak the property checks, or start relaxing the invariant. Either way, adjust this test to match what I've decided to do about this.
        let mutable v = step2
        for i = 1 to Literals.blockSize * 3 do
            v <- v |> RRBVector.push 42
            RRBVectorProps.checkProperties v <| sprintf "After %d pushes" i

    testCase "fixed property"     <| fixedSpecTest RRBVectorFsCheckCommands.fixedSpec
    testCase "x-small property"   <| fixedSpecTest RRBVectorFsCheckCommands.xsSpec
    testCase "medium property"    <| fixedSpecTest RRBVectorFsCheckCommands.medSpec
    testCase "medium property 2"  <| fixedSpecTest RRBVectorFsCheckCommands.med2Spec
    testCase "shorten property 1" <| fixedSpecTest RRBVectorFsCheckCommands.shortenSpec1
    testCase "shorten property 2" <| fixedSpecTest RRBVectorFsCheckCommands.shortenSpec2
    testCase "shorten property 3" <| fixedSpecTest RRBVectorFsCheckCommands.shortenSpec3
    testCase "shorten property 4" <| fixedSpecTest RRBVectorFsCheckCommands.shortenSpec4

    testCase "inserting into first leaf can still maintain last-leaf-is-full invariant" <| fun _ ->
        let initialVec = RRBVectorGen.treeReprStrToVec "M*M T1"
        let step1 = initialVec |> RRBVector.remove 2
        RRBVectorProps.checkProperties step1 "Vector after removing one item from first leaf"
        let step2 = step1 |> RRBVector.remove (step1.Length - 2)
        RRBVectorProps.checkProperties step2 "Vector after removing one item from first leaf, then one item from last leaf"
        let step3 = step2 |> RRBVector.insert 2 -512
        RRBVectorProps.checkProperties step3 "Vector after removing one item from first leaf, then one item from last leaf, then inserting one item in first leaf"

    testCase "removing from last leaf maintains last-leaf-is-full invariant" <| fun _ ->
        let vec = RRBVectorGen.treeReprStrToVec "M*M T1"
        let result = vec |> RRBVector.remove (vec.Length - 2)
        RRBVectorProps.checkProperties result "Vector after removing one item from last leaf"
  ]

let arrayTests =
  testList "Array extension functions" [
    testProp "copyAndAppend" <| fun (ArrayAndIdx (arr,_)) ->
        let expected = Array.init (arr.Length + 1) (fun i -> if i = Array.length arr then 512 else arr.[i])
        let actual = arr |> Array.copyAndAppend 512
        Expect.equal actual expected "copyAndAppend did not append the test value (512) at the right place"

    testProp "copyAndSet" <| fun (NonEmptyArrayAndIdx (arr,i)) ->
        let expected = Array.copy arr
        expected.[i] <- 512
        let actual = arr |> Array.copyAndSet i 512
        Expect.equal actual expected "copyAndSet did not set the test value (512) at the right place"

    testProp "copyAndInsertAt" <| fun (NonEmptyArrayAndIdx (arr,i)) ->
        let expected = Array.append (Array.append (Array.take i arr) [|512|]) (Array.skip i arr)
        let actual = arr |> Array.copyAndInsertAt i 512
        Expect.equal actual expected "copyAndInsertAt did not set the test value (512) at the right place"

    testProp "copyAndRemoveAt" <| fun (NonEmptyArrayAndIdx (arr,i)) ->
        let expected = Array.append (Array.take i arr) (Array.skip (i+1) arr)
        let actual = arr |> Array.copyAndRemoveAt i
        Expect.equal actual expected "copyAndRemoveAt did not set the test value (512) at the right place"

    testProp "copyAndPop" <| fun (NonEmptyArrayAndIdx (arr,_)) ->
        let expected = Array.take (Array.length arr - 1) arr
        let actual = arr |> Array.copyAndPop
        Expect.equal actual expected "copyAndPop did not set the test value (512) at the right place"

    testProp "splitAt" <| fun (ArrayAndIdx (arr,i)) ->
        let expected = (Array.take i arr, Array.skip i arr)
        let actual = arr |> Array.splitAt i
        Expect.equal actual expected "splitAt did not produce the right results"

    testProp "appendAndSplitAt" <| fun (idx:int) (a:int[]) (b:int[]) ->
        let joined = Array.append a b
        let idx = (abs idx) % (Array.length joined + 1)
        let expected = joined |> Array.splitAt idx
        let actual = Array.appendAndSplitAt idx a b
        Expect.equal actual expected <| sprintf "appendAndSplitAt did not produce the right results at idx %d and with input arrays %A and %A" idx a b

    testProp "appendAndInsertAndSplitEvenly" <| fun (idx:int) (a:int[]) (b:int[]) ->
        let joined = Array.append a b
        let idx = (abs idx) % (Array.length joined + 1)
        let expected = joined |> Array.copyAndInsertAt idx 512 |> Array.splitAt (((Array.length a + Array.length b) >>> 1) + 1)
        let actual = Array.appendAndInsertAndSplitEvenly idx 512 a b
        Expect.equal actual expected <| sprintf "appendAndInsertAndSplitEvenly did not produce the right results at idx %d and with input arrays %A and %A" idx a b

    testProp "insertAndSplitEvenly" <| fun (NonEmptyArrayAndIdx (arr,idx)) ->
        let expected = arr |> Array.copyAndInsertAt idx 512 |> Array.splitAt (((Array.length arr) >>> 1) + 1)
        let actual = arr |> Array.insertAndSplitEvenly idx 512
        Expect.equal actual expected "insertAndSplitEvenly did not produce the right results"
  ]

let apiTests =
  testList "API tests" [
    // Disabled test until we restore the windowed function
    // testProp "windowed" <| fun (VecPlusArrAndIdx (v,a,i)) ->
    //     if i < 1 then () else
    //     let expected = a |> Array.windowed i
    //     let actual = v |> RRBVector.windowed i |> Seq.map RRBVector.toArray |> Seq.toArray
    //     Expect.equal actual expected "RRBVector.windowed did not produce the right results"

    testProp "slice notation" <| fun (VecPlusArrAndIdx (v,a,idx)) (PositiveInt endIdx) ->
        let endIdx = if v.Length = 0 then 0 else endIdx % v.Length
        let idx, endIdx = if idx <= endIdx then idx,endIdx else endIdx,idx
        RRBVectorProps.checkProperties v "Original vector"
        let v' = v.[idx..endIdx]
        RRBVectorProps.checkProperties v' <| sprintf "Vector after slicing from %d to %d" idx endIdx
        if a.Length > 0 then
            let a' = a.[idx..endIdx]
            Expect.vecEqualArr v' a' "Sliced vector should equal equivalent slice from array"
  ]

let nodeVecGenerationTests =
  // Not sure these are worth keeping any more. TODO: Get rid of these if they're duplicates
  testList "Generate vectors from various sources" [
    testProp "Tree from array" <| fun (arr:int[]) ->
        let expected = arr
        let actual = RRBVector.ofArray arr
        Expect.equal (RRBVector.toArray actual) expected "Tree did not get built properly from array"

    testProp "Tree from seq" <| fun (arr:int[]) ->
        let expected = arr
        let s = arr |> Seq.ofArray
        let actual = s |> RRBVector.ofSeq
        Expect.equal (RRBVector.toArray actual) expected "Tree did not get built properly from array"
  ]

let longRunningTests =
  testList "Long-running tests, skipped by default" [

    // joining two unrelated vectors is equivalent to array-appending their array equivalents passed in 00:03:42.0410000
    // Skipped because it takes too long
    ptestProp "joining two unrelated vectors is equivalent to array-appending their array equivalents" <| fun (v1 : RRBVector<int>) (v2 : RRBVector<int>) ->
        let a1 = RRBVector.toArray v1
        let a2 = RRBVector.toArray v2
        let joined = RRBVector.append v1 v2
        let joined' = RRBVector.append v2 v1
        let r1 = RRBVectorGen.vecToTreeReprStr v1
        let r2 = RRBVectorGen.vecToTreeReprStr v2
        RRBVectorProps.checkProperties joined (sprintf "Joined vector from %A and %A" r1 r2)
        RRBVectorProps.checkProperties joined' (sprintf "Opposite-joined vector from %A and %A" r2 r1)
        Expect.vecEqualArr joined (Array.append a1 a2) "Joined vectors did not equal equivalent appended arrays"
        Expect.vecEqualArr joined' (Array.append a2 a1) "Opposite-joined vectors did not equal equivalent appended arrays"

    // joining two unrelated vectors is equivalent to list-appending their list equivalents passed in 00:02:44.0550000
    ptestProp "joining two unrelated vectors is equivalent to list-appending their list equivalents" <| fun (v1 : RRBVector<int>) (v2 : RRBVector<int>) ->
        let l1 = RRBVector.toList v1
        let l2 = RRBVector.toList v2
        let joined = RRBVector.append v1 v2
        let joined' = RRBVector.append v2 v1
        let r1 = RRBVectorGen.vecToTreeReprStr v1
        let r2 = RRBVectorGen.vecToTreeReprStr v2
        RRBVectorProps.checkProperties joined (sprintf "Joined vector from %A and %A" r1 r2)
        RRBVectorProps.checkProperties joined' (sprintf "Opposite-joined vector from %A and %A" r2 r1)
        Expect.equal (joined |> RRBVector.toList) (List.append l1 l2) "Joined vectors did not equal equivalent appended lists"
        Expect.equal (joined' |> RRBVector.toList) (List.append l2 l1) "Opposite-joined vectors did not equal equivalent appended lists"

    // joining two unrelated vectors is equivalent to seq-appending their seq equivalents passed in 00:04:09.4570000
    // Skipped because it takes too long
    ptestProp "joining two unrelated vectors is equivalent to seq-appending their seq equivalents" <| fun (v1 : RRBVector<int>) (v2 : RRBVector<int>) ->
        let s1 = RRBVector.toSeq v1
        let s2 = RRBVector.toSeq v2
        let joined = RRBVector.append v1 v2
        let joined' = RRBVector.append v2 v1
        let r1 = RRBVectorGen.vecToTreeReprStr v1
        let r2 = RRBVectorGen.vecToTreeReprStr v2
        RRBVectorProps.checkProperties joined (sprintf "Joined vector from %A and %A" r1 r2)
        RRBVectorProps.checkProperties joined' (sprintf "Opposite-joined vector from %A and %A" r2 r1)
        Expect.equal (joined |> RRBVector.toArray) (Seq.append s1 s2 |> Array.ofSeq) "Joined vectors did not equal equivalent appended seqs"
        Expect.equal (joined' |> RRBVector.toArray) (Seq.append s2 s1 |> Array.ofSeq) "Opposite-joined vectors did not equal equivalent appended seqs"

    // TODO: Decide whether we need all three of those

    // split+join recreates same vector passed in 00:05:44.0870000
    // split+join recreates same vector passed in 00:10:17.7110000 -- too long, skipping
    ptestProp "split+join recreates same vector" <| fun (vec : RRBVector<int>) (i : int) ->
        let i = (abs i) % (RRBVector.length vec + 1)
        let vL, vR = doSplitTest vec i
        // logger.debug (eventX "vR = {vec}" >> setField "vec" (RRBVectorGen.vecToTreeReprStr vR))
        let vec' = RRBVector.append vL vR
        RRBVectorProps.checkProperties vec' "Joined vector"
        Expect.vecEqual vec' vec "Vector halves after split, when put back together, did not equal original vector"

    // split+reverse+join recreates reverse vector passed in 00:07:54.1070000 -- too long, skipping
    ptestProp "split+reverse+join recreates reverse vector" <| fun (vec : RRBVector<int>) (i : int) ->
        let i = (abs i) % (RRBVector.length vec + 1)
        let vL, vR = doSplitTest vec i
        let revL = RRBVector.rev vL
        let revR = RRBVector.rev vR
        let vec' = RRBVector.append revR revL
        RRBVectorProps.checkProperties vec' "Joined vector"
        Expect.vecEqual vec' (RRBVector.rev vec) "Vector halves after split+reverse, when put back together, did not equal reversed vector"

    // big join, test 1 passed in 00:03:27.6860000
    testCase "big join, test 1" <| fun _ ->
        let bigNum = 5 <<< (Literals.blockSizeShift * 3)
        let v1 = seq {0..bigNum+6} |> RRBVector.ofSeq  // Top level has 5 completely full FullNodes, tail has 7 items
        let v2 = RRBVectorGen.treeReprStrToVec "[M 2 M/2*M-6 M-1 M-2 M-1 M/2-1] [M*M-6 M-1 M-3 M M M-3 M] [M-2 M-3 M-2 M-2 M-1 M-2*M-6 M] TM"
        doJoinTest v1 v2

    // big join, test 2 passed in 00:00:40.3750000
    testCase "big join, test 2" <| fun _ ->
        let bigNum = 1 <<< (Literals.blockSizeShift * 3)
        let v1 = RRBVectorGen.treeReprStrToVec "M M-1 M*M-5 M-1*2 M T4"
        let v2 = seq {0..bigNum+2} |> RRBVector.ofSeq
        doJoinTest v1 v2

    // big join, test 3 passed in 00:00:35.1910000
    testCase "big join, test 3" <| fun _ ->
        let v1 = RRBVectorGen.treeReprStrToVec "M*M/2-1 M-2 M-1 M*M/2-1 T3"
        let v2 = RRBVectorGen.treeReprStrToVec "[M*M] [M-1*M-1]*M-2 [M-1 M-2 M*M-4] T6"
        doJoinTest v1 v2

    testCase "big join, test 4 (really big)" <| fun _ ->
        let v1 = RRBVectorGen.treeReprStrToVec "M T1"
        let v2 = RRBVectorGen.treeReprStrToVec "[[M 28 M M M M M M M 30 M M M M M M M M M 30 M M M M M 28 M M M 30 M M] [M M 27 M-1 M-1 M M M-1 M 26 M M M M M M M M-1 M-1 M-1 M M M M M M M-1 M M-1 M M M] [M M M-1 M M M M-1 M M M M M M M M-1 M M M-1 M M M M M M M M M M M M M M] [M M M M M M M M M M 28 M M M M M M M M M M M 30 M M M M M M M 29 M] [29 M-1 25 M M M 26 30 25 28 30 26 M-1 29 29 M M M M-1 M 29 28 23 22 M-1 M 28 28 29 M-1 29 30] [30 M-1 30 M 24 26 M 26 30 25 M-1 M 30 28 26 M 30 26 29 24 M M 30 M 27 M M-1 M 29 27 28 23] [27 29 26 M M M 29 25 26 M 27 M M 25 30 30 30 28 27 M M-1 M 27 M 26 M M 29 26 30 M M] [M-1 M M 25 27 M M M 26 M 29 29 M 30 24 24 M 28 28 28 30 30 M M 29 30 24 26 28 M M-1 30] [M-1 28 29 30 M M M M M M 29 23 M M 30 M-1 28 26 30 M-1 M M 26 27 28 M 29 29 26 M-1 M 28] [27 M M M-1 28 25 M 30 28 25 M 26 M M M M M 28 29 25 M M 28 M 29 M 26 27 M-1 29 M 29] [M-1 28 M-1 M-1 M M-1 M 29 M 25 M M M M M 29 30 M M-1 M M 29 M-1 24 26 20 M M 30 M 28] [M M M 28 M 27 M 24 29 M 30 29 M 30 29 27 30 M 28 29 28 30 M M 28 M 27 M 30 28 M M] [M 28 M-1 30 M M-1 M 25 M M M M 27 29 M 26 M-1 M-1 M-1 M M 30 M 28 M M 29 29 28 30 M] [27 M M 24 M-1 M M 30 M-1 30 M M 30 28 29 M M 28 M 27 M 27 30 M 29 30 M 29 30 M M-1 M] [29 30 M 28 30 M-1 M-1 M 30 M 27 27 M M M M M M M M 30 M M M 30 30 M M M M 29 26] [M 29 M M-1 M M M-1 27 29 M M M 27 M 28 30 30 28 29 M M M M M-1 M-1 M M M 29 29 M 28] [M M 28 M M M M M 26 M M M 30 M M M 30 M M M-1 M 29 M M M 29 25 M 30 M M M] [M-1 M 27 M M M M 30 M-1 M M M 30 M M-1 M 24 28 M M M 29 M-1 M M M-1 29 29 M M M M] [M 30 M M M M 28 M M M 25 M M M M M M M M M 30 M M M-1 M-1 30 M-1 M M M M M] [M 25 M M M 27 29 M M M-1 M 28 M M M M 28 M 26 M M-1 27 M M M M 29 M 27 29 M M] [M M 29 M 28 M M 28 M-1 28 28 M 28 M M 28 M-1 M M-1 M 25 30 29 M M M M M 29 M 28 29] [M M M M M 25 M M-1 M M 29 M M M-1 29 29 M 30 M M M 29 30 29 M M M M 26 M 29 M] [M 27 M M 29 M M M 28 M M M M M-1 M 29 30 M M M 26 M M M-1 M-1 M-1 29 M 25 M M 27] [M-1 M-1 28 M-1 M M M 30 M M M M-1 M M 26 30 M M-1 M M M M M M M 29 27 M M] [M M-1 M M M 26 M M M M M 27 M 29 27 M M 24 M M 28 M M M M M M M] [M-1 M M M M 30 M 27 M M M M M M-1 M M M M M 30 27 M M M M M M-1 M M] [M 29 M 29 M M M 28 M M M M 30 28 M M M M 26 M M M M M M M M M M M M M]] [[M 28 M M M 25 30 M-1 26 27 M 29 M M 26 M 29 M-1 M M M 30 M 24 28 M-1 28 M M M M 29] [M M-1 29 30 M M M M 29 21 M M M 28 M-1 M M 30 M M M M M M-1 M 28 23 M M 28 M M] [M 26 27 M M M 29 M M M 26 30 M M M 27 30 M M M 26 M 30 M 30 29 M 28 M 28 M M] [28 21 M-1 M M 30 M M 30 29 M 30 M M 30 M M M M-1 M M M M M M M 25] [M M M M 30 M M M M 29 M-1 29 29 M M-1 M 27 M-1 30 M M M M M M M 27 29 M 30 28 27] [M M M M 29 M 30 30 M M M 30 M M M-1 30 M M-1 27 M M M 27 M M M M M M 30 M M] [M M-1 M 27 M M M M M M 28 M M M 27 M M-1 29 M-1 M 28 M M 30 M 29 M-1 M M M M M] [29 30 M M M M M M M M M M M-1 M M M 30 M 29 M M-1 M M 29 M M M M M M M M] [M M M M M M-1 M M-1 M M M 29 M M M M M M M M 27 M M M M M M M M-1 M 29 26] [M M M M M M M M M M 30 M M M M M M M-1 M M-1 M M M M M M M M M M M M-1] [M M M M M M M 27 M M M M M-1 M M M 30 M M M M M M M 30 M] [27 24 M-1 29 M-1 M M-1 M M-1 25 30 28 M M 25 24 30 M 27 M-1 28 24 M 30 28 M-1 29 M 27 27 M 29] [M*M] [M 28 29 29 28 28 28 M M-1 M 26 28 27 M M 30 M-1 28 29 30 29 30 30 M M 27 M-1 28 M 29 28 25] [M M-1 M-1 21 M-1 M 28 25 30 M 30 26 M-1 28 M-1 M M 26 21 27 M 30 M-1 30 M 27 M 23 M-1 29 M M] [M M-1 M M-1 27 26 30 M M 28 M 29 30 M 30 30 24 27 M-1 28 25 M 29 M-1 M 30 M 26 27 M M-1 M] [M M M 24 30 26 M-1 27 29 M M M-1 27 M M-1 M 27 26 M M-1 30 30 30 30 26 25 M 27 28 M 30 M] [25 29 M 29 M 23 28 M-1 27 30 29 M-1 M-1 28 M 29 M M M M 30 M M 27 29 M M M M M 27 M] [23 28 M 25 M-1 M-1 M 28 30 M M 23 M-1 30 M-1 M M 28 27 M 30 M M 28 M-1 M 29 M 28 M M 30] [M M M M 26 30 29 M 25 26 M 30 M-1 30 M-1 M M M-1 28 M 29 M M M 30 M 29 28 M M M 28] [30 M 27 29 M M 27 M M M 25 M 28 M M 29 25 25 M M M 30 30 30 30 30 M M M M M-1 30] [M M 28 M-1 M 30 M M 27 M M-1 M M 29 28 M M M M M-1 27 M M M 29 25 M M M-1 M-1 M 29] [M 29 M-1 M M M-1 29 M M M M M 24 26 30 M M-1 M M M M 30 M-1 M M M 26 30 29 M-1 M 26] [M-1 M 26 M M 28 M M 30 M-1 M M M 30 28 M M M M M 30 M-1 M M 30 M M M M 26 M M] [28 M M M M 29 M M 25 M M M 28 M M-1 M M 29 30 M 28 M M 29 M M M 30 M 29 M M] [29 M M M M M 30 M 28 M-1 M M M M M M M 29 30 28 M M M M M M M M 30] [29 M 30 M M M-1 M M 29 M M M M 26 M 28 M M M M M M 30 M-1 M M M M 28] [M M M M M M M 28 M M M M M M M M M M M 30 M M M M M M M M M 27 M M] [M M M M-1 M M M M M M M-1 30 M M M M M-1 M M-1 M M 29 29 M M 28 M M M M M M] [M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M-1 M M]] [[M M 30 M M 22 M M 23 23 25 M 30 27 25 M 28 M 22 27 27 M 20] [27 30 M 28 26 25 M M-1 M M 29 M 24 30 M 27 M M 24 24 21 23 27 27 M 30 M-1] [M 30 M M M M 27 27 M M 25 M-1 M 27 25 M 29 24 26 M-1 29 M M 27 M 30 23 24 M] [M M 28 M-1 M-1 M 28 M M 27 24 30 30 24 M M 30 24 30 29 24 30 M M 30 25 30 26 27 M] [M 20 M M 26 M M 28 29 M 29 28 30 20 M-1 M M 23 M 29 M M M 25 M-1 M M M] [M 23 M M 30 28 29 24 29 M-1 29 M-1 27 29 M M 28 29 M M M 28 M 30 27 25 28 M M M M M-1] [27 M 30 27 M 29 27 M M 29 M M M M M 23 M 30 23 M M 27 M M 25 M M 30] [M 27 24 M M 27 M-1 26 M-1 28 M 25 M 29 25 M M 28 M-1 M 30 29 M 28 M-1 M M M] [M 30 30 M M 28 25 26 30 M M M M M 30 M M M M M 28 27 M M 28 M M 29 26 M] [30 28 M M M M 29 M-1 27 26 M-1 30 M M M-1 26 24 30 M M M M-1 30 28 M M M M 29 M-1] [29 M 27 M M M-1 29 M-1 M M M 28 M M 28 M M 27 M 30 M M M 30 29 M M M 30 30 M M] [27 M-1 M M M 29 M M M M 30 M M-1 M M-1 26 M M-1 M M 28 28 M M-1 28 30 M M M M 28 29] [M 29 M 30 M M M-1 M 26 M 30 M-1 M M M M 30 M 29 M M 28 M 30 M M M-1 M M M M M] [26 M M M M M 29 M M M M M M M-1 25 M M M M M M-1 M-1 M-1 M 30 M M M 25 29] [28 M M M M 30 M 30 M M 30 29 M M M M M M 30 M M M M M M M M M M 30 M M] [M 30 M-1 M 28 26 M 29 M M M-1 M M-1 30 M M M M M M M M M M M 30 M M M M M] [M M M M-1 M M M M 30 M M-1 M M M M M M M M M M M M M-1 M M M M M 30 M] [M M M M 29 M 29 M 29 M M M M M M M M M M M M M M 29 M M M M M M M] [M M-1 30 24 29 29 22 M-1 M-1 30 M M 25 29 M-1 29 M 24 29 24 27 28 M 27 M 30 28 26 29] [M M M-1 M M M M M M M M M M M M M M M M M M M-1 M M] [19 28 M 30 M-1 30 27 22 29 26 M M M M M-1 29 28 30 M M M-1 25 24 25 30 M M M 30] [24 26 29 M 30 M M M 22 27 25 24 26 M M M 23 22 29 28 M 28 M-1 30 M] [M 29 24 25 M M M 30 22 M-1 M 26 22 27 M M 29 30 29 M-1 M 25 27 M] [24 M M 29 M 30 24 29 M 27 M-1 M 29 26 M M-1 24 M M 22 M M 21 M M 29 M 26] [M 25 27 M 26 M-1 M 29 28 M 28 M-1 30 27 M M M 30 M-1 28 M 28 30 M 28 M M M 30 26] [M M 28 30 30 M-1 27 M 26 M 25 M 25 28 M 26 M M 30 30 20 30 26 M] [29 M M M-1 27 30 M M-1 M M M 29 M M M M M M 21 28 26 M M-1 30 28 28 26]] [[29 26 M-1 M 30 28 24 M M-1 27 M 26 M M 27 M-1 28 M M 25 29 30 M 28 28 26 27 M 28 M M 25] [28 28 29 25 25 M 22 22 24 27 M-1 29 M M-1 M M 30 26 M M M-1 29 28 M 30 23 29] [30 M 29 27 30 29 27 M-1 M 21 26 27 28 29 28 25 27 28 24 M] [27 M M 26 26 M 25 M-1 28 M 29 26 29 29 29 26 26 30 24 30 27 25] [26 M M M 29 28 M 21 30 M-1 M M 27 M-1 M M 26 M M 26 26 M 29 28 M 30 M-1 M 25] [23 30 24 M M M M 30 30 M 28 M 28 M M M M-1 21 M 30 M M 26 M M 30 M 24 M 25 27 M] [M 25 M 27 M 29 M M 26 26 25 M M M-1 M M M 28 M M-1 29 M-1 M 30 M M-1 29 30 M 26 M] [M M M 22 M 30 30 M M M M-1 M 30 30 M M M 24 M 29 25 M-1 21 28 M 28 26] [M-1 M M-1 M 30 M M M-1 M 30 M M M 27 27 M M 28 M 30 M-1 M-1 28 29 M-1 28 M M-1 30 26 M 30] [26 M M M M 30 M M 29 M M-1 M M M M 30 M 26 M 24 M-1 25 29 M M 28 29 30 M-1 30 30 M] [M 26 M 28 M M 29 29 25 25 27 M-1 M 26 29 30 M-1 28 M 29 M-1 M M M M 26 M M-1 M 28 23 28] [M-1 27 M 27 M M-1 M 23 27 M M M M 23 30 M 27 M-1 M M M 26 25 M 24 M-1] [29 M 29 M M 30 29 20 M-1 M 27 M M 27 M 28 M 28 26 26 30 M M 29 26 M M-1 M M 30 M 30] [27 M 28 28 M M M M M 29 M M M M 29 27 M 28 25 M M M 28 30 25 26 M M M-1 M] [M-1 28 30 30 27 23 30 M-1 30 M 29 26 M M 28 M M M M M 26 29 29 27 M M M M-1 M-1] [M M 28 M M M M M 26 M 25 M 28 26 30 M M-1 M M-1 23 27 M M M M M M] [30 M 21 M 29 29 28 29 M M M M 27 M M 28 M 25 29 M M M 30 M 30 M M M M-1 M M M] [M-1 M M M-1 M-1 28 28 M M M 27 26 25 M M M M M-1 M 30 M M 29 M M M] [30 M M 24 M M M M M M 26 25 30 M M-1 M M M 25 30 M M M-1 M M M 28 M M M] [M M M-1 30 M M M M M 29 M M-1 M M M 26 M M M 30 M M M M-1 27 28 M M M] [M M M M M-1 M 30 M M 30 M M 30 26 M M M 23 M-1 M M-1 26 M M M M M] [M M M-1 27 M 27 M M M M M M M M M M M M M M M-1 M M M M M M 30 M M-1 M] [25 M M-1 M 27 24 24 M 30 M M M 29 M 23 M M-1 M 29 M M M 30] [M 26 27 M-1 28 M M 30 M 30 M M 29 M M M M 23 30 27 M M 30 29 26 M M M 29 M-1 M M]] [[30 M-1 26 M M 29 M 29 M M-1 M 29 M-1 24 M-1 M M M M M 29 29 M M M 29 M M M M M] [M M M 30 M M M M M M M M M M M M M 29 M 28 30 M 30 M-1 M 28 27] [M 30 M 29 M M M M M M M M M 28 M 30 M M M M 25 M M 28 M M M M-1 27 M M M] [25 30 M M M M M M M M M M M-1 M M M M M M M M M M M M 29 M M M M M] [M M M M-1 M M M M-1 M M M M M M 30 25 M 30 M M M 30 M M M 29 M] [M M M M M M M M M M M M-1 M M 30 M M M M M M M M M M M M M] [M M M M M M M M M M M M M M M M M M M M M M M M M 24 M M M] [27 M 30 26 19 M 25 30 M M 27 27 M 20 30 M 24 M M-1 29 28 26 30 M-1 27] [28 24 26 30 28 28 M M-1 26 29 M M-1 M M 27 26 M 25 29 26 30 M M 26 29 19 28] [30 30 26 M M 30 M M 26 26 M-1 M-1 M 28 28 27 M-1 M 29 28 28 M M M-1 M 27 26 28 29 26 29 M] [M-1 22 M M 21 26 M 24 M 24 23 21 M 24 M-1 30 M 23 M M M M M] [M-1 21 M 29 M M 28 M 27 28 30 29 M-1 29 30 M 29 22 M-1 M-1 M 27 M 30 27 M M 29 M M] [M M-1 M 25 23 28 M M M M 25 23 M M 26 M M 30 26 24 28 M M M-1 M 29 30 27 M M] [M 27 27 28 M-1 29 30 M-1 M M M-1 M-1 27 25 23 29 25 M-1 M M-1 M 29] [M 28 28 M M M-1 25 M M 28 M-1 M 30 M 28 29 25 M 30 30 M 29 28 M M M 29 28 M 27 M-1 29] [M 25 27 M M M 30 M 24 M M 28 M M 27 30 M M 29 M M 28 M M-1 29 M M 27] [28 M 27 M-1 29 30 M M M M 26 29 30 M M M 27 30 M 23 M M M M 29 23] [M M 30 M M M M 30 30 27 M M 27 M 30 M 30 M M 28 28 M M M M-1 M 29 28 29 M] [30 M M M M 30 26 M-1 30 M M 29 M 21 M M M-1 M M M M M-1 26 M M M M-1 30 26] [M M 27 M M M M 30 M M M 30 29 M M M M M 24 30 M M M M M M M 30 27 M M M] [M M M 28 M M M M 30 M M 28 M 28 29 29 M M M-1 M-1 26 M-1 M M M M 29 M M M 30 M] [M M M M M M 30 M M M M 30 27 M M M M M M M 24 M 30 M M] [M-1 M M-1 M-1 M M M M M M M 28 M M 30 30 M M-1 M M M 29 M 30 28 M-1 30 M M] [28 M M M M M M M 30 M M M M M M M M M M M-1 M M M M M M M M-1 M M M M-1] [M M M M M M M-1 M M M M M-1 M M M 28 M M M M M M 27 M M 29 M M] [M 29 M M 28 26 29 M M M M 30 M M-1 M 30 27 M M 29 M M M M M M 29 27 M-1 M M 29]] [[M M M 28 M M M M M M M 25 M-1 M-1 27 M M 29 M M M 30 30 M M-1 M 25 27 M M M-1] [30 M M 30 M 29 29 M M M M M-1 M M-1 M M M M M M M M M M-1 M M-1 M 29 29 M M 28] [M M 30 M M-1 M 27 M M M M M M M 30 M M-1 26 M M M M M 29 30 29 29 M M M M-1] [M 28 29 M M M M M M M-1 M M-1 M M M M M 28 M-1 M M M M M M] [M-1 29 M M 29 M 30 M 29 M M M M-1 30 M M 29 M M M-1 M] [30 M 30 M M M M M M M M M M M M M M M M M M M M M M M M M M M M M] [M M M M M M M 28 29 M M 30 M M M M M M-1 M M-1 M M M-1 M M 30 28 M 28 28 M 27] [M-1 M M M 25 M M-1 M 30 M M M-1 29 27 M 27 M M M M-1 M-1 M M-1 M M-1 29 M M M M 28 M] [M M M M M 25 M M-1 M 28 M-1 M M 29 M M M 29 M 30 M M M] [M 29 M M 27 M M M 29 M M M M M M M M-1 M M M-1 28 M M 27 M M M M-1 M 30 M-1] [M M M M M M M M 27 M M M M M M M M M M M M 29 M-1 M M M M M 30 M M M] [M M-1 M M M 30 M M M M 29 30 M M M M-1 M M M M M M 28 M M M-1 M M 30 M M M] [M-1 M M M M M M M M M M M M M M M M M M M M M M M M M M M] [M M M M M-1 M M M M M M M M M M M M M 30 M M M M-1 30 M M] [29 M-1 M 29 30 28 24 26 25 M 30 26 M M M-1 M-1 30 M-1 26 M M 27 M 26 M 27 M 26 M 29 23] [M 27 30 22 M 30 30 27 22 M-1 29 M M 24 M 29 M 30 M 27 M M M 26 27 26 M M 29 24 26 M] [M 30 28 M 29 30 28 28 29 M-1 30 25 29 M M M-1 M 21 26 29 M 30 M 30 M M 24 30 M-1 M M 26] [M M M M M M M M M M M M M M 25 M 26 M M 30 M M M M M M M] [M 29 M M 29 M-1 M M M M 23 M M M M M M M M M 29 M M M-1] [M-1 M M-1 30 M M M-1 M M M M M M M M M M M M M M M M M M M M M M] [M M M-1 M 29 M M M M M M M M M M M 29 30 M M M M M M M M M M M M-1 M M] [27 M 28 29 M M 26 28 M-1 M M 26 M-1 25 22 M M M-1 27 M 28 M 23 28 29 M-1 27 M 29 23] [M*21] [28 24 30 29 M M M 24 30 M 26 M 30 M-1 26 M-1 27 24 M 27 28 M 25 25 30 M M M M]] [[M 29 M M-1 27 M 28 27 27 22 M 28 M 24 M-1 M M-1 M M M M 28 M 27 M M M-1 28 M-1 M 29 M] [27 M M 22 M M 28 M M 20 M M 24 M M 24 M M 30 28 M 25 M M M 22 M M] [28 M M M M M-1 28 29 M M M-1 30 M M M-1 25 M 29 M M-1 M M 24 28 M M 30 M 28 22] [29 27 M 27 29 M M-1 25 M 30 M 29 M-1 M M M-1 M M M 29 25 M 26 M 24 M 27] [M M M 30 28 M M M M 28 29 28 30 28 M M-1 27 M-1 M M 30 M M-1 25 27 M] [M M M 25 26 M 27 M-1 M-1 M M 27 M 27 M M 29 M M M-1 24 30 27 M M-1] [M M M M 27 26 M 30 M M M 29 M-1 M M 30 30 28 M M M-1 28 M-1 29 M M M M M-1 M M M] [M M M M M M 28 M 28 M M 28 30 28 M 30 29 M 28 M 30 M M M M 29 M M 30 29 29 M] [M M M M 30 M M 29 M-1 30 M M M M M M 29 29 M M M-1 M M 29 M-1 M-1 M M M M M 27] [M M M-1 M 30 M M M M M M M-1 24 29 M M 29 M M 29 27 M M M M M M-1 M 29 M] [29 M M M M M-1 M M M-1 M M M M M M M M M M M M M 27 M M M 29 M M M 30 M] [M M M M 24 M M M M M M M M M-1 30 M M M M M M M M M M M 29 26 M] [M-1 M M M M M M M M 30 M M M M M M M 30 M M M M M M M M M M M M M M] [M M 27 M M M M M M M M M M M M M M M M M M 28 M-1 M M M M M M M M M] [27 M 30 28 M-1 M-1 M M M M 26 22 29 M-1 30 29 20 26 24 26 23 29 M M 26 M 29] [M*29] [27 25 27 29 M M M M M 24 M 30 29 27 23 M 28 30 28 29 29 M 30 M 26 M M M M-1 27 M 28] [25 22 29 M M 28 30 29 M M 28 28 M M M 28 30 24 M 28 M 25 24 M-1 30 27 29 28 M M M-1 M] [28 30 M 21 M-1 M 24 M M M 26 M 26 M-1 M 30 M M M 29 M 30 21 M-1 M 19 M M] [28 M 30 M M 27 28 27 M 27 26 22 M M M-1 29 28 29 M M M M M M 30 29 22 M M M M 23] [30 29 24 30 M 25 M 29 M 30 M M-1 27 M M 30 30 30 M M 25 M 28 M 23 M M 30 M M M M] [23 M M M M M M 28 26 27 28 30 30 29 M M 24 27 M 30 25 M 26 25 30 M M M-1 28 27 26] [30 29 27 27 23 M M-1 28 M 26 23 28 28 21 M 27 30 28 26 M 27 M 28 M-1 M M 29] [25 30 27 M M-1 M 29 22 26 M 28 M M 28 23 24 M M M 27 M M 24 28 M M 30 M M M] [30 30 25 28 30 29 30 26 23 28 29 M M M 28 30 30 29 28 30 30 30 M M M 28 M-1 M 30 25 28] [M 30 M 27 M 27 28 M M 27 M M 27 M M M 30 M M 20 27 28 M M M 25 30 29 25] [M 26 M 29 M M M 30 29 M-1 M 25 M-1 M 24 25 27 M 29 M-1 M M-1 28 25 M 30 M-1 29 27 M M 29] [30 30 30 M M M M M M-1 22 M M M M 29 M-1 M 28 M-1 30 M M M-1 28 27 M 29 27 26 M M 26] [28 M-1 28 M M M 29 M M-1 M 28 30 M 27 27 M M 30 30 M 26 26 28 M 28 30 M M M 29 27] [M-1 M M 25 M M 28 M M 27 M M M M M 22 M M M M M 23 29 M M 26 M 29] [30 29 M M M-1 25 M M M 26 29 M M M M 27 27 M 30 M-1 27 M-1 M M M M 28 M 28 M M 28]] [[M M M M M M 26 28 M M M-1 M M-1 M-1 25 M 27 M 29 25 M M 26 25 26 M M 29 30 26 M-1 M] [30 30 M-1 M M 28 27 28 30 29 30 M-1 M M M 28 26 M M M 28 M 28 M-1 25 M-1 28 28 29 30 29 30] [30 M M M M M M M M M M M M M M M M M M M M M M] [25 27 M-1 27 28 M M 28 M M 28 28 29 M 30 M 30 25 M 26 28 M M 22 M M 26 26 29] [26 M 25 M M 27 M 27 M M 30 30 29 M 28 29 28 28 M-1 24 24 M 28 M M M 30 29 23 27 M 29] [29 M 29 29 29 M 25 27 M 29 M 27 28 29 27 M-1 M 28 27 M 30 30 M 26 M-1 M-1 30 M M-1 7] [25 30 26 M M M M M 26 M 27 M-1 30 30 M M 30 27 28 M-1 M M-1 M 26 27 28 M M M 27 M 28] [M-1 M M M M M M M 29 M M M M 29 M 29 M 30 26 M M M M 24 24 26 M-1 M M 30 30 24] [26 M-1 M-1 M-1 M M 26 M M M M 29 25 M M 28 M 29 M 27 30 29 M 29 28 28 M 27 M M M] [M M M M 30 M 27 26 29 M 30 M M 30 M M M M 30 25 24 M M-1 M M M M M M 27 M] [M M 26 M 29 M 29 26 M M M M-1 M M 25 M 28 M M M M M M 24 M-1 30 26 M M M-1 M] [26 30 27 M M M 28 M 27 M 29 M-1 26 26 M M M 29 23 28 M 28 M 25 M M M 29 25 M 29 28] [M 29 28 28 26 M M-1 M M M 23 25 M 27 M M M M-1 29 24 24 24 M M M] [M 23 30 M 28 M-1 M 29 20 M M-1 M M M 28 M M 23 27 M 27 30 M 27 M M 25 29] [27 M M M M 29 30 M M 26 24 M M 28 26 28 M M M M M-1 M M M M 29 M M-1 27 M 23 M] [M-1 29 27 27 M M-1 M M M M-1 28 30 26 30 M 27 M 30 M 28 M M M-1 26 29 27 M M-1 27] [M 28 30 M-1 M M M M M 30 29 28 M-1 M 29 M 28 29 30 28 M M 29 M M 28 28 M M M-1 M-1 M-1] [M 27 26 M M-1 27 M M M-1 29 M-1 27 M M M 29 27 M 28 27 M M 29 M M M M M 27] [M M M M M-1 29 M M M M M 24 M M M M 30 M M 28 28 M M 27 M M 24 30 M M] [M 28 30 26 M M M-1 29 M M-1 30 30 M M 27 M M 26 28 M M M M M M M-1 30 30 M-1 M M M] [M M 28 M M M M M M M 28 29 M 29 M-1 M M M 28 26 M 30 M M M] [29 M-1 M 30 M M-1 M M 29 M M-1 M 28 M 29 M M M 29 M 30 M M 29 M M M 30 M M 28] [30 M M M 30 M M M M M 29 M M-1 M M M 29 M M M M M M M M 30 M 28 M M M] [M M-1 M M M M-1 M 27 M M M-1 M M M M-1 M M M M M-1 M M M M 29 M 26 M-1 M M M 30] [M M 30 M M M M 30 M M M M M M M M M M M M M M M M M M M 29 M M M M] [M M M 29 M-1 M M M M M M M M M M-1 M M M 28 30 M M M M M-1 M M M M M M M] [30 M M 30 26 27 30 30 26 28 M 25 27 25 M-1 M 24 M M-1 25 26 M 26 M M M 25 28 28 M-1] [M M M 30 M M M M M M M M M M M M M M M M M M M M M M M M M M M M] [25 M 30 29 26 30 M 30 29 M 26 29 25 24 M 30 M-1 27 M-1 29 M M M M M-1 27 M 26 M 30 25 M-1] [M 24 30 30 M M M M-1 24 30 26 M 27 M 27 M 27 26 30 M 24 27 29 M M M 29 M-1 27 27 26]] [[M M-1 M M M 27 M 30 29 M 25 M-1 M 25 M M 30 M M 27 M M 23 M M M] [M 27 M M M 30 M M-1 29 M M-1 30 29 M M 26 26 30 M 25 M M M M 26 29 M M M M] [M M 29 M 27 M-1 M M M-1 M-1 M M M M 28 M 29 M M-1 25 M M M M M M M M-1 30 M M 29] [M 30 M M-1 29 28 M M M 30 M 29 29 26 M 28 M M M 29 M-1 M M 28 M-1] [M M 29 28 M M 30 M M M 29 M M 28 M M M M M-1 M M M M M 29 30 M M M M M M] [M 30 M M 29 M M M M 28 30 28 M M 28 M M M M M M M 29 29 M M M M M-1 M M M-1] [M M M M M M M M M M 30 M M M M M M M-1 28 M M M M M-1 M M M M M 28 M] [28 M M M M M M M 30 M M M M M M M M M M M M M M 27 M M M M M 26] [M M-1 M M M M M M M M M M M M M M M M M M M M M M M M M M M M M-1 M] [M 30 M 28 M M M M M M M M M M M M M M M M M M M M M-1 M M M M M M M] [30 30 25 M 30 29 29 29 M-1 29 M 29 24 29 26 M-1 28 M M 29 28 27 28 28 M 28 28 M M 26 29] [24 M-1 M 26 M 27 M 30 M-1 25 26 M M 26 M 24 M 29 28 M-1 26 28 M M-1 M 28 M 28 27 M-1 25 29] [M 29 M M 24 M 29 24 M-1 M M 29 M M M 30 28 29 M M 28 25 M-1 29 M M-1 27 28 24 25 M 29] [30 27 M 27 M 30 M M 30 25 26 23 M 26 21 20 M M 28 M-1 M 30 26 29 M M M] [30 M M 29 28 26 M 28 30 30 27 M 26 27 M M M 25 M M M 24 26 M M 30 M M 30 M 28 M] [28 M-1 M M M M 28 30 25 M M M-1 26 27 M-1 30 30 30 27 M-1 M-1 M 29 24 29 M M-1 M-1 M 30 28 25] [M 28 M M-1 M M 29 25 28 29 M M M M 27 M 25 M 25 29 24 M-1 M M-1 M M 29 M-1 M M M M] [22 25 M M 29 M M M 21 30 30 M 29 M M 30 30 M 27 M M M 29 M-1 29 M M M 25 30] [M M-1 M-1 M M M M M 24 29 30 30 28 23 29 M M 30 M M-1 27 M 27 M M M 30] [M M 28 M 26 29 29 26 M M M M M M 28 M M 27 30 M-1 29 M M M M M-1 26 M 29 26 M] [M 29 M M 29 M M 30 M M M M-1 M M M 25 28 29 M 28 28 25 M M M M M] [M-1 M M M M 26 M M M M M M 30 M M 28 M M M 29 29 23 30 M 29 M 30 M M 25 M-1 M] [M M M M 28 29 M M M M M M 25 M M M-1 M 27 M M M 29 M M 27 M M M M M M]] [[23 M M M M M M M M M 28 M M 25 M M M-1 M M 24 M M 26 M M M-1 29 M] [30 M M M M M 29 M M 28 M M M M 24 M M M M M-1 M 30 28 30 M 29 27 M M M 26 28] [M M 26 M M M M M M M M M M M 29 M-1 M-1 29 M M M 26 M 29 30 M 28 M M M M] [27 M M M M M 30 M 30 28 M M M M 30 M M M M-1 29 29 27 26 M M-1 M M M M M M] [M M-1 M 30 M M-1 30 M M M M M-1 29 M M M 27 M M M M-1 M M M-1 M M M M 30 M] [30 M M M M-1 M 30 28 28 M M M M M 28 M M M 26 M M M M M M M M M M M-1 M M] [M M M M M M M M M M M M M M 27 M M M M M M M M 28 M M M M] [M M M M M 30 M-1 M M-1 M M M M M 28 28 30 M M M M M M M M M M M M M M M] [25 M 30 21 25 27 28 M-1 M 24 30 M 24 M-1 M-1 M 28 M 26 23 29 M 30 M M-1 26 M 30 27] [M M M M M 28 M 28 M M 30 M-1 M M M M M 30 M M-1 26 M M 30 M M M M-1 M 25 M 29] [M M M M M 27 M 26 M M 29 M M M M M M M M M 24 M 29 30 M 28 M M M M 28 29] [28 M M M M M M-1 M 28 M M M M M M M M M M M 27 M M 24 M M M M-1 M M M] [M M M M M M M 30 26 M M M 30 M 26 M M M M M M 27 29 M 28 M M] [M-1 28 M M M M M M M 30 M M 28 M M M M M-1 M M M M M M M M M M M M M M-1] [M M M M M M M M 28 M-1 M M M 26 M M M M M M 28 M-1 M 30 M M M M M M M M] [M M M M M M M M M M M M M M M M M M M M M M 29 M M M M M M M M] [M M M M 29 M M M M-1 M M M M M M M M M M M M M M-1 M 29 M M] [M M 24 26 30 M-1 M M 27 M M M 23 M 30 M 26 26 21 30 M M-1 M 28 26 24 24 30 30 M M] [M 30 26 22 30 29 29 M 28 29 M-1 26 28 M M 29 27 30 28 22 26 M 30 M-1 M 25 26 M 28] [27 M M 30 M-1 29 M-1 M M M 28 24 23 23 27 M M M 30 28 M 28 29 M-1 29 M-1 23 M 27] [M M-1 M 24 23 27 M M M-1 M M 29 26 29 M M 25 M M-1 26 25 M M M 25 29 24 23 M M] [27 M 28 28 29 30 M-1 M 29 M-1 30 29 M M M M-1 M-1 M-1 28 28 26 29 29 M M M M-1 27 28 M 24] [M 24 28 M 28 28 28 M M-1 M M 27 M-1 29 27 M 28 30 24 26 M 29 M 26 M-1 M 29 M M M M 30] [30 26 M 30 30 M M M M-1 M-1 20 M 30 M M M 26 M 29 M-1 27 M-1 28 30 M-1 M M M 29 M 27] [M 30 M M M 30 30 26 M M 28 29 28 30 M M 29 22 27 M M 29 26 M M-1 M M M-1 30 24] [M M M M M M 30 28 28 M M 27 30 M-1 M-1 M-1 30 M 29 M M M M 29 M 29 M 29 M 28 21 M] [30 M M M M 29 25 28 M 30 M M 28 M-1 M M M-1 24 27 M M M M-1 M M M-1 M 26 30 M M 25] [M M 26 24 M-1 M M M M 23 30 M 28 M M M 28 M 30 M M M M M M-1 M-1 M-1 M M]] [[M M M M-1 29 M M M M M-1 M 27 30 M-1 29 M M M M M M 28 M 30 M 26 29 M 30 29 29 28] [30 28 M M M M M-1 M-1 21 27 M M M M-1 26 M 29 M M M 28 22 M M M M M M M] [M M M 26 M 30 M M 30 M 29 30 M 27 M 30 M M M M M M 29 M M M 30 M M M 26 M] [M M M-1 M M M-1 M-1 M M M-1 M M-1 M-1 M 25 M M 24 M M M 30 M 25 30 M M-1 M 27 M M M] [M-1 M M M 26 M M-1 M M M M M M-1 30 M M M M M 30 M M-1 M M M M M 30 M M M-1 26] [M-1 M M M M M M-1 M M M 26 M M M-1 29 M-1 M M-1 M M M M M-1 29 30 M 30 30 M M M-1 29] [M M M M M M M M M M M M M M-1 M 28 M M M M M M 30 28 M M M M M M 30 M] [M M M-1 M 29 M 28 M M M M M M M 29 M M M M M M M 30 M 30 30 M-1 M M M M] [M M M-1 M M M M M M M M M-1 M M M M M M M M M M-1 M M M M M M] [M M M M M M M-1 M 29 M M M M M M M M M M M M M M M M M-1 M-1 30 M M M M] [M 29 M 24 M-1 30 26 M-1 29 29 M 29 26 27 M M-1 M M 28 27 26 M M-1 29 29 28 25 29 29 M 26 30] [M M-1 27 23 27 M 26 M-1 M 27 M 27 27 30 25 M 28 M M 26 27 24 M M 29 M 26 M M-1 28 M 26] [30 M M M M M M M-1 M M M M M-1 M 28 M M-1 M M M 30 M-1 26 M M M M 28 M 30 M M-1] [M M M M M M M M M M M M M M M-1 28 M M 30 M M M 29 M M M M M M M M M] [M M M M M-1 M M M 27 M M-1 28 M M M M M 30 M-1 M-1 M M M M M M M] [M*27] [28 M M M M M M M M M M M M M M M M M-1 M M M M M M M M M M M M M M] [M 28 M 29 24 30 27 29 30 M 23 M-1 28 M-1 25 M 30 M-1 27 29 30 28 M 28 29 M-1 25 27 M] [25 25 M M 28 30 24 M 29 M M 27 M M M 27 27 M 29 25 28 24 M 26 M M-1 30 M-1 26 27 M 30] [30 M 25 26 M-1 M-1 M M 28 M 30 30 29 M 25 27 29 M-1 M M-1 M M M-1 27 24 M 29 M-1 30 26 M 27] [28 M 29 M-1 24 27 29 M-1 28 M 28 26 30 29 M 30 30 M 28 M 25 29 M 28 29 26 27 M M M M-1 M] [M M 27 M-1 29 M M M 29 23 22 M M M-1 M-1 M 29 26 27 M M M M M M-1 28 28 29 25 M] [27 26 26 M-1 26 24 M-1 30 M 24 M-1 M 29 25 28 M-1 M-1 M M 29 30 28 M M 30 M M] [25 M 30 M M 29 M 28 M 30 M M M 29 M M M-1 28 M M 27 28 29 M 26 29 M M 29 30 30 30] [24 M 30 28 M 27 29 29 27 27 28 M M 28 M 21 27 M M M 26 M M 29 26 26 M-1 M 25 M 30 M] [M 26 27 30 28 28 28 M 28 30 26 25 30 M M-1 M-1 27 25 M-1 M M-1 27 M-1 27 28 26 27 28 30 M M M-1] [29 30 29 27 M M M M M 25 M M-1 M 25 M 27 M 27 30 29 26 M M-1 30 29 25 30 29 29 30 29 27] [23 29 M M 30 M-1 M-1 26 M 24 30 29 M M M 29 M 24 M 27 M 28 28 M 28 25 24 M M M-1 30 28] [M 29 29 28 M-1 M M M M 24 27 M 30 29 M M-1 M M-1 26 30 26 M-1 M 28 M 25 30 M 30 M 28 27]] [[M 27 M M-1 26 M M M 26 M 29 30 27 M-1 28 27 M 29 M 30 M-1 M 22 23 23 M 28 30 25 28 M] [M 30 M 29 M 30 M 27 29 20 29 M 29 29 30 25 28 29 M M M-1 26 30 M 30 30 M M-1 M M-1 26 M-1] [M 27 29 27 M M 26 27 M M M 27 M 25 M 29 28 26 26 M M-1 M-1 29 26 M M 26 M 29 M 30 30] [M M M M 29 27 M M M-1 M M 30 26 28 M M-1 M-1 M M 26 30 26 M M M 28 M 23 23 27] [30 M M M M-1 M 30 23 M 27 29 M 30 27 M M M M-1 M 25 23 M 27 M M M 19 29 30] [M 30 M M-1 30 M-1 M M M M 28 M 29 M-1 28 30 M-1 M 28 M 25 M-1 27 M M 29 M-1 28 M-1 M-1 30 28] [M 29 M M 28 29 26 M M M-1 29 29 M 28 M 28 M-1 M 28 M-1 M M M 27 M M 30 26 27 M M 28] [M M M-1 M 29 26 M 30 M M M M M M 28 30 M M 30 28 M 30 M M 27 M M M 29 25 29 M] [M M 30 M 29 M 25 M-1 30 M-1 M M M M 24 M 30 M M 30 28 M-1 27 M M 30 M-1 M 24 M] [M M M M 26 27 28 M M M M M M M M M 26 26 30 29 M-1 M M M M M M M-1 M M M] [M M-1 M M M M M-1 M M 27 27 28 M M M-1 28 M M M M M M 24 25 M M M M 30 M M M-1] [M M M M M M M 27 M M M M 25 M M M M 28 M-1 30 M M M M 28 M M M M M M-1 M] [29 M 30 29 M 28 M M M M M M 27 M M M M M M-1 M M M M M 21 M M M M M M] [M M M M M M M M 25 M 30 30 M 30 M M M-1 M M M M M M M M M M] [M-1 M M M 26 M M 29 M M M M-1 M 30 M M M M M M M M M M M M 26 M M] [M M M M M M M M 30 M M M 30 M M M M M M M M M M M M M M M M M M] [M M M 30 M M M M M M-1 M M M M M M M M M M 28 30 M M M M M M M M M M] [30 28 28 M 30 M-1 30 21 24 30 M M-1 28 M M-1 M 27 M M M 30 25 M-1 30 M-1 M-1 27 28 26 28 30 24] [M 26 M M 26 19 M 26 22 M M 29 M M-1 24 M 30 22 30 M 24 M M 25 21 M 30 M] [24 M M 22 M-1 M M 28 M-1 30 28 M 30 M M 25 29 27 27 M 30 28 27 M 27 M-1 M 28 M M-1 28 30] [M M 26 M-1 M M 30 29 M-1 M M 27 27 25 M 25 26 30 24 M M M M-1 27 M-1 27 27 M 26 26 29 M] [M M 28 30 M 30 29 M 30 30 M M M M 27 M 25 M 29 28 23 M M-1 M 27 M 26 28 29 29 M 27] [M 27 M M 26 28 M 30 30 M 28 M 28 29 21 M M-1 30 M-1 M 25 M 26 29 26 M 26 30] [28 M 26 M M M 30 30 25 29 29 M M 28 28 24 26 M M M 30 24 M M-1 M M]] [[27 25 29 M 28 28 29 29 26 29 M-1 M M-1 26 23 28 27 27 M M 29 30 M M-1 24 30 29 M-1 M-1 M M] [25 27 29 M M-1 29 28 28 M 26 M 25 23 30 28 M M M M-1 M M M-1 M-1 M-1 M 29 29 M M M 28 26] [29 29 M 29 29 26 M-1 22 24 30 27 M-1 30 26 M-1 23 30 M 27 M M 28 M M M M M 29 30 M-1] [30 M M 27 M M M M 26 M-1 28 M 28 M 30 27 M 26 29 M 25 M 25 M M M-1 M 23 M M 29 M] [26 26 M M M 29 M 26 26 M M M-1 26 27 M-1 27 M 29 M 30 26 M 29 28 M 30 M-1 30 M 30] [30 M-1 M M 30 M M M 28 M M-1 M 28 M 29 26 29 M M M M M 26 26 21 M M 26 M M M] [28 M 25 M 29 M 29 26 M M M 30 25 M 30 30 M 29 M 27 M M 30 28 M M M 26 M 28 M M] [M M M M M 26 M-1 M M M M-1 M M M 28 29 29 M M M M 21 30 29 M M-1 M 28 M 27 M-1 M] [M 29 M-1 M M M 25 30 29 M M-1 29 M M-1 28 M 26 M-1 28 27 M 25 28 27 30 M M M-1 M-1 25 29 M-1] [28 M-1 28 M 23 28 M 26 M-1 26 27 26 28 24 M M M M M M M M M M 26 29 29 M 25 M] [30 M-1 26 M M 25 M-1 M M M M 27 29 M 29 28 M M-1 M M M M-1 29 28 30 27 27 28 M M-1 M 28] [M M-1 28 28 M M 29 M 30 M 29 M M M M M 25 M-1 26 28 M-1 24 26 M M 27 M M 23 29 M] [25 M 30 M M 26 M 28 28 M M M M-1 M M M 27 M 27 M M M 28 24 27 M M M-1 M M] [M M 23 M M M M M-1 29 M M M 29 M 28 M-1 M-1 M 26 24 27 M 27 M M M M M M 28 25 M] [M M 29 M M 30 M-1 M M M M M 27 29 M M M 28 M 29 27 27 M M 29 M M M 24 M] [M M 28 29 26 M M 26 27 M M 30 M 26 M 30 M M M M M M M-1 M-1 M M M-1 M 28 29 30 M-1] [M M M M 29 M 30 M M M M 29 28 M 30 M-1 30 M M M M 25 M M 28 M M M M M-1 M 29] [30 M-1 M 26 30 27 M M M M-1 29 M M M M M M M 30 M-1 28 M M-1 M M M-1 M-1 27 30 M M] [M-1 M-1 M M M M 29 M M M M M M M M 23 M 27 M-1 M M M M M-1 M M M M M-1 M M M] [M M M 27 M M 29 30 27 M M M 27 M 28 M-1 M M 28 M M M M 26 26 M 30 26 27 M 30] [25 M 20 29 M M M-1 M 22 28 28 M 29 M M 30 M 30 29 M M-1 30 M 30 28 M-1 M M M] [M-1 29 M M M 26 M M M M M-1 30 28 29 M 27 27 M M M M M 26 25 M M M M 29 M M M] [M M-1 M M-1 M M 28 M-1 26 M M M 30 30 M 29 26 M 30 M 29 M M 28 M 29 M-1 M 29 M-1 25 M-1] [M 27 M M M-1 30 27 M M M M 29 M M M M M M M M 28 M-1 M M M-1 29 30 M 29 M 29 29] [M 30 M-1 M M M-1 30 M 29 M M M M M-1 30 M M 26 M 29 M M 29 M 27 M-1 29 M 28 M-1 M 29] [29 M M 30 M M-1 30 M 30 M M M M M 30 30 M M M M M M 27 M M-1 30 M M M M 28 M] [M M M M M M M M 25 M-1 M M M 28 29 M-1 30 30 M M-1 30 27 M M 29 M M] [29 30 M M M M M M 30 30 M M M M M M M M M 29 M M M M M 30 M-1 M-1 M M M M]] [[M M M-1 M M M M M M M M 30 30 M M M M M M-1 M M 28 30 M M M 26 M M-1 M M-1 M] [29 M M M M M 25 M M M 30 M M M M M M M 28 28 M 30 M M M M M M M M 29] [M M M M M M 30 M M M M M 29 M M M M 29 M M M M M M M M M M 30 M M M] [M M M M M M 30 M M-1 M M M M 29 M M M M 30 M M M M M M 30 30 M M M 29 M] [M*M] [M M M M M M M M M 27 M M M M M M M M M M M M M M M M M M M M M M] [27 30 M 30 M 26 30 28 M 26 25 M 30 M 25 30 M 29 25 29 M 30 M 28 30 27 M-1 28 28 M-1 27 M] [M-1 29 M 27 29 30 25 29 M M-1 M 29 M 27 M 29 27 26 25 M-1 28 29 29 28 27 30 26 30 27 M-1 M M-1] [26 M-1 M-1 29 28 25 M-1 M M 27 M 28 M M 28 28 M M M 26 M-1 22 M 28 M 27 30 M M 27 M 29] [26 30 29 25 29 25 29 27 M M M-1 M 30 19 M-1 27 M M M M M M-1 24 M M M M 21 M 30 M-1 M] [M M-1 M 24 M-1 30 28 M M-1 M M-1 28 M 27 28 20 M M M 30 29 27 M-1 30 M 30 M M M 30 26 M] [M M M M 30 M 28 30 M 25 M-1 29 M 27 M M 29 M 29 28 29 26 M-1 28 30 M 28 M-1 24 M-1 25 M] [M M 28 M-1 M M M-1 27 28 M M M M M M 30 25 24 28 M M 28 M 28 27 M 30 M M 30 M-1 30] [M-1 M M M 29 M M 24 M M 29 M M 24 M-1 M-1 M M 22 30 26 30 30 M 29 30 30 28 29] [28 M 27 29 M M 29 M 26 M M 26 M 26 M M M M M-1 M M M M M-1 M M M M 30 M 23 M] [26 29 M M 27 M M M M-1 30 30 27 28 M-1 M M M M M 30 30 M 23 30 M M 28 M 29 M M M] [27 M 30 M-1 M M M M M M M M 28 M M M M-1 30 24 M 27 M M M 30 M M 29 M 29 M M] [29 30 M M 30 30 M M M-1 M 30 M 29 M-1 M 29 M-1 M 29 30 M-1 M 30 M M 28 M 29 M M 29 29] [30 M M 30 30 M M M 30 M M M M 27 M M-1 M-1 M M M M M-1 M M M M M 27 M M 28 M-1] [M-1 M M M 28 M M M 25 28 M M M M 23 M M M M M 27 M-1 M M M M M M M M M M] [29 M-1 M M M 30 M 30 M M M M M M M M-1 30 M 28 M M M M M-1 M M M M M M M M] [M M M M-1 M M M M 29 M M M M M-1 M M M M-1 M M 26 M M 29 M-1 M-1 M M M M 28 M] [M-1 M M M M M M 28 M M M M M M M M M M M M M M M M M M M M M M-1 M M] [M M M 30 29 28 M M M M M-1 29 30 M M M M M 29 M M 29 M M M M 24 M 28 M] [27 M M M M-1 30 M 27 29 M M-1 M M M M 26 M 29 30 M M-1 M 29 M M 29 M M M 29 M M] [M M M M-1 M M M M M M M M 30 M M 29 M 28 M M 25 M M M 30 M 30 M 30 M M M] [M-1 M M-1 M 28 M 28 M-1 30 M M M M M M M 27 M M M M-1 M M 29 M M M M-1 M 29 30 M] [M M M M M 30 30 M M M M M M M M M M M-1 M M M M M-1 M M M M M 28 M 29 M] [28 30 M M 29 M-1 M M 29 M M M M M M M 29 M M M 30 M M M M M M M M M M M] [M M M M M M M M M M M M M 30 M-1 M M M M M M M M M M M M M M M M M] [M M M M M M M M 26 M M M M M M M M M M M M M M 30 M M M M M M] [30 27 25 25 M M-1 30 27 M 26 29 M 27 M 29 29 M 27 M M 28 26 22 M M M-1 M 29 M 27 26 M]] T21"
        doJoinTest v1 v2

    testCase "big join, test 5" <| fun _ ->
        let v1 = RRBVectorGen.treeReprStrToVec "[M*M]*M T22"
        let v2 = RRBVectorGen.treeReprStrToVec "[M*M]*29 T8"
        doJoinTest v1 v2

    testCase "joining vectors may end up with a full node as root" <| fun _ ->
        let v1 = RRBVectorGen.treeReprStrToVec "M TM"
        let v2 = RRBVectorGen.treeReprStrToVec "[M*30] [M*M] T3"
        doJoinTest v1 v2

    // split + remove idx 0 of left + join = remove idx 0 of entire passed in 00:02:39.1100000
    // Now split + remove idx 0 of left + join = remove idx 0 of entire passed in 00:08:46.5880000 -- why the slowdown?
    // Skipped because it takes too long
    ptestProp "split + remove idx 0 of left + join = remove idx 0 of entire" <| fun (vec : RRBVector<int>) (i: int) ->
        if vec.Length > 0 then
            let i = (abs i) % (RRBVector.length vec)
            let vL, vR = doSplitTest vec i
            let vL', vR' =
                if vL.Length > 0 then
                    RRBVector.remove 0 vL, vR
                else
                    // Can't remove from an empty vector -- but in this case, we know the right vector is non-empty
                    vL, RRBVector.remove 0 vR
            let joined = RRBVector.append vL' vR'
            RRBVectorProps.checkProperties joined "Joined vector"
            Expect.vecEqual joined (RRBVector.remove 0 vec) "Split + remove idx 0 of left + joined vectors did not equal original vector with its idx 0 removed"

    // split + pop right + join = pop entire passed in 00:03:57.7930000
    // split + pop right + join = pop entire passed in 00:08:44.7000000 -- too long, skipping
    ptestProp "split + pop right + join = pop entire" <| fun (vec : RRBVector<int>) (i: int) ->
        if vec.Length > 0 then
            let i = (abs i) % (RRBVector.length vec + 1)
            let vL, vR = doSplitTest vec i
            let vL', vR' =
                if vR.Length > 0 then
                    vL, RRBVector.pop vR
                else
                    // Can't pop from an empty vector -- but in this case, we know the left vector is non-empty
                    RRBVector.pop vL, vR
            let joined = RRBVector.append vL' vR'
            RRBVectorProps.checkProperties joined "Joined vector"
            Expect.vecEqual joined (RRBVector.pop vec) "Split + pop right + joined vectors did not equal popped original vector"

    // yet another join that could break the "last leaf is full if parent is full" invariant passed in 00:00:36.6560000
    testCase "yet another join that could break the \"last leaf is full if parent is full\" invariant" <| fun _ ->
        let vL = RRBVectorGen.treeReprStrToVec "[M*M]*M-2 [M*M-1] TM-1"
        let vR = RRBVectorGen.treeReprStrToVec "T1"
        doJoinTest vL vR

    // starting with 2 vectors, pop right + join = pop entire passed in 00:04:16.8760000
    // Skipped because it takes too long
    ptestProp (*726063586, 296602124*) "starting with 2 vectors, pop right + join = pop entire" <| fun (vL : RRBVector<int>) (vR : RRBVector<int>) ->
        if vR.Length > 0 then
            let vR' = RRBVector.pop vR
            let joinedOrig = RRBVector.append vL vR
            RRBVectorProps.checkProperties joinedOrig "Joined orig vector"
            let joinedOrigThenPop = RRBVector.pop joinedOrig
            RRBVectorProps.checkProperties joinedOrigThenPop "Joined orig vector, then popped"
            let joined = RRBVector.append vL vR'
            RRBVectorProps.checkProperties joined "Joined vector"
            Expect.vecEqual joined (RRBVector.pop joinedOrig) "pop right + join did not equal join + pop"

    // a few joins that could break the "leaf nodes must only be at 0 shift" invariant passed in 00:00:58.0750000
    testCase "a few joins that could break the \"leaf nodes must only be at 0 shift\" invariant" <| fun _ ->
        let vL = RRBVectorGen.treeReprStrToVec "[M*M]*M/2-1 T3"
        let vR = RRBVectorGen.treeReprStrToVec "T3"     // tail-only, height=0
        doJoinTest vL vR
        let vR = RRBVectorGen.treeReprStrToVec "M TM"   // root+tail, height=0
        doJoinTest vL vR
        let vR = RRBVectorGen.treeReprStrToVec "M M TM" // height=1
        doJoinTest vL vR
// [01:13:29 DBG] Nodes and vectors/All tests/Long-running tests, skipped by default/split + remove idx 0 of left + join = remove idx 0 of entire passed in 00:08:46.5880000. <Expecto>

    // starting with 2 vectors, remove idx 0 of left + join = remove idx 0 of entire passed in 00:03:26.0550000
    // Now it takes close to half an hour. Wonder why? At any rate, it takes too long
    // testProp "starting with 2 vectors, remove idx 0 of left + join = remove idx 0 of entire" <| fun (vL : RRBVector<int>) (vR : RRBVector<int>) ->
    //     if vL.Length > 0 then
    //         let vL' = RRBVector.remove 0 vL
    //         let joinedOrig = RRBVector.append vL vR
    //         let joined = RRBVector.append vL' vR
    //         RRBVectorProps.checkProperties joined "Joined vector"
    //         Expect.vecEqual joined (RRBVector.remove 0 joinedOrig) "remove idx 0 of left + join did not equal join + remove idx 0"

    // insert item at idx = split at idx + push item onto end of left + join passed in 00:02:03.4830000
    // insert item at idx = split at idx + push item onto end of left + join passed in 00:13:13.6790000 -- too long, skipping
    ptestProp "insert item at idx = split at idx + push item onto end of left + join" <| fun (vec : RRBVector<int>) (i: int) ->
// Big vector represented by [M M M M 30 M M M 29 M M M M M M M-1 M M 27 M M-1 M 30 M-1 29 M M M M M 30 M] [M 28 M M M-1 M-1 M M 29 M M M M 29 M M 28 M M M M M M 30 M M M M] [M 29 M 29 29 M-1 27 25 M M-1 M-1 26 M M M-1 M M-1 30 M M M M 30 26 25 M 30 22] [28 M 29 M M M 29 M 30 28 26 30 24 M 28 M M M-1 M M M 23] [M-1 M 26 25 M-1 M 29 30 M-1 M 28 M 30 M M M-1 M M-1 30 28 28 27 M M M-1 29 29] [M M 27 M M M M 30 M M M M M 30 M M 30 27 M M M M M 28 26 27 28 M M M-1 M] [28 29 M M-1 26 M-1 27 25 30 23 M 29 M M M M M M M M M M M M M M M] [M M 28 M M M 30 M M M M M M-1 M M 30 24 M M M M 28 M-1 M M M M 29 M-1 M M] [M M 30 M 30 27 29 M M-1 30 27 28 M M 29 M 29 M M-1 M M M M M] [M M M M M-1 30 M M M M M M M 28 30 M-1 M M 30 28 M M] [M M M M M M M M M M M M M-1 M M M M 23 30 25 M M M M M M M 30 M] [M M M M M M M M M M M M 30 M M M-1 M M M M M-1 M M M M M M 30 M M] [M M M M M M M M 30 28 28 M M M M M M M M M M M M M M M M M-1 M M M M] T31 <Expecto>
// TODO: Make that a separate unit test
        let i = (abs i) % (RRBVector.length vec + 1)
        let vec' = vec |> RRBVector.insert i 512
        RRBVectorProps.checkProperties vec' (sprintf "Vector after inserting 512 at idx %d" i)
        let vL, vR = doSplitTest vec i
        let vL' = vL |> RRBVector.push 512
        let joined = RRBVector.append vL' vR
        RRBVectorProps.checkProperties joined "Joined vector"
        Expect.vecEqual joined vec' "Split + push left + joined vectors did not equal insertion into original vector"

  ]

open RRBVectorMoreCommands.ParameterizedVecCommands
let isolatedTest =
  testList "Isolated test" [
    // Passed: (1380433896, 296477427), (788968584, 296477381)
    // testProp "More command tests from empty" (Command.toProperty (RRBVectorMoreCommands.specFromData RRBVector.empty))
    // Failed case: [push 38; push 38; pop 58; push 66; mergeL "0 T19"; push 47; pop 54; pop 66; mergeL "0 T24"; push 8; pop 61]
    // Sizes: [38; 76; 18; 84; 103 (left node 19); 150 (left node still 19?); 96 (left node still 19?); 30 (left node still 19?); 54 (is it 24-19-11? or less?); 62; 1]
    // Also: ftestPropertyWithConfig (788968584, 296477381) "More command tests from empty"
    // Passed: (498335399, 296478517), (2044959467, 296477380)
    // testProp "Try command tests from data" <| fun (vec : RRBVector<int>) -> logger.info (eventX "Starting test with {vec}" >> setField "vec" (RRBVectorGen.vecToTreeReprStr vec)); (Command.toProperty (RRBVectorMoreCommands.specFromData vec))
    // Failed case: Initial: "M T9", Actions: [pop 37; push 47; mergeR "0 T15"; mergeR "0 T11"; mergeL "0 T9"; mergeL "0 T10"; pop 48; pop 47]
    // Also: ftestPropertyWithConfig (2044959467, 296477380) "Try command tests from data"
    // Also  (498335399, 296478517) which is "[M*M]*M TM-3" with commands [push 38; rev]

    testCase "Manual test from empty" <| fun _ ->
        let mergeL = mergeL << RRBVectorGen.treeReprStrToVec
        let mergeR = mergeR << RRBVectorGen.treeReprStrToVec
        let actions = [
            mergeR "0 T14"; push 155; mergeR "26 30 M 28 28 23 29 30 30 26 M T24";
            mergeL "M T18"; map id ]
            // pop 72
        // TODO: This is a failure in IterLeaves() when there's an ExpandedNode in there that could have nulls in its leaf arrays. Need to fix that.
        let vec = { 0..500 } |> RRBVector.ofSeq
        let mutable current = vec
        let logVec action = ignore
        // let logVec action vec = logger.info (eventX "After {action}, vec was {vec}" >> setField "action" (action.ToString()) >> setField "vec" (RRBVectorGen.vecToTreeReprStr vec))
        for action in actions do
            current <- current |> action.RunActual
            logVec action current
            RRBVectorProps.checkProperties current <| sprintf "Vector after %s" (action.ToString())

    // TODO: I think this is equivalent to one of the tests in the regressionTests list
    testCase "Manual test of a NullReference bug during remove operation" <| fun _ ->
        let scanf (x : int) a = a + 1  // So that scans will produce increasing sequences
        // let mergeR = mergeR << RRBVectorGen.treeReprStrToVec
        // let actions = [mergeR "M T13"; push 166; scan scanf 8; pop 157; remove 51]
        // TODO: Check if starting with the `scan` results, then doing 157 pops, is enough here
        let vec = { 0..848 } |> RRBVector.ofSeq
        let mutable current = vec
        let logVec action = ignore
        // let logVec action vec = printfn "After %O, vec was %s" action (RRBVectorGen.vecToTreeReprStr vec)
        // for action in actions do
        //     current <- current |> action.RunActual
        //     logVec action current
        //     RRBVectorProps.checkProperties current <| sprintf "Vector after %s" (action.ToString())

        // Crash is:
        // System.NullReferenceException: Object reference not set to an instance of an object
        //   at Ficus.RRBVectorNodes+NodeCreation.treeSize[T] (System.Int32 shift, System.Object node) [0x00004] in <5b56e30f3ec9364ca74503830fe3565b>:0
        //   at Ficus.RRBVectorNodes+NodeCreation.createSizeTable[T] (System.Int32 shift, System.Object[] array) [0x0000f] in <5b56e30f3ec9364ca74503830fe3565b>:0
        //   at Ficus.RRBVector+RRBTree`1[T].Ficus-RRBVector-IRRBInternal`1-RemoveImpl (System.Boolean shouldCheckForRebalancing, System.Int32 idx) [0x000e0] in <5b56e30f3ec9364ca74503830fe3565b>:0
        //   at Ficus.RRBVector+RRBTree`1[T].Remove (System.Int32 idx) [0x00000] in <5b56e30f3ec9364ca74503830fe3565b>:0
        //   at Ficus.Tests.RRBVectorExpectoTest+isolatedTest@1537-2.Invoke (Microsoft.FSharp.Core.Unit _arg2) [0x000cc] in <5b56e315492c3c0ba745038315e3565b>:0
        //   at Expecto.Impl+execTestAsync@878-1.Invoke (Microsoft.FSharp.Core.Unit unitVar) [0x00027] in <5a9db3dddf69a9a4a7450383ddb39d5a>:0
        //   at Microsoft.FSharp.Control.AsyncBuilderImpl+callA@522[b,a].Invoke (Microsoft.FSharp.Control.AsyncParams`1[T] args) [0x00051] in <5a7d678a904cf4daa74503838a677d5a>:0

        current <- RRBVector.append current (RRBVectorGen.treeReprStrToVec "M T13")
        logVec "mergeR \"M T13\"" current
        RRBVectorProps.checkProperties current "Vector after mergeR \"M T13\""
        for i = 1 to 166 do
            current <- current.Push i
        logVec "push 166" current
        RRBVectorProps.checkProperties current "Vector after push 166"
        current <- current |> RRBVector.scan scanf 8
        logVec "scan scanf 8" current
        RRBVectorProps.checkProperties current "Vector after scan scanf 8"
        for i = 1 to 157 do
            current <- current.Pop()
        logVec "pop 157" current
        RRBVectorProps.checkProperties current "Vector after pop 157"
        current <- current |> RRBVector.remove 51
        logVec "remove 51" current
        RRBVectorProps.checkProperties current "Vector after remove 51"


    testCase "Manual test" <| fun _ ->
        let vec = RRBVectorGen.treeReprStrToVec "M T9"
        // logger.info (eventX "Starting with {vec}" >> setField "vec" (RRBVectorGen.vecToTreeReprStr vec))
        let mutable current = vec
        let actions = [pop 37; push 47; mergeR <| RRBVectorGen.treeReprStrToVec "0 T15"; mergeR <| RRBVectorGen.treeReprStrToVec "0 T11"; mergeL <| RRBVectorGen.treeReprStrToVec "0 T9"; mergeL <| RRBVectorGen.treeReprStrToVec "0 T10"; pop 48; pop 47]
        let logVec action = ignore
        // let logVec action vec = logger.info (eventX "After {action}, vec was {vec}" >> setField "action" (action.ToString()) >> setField "vec" (RRBVectorGen.vecToTreeReprStr vec))
        for action in actions do
            current <- current |> action.RunActual
            logVec action current
            RRBVectorProps.checkProperties current <| sprintf "Vector after %s" (action.ToString())

    testCase "Large manual test" <| fun _ ->
        let bigReprStr = """
[M 27 M 29 28 M M-1 22 25 M-1 M-1 M 27 M 25 26 28 M 26 M M 28 M-1 30 M 25 M-1 M 25 M-1 M-1 24]
[M-1 28 29 M M-1 29 M 26 30 26 M M 27 M M 27 29 29 28 M 28 M 29 28 26 M-1 30 28 28 M-1 M-1 M-1]
[30 28 26 M-1 M-1 M 29 M-1 27 M M M 30 M M 26 29 26 29 M M M M 26 29 26 29 29 26 29 29 26]
[M 27 24 M-1 M-1 M-1 28 25 30 28 29 M 28 M M-1 M-1 25 30 M M 28 M 27 M M M M M-1 M-1 28 M M-1]
[26 27 29 M-1 30 26 M-1 30 M 29 25 25 27 30 28 24 M-1 26 26 30 28 M-1 30 M-1 M-1 M M M 30 28 30 M]
[29 M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M]
[30 M-1 M 29 29 29 30 M M-1 M 30 M-1 M 23 M 28 26 M-1 29 29 M-1 27 M 29 M-1 29 26 25 28 M 26 28]
[27 30 30 25 27 29 M-1 M M 29 29 29 M 30 29 29 27 M M 26 M 28 M 29 27 25 28 30 M 29 M-1 25]
[27 29 29 24 M 30 28 27 27 28 30 30 29 M-1 30 30 M-1 M-1 M 28 M M M-1 30 27 30 29 M M-1 M 29 M]
[28 M 28 30 M M-1 27 M 27 25 29 30 M M 28 27 26 M 28 26 M 28 M-1 30 M 28 M M M-1 27 28 M]
[M M M-1 M M 29 30 M-1 30 26 M M M M 28 M 27 30 M 25 M M-1 M-1 27 24 M 28 30 M 25 29 M]
[25 30 26 M M-1 26 M M 29 27 M M 30 28 30 M M 27 M 29 M M 29 M 27 29 29 M 30 27 M 30]
[M M 28 30 M M M 26 M-1 M M-1 M 28 M-1 M-1 M M 25 29 M-1 M-1 M 28 M-1 29 27 M M M 28 27 M]
[M 29 M 30 M-1 M 27 M 30 M 29 27 28 M M-1 29 28 M-1 M-1 M M-1 M 27 25 M M-1 M 30 30 M 27 M-1]
[M 29 M M M-1 M 28 M M 30 M 27 30 M M M 29 M 28 30 29 M 28 M M M M 27 27 M M M-1]
[27 M M-1 M M-1 M 27 M M M M 30 M M M 27 M-1 29 M 29 M M 26 27 29 M 30 M 29 M 28 M]
[M 29 M M M M-1 30 M M M 27 M M M 30 M M-1 M 29 M M M 30 M M M-1 26 M 27 M M-1 30]
[M M 28 M 22 M-1 M M M 23 M M-1 M M M 30 M M M M M-1 M 30 M 29 M 29 M M M-1 30 M]
[M M 30 M 29 29 M M M 30 M M 29 M M 28 M M M 30 M-1 M 30 M M M M-1 M M M M M-1]
[30 M 27 M 30 M M M M M M-1 M M-1 30 M M-1 M M M 29 28 M-1 M M 30 M M 30 M 29 M M]
[M M M M M M M-1 M 29 M M-1 M M M M M-1 M M M-1 M-1 M M M M M 28 M 30 M M M M]
[M 29 M-1 M-1 M M M 29 M M M M M M M M M M-1 28 M 29 M M M M M M M M 29 M M]
[M M M M M M M-1 M M M M M M M-1 M M M 30 M M M M M M M M M M M M M M]
[M M M M M M M M M M M-1 M M M-1 M M M 30 M-1 M M M M M-1 M M M 29 M M M M]
[M M M M M M M M-1 29 29 M 28 28 M M M 29 M M M M 28 27 28 M M 27 M-1 27 M-1 M M]
[M M M M 30 M 26 M M M M M M M M M 29 M M 28 30 M M M M M 29 29 M 27 M M]
[30 M M M 30 27 25 M M M M 28 M M-1 30 M 28 M M M M M-1 M M 30 M M-1 30 M M M M]
[M-1 M M 28 M M M M M M M-1 M 29 M M-1 M M M M M M M 27 M M M M M M M 29 M]
[M M M M-1 M M M 27 M M M M M M M M M 27 M 30 30 M 28 M M-1 M M M M M 29 M]
[M-1 M M M 30 M M M M M M M 30 M M M M M-1 M M 30 M M M M M M M M M M M]
T26
"""
        let vec = bigReprStr.TrimStart('\n').Replace("\n", " ") |> RRBVectorGen.treeReprStrToVec
        // logger.info (eventX "Starting with {vec}" >> setField "vec" (RRBVectorGen.vecToTreeReprStr vec))
        let mutable current = vec
        let mergeL = RRBVectorGen.treeReprStrToVec >> RRBVectorMoreCommands.ParameterizedVecCommands.mergeL
        let mergeR = RRBVectorGen.treeReprStrToVec >> RRBVectorMoreCommands.ParameterizedVecCommands.mergeR
        let actions = [ mergeL "M T19"; push 99; mergeL "0 T28"; push 80; push 127; push 17; push 30;
                        push 138; remove -109; mergeR "M T9"; push 91; push 72; insert (-90,126); push 64;
                        push 52; insert (-138,1); push 130 ]
        // let logVec action vec = logger.info (eventX "After {action}, vec was {vec}" >> setField "action" (action.ToString()) >> setField "vec" (RRBVectorGen.vecToTreeReprStr vec))
        let logVec action = ignore
        for action in actions do
            current <- current |> action.RunActual
            logVec action current
            RRBVectorProps.checkProperties current <| sprintf "Vector after %s" (action.ToString())

    testCase "Another large manual test" <| fun _ ->
        let vec = RRBVectorGen.treeReprStrToVec "[M*M]*M TM-3"
        // printfn "Starting with %s" (RRBVectorGen.vecToTreeReprStr vec)
        let mutable current = vec
        let actions = [push 38; rev()]
        // let logVec action vec = logger.info (eventX "After {action}, vec was {vec}" >> setField "action" (action.ToString()) >> setField "vec" (RRBVectorGen.vecToTreeReprStr vec))
        let logVec action = ignore
        for action in actions do
            current <- current |> action.RunActual
            logVec action current
            RRBVectorProps.checkProperties current <| sprintf "Vector after %s" (action.ToString())

  ]


let mkTestSuite name startingVec =
  let runTest testName f =
    RRBVectorProps.checkProperties startingVec <| sprintf "Starting vector in %s test in %A suite" testName name
    let result = startingVec |> f
    RRBVectorProps.checkProperties result <| sprintf "Result of %s test in %A suite" testName name

  let mkTest testName f =
    testCase (sprintf "%s: %s" name testName) <| fun _ -> runTest testName f

  let pos = if RRBVector.length startingVec = 0 then 0 else (Literals.blockSize + 3) % (RRBVector.length startingVec)

  testList name [
    mkTest "remove" (if RRBVector.length startingVec = 0 then id else RRBVector.remove pos)
    mkTest "insert" (RRBVector.insert pos -512)
    mkTest "pop" (if RRBVector.length startingVec = 0 then id else RRBVector.pop)
    mkTest "push" (RRBVector.push -512)
    mkTest "push many" (fun vec -> { 0 .. Literals.blockSize } |> Seq.fold (fun v i -> v |> RRBVector.push i) vec)
    mkTest "take" (RRBVector.take pos)
    mkTest "truncate" (RRBVector.truncate (Literals.blockSize + 3))
    mkTest "truncate more" (RRBVector.truncate (Literals.blockSize * Literals.blockSize + 3))
    mkTest "skip" (RRBVector.skip pos)
    mkTest "choose" (RRBVector.choose (fun n -> if n % 2 = 0 then Some (n / 2) else None))
    mkTest "distinct" (RRBVector.distinct)
    mkTest "map id" (RRBVector.map id)
    mkTest "scan (+)" (RRBVector.scan (+) 0)
    mkTest "scanBack (+)" (RRBVector.scanBack 0 (+))
    mkTest "mergeL tail-only" (RRBVector.append (RRBVectorGen.treeReprStrToVec "T28"))
    mkTest "mergeR tail-only" (fun vec -> RRBVector.append vec (RRBVectorGen.treeReprStrToVec "T28"))
    mkTest "mergeL sapling" (RRBVector.append (RRBVectorGen.treeReprStrToVec "M T28"))
    mkTest "mergeR sapling" (fun vec -> RRBVector.append vec (RRBVectorGen.treeReprStrToVec "M T28"))
    mkTest "mergeL small non-sapling" (RRBVector.append (RRBVectorGen.treeReprStrToVec "M M T28"))
    mkTest "mergeR small non-sapling" (fun vec -> RRBVector.append vec (RRBVectorGen.treeReprStrToVec "M M T28"))
    mkTest "merge with self" (fun vec -> RRBVector.append vec vec)
    // TODO: More tests like these
    // testProp "Command tests from constructed vector" <| fun _ -> (Command.toProperty (RRBVectorMoreCommands.specFromData startingVec))
  ]

let startingVecForTransientResidueTests =
    let mutable current = { 0..(Literals.blockSize * Literals.blockSize + Literals.blockSize) } |> RRBVector.ofSeq
    // for i = 0 to Literals.blockSize do
    //   current <- current.Pop()
    // We now have a vector of one ExpandedNode and a tail, and the ExpandedNode has a single null in its last array element.
    // RRBVectorProps.checkProperties current "StartingVec"
    current

let transientResidueTests =
    mkTestSuite "Tests on transient-residue vectors" startingVecForTransientResidueTests

let moreTransientResidueTests =
  { 0..(Literals.blockSize * (Literals.blockSize - 3)) }
  |> RRBVector.ofSeq
  |> mkTestSuite "Tests on some more transient-residue vectors"
  // That produces an ExpandedNode with a few nulls at the end

let emptyTests = mkTestSuite "Tests on empty vectors" RRBVector.empty
let singletonTests = mkTestSuite "Tests on 1-item vectors" (RRBVector.singleton 0)
let dualTests = mkTestSuite "Tests on 2-item vectors" (RRBVector.singleton 0 |> RRBVector.push 1)
let halfFullTailTests = mkTestSuite "Tests on vectors with half-full tails" ({ 1 .. Literals.blockSize / 2} |> RRBVector.ofSeq)
let fullTailTests = mkTestSuite "Tests on vectors with full tails" ({ 1 .. Literals.blockSize} |> RRBVector.ofSeq)
let fullTailPlusOneTests = mkTestSuite "Tests on vectors with full tails plus one" ({ 0 .. Literals.blockSize} |> RRBVector.ofSeq)
let fullSaplingMinusOneTests = mkTestSuite "Tests on full saplings minus one" ({ 2 .. Literals.blockSize * 2} |> RRBVector.ofSeq)
let fullSaplingTests = mkTestSuite "Tests on full saplings" ({ 1 .. Literals.blockSize * 2} |> RRBVector.ofSeq)
let fullSaplingPlusOneTests = mkTestSuite "Tests on full saplings plus one" ({ 0 .. Literals.blockSize * 2} |> RRBVector.ofSeq)
let threeLevelVectorTests = mkTestSuite "Tests on three-level vector" (RRBVectorGen.treeReprStrToVec "[[M*M]*M]*3 TM/2")

[<Tests>]
let tests =
  testList "All tests" [
    // longRunningTests
    testCase "Single isolated test for pushing in transients" <| fun _ ->
        let size = 65568
        let mutable vec = RRBTransientVector()
        RRBVectorProps.checkProperties vec "Empty transient vector"
        for i = 1 to size do
            vec <- vec.Push i :?> RRBTransientVector<_>
        RRBVectorProps.checkProperties vec <| sprintf "Transient vector of size %d" size
        Expect.equal vec.Length size <| sprintf "Vector should have gotten %d items pushed" size
        let v2 = vec
        // logger.debug (
        //     eventX "Tree {vec} passed all the checks"
        //     >> setField "vec" (sprintf "%A" v2)
        // )
        vec <- vec.Push (size + 1) :?> RRBTransientVector<_>
        RRBVectorProps.checkProperties vec <| sprintf "Transient vector of size %d" (size + 1)
        Expect.equal vec.Length (size + 1) <| sprintf "Vector should have gotten %d items pushed after one last push" (size + 1)

    testCase "Single isolated test for popping in persistents" <| fun _ ->
        let mutable current = { 0..(Literals.blockSize * Literals.blockSize + Literals.blockSize) } |> RRBVector.ofSeq
        RRBVectorProps.checkProperties current "Persistent vector before pops"
        for i = 0 to Literals.blockSize do
          current <- current.Pop()
          RRBVectorProps.checkProperties current "Persistent vector during pops"
        // We now have a vector of one ExpandedNode and a tail, and the ExpandedNode has a single null in its last array element.
        // let size = Literals.blockSize * Literals.blockSize + Literals.blockSize + 1
        // let mutable vec = { 1..size } |> RRBVector.ofSeq
        // RRBVectorProps.checkProperties vec "Persistent vector before pops"
        // for i = 1 to Literals.blockSize+2 do
        //     vec <- vec.Pop() :?> RRBPersistentVector<_>
        //     RRBVectorProps.checkProperties vec <| sprintf "Persistent vector of size %d" (size - i)
        // let v2 = vec
        // logger.debug (
        //     eventX "Tree {vec} passed all the checks"
        //     >> setField "vec" (sprintf "%A" v2)
        // )
        // Expect.equal vec.Length (size - Literals.blockSize - 1) <| sprintf "Vector has wrong size after pops"

    longRunningTests
    splitTransientTests
    regressionTests
    threeLevelVectorTests
    transientResidueTests
    moreTransientResidueTests
// //   ]
// // ignore
// //   [

    isolatedTest
    emptyTests
    singletonTests
    dualTests
    halfFullTailTests
    fullTailTests
    fullTailPlusOneTests
    fullSaplingMinusOneTests
    fullSaplingTests
    fullSaplingPlusOneTests

    arrayTests
    simpleVectorTests
    manualVectorTests
    constructedVectorSplitTests
    splitJoinTests
    insertTests
    operationTests // Operational tests not yet ported to new API
    vectorTests
    nodeVecGenerationTests
    mergeTests
    // apiTests

    // perfTests
  ]

// TODO: Tests to write to cover code we're not yet testing:
(*
windowedSeq - exercise Array.popFirstAndPush
Something to exercise the final else block in Array.appendAndSplitAt (lenL > splitIdx)
  It gets used in our code when we append two vectors, the right one was tail-only, and tailLenL + tailLenR > blockSize
  Since the split index is the block size, and tailLenL is never allowed to be greater than the block size,
  this will never get exercised "normally", so I should just add something to the array extensions test suite.

findMergeCandidates - write test that makes sure the one-pass version never finds *better* candidates than the two-pass version
GetEditableNodeOfBlockSizeLength on tree nodes (currently we only ever call it on leaf nodes)
  Actually, we should change that to be a method on the leaf node, not on an RRBNode since it doesn't actually make sense there
ExpandRightSpine of full nodes - the "if shift <= Literals.blockSizeShift || this.NodeSize = 0 then" branch is always true so far
  We need to write tests making persistent trees of many sizes (some manually) and making them transient, then persistent again,
    and verifying that they kept their "shape". Also do a few operations (push, insert, remove, pop) on the transient and make sure
    they correspond to the same thing on the persistent.
ToRelaxedNodeIfNeeded on full nodes isn't, apparently, being called (?)
  It's being called in InsertChildS of *expanded* nodes, but not of regular full nodes??
  Ditto for AppendNChildren and MkNodeForRebalance of *expanded* nodes, but not of regular full nodes. Why??
SlideChildren{Left,Right} of full nodes - aren't testing the n = 0 case (but then, we only tested each of those twice total)
InsertAndSlideChildrenLeft of full nodes - the "Special case since the algorithm below would fail on this one scenario" block isn't being run
  Also the final else block isn't being run
  But then, we only ran it twice total. Need more test cases that exercise this part of the code.
InsertAndSlideChildrenRight of full nodes - final else block isn't being run (comment says we need special test case)
ConcatTwigsPlusLeaf - final failwith is never called (good, but we might need a rewrite to add an #if DEBUG in there so we don't have a dangling else clause)
  Or we might want to rewrite it so that it can fail, and the code above it takes a different path. Maybe not, but we should make sure it's internal.
MergeTree of full nodes (the only implementation, thankfully) - the childL.MergeTree call in the shift = rightShift branch only produced (child', None) once,
  which isn't enough for full testing: the "if right.NodeSize > 1" test went down the "then" branch, and we haven't yet tested a case where right.NodeSize = 1
  TODO: Run our tests in debug mode with some breakpoints in there, and see when those breakpoints ever get hit

MaybeExpand of expanded full nodes - the "if not (isSameObj lastChild expandedLastChild) then" is never true
  (the expanded last child is *always* the same object), so I need to find a scenario where it will be false so I have more confidence
MkNodeForRebalance of expanded full nodes - the "if isSameObj arr this.Children && this.IsEditableBy owner" branch is never true
  Need to write a manual test for this one since it involves merging two carefully-crafted transient trees.
NewParent of expanded full nodes - the "if size = 1 || (this.NodeSize = Literals.blockSize && this.FullNodeIsTrulyFull shift) then" test
  is always true. Note the TODO comment before it: maybe a static RRBExpandedRelaxedNode.Create method is a good idea here.

Shrink of expanded relaxed nodes - the "if this.IsEditableBy owner && size = Literals.blockSize then" test is never true.
  This needs a manual test, where a full tree of height 2 has several items removed from the leaves of the rightmost twig,
  so that it becomes a relaxed node of size M... and then the tree is made transient so that it's an expanded relaxed node
  of size M. Then make the tree persistent again, and that will exercise this code path.
RemoveChild of expanded relaxed nodes - the final else block (where newSize <= 0) is never exercised. We need some tests
  that remove the last item in a skinny path (size 1, size 1, size 1, leaf size 1) of a transient.
RemoveLastChild of expanded relaxed nodes - ditto.
KeepNLeft of expanded relaxed nodes - we never exercise this where n = 0

MaybeExpand of expanded relaxed nodes is *never* exercised at all.
MkNodeForRebalance of expanded relaxed nodes - the "if isSameObj arr this.Children && this.IsEditableBy owner then" branch is never true.
  Need some kind of carefully-crafted test here. TODO: Think about how to write one.
NewParent of expanded relaxed nodes - only exercised twice, always with siblings being size 1 or 0 so that the
  "size = 1 || (this.NodeSize = Literals.blockSize && this.FullNodeIsTrulyFull shift)" test is always true

AppendedItem of leaf nodes is never exercised.
PopLastItem of leaf nodes is never exercised. Should be remove it?

============
RRBVector.fs
============

RRBPersistentVector:
  new(ownerToken) - not exercised (manual test)
  static MkEmptyWithToken - ditto (use this interface to exercise new(ownerToken) since that does both at once)
  Peek() on empty vector - test verifies that it throws an exception
    Also construct an illegal vector with empty tail, then Peek() it to exercise the other exception-throwing path
  Pop() on empty vector - test verifies that it throws an exception
  Skip(n) where n >= Count - not yet exercised
  GetSlice() - not yet exercised
  Append() - wildcard match case cannot be reached, ignore
  Insert() - two match cases cannot be reached, ignore
  Remove - idx >= this.TailOffset and this.Count - this.TailOffset = 1 (removing the last item from a one-item tail but not doing a Pop)
  EnsureValidIndex - exercise by testing that exceptions are thrown correctly

RRBTransientVector:
  ToString() - convert to a useful-for-users format, then test
  ThrowIfNotValid - set up several operations where the transient is NOT valid (many manual tests)
  ShortenTree - we only exercise this at height 1. We need some taller transient trees to test.
  ShiftNodesFromTailIfNeeded - the "if not <| isSameObj newRoot this.Root then" paths are never followed, but I can't come up with
  a scenario in which they would ever be. The root is going to have the right owner already, so we can't make that happen. Ignore.
  IsEmpty - manual tests, a couple of them
  StringRepr - convert to a useful-for-users format, then test
  IterEditableLeavesWithoutTail - not exercised. Need to come up with a way to do so.
  RevIterLeaves - ditto.
  RevIterEditableLeavesWithoutTail - ditto.
  RevIterItems - ditto; this would exercise RevIterLeaves
  Peek - manual test, pretty simple
  Pop() on empty vector - manual test to verify that exception thrown
  Take - not at all tested
  Skip - ditto
  Split - ditto
  Slice, GetSlice - ditto, WIP (will exercise Take and Skip at the same time)
  Append - not at all tested
  Insert - "if not <| isSameObj newRoot this.Root then" path not taken. Might need to exercise "insert pushes root up" scenario (initially full tree, insert into full tail)
           Also, SplitNode not exercised. Exercise the "insert pushes root up" with insert in the main body of a full tree. Other two match paths are impossible; ignore.
  RemoveWithoutRebalance - not exercised. Property test for this, making sure it returns same result as Remove albeit with a possible-less-efficient tree.
    (Note: windowedSeq cannot exercise this, as it only does RemoveWithoutRebalance on persistents)
  RemoveImpl - haven't exercised the code path where you remove one item from a length-one tail (last item of vector)
  Update - not exercised
  GetItem - not exercised
  EnsureValidIndex - exercise by testing that exceptions are thrown correctly

RRBVector module:
  TODO. Lots of functions here.
*)
