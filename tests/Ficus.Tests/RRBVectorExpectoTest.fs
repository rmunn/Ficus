module ExpectoTemplate.RRBVectorExpectoTest

open Expecto
open Ficus
open Ficus.RRBArrayExtensions
open Ficus.RRBVectorBetterNodes
open Ficus.RRBVector
open FsCheck
open Expecto.Logging
open Expecto.Logging.Message

module Literals = Ficus.Literals
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
            override x.Generator = RRBVecGen.sizedGenVec<'a>
            override x.Shrinker _ = Seq.empty }
    static member arbSplitTest() =
        { new Arbitrary<RRBVectorTransientCommands.SplitTestInput>() with
            override x.Generator = RRBVectorTransientCommands.genInput RRBVectorTransientCommands.cmdsMedium
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

// A few vectors either generated by hand, or generated by the RRBVecGen code, which have proven useful in testing certain scenarios

let ridiculouslyBigVectorAtBlockSize8 : RRBVector<int> =  // TODO: Construct a similar test for M=32?
    RRBVecGen.treeReprStrToVec
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
    let repr = RRBVecGen.vecToTreeReprStr vec
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
    let vec = RRBVecGen.treeReprStrToVec repr
    splitTest vec i

module Expect =
    let vecEqual (v1 : RRBVector<'T>) (v2 : RRBVector<'T>) msg =
        Expect.equal (v1.Length) (v2.Length) <| sprintf "Vectors should be equal but had different lengths: expected %d and got %d\n%s" v1.Length v2.Length msg
        for i = 0 to v1.Length - 1 do
            Expect.equal (v1.Item i) (v2.Item i) <| sprintf "Not equal at idx %d: expected %A and got %A\n%s" i (v1.Item i) (v2.Item i) msg
        Expect.equal (RRBVector.toArray v1) (RRBVector.toArray v2) msg

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
    testCase "push M+1 items onto an empty vector" <| fun _ ->
        let mutable vec = RRBVector.empty<int>
        for i = 1 to Literals.blockSize + 1 do
            vec <- vec |> RRBVector.push i
            RRBVectorProps.checkProperties vec (sprintf "Vector after %d push%s" i (if i = 1 then "" else "es"))
    testCase "push M+2 items onto a vector that starts with a nearly-full tail" <| fun _ ->
        let mutable vec = RRBVecGen.treeReprStrToVec "TM-1"
        for i = 1 to Literals.blockSize + 2 do
            vec <- vec |> RRBVector.push i
            RRBVectorProps.checkProperties vec (sprintf "Vector after %d push%s" i (if i = 1 then "" else "es"))
    testCase "push 2 items onto a vector that starts with a full root and a nearly-full tail" <| fun _ ->
        let mutable vec = RRBVecGen.treeReprStrToVec "M TM-1"
        for i = 1 to 2 do
            vec <- vec |> RRBVector.push i
            RRBVectorProps.checkProperties vec (sprintf "Vector after %d push%s" i (if i = 1 then "" else "es"))
    testCase "insert into two full nodes" <| fun _ ->
        let vec = RRBVecGen.treeReprStrToVec "M M T1"
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
    ftestCase "splitInto with not uneven input leaves smaller last vector" (fun _ ->
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
        let mutable vec = RRBVecGen.treeReprStrToVec "M M TM"
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
  testList "foo" [
    testCase "Shorten trees after split" <| fun _ ->
        let v = RRBVecGen.treeReprStrToVec "M M TM/2"
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
        let vec = RRBVecGen.treeReprStrToVec "[[M*M-1 5]] [[5 5]] T1"
        RRBVectorProps.checkProperties vec "Original vector"
        let vec' = vec |> RRBVector.pop
        RRBVectorProps.checkProperties vec' "Popped vector"

    testCase "Slicing near tail can shift into tail" <| fun _ ->
        // This test proves that we need .adjustTree() instead of .shortenTree() in the final (main) branch of RRBVector.Skip()
        let vec = RRBVecGen.treeReprStrToVec "[[M*M-1 5]] [[5 5]] T1"
        RRBVectorProps.checkProperties vec "Original vector"
        // TODO: Write this test in such a way that the original vector *passes* properties, so that it's a fair test.
        let mutable vec' = vec.Skip (Literals.blockSize * (Literals.blockSize - 1) + 10)
        RRBVectorProps.checkProperties vec' "Popped vector"
        for n = 1 to Literals.blockSize + 1 do
            vec' <- vec' |> RRBVector.push n
            RRBVectorProps.checkProperties vec' <| sprintf "Right half after %d pushes" n

(* Disabling this test since we no longer apply this invariant, and instead we allow saplings to have a mix of non-full root and tail
    // Note that by allowing saplings to have a mix of non-full root and tail, it allows us to reuse the node arrays and go faster in this particular scenario
    testCase "Slicing at tail can promote new tail and adjust tree so last leaf is full" <| fun _ ->
        let vec = RRBVecGen.treeReprStrToVec "M-1 M T1"
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
        let a = RRBVecGen.treeReprStrToVec "[M*M] [M*M] [M*M] [M*20] TM"
        let b = RRBVecGen.treeReprStrToVec "M M M M 5 TM-3"
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
        let a = RRBVecGen.treeReprStrToVec "M M T1"
        let b = RRBVecGen.treeReprStrToVec "M T1"
        RRBVectorProps.checkProperties a "Original A"
        RRBVectorProps.checkProperties b "Original B"
        let joined = RRBVector.append a b
        RRBVectorProps.checkProperties joined "Joined"

    testCase "Merging short left tree will have correct length" <| fun _ ->
        let a = RRBVecGen.treeReprStrToVec "TM/2"
        let b = RRBVecGen.treeReprStrToVec "[M M M-2] [M-1 M-3 M] T6"
        doJoinTest a b

    testCase "Merging two VERY short vectors will have correct length" <| fun _ ->
        let vL = RRBVecGen.treeReprStrToVec "T1"
        let vR = RRBVecGen.treeReprStrToVec "M T1"
        doJoinTest vL vR

(* Disabling test since we no longer apply this invariant, and instead we allow saplings to have a mix of non-full root and tail
    testCase "Splitting root+tail vector to two tail-only vectors has empty root on both vectors" <| fun _ ->
        let vec = RRBVecGen.treeReprStrToVec "M T1"
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
        let vec = RRBVecGen.treeReprStrToVec "M M TM/2"
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
        let a = RRBVecGen.treeReprStrToVec "M M T1"
        let b = RRBVecGen.treeReprStrToVec "M*M T1"
        let bShift = (b :?> RRBPersistentVector<int>).Shift
        let joined = RRBVector.append a b :?> RRBPersistentVector<int>
        RRBVectorProps.checkProperties joined "Joined"
        Expect.equal joined.Length (a.Length + b.Length) "Wrong length for joined vector"
        Expect.equal joined.Shift (bShift + Literals.blockSizeShift) "Joined vector should have pushed up a new root"
        Expect.equal joined.Root.NodeSize 2 "Joined vector should have pushed up a new root of size 2"
        Expect.equal joined.TailOffset (joined.Length - 1) "Joined vector should have just one item in its tail"

    testCase "Removing an item from the last leaf maintains the invariant" <| fun _ ->
        let vec = RRBVecGen.treeReprStrToVec "M M T2"
        let vec' = vec.Remove (Literals.blockSize + 1) :?> RRBPersistentVector<int>
        RRBVectorProps.checkProperties vec' "Vector after removal from last leaf"
        Expect.equal vec'.Tail.Length 1 "Vector adjustment should have removed one item from the tail to maintain the invariant"

    testCase "Removing an item from the last leaf can promote new leaf to maintain the invariant" <| fun _ ->
        let vec = RRBVecGen.treeReprStrToVec "M M T1"
        let oldShift = (vec :?> RRBPersistentVector<int>).Shift
        let vec' = vec.Remove (Literals.blockSize + 1) :?> RRBPersistentVector<int>
        RRBVectorProps.checkProperties vec' "Vector after removal from last leaf"
        Expect.equal vec'.Tail.Length Literals.blockSize "Vector adjustment should have promoted new leaf"
        Expect.equal vec'.Shift Literals.blockSizeShift "Vector adjustment should have left tree with height 1"
        Expect.equal vec'.Root.NodeSize 1 "Vector adjustment should have left tree with single-node root"

    testCase "Inserting into last leaf can still maintain invariant when height is not affected" <| fun _ ->
        let vec = RRBVecGen.treeReprStrToVec "M/2-1 M T2"
        let vec' = vec.Insert (Literals.blockSize / 2) 512
        RRBVectorProps.checkProperties vec "Original vector"
        RRBVectorProps.checkProperties vec' "Vector after insertion"

    testCase "Inserting into last leaf can still maintain invariant when rebalance causes lower height" <| fun _ ->
        let vec = RRBVecGen.treeReprStrToVec "M-2 M T1"
        let vec' = vec.Insert (vec.Length - 2) 512
        // You'd think this would turn into "M M-1 T1", but that would break the invariant. So instead a node is shifted out of the tail to make M TM, which has height 0.
        RRBVectorProps.checkProperties vec "Original vector"
        RRBVectorProps.checkProperties vec' "Vector after insertion"

    testCase "Pushing tail down in root+tail tree, when root size is equal to blockSizeShift, creates a correct tree" <| fun _ ->
        // This was a particularly subtle bug, since it only triggered in very specific cases.
        let vec = RRBVecGen.treeReprStrToVec "5 TM"
        let step1 = vec |> RRBVector.insert (vec.Length - 2) -512
        RRBVectorProps.checkProperties step1 "Vector after first step"
        let step2 = step1 |> RRBVector.remove 5
        RRBVectorProps.checkProperties step2 "Vector after second step"

    testCase "An insert that splits the last leaf will not cause later pushes to break the invariant" <| fun _ ->
        let vec = RRBVector.ofSeq { 0..1982 }
        let vec = RRBVecGen.treeReprStrToVec "[M*M] [M*M-3] T31"
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
        let vL = RRBVecGen.treeReprStrToVec "[M 6 7 M 6 7] [6 6 7 5 6 M 7] [5 6 6 5 4 4 6] [M*M-1] T4"
        let vR = RRBVecGen.treeReprStrToVec "M T1"
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
  testList "merge tests" [
    testCase "adjustTree is needed in merge algorithm when right tree is root+tail" <| fun _ ->
        let vL = RRBVecGen.treeReprStrToVec <| sprintf "M*M T1"
        let vR = RRBVecGen.treeReprStrToVec <| sprintf "M T1"
        RRBVectorProps.checkProperties vL "Left half of merge"
        RRBVectorProps.checkProperties vR "Right half of merge"
        let joined = RRBVector.append vL vR
        RRBVectorProps.checkProperties joined <| sprintf "Joined vector"

    testCase "adjustTree is needed in merge algorithm when tree height increases" <| fun _ ->
        let vL = RRBVecGen.treeReprStrToVec <| sprintf "M*M-2 TM/2"
        let vR = RRBVecGen.treeReprStrToVec <| sprintf "M-1 M M-4 T1"
        RRBVectorProps.checkProperties vL "Left half of merge"
        RRBVectorProps.checkProperties vR "Right half of merge"
        let joined = RRBVector.append vL vR
        RRBVectorProps.checkProperties joined <| sprintf "Joined vector"

    testCase "adjustTree is needed in merge algorithm when tree height does not increase" <| fun _ ->
        let vL = RRBVecGen.treeReprStrToVec <| sprintf "M*M/2 TM"
        // let vR = RRBVecGen.treeReprStrToVec <| sprintf "M M M-1 T2" // This makes an invalid tree
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
        let vL = RRBVecGen.treeReprStrToVec "[M*M]*3 TM/4"
        let vR = RRBVecGen.treeReprStrToVec "M*M/2 TM"
        RRBVectorProps.checkProperties vL "Left half of merge"
        RRBVectorProps.checkProperties vR "Right half of merge"
        let joined = RRBVector.append vL vR
        RRBVectorProps.checkProperties joined <| sprintf "Joined vector"

    testCase "Joining vectors will rebalance properly at heights above leaf level" <| fun _ ->
        // There used to be a subtle bug in the rebalancing code, where rebalancing at the twig (or higher) levels would make incorrectly-sized nodes.
        let vL = RRBVecGen.treeReprStrToVec "[28 M-1 26 M-1 M 21 M 26 M M 28 M M-1 29 26 29 28 23 M M M M 26 M 29 29 M 23 M-1 M M M] [M M M M M] T30"
        let vR = RRBVecGen.treeReprStrToVec "[25 M-1 19 25 M-1 M M M M M M 29 24 28 30 M M M M 27 30 27 M 27 29 25 27] [M M M M M M M M M M M M M M M M M 28 M M M M-1 M] [M M M M M M M 28 M M M M M M M 28 M-1 M M M M M M M-1 M] [22 22 26 30 M 29 M M 27 27 M M M M M M M 26 29 M 28 27 24 27 26 22 29] [M M M M M M M M M M M M M M M M M M M M M M] [27 28 24 M M 29 28 M-1 30 30 23 29 25 24 25 M M 25 M-1 M 26 28] [26 23 M 27 M-1 29 23 M 27 26 26 29 M M M M 30 24 M-1 27 28 29 27 27] [M M 21 M M 30 19 M M 27 M M 26 M 27 24 M M 29 30 25 M 23] [30 M-1 M 26 M 26 M-1 M-1 M-1 28 M 24 30 30 27 27 29 27 27 M 25 M-1 28 M-1 28 30] [27 M 22 28 M 27 M 28 M 29 M M M-1 27 29 27 M-1 26 M 23 M M] [M-1 26 M M 25 M 26 24 M 29 M-1 30 29 M M 28 26 28 26 27 M 28] [29 26 M 28 M 30 M 30 M M 23 M M M 30 M 29 M 28 27 M 21 M M M M] T27"
        RRBVectorProps.checkProperties vL "Left half of merge"
        RRBVectorProps.checkProperties vR "Right half of merge"
        let joined = RRBVector.append vL vR
        RRBVectorProps.checkProperties joined <| sprintf "Joined vector"

    testCase "Joining any number of small vectors on the left (from tail-only to root+tail) with a large vector on the right will produce correct results" <| fun _ ->
        // Regression test for the "left vector is short" logic in the vector-join algorithm
        let vL = RRBVecGen.treeReprStrToVec "T8"
        let vR = RRBVecGen.treeReprStrToVec "[M 29 M M M M M 29 M M 29 M M M 29 27 M] [M M-1 M-1 M M M 30 M-1 29 29] [23 M M M 23 21 M 24 27 M-1 30 29 21 23 28] [M M M M-1 30 28 28 M M M-1 M M 28 M] [24 24 23 27 24 17 16 20 M] T25"
        RRBVectorProps.checkProperties vL "Left half of merge"
        RRBVectorProps.checkProperties vR "Right half of merge"
        for i in 1..Literals.blockSize do
            let mutable vL' = vL
            for x in 1..i do
                vL' <- vL' |> RRBVector.push x
            RRBVectorProps.checkProperties vL' "Left half of merge"
            let joined = RRBVector.append vL' vR
            RRBVectorProps.checkProperties joined <| sprintf "Joined vector (with left tail %d)\nvL was %s and vR was %s" i (RRBVecGen.vecToTreeReprStr vL') (RRBVecGen.vecToTreeReprStr vR)
            Expect.equal joined.Length (vL'.Length + vR.Length) "Joined vector length should be sum of original vectors' lengths"

    testCase "Joining two large vectors can correctly trigger a rebalance" <| fun _ ->
        // Without a rebalance, the joined vector's root would end up with six nodes, but with the rebalance, the left vector can be squeezed into the right vector's leftmost twig.
        let vL = RRBVecGen.treeReprStrToVec "M*15 T24" :?> RRBPersistentVector<int>
        let vR = RRBVecGen.treeReprStrToVec "[M 29 M M M M M 29 M M 29 M M M 29 27 M] [M M-1 M-1 M M M 30 M-1 29 29] [23 M M M 23 21 M 24 27 M-1 30 29 21 23 28] [M M M M-1 30 28 28 M M M-1 M M 28 M] [24 24 23 27 24 17 16 20 M] T25" :?> RRBPersistentVector<int>
        RRBVectorProps.checkProperties vL "Left half of merge"
        RRBVectorProps.checkProperties vR "Right half of merge"
        let joined = RRBVector.append vL vR :?> RRBPersistentVector<int>
        RRBVectorProps.checkProperties joined <| sprintf "Joined vector\nvL was %s and vR was %s" (RRBVecGen.vecToTreeReprStr vL) (RRBVecGen.vecToTreeReprStr vR)
        Expect.equal joined.Root.NodeSize vR.Root.NodeSize "Rebalance should have left the joined vector's root the same size as the original right vector's root"
  ]

let splitTransientTests =
  testList "split transient tests" [
    etestPropSm (1636012108, 296642056) "how long does this take?" <| fun (RRBVectorTransientCommands.SplitTestInput (vec, cmds)) ->
    // Failures: (1636012108, 296642056); (1745505811, 296642056); (238202286, 296642062); (262408519, 296642062)
        let vec = if vec |> isTransient then (vec :?> RRBTransientVector<_>).Persistent() else vec :?> RRBPersistentVector<_>
        let mailbox = RRBVectorTransientCommands.startSplitTesting vec cmds
        let mutable error = None
        mailbox.Error.Add (fun e -> error <- Some e)  // HOPEFULLY this should be enough to fail the test...?
        let result = mailbox.PostAndReply RRBVectorTransientCommands.AllThreadsResult.Go
        match result with
        | RRBVectorTransientCommands.AllThreadsResult.Go _ -> failtest "Oops"
        | RRBVectorTransientCommands.AllThreadsResult.OneFailed (position, cmdsDone, vec, arr, cmd, errorMsg) ->
            failtestf "Split vector number %d failed on %A after %d commands, with message %A; vector was %A and corresponding array was %A" position cmd cmdsDone errorMsg vec arr
        | RRBVectorTransientCommands.AllThreadsResult.AllCompleted _ -> ()
    testCase "Removing one item from full-sized root of transient preserves tail" <| fun _ ->
        let arr = [| 1 .. Literals.blockSize + 6 |]
        let vec = RRBVector.ofArray arr
        let tvec = (vec :?> RRBPersistentVector<_>).Transient()
        let newVec = tvec.Remove 3
        let newArr = arr |> Array.copyAndRemoveAt 3
        RRBVectorProps.checkProperties newVec "New vector"
        Expect.equal (newVec |> RRBVector.toArray) newArr "New vector did not match array"
    testCase "Removing one item from root of transient of length M+1 moves entire new M-sized root into tail" <| fun _ ->
        let arr = [| 1 .. Literals.blockSize + 1 |]
        let vec = RRBVector.ofArray arr
        let tvec = (vec :?> RRBPersistentVector<_>).Transient()
        let newVec = tvec.Remove 3
        let newArr = arr |> Array.copyAndRemoveAt 3
        RRBVectorProps.checkProperties newVec "New vector"
        Expect.equal (newVec |> RRBVector.toArray) newArr "New vector did not match array"
    testCase "Inserting one item at start of full-sized tail of transient with empty root preserves tail size" <| fun _ ->
        let arr = [| 1 .. Literals.blockSize |]
        let vec = RRBVector.ofArray arr
        let tvec = (vec :?> RRBPersistentVector<_>).Transient()
        let newVec = tvec.Insert 0 3
        let newArr = arr |> Array.copyAndInsertAt 0 3
        RRBVectorProps.checkProperties newVec "New vector"
        Expect.equal (newVec |> RRBVector.toArray) newArr "New vector did not match array"
    testCase "Inserting one item at start of not-quite-full-size tail of transient with empty root leaves full tail and empty root" <| fun _ ->
        let arr = [| 1 .. Literals.blockSize - 1 |]
        let vec = RRBVector.ofArray arr
        let tvec = (vec :?> RRBPersistentVector<_>).Transient()
        Expect.equal (tvec :?> RRBTransientVector<_>).Tail.[Literals.blockSize - 1] 0 "Transient vector's tail should initially end in 0"
        let newVec = tvec.Insert 0 3
        let newArr = arr |> Array.copyAndInsertAt 0 3
        RRBVectorProps.checkProperties newVec "New vector"
        Expect.equal (newVec |> RRBVector.toArray) newArr "New vector did not match array"
        Expect.equal (newVec :?> RRBTransientVector<_>).Root.NodeSize 0 "New vector's root should still be empty"
        Expect.equal (newVec :?> RRBTransientVector<_>).Tail.Length Literals.blockSize "New vector's tail should still be full"
        Expect.notEqual (newVec :?> RRBTransientVector<_>).Tail.[Literals.blockSize - 1] 0 "New vector's tail should not end in 0"
    testCase "Shifting nodes into tail twice, leaving empty root, preserves tail correctly" <| fun _ ->
        let vec = RRBVecGen.treeReprStrToVec "M/2-3 M/2-1 T1"
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
        let newVec = tvec.Pop()
        let newArr = arr |> Array.copyAndPop
        RRBVectorProps.checkProperties newVec "New vector"
        Expect.equal (newVec :?> RRBTransientVector<_>).Root.NodeSize 0 "New vector's root should be empty now"
        Expect.equal (newVec :?> RRBTransientVector<_>).Tail.Length Literals.blockSize "New vector's tail should still be full"
        Expect.equal (newVec :?> RRBTransientVector<_>).Tail.[Literals.blockSize - 1] 0 "New vector's tail should end in 0"
        Expect.equal (newVec |> RRBVector.toArray) newArr "New vector did not match array"
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
  testList "split + join tests" [
    testCase "Pushing the tail down in a root+tail node should not cause it to break the invariant, whether or not the root was full" <| fun _ ->
        let vec = RRBVecGen.treeReprStrToVec "M TM"
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
                let reprL = RRBVecGen.vecToTreeReprStr vL
                let reprR = RRBVecGen.vecToTreeReprStr vR
                RRBVectorProps.checkProperties vec' (sprintf "Joined vector from reprL %A and reprR %A" reprL reprR)
                Expect.vecEqual vec' vec "Vector halves after split, when put back together, did not equal original vector"
                vec'
            ) vec
        Expect.vecEqual vecResult vec "After all split+join operations, resulting vector did not equal original vector"
*)
    testCase "Splitting a vector will adjust it to maintain the invariant, and so will joining it together again" <| fun _ ->
        // (Shrunk: RRBVector<length=21,shift=3,tailOffset=18,root=RRBNode(sizeTable=[|5; 10; 18|],children=[|FullNode([|-16; -6; 4; -7; 14|]); FullNode([|15; -17; 14; 5; -1|]);
        // FullNode([|4; 1; -14; 14; 1; -5; -14; 15|])|]),tail=[|7; -7; 17|]> [1])
        let vec = RRBVecGen.treeReprStrToVec "M/2+1 M/2+1 M T3"
        let vL, vR = doSplitTest vec 1
        RRBVectorProps.checkProperties vL "Left half of split"
        RRBVectorProps.checkProperties vR "Right half of split"
        let vec' = RRBVector.append vL vR
        let reprL = RRBVecGen.vecToTreeReprStr vL
        let reprR = RRBVecGen.vecToTreeReprStr vR
        RRBVectorProps.checkProperties vec' (sprintf "Joined vector from reprL %A and reprR %A" reprL reprR)
        Expect.vecEqual vec' vec "Vector halves after split, when put back together, did not equal original vector"

    testCase "Manual test for one scenario that failed the \"split + remove idx 0 of left + join = remove idx 0 of entire\" property" <| fun _ ->
        let vecRepr = "5 M*M-1 T7"
        let vec = RRBVecGen.treeReprStrToVec vecRepr
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
        let v1 = RRBVecGen.treeReprStrToVec "T1"
        let v2 = RRBVecGen.treeReprStrToVec "T1"
        let joined = RRBVector.append v1 v2
        RRBVectorProps.checkProperties joined (sprintf "Joined vector from %A and %A" v1 v2)
        Expect.equal joined.Length 2 "Joined vector should be length 2"
        Expect.equal (joined |> RRBVector.item 0) (v1 |> RRBVector.item 0) "First item of joined vector should be first item of vector 1"
        Expect.equal (joined |> RRBVector.item 1) (v2 |> RRBVector.item 0) "Second item of joined vector should be first item of vector 2"

    testCase "joining two unbalanced vectors will trigger a rebalance" <| fun _ ->
        // NOTE: This test will only work when blockSize = 32. It will have to be rewritten with completely different input if blockSize is ever changed.
        let vL = RRBVecGen.treeReprStrToVec "[28 31 26 31 32 21 32 26 32 32 28 32 31 29 26 29 28 23 32 32 32 32 26 32 29 29 32 23 31 32 32 32] [32 32 32 32 32] T30"
        let vR = RRBVecGen.treeReprStrToVec "[25 31 19 25 31 32 32 32 32 32 32 29 24 28 30 32 32 32 32 27 30 27 32 27 29 25 27] [32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 28 32 32 32 31 32] [32 32 32 32 32 32 32 28 32 32 32 32 32 32 32 28 31 32 32 32 32 32 32 31 32] [22 22 26 30 32 29 32 32 27 27 32 32 32 32 32 32 32 26 29 32 28 27 24 27 26 22 29] [32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32 32] [27 28 24 32 32 29 28 31 30 30 23 29 25 24 25 32 32 25 31 32 26 28] [26 23 32 27 31 29 23 32 27 26 26 29 32 32 32 32 30 24 31 27 28 29 27 27] [32 32 21 32 32 30 19 32 32 27 32 32 26 32 27 24 32 32 29 30 25 32 23] [30 31 32 26 32 26 31 31 31 28 32 24 30 30 27 27 29 27 27 32 25 31 28 31 28 30] [27 32 22 28 32 27 32 28 32 29 32 32 31 27 29 27 31 26 32 23 32 32] [31 26 32 32 25 32 26 24 32 29 31 30 29 32 32 28 26 28 26 27 32 28] [29 26 32 28 32 30 32 30 32 32 23 32 32 32 30 32 29 32 28 27 32 21 32 32 32 32] T27"
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
        let vec = RRBVecGen.treeReprStrToVec "[M/2 M/2+1] T1" :?> RRBPersistentVector<int>
        // RRBVectorProps.checkProperties vec "Original vector"  // Original vector is *not* compliant with the "vectors shouldn't be too tall" property
        Expect.equal vec.Shift (Literals.blockSizeShift * 2) "Original vector should have height of 2"
        let vec' = vec |> RRBVector.remove 0 :?> RRBPersistentVector<int>
        RRBVectorProps.checkProperties vec' "Vector after one item removed at idx 0"
        Expect.equal vec'.Shift Literals.blockSizeShift "After removal, vector should have height of 1"
        Expect.equal vec'.Root.NodeSize 2 "Removal should not rebalance this tree"

    testCase "pop will slide nodes into tail if it needs to" <| fun _ ->
        let vec = RRBVecGen.treeReprStrToVec "[M*M-1 M-1] [M] T1" :?> RRBPersistentVector<int>
        RRBVectorProps.checkProperties vec "Original vector"
        Expect.equal vec.Shift (Literals.blockSizeShift * 2) "Original vector should have height of 2"
        let vec' = vec |> RRBVector.pop :?> RRBPersistentVector<int>
        RRBVectorProps.checkProperties vec' "Vector after one item popped"
        Expect.equal vec'.Tail.Length (Literals.blockSize - 1) "After pop, 1 item should have been slid back into the vector"
        Expect.equal vec'.Shift (Literals.blockSizeShift) "After pop, vector should have height of 1"

    testCase "remove can rebalance trees when they become too unbalanced" <| fun _ ->
        let vec = RRBVecGen.treeReprStrToVec "M/4 M/4+1 M/4+1 M/4 T1" :?> RRBPersistentVector<int>
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
        let vec = RRBVecGen.treeReprStrToVec "M/4 M/4+1 M/4+1 M/4 T1" :?> RRBPersistentVector<int>
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
        let orig = RRBVecGen.treeReprStrToVec "[M-3 M-3] [2 1 3] [M] TM-1"
        RRBVectorProps.checkProperties orig "Original vector"
        let left, right = orig |> RRBVector.split 17
        RRBVectorProps.checkProperties left "Left vector before remove"
        RRBVectorProps.checkProperties right "Right vector"
        let left = left |> RRBVector.remove 0
        RRBVectorProps.checkProperties left "Left vector after remove"
        let joined = RRBVector.append left right
        RRBVectorProps.checkProperties joined "Joined vector"

    testCase "Manual test for pop right + join" <| fun _ ->
        let vL = RRBVecGen.treeReprStrToVec "M TM"
        let vR = RRBVecGen.treeReprStrToVec "[[M-3 M-3]] [[M]] T1"
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
        let vL = RRBVecGen.treeReprStrToVec "M*M-1 TM-2"
        let vR = RRBVecGen.treeReprStrToVec "T2"
        doJoinTest vL vR
    )
    testCase "another join that could break the \"last leaf is full if parent is full\" invariant" (fun _ ->
        let vL = RRBVecGen.treeReprStrToVec "M*M-1 TM/2"
        let vR = RRBVecGen.treeReprStrToVec "TM-1"
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
    RRBVecGen.treeReprStrToVec treeRepr |> manualInsertTestWithVec idx item

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
            >> setField "vec" (RRBVecGen.vecToTreeReprStr vec)
        )
        vec <- cmd.RunActual vec
        logger.debug (
            eventX "After  {cmd}, vec was {vec}"
            >> setField "cmd" (sprintf "%A" cmd)
            >> setField "vec" (RRBVecGen.vecToTreeReprStr vec)
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
        let mutable vec = RRBVecGen.treeReprStrToVec "17 16 M M M M M M M M M M M M M M M M M M 17 16 M 17 16 M M M M M M M T32"   // A nearly-full vector containing a few insertion splits
        for i = 1 to 33 do
            vec <- vec.Push i
        RRBVectorProps.checkPropertiesSimple vec

    testCase "Inserting into a full tail with empty root will not cause an invariant break" <| fun _ ->
        let vec = RRBVecGen.treeReprStrToVec "TM"
        let step1 = vec |> RRBVector.insert 7 1
        RRBVectorProps.checkProperties step1 "After one insert"
        let step2 = step1 |> RRBVector.pop
        RRBVectorProps.checkProperties step2 "After one insert then one pop"
        let step3 = step2.Remove 3
        RRBVectorProps.checkProperties step3 "After one insert then one pop and one remove from first leaf"

    testCase "Removing from the root of a root+tail vector maintains the invariant" <| fun _ ->
        let vec = RRBVecGen.treeReprStrToVec "M T5"
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
        let initialVec = RRBVecGen.treeReprStrToVec "M*M T1"
        let step1 = initialVec |> RRBVector.remove 2
        RRBVectorProps.checkProperties step1 "Vector after removing one item from first leaf"
        let step2 = step1 |> RRBVector.remove (step1.Length - 2)
        RRBVectorProps.checkProperties step2 "Vector after removing one item from first leaf, then one item from last leaf"
        let step3 = step2 |> RRBVector.insert 2 -512
        RRBVectorProps.checkProperties step3 "Vector after removing one item from first leaf, then one item from last leaf, then inserting one item in first leaf"

    testCase "removing from last leaf maintains last-leaf-is-full invariant" <| fun _ ->
        let vec = RRBVecGen.treeReprStrToVec "M*M T1"
        let result = vec |> RRBVector.remove (vec.Length - 2)
        RRBVectorProps.checkProperties result "Vector after removing one item from last leaf"
  ]

let arrayTests =
  testList "Array extension functions" [
    testProp "copyAndAppend" (fun (ArrayAndIdx (arr,_)) ->
        let expected = Array.init (arr.Length + 1) (fun i -> if i = Array.length arr then 512 else arr.[i])
        let actual = arr |> Array.copyAndAppend 512
        Expect.equal actual expected "copyAndAppend did not append the test value (512) at the right place"
    )
    testProp "copyAndSet" (fun (NonEmptyArrayAndIdx (arr,i)) ->
        let expected = Array.copy arr
        expected.[i] <- 512
        let actual = arr |> Array.copyAndSet i 512
        Expect.equal actual expected "copyAndSet did not set the test value (512) at the right place"
    )
    testProp "copyAndSetLast" (fun (NonEmptyArrayAndIdx (arr,_)) ->
        let expected = Array.copy arr
        expected.[Array.length arr - 1] <- 512
        let actual = arr |> Array.copyAndSetLast 512
        Expect.equal actual expected "copyAndSetLast did not set the test value (512) at the right place"
    )
    testProp "copyAndInsertAt" (fun (NonEmptyArrayAndIdx (arr,i)) ->
        let expected = Array.append (Array.append (Array.take i arr) [|512|]) (Array.skip i arr)
        let actual = arr |> Array.copyAndInsertAt i 512
        Expect.equal actual expected "copyAndInsertAt did not set the test value (512) at the right place"
    )
    testProp "copyAndRemoveAt" (fun (NonEmptyArrayAndIdx (arr,i)) ->
        let expected = Array.append (Array.take i arr) (Array.skip (i+1) arr)
        let actual = arr |> Array.copyAndRemoveAt i
        Expect.equal actual expected "copyAndRemoveAt did not set the test value (512) at the right place"
    )
    testProp "copyAndRemoveFirst" (fun (NonEmptyArrayAndIdx (arr,_)) ->
        let expected = Array.skip 1 arr
        let actual = arr |> Array.copyAndRemoveFirst
        Expect.equal actual expected "copyAndRemoveFirst did not set the test value (512) at the right place"
    )
    testProp "copyAndPop" (fun (NonEmptyArrayAndIdx (arr,_)) ->
        let expected = Array.take (Array.length arr - 1) arr
        let actual = arr |> Array.copyAndPop
        Expect.equal actual expected "copyAndPop did not set the test value (512) at the right place"
    )
    testProp "splitAt" (fun (ArrayAndIdx (arr,i)) ->
        let expected = (Array.take i arr, Array.skip i arr)
        let actual = arr |> Array.splitAt i
        Expect.equal actual expected "splitAt did not produce the right results"
    )
    testProp "append3" (fun (a:int[]) (b:int[]) (c:int[]) ->
        let expected = Array.append (Array.append a b) c
        let actual = Array.append3 a b c
        Expect.equal actual expected "append3 did not produce the right results"
    )
    testProp "append3'" (fun (a:int[]) (b:int) (c:int[]) ->
        let expected = Array.append (Array.append a [|b|]) c
        let actual = Array.append3' a b c
        Expect.equal actual expected "append3' did not produce the right results"
    )
    testProp "appendAndInsertAndSplitEvenly" <| fun (idx:int) (a:int[]) (b:int[]) ->
        let joined = Array.append a b
        let idx = (abs idx) % (Array.length joined + 1)
        let expected = joined |> Array.copyAndInsertAt idx 512 |> Array.splitAt (((Array.length a + Array.length b) >>> 1) + 1)
        let actual = Array.appendAndInsertAndSplitEvenly idx 512 a b
        Expect.equal actual expected <| sprintf "appendAndInsertAndSplitEvenly did not produce the right results at idx %d and with input arrays %A and %A" idx a b
    testProp "insertAndSplitEvenly" (fun (NonEmptyArrayAndIdx (arr,idx)) ->
        let expected = arr |> Array.copyAndInsertAt idx 512 |> Array.splitAt (((Array.length arr) >>> 1) + 1)
        let actual = arr |> Array.insertAndSplitEvenly idx 512
        Expect.equal actual expected "insertAndSplitEvenly did not produce the right results"
    )
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

(* let perfTests =
  testSequenced <| testList "performance tests" [
    testCase "appendAndInsertAndSplitEvenly performance" <| fun _ ->
        let idx, a, b = 47, [|1..32|], [|33..60|]
        let joined = Array.append a b
        let idx = (abs idx) % (Array.length joined + 1)
        let calcExpected = fun () ->
            let mutable list = []
            for i = 1 to 10000 do
                list <- (joined |> Array.copyAndInsertAt idx 512 |> Array.splitAt (((Array.length a + Array.length b) >>> 1) + 1)) :: list
            list
        let calcActual = fun () ->
            let mutable list = []
            for i = 1 to 10000 do
                list <- (Array.appendAndInsertAndSplitEvenly idx 512 a b) :: list
            list
        Expect.isFasterThan calcActual calcExpected "Array.appendAndInsertAndSplitEvenly should be faster than the intermediate-array version"

    testCase "appendAndInsertAt performance" <| fun _ ->
        let idx, a, b = 47, [|1..32|], [|33..60|]
        let calcExpected = fun () ->
            let mutable list = []
            for i = 1 to 10000 do
                let joined = Array.append a b
                list <- (joined |> Array.copyAndInsertAt idx 512) :: list
            list
        let calcActual = fun () ->
            let mutable list = []
            for i = 1 to 10000 do
                list <- (Array.appendAndInsertAt idx 512 a b) :: list
            list
        Expect.isFasterThan calcActual calcExpected "Array.appendAndInsertAt should be faster than the intermediate-array version"

    testCase "append3 performance" <| fun _ ->
        let a, b, c = [|1..32|], [|33..60|], [|61..92|]
        let calcExpected = fun () ->
            let mutable list = []
            for i = 1 to 10000 do
                list <- (Array.append (Array.append a b) c) :: list
            list
        let calcActual = fun () ->
            let mutable list = []
            for i = 1 to 10000 do
                list <- (Array.append3 a b c) :: list
            list
        Expect.isFasterThan calcActual calcExpected "Array.append3 should be faster than the intermediate-array version"

    testCase "append3' performance" <| fun _ ->
        let a, middle, b = [|1..32|], 47, [|33..60|]
        let calcExpected = fun () ->
            let mutable list = []
            for i = 1 to 10000 do
                let a' = a |> Array.copyAndAppend middle
                list <- (Array.append a' b) :: list
            list
        let calcActual = fun () ->
            let mutable list = []
            for i = 1 to 10000 do
                list <- (Array.append3' a middle b) :: list
            list
        Expect.isFasterThan calcActual calcExpected "Array.append3' should be faster than the intermediate-array version"

    testCase "appendAndSplit performance" <| fun _ ->
        let idx, a, b = 47, [|1..32|], [|33..60|]
        let calcExpected = fun () ->
            let mutable list = []
            for i = 1 to 10000 do
                let joined = Array.append a b
                list <- (joined |> Array.splitAt idx) :: list
            list
        let calcActual = fun () ->
            let mutable list = []
            for i = 1 to 10000 do
                list <- ((a,b) |> Array.appendAndSplit idx) :: list
            list
        Expect.isFasterThan calcActual calcExpected "Array.appendAndSplit should be faster than the intermediate-array version"

    testCase "insertAndSplitEvenly performance" <| fun _ ->
        let idx, arr = 21, [|1..32|]
        let calcExpected = fun () ->
            let mutable list = []
            for i = 1 to 10000 do
                let inserted = arr |> Array.copyAndInsertAt idx 512
                list <- (inserted |> Array.splitAt (32 / 2 + 1)) :: list
            list
        let calcActual = fun () ->
            let mutable list = []
            for i = 1 to 10000 do
                list <- (arr |> Array.insertAndSplitEvenly idx 512) :: list
            list
        Expect.isFasterThan calcActual calcExpected "Array.insertAndSplitEvenly should be faster than the intermediate-array version"
  ] *)
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
        let r1 = RRBVecGen.vecToTreeReprStr v1
        let r2 = RRBVecGen.vecToTreeReprStr v2
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
        let r1 = RRBVecGen.vecToTreeReprStr v1
        let r2 = RRBVecGen.vecToTreeReprStr v2
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
        let r1 = RRBVecGen.vecToTreeReprStr v1
        let r2 = RRBVecGen.vecToTreeReprStr v2
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
        // logger.warn (eventX "vR = {vec}" >> setField "vec" (RRBVecGen.vecToTreeReprStr vR))
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
        let v2 = RRBVecGen.treeReprStrToVec "[M 2 M/2*M-6 M-1 M-2 M-1 M/2-1] [M*M-6 M-1 M-3 M M M-3 M] [M-2 M-3 M-2 M-2 M-1 M-2*M-6 M] TM"
        doJoinTest v1 v2

    // big join, test 2 passed in 00:00:40.3750000
    testCase "big join, test 2" <| fun _ ->
        let bigNum = 1 <<< (Literals.blockSizeShift * 3)
        let v1 = RRBVecGen.treeReprStrToVec "M M-1 M*M-5 M-1*2 M T4"
        let v2 = seq {0..bigNum+2} |> RRBVector.ofSeq
        doJoinTest v1 v2

    // big join, test 3 passed in 00:00:35.1910000
    testCase "big join, test 3" <| fun _ ->
        let v1 = RRBVecGen.treeReprStrToVec "M*M/2-1 M-2 M-1 M*M/2-1 T3"
        let v2 = RRBVecGen.treeReprStrToVec "[M*M] [M-1*M-1]*M-2 [M-1 M-2 M*M-4] T6"
        doJoinTest v1 v2

    testCase "big join, test 4 (really big)" <| fun _ ->
        let v1 = RRBVecGen.treeReprStrToVec "M T1"
        let v2 = RRBVecGen.treeReprStrToVec "[[M 28 M M M M M M M 30 M M M M M M M M M 30 M M M M M 28 M M M 30 M M] [M M 27 M-1 M-1 M M M-1 M 26 M M M M M M M M-1 M-1 M-1 M M M M M M M-1 M M-1 M M M] [M M M-1 M M M M-1 M M M M M M M M-1 M M M-1 M M M M M M M M M M M M M M] [M M M M M M M M M M 28 M M M M M M M M M M M 30 M M M M M M M 29 M] [29 M-1 25 M M M 26 30 25 28 30 26 M-1 29 29 M M M M-1 M 29 28 23 22 M-1 M 28 28 29 M-1 29 30] [30 M-1 30 M 24 26 M 26 30 25 M-1 M 30 28 26 M 30 26 29 24 M M 30 M 27 M M-1 M 29 27 28 23] [27 29 26 M M M 29 25 26 M 27 M M 25 30 30 30 28 27 M M-1 M 27 M 26 M M 29 26 30 M M] [M-1 M M 25 27 M M M 26 M 29 29 M 30 24 24 M 28 28 28 30 30 M M 29 30 24 26 28 M M-1 30] [M-1 28 29 30 M M M M M M 29 23 M M 30 M-1 28 26 30 M-1 M M 26 27 28 M 29 29 26 M-1 M 28] [27 M M M-1 28 25 M 30 28 25 M 26 M M M M M 28 29 25 M M 28 M 29 M 26 27 M-1 29 M 29] [M-1 28 M-1 M-1 M M-1 M 29 M 25 M M M M M 29 30 M M-1 M M 29 M-1 24 26 20 M M 30 M 28] [M M M 28 M 27 M 24 29 M 30 29 M 30 29 27 30 M 28 29 28 30 M M 28 M 27 M 30 28 M M] [M 28 M-1 30 M M-1 M 25 M M M M 27 29 M 26 M-1 M-1 M-1 M M 30 M 28 M M 29 29 28 30 M] [27 M M 24 M-1 M M 30 M-1 30 M M 30 28 29 M M 28 M 27 M 27 30 M 29 30 M 29 30 M M-1 M] [29 30 M 28 30 M-1 M-1 M 30 M 27 27 M M M M M M M M 30 M M M 30 30 M M M M 29 26] [M 29 M M-1 M M M-1 27 29 M M M 27 M 28 30 30 28 29 M M M M M-1 M-1 M M M 29 29 M 28] [M M 28 M M M M M 26 M M M 30 M M M 30 M M M-1 M 29 M M M 29 25 M 30 M M M] [M-1 M 27 M M M M 30 M-1 M M M 30 M M-1 M 24 28 M M M 29 M-1 M M M-1 29 29 M M M M] [M 30 M M M M 28 M M M 25 M M M M M M M M M 30 M M M-1 M-1 30 M-1 M M M M M] [M 25 M M M 27 29 M M M-1 M 28 M M M M 28 M 26 M M-1 27 M M M M 29 M 27 29 M M] [M M 29 M 28 M M 28 M-1 28 28 M 28 M M 28 M-1 M M-1 M 25 30 29 M M M M M 29 M 28 29] [M M M M M 25 M M-1 M M 29 M M M-1 29 29 M 30 M M M 29 30 29 M M M M 26 M 29 M] [M 27 M M 29 M M M 28 M M M M M-1 M 29 30 M M M 26 M M M-1 M-1 M-1 29 M 25 M M 27] [M-1 M-1 28 M-1 M M M 30 M M M M-1 M M 26 30 M M-1 M M M M M M M 29 27 M M] [M M-1 M M M 26 M M M M M 27 M 29 27 M M 24 M M 28 M M M M M M M] [M-1 M M M M 30 M 27 M M M M M M-1 M M M M M 30 27 M M M M M M-1 M M] [M 29 M 29 M M M 28 M M M M 30 28 M M M M 26 M M M M M M M M M M M M M]] [[M 28 M M M 25 30 M-1 26 27 M 29 M M 26 M 29 M-1 M M M 30 M 24 28 M-1 28 M M M M 29] [M M-1 29 30 M M M M 29 21 M M M 28 M-1 M M 30 M M M M M M-1 M 28 23 M M 28 M M] [M 26 27 M M M 29 M M M 26 30 M M M 27 30 M M M 26 M 30 M 30 29 M 28 M 28 M M] [28 21 M-1 M M 30 M M 30 29 M 30 M M 30 M M M M-1 M M M M M M M 25] [M M M M 30 M M M M 29 M-1 29 29 M M-1 M 27 M-1 30 M M M M M M M 27 29 M 30 28 27] [M M M M 29 M 30 30 M M M 30 M M M-1 30 M M-1 27 M M M 27 M M M M M M 30 M M] [M M-1 M 27 M M M M M M 28 M M M 27 M M-1 29 M-1 M 28 M M 30 M 29 M-1 M M M M M] [29 30 M M M M M M M M M M M-1 M M M 30 M 29 M M-1 M M 29 M M M M M M M M] [M M M M M M-1 M M-1 M M M 29 M M M M M M M M 27 M M M M M M M M-1 M 29 26] [M M M M M M M M M M 30 M M M M M M M-1 M M-1 M M M M M M M M M M M M-1] [M M M M M M M 27 M M M M M-1 M M M 30 M M M M M M M 30 M] [27 24 M-1 29 M-1 M M-1 M M-1 25 30 28 M M 25 24 30 M 27 M-1 28 24 M 30 28 M-1 29 M 27 27 M 29] [M*M] [M 28 29 29 28 28 28 M M-1 M 26 28 27 M M 30 M-1 28 29 30 29 30 30 M M 27 M-1 28 M 29 28 25] [M M-1 M-1 21 M-1 M 28 25 30 M 30 26 M-1 28 M-1 M M 26 21 27 M 30 M-1 30 M 27 M 23 M-1 29 M M] [M M-1 M M-1 27 26 30 M M 28 M 29 30 M 30 30 24 27 M-1 28 25 M 29 M-1 M 30 M 26 27 M M-1 M] [M M M 24 30 26 M-1 27 29 M M M-1 27 M M-1 M 27 26 M M-1 30 30 30 30 26 25 M 27 28 M 30 M] [25 29 M 29 M 23 28 M-1 27 30 29 M-1 M-1 28 M 29 M M M M 30 M M 27 29 M M M M M 27 M] [23 28 M 25 M-1 M-1 M 28 30 M M 23 M-1 30 M-1 M M 28 27 M 30 M M 28 M-1 M 29 M 28 M M 30] [M M M M 26 30 29 M 25 26 M 30 M-1 30 M-1 M M M-1 28 M 29 M M M 30 M 29 28 M M M 28] [30 M 27 29 M M 27 M M M 25 M 28 M M 29 25 25 M M M 30 30 30 30 30 M M M M M-1 30] [M M 28 M-1 M 30 M M 27 M M-1 M M 29 28 M M M M M-1 27 M M M 29 25 M M M-1 M-1 M 29] [M 29 M-1 M M M-1 29 M M M M M 24 26 30 M M-1 M M M M 30 M-1 M M M 26 30 29 M-1 M 26] [M-1 M 26 M M 28 M M 30 M-1 M M M 30 28 M M M M M 30 M-1 M M 30 M M M M 26 M M] [28 M M M M 29 M M 25 M M M 28 M M-1 M M 29 30 M 28 M M 29 M M M 30 M 29 M M] [29 M M M M M 30 M 28 M-1 M M M M M M M 29 30 28 M M M M M M M M 30] [29 M 30 M M M-1 M M 29 M M M M 26 M 28 M M M M M M 30 M-1 M M M M 28] [M M M M M M M 28 M M M M M M M M M M M 30 M M M M M M M M M 27 M M] [M M M M-1 M M M M M M M-1 30 M M M M M-1 M M-1 M M 29 29 M M 28 M M M M M M] [M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M-1 M M]] [[M M 30 M M 22 M M 23 23 25 M 30 27 25 M 28 M 22 27 27 M 20] [27 30 M 28 26 25 M M-1 M M 29 M 24 30 M 27 M M 24 24 21 23 27 27 M 30 M-1] [M 30 M M M M 27 27 M M 25 M-1 M 27 25 M 29 24 26 M-1 29 M M 27 M 30 23 24 M] [M M 28 M-1 M-1 M 28 M M 27 24 30 30 24 M M 30 24 30 29 24 30 M M 30 25 30 26 27 M] [M 20 M M 26 M M 28 29 M 29 28 30 20 M-1 M M 23 M 29 M M M 25 M-1 M M M] [M 23 M M 30 28 29 24 29 M-1 29 M-1 27 29 M M 28 29 M M M 28 M 30 27 25 28 M M M M M-1] [27 M 30 27 M 29 27 M M 29 M M M M M 23 M 30 23 M M 27 M M 25 M M 30] [M 27 24 M M 27 M-1 26 M-1 28 M 25 M 29 25 M M 28 M-1 M 30 29 M 28 M-1 M M M] [M 30 30 M M 28 25 26 30 M M M M M 30 M M M M M 28 27 M M 28 M M 29 26 M] [30 28 M M M M 29 M-1 27 26 M-1 30 M M M-1 26 24 30 M M M M-1 30 28 M M M M 29 M-1] [29 M 27 M M M-1 29 M-1 M M M 28 M M 28 M M 27 M 30 M M M 30 29 M M M 30 30 M M] [27 M-1 M M M 29 M M M M 30 M M-1 M M-1 26 M M-1 M M 28 28 M M-1 28 30 M M M M 28 29] [M 29 M 30 M M M-1 M 26 M 30 M-1 M M M M 30 M 29 M M 28 M 30 M M M-1 M M M M M] [26 M M M M M 29 M M M M M M M-1 25 M M M M M M-1 M-1 M-1 M 30 M M M 25 29] [28 M M M M 30 M 30 M M 30 29 M M M M M M 30 M M M M M M M M M M 30 M M] [M 30 M-1 M 28 26 M 29 M M M-1 M M-1 30 M M M M M M M M M M M 30 M M M M M] [M M M M-1 M M M M 30 M M-1 M M M M M M M M M M M M M-1 M M M M M 30 M] [M M M M 29 M 29 M 29 M M M M M M M M M M M M M M 29 M M M M M M M] [M M-1 30 24 29 29 22 M-1 M-1 30 M M 25 29 M-1 29 M 24 29 24 27 28 M 27 M 30 28 26 29] [M M M-1 M M M M M M M M M M M M M M M M M M M-1 M M] [19 28 M 30 M-1 30 27 22 29 26 M M M M M-1 29 28 30 M M M-1 25 24 25 30 M M M 30] [24 26 29 M 30 M M M 22 27 25 24 26 M M M 23 22 29 28 M 28 M-1 30 M] [M 29 24 25 M M M 30 22 M-1 M 26 22 27 M M 29 30 29 M-1 M 25 27 M] [24 M M 29 M 30 24 29 M 27 M-1 M 29 26 M M-1 24 M M 22 M M 21 M M 29 M 26] [M 25 27 M 26 M-1 M 29 28 M 28 M-1 30 27 M M M 30 M-1 28 M 28 30 M 28 M M M 30 26] [M M 28 30 30 M-1 27 M 26 M 25 M 25 28 M 26 M M 30 30 20 30 26 M] [29 M M M-1 27 30 M M-1 M M M 29 M M M M M M 21 28 26 M M-1 30 28 28 26]] [[29 26 M-1 M 30 28 24 M M-1 27 M 26 M M 27 M-1 28 M M 25 29 30 M 28 28 26 27 M 28 M M 25] [28 28 29 25 25 M 22 22 24 27 M-1 29 M M-1 M M 30 26 M M M-1 29 28 M 30 23 29] [30 M 29 27 30 29 27 M-1 M 21 26 27 28 29 28 25 27 28 24 M] [27 M M 26 26 M 25 M-1 28 M 29 26 29 29 29 26 26 30 24 30 27 25] [26 M M M 29 28 M 21 30 M-1 M M 27 M-1 M M 26 M M 26 26 M 29 28 M 30 M-1 M 25] [23 30 24 M M M M 30 30 M 28 M 28 M M M M-1 21 M 30 M M 26 M M 30 M 24 M 25 27 M] [M 25 M 27 M 29 M M 26 26 25 M M M-1 M M M 28 M M-1 29 M-1 M 30 M M-1 29 30 M 26 M] [M M M 22 M 30 30 M M M M-1 M 30 30 M M M 24 M 29 25 M-1 21 28 M 28 26] [M-1 M M-1 M 30 M M M-1 M 30 M M M 27 27 M M 28 M 30 M-1 M-1 28 29 M-1 28 M M-1 30 26 M 30] [26 M M M M 30 M M 29 M M-1 M M M M 30 M 26 M 24 M-1 25 29 M M 28 29 30 M-1 30 30 M] [M 26 M 28 M M 29 29 25 25 27 M-1 M 26 29 30 M-1 28 M 29 M-1 M M M M 26 M M-1 M 28 23 28] [M-1 27 M 27 M M-1 M 23 27 M M M M 23 30 M 27 M-1 M M M 26 25 M 24 M-1] [29 M 29 M M 30 29 20 M-1 M 27 M M 27 M 28 M 28 26 26 30 M M 29 26 M M-1 M M 30 M 30] [27 M 28 28 M M M M M 29 M M M M 29 27 M 28 25 M M M 28 30 25 26 M M M-1 M] [M-1 28 30 30 27 23 30 M-1 30 M 29 26 M M 28 M M M M M 26 29 29 27 M M M M-1 M-1] [M M 28 M M M M M 26 M 25 M 28 26 30 M M-1 M M-1 23 27 M M M M M M] [30 M 21 M 29 29 28 29 M M M M 27 M M 28 M 25 29 M M M 30 M 30 M M M M-1 M M M] [M-1 M M M-1 M-1 28 28 M M M 27 26 25 M M M M M-1 M 30 M M 29 M M M] [30 M M 24 M M M M M M 26 25 30 M M-1 M M M 25 30 M M M-1 M M M 28 M M M] [M M M-1 30 M M M M M 29 M M-1 M M M 26 M M M 30 M M M M-1 27 28 M M M] [M M M M M-1 M 30 M M 30 M M 30 26 M M M 23 M-1 M M-1 26 M M M M M] [M M M-1 27 M 27 M M M M M M M M M M M M M M M-1 M M M M M M 30 M M-1 M] [25 M M-1 M 27 24 24 M 30 M M M 29 M 23 M M-1 M 29 M M M 30] [M 26 27 M-1 28 M M 30 M 30 M M 29 M M M M 23 30 27 M M 30 29 26 M M M 29 M-1 M M]] [[30 M-1 26 M M 29 M 29 M M-1 M 29 M-1 24 M-1 M M M M M 29 29 M M M 29 M M M M M] [M M M 30 M M M M M M M M M M M M M 29 M 28 30 M 30 M-1 M 28 27] [M 30 M 29 M M M M M M M M M 28 M 30 M M M M 25 M M 28 M M M M-1 27 M M M] [25 30 M M M M M M M M M M M-1 M M M M M M M M M M M M 29 M M M M M] [M M M M-1 M M M M-1 M M M M M M 30 25 M 30 M M M 30 M M M 29 M] [M M M M M M M M M M M M-1 M M 30 M M M M M M M M M M M M M] [M M M M M M M M M M M M M M M M M M M M M M M M M 24 M M M] [27 M 30 26 19 M 25 30 M M 27 27 M 20 30 M 24 M M-1 29 28 26 30 M-1 27] [28 24 26 30 28 28 M M-1 26 29 M M-1 M M 27 26 M 25 29 26 30 M M 26 29 19 28] [30 30 26 M M 30 M M 26 26 M-1 M-1 M 28 28 27 M-1 M 29 28 28 M M M-1 M 27 26 28 29 26 29 M] [M-1 22 M M 21 26 M 24 M 24 23 21 M 24 M-1 30 M 23 M M M M M] [M-1 21 M 29 M M 28 M 27 28 30 29 M-1 29 30 M 29 22 M-1 M-1 M 27 M 30 27 M M 29 M M] [M M-1 M 25 23 28 M M M M 25 23 M M 26 M M 30 26 24 28 M M M-1 M 29 30 27 M M] [M 27 27 28 M-1 29 30 M-1 M M M-1 M-1 27 25 23 29 25 M-1 M M-1 M 29] [M 28 28 M M M-1 25 M M 28 M-1 M 30 M 28 29 25 M 30 30 M 29 28 M M M 29 28 M 27 M-1 29] [M 25 27 M M M 30 M 24 M M 28 M M 27 30 M M 29 M M 28 M M-1 29 M M 27] [28 M 27 M-1 29 30 M M M M 26 29 30 M M M 27 30 M 23 M M M M 29 23] [M M 30 M M M M 30 30 27 M M 27 M 30 M 30 M M 28 28 M M M M-1 M 29 28 29 M] [30 M M M M 30 26 M-1 30 M M 29 M 21 M M M-1 M M M M M-1 26 M M M M-1 30 26] [M M 27 M M M M 30 M M M 30 29 M M M M M 24 30 M M M M M M M 30 27 M M M] [M M M 28 M M M M 30 M M 28 M 28 29 29 M M M-1 M-1 26 M-1 M M M M 29 M M M 30 M] [M M M M M M 30 M M M M 30 27 M M M M M M M 24 M 30 M M] [M-1 M M-1 M-1 M M M M M M M 28 M M 30 30 M M-1 M M M 29 M 30 28 M-1 30 M M] [28 M M M M M M M 30 M M M M M M M M M M M-1 M M M M M M M M-1 M M M M-1] [M M M M M M M-1 M M M M M-1 M M M 28 M M M M M M 27 M M 29 M M] [M 29 M M 28 26 29 M M M M 30 M M-1 M 30 27 M M 29 M M M M M M 29 27 M-1 M M 29]] [[M M M 28 M M M M M M M 25 M-1 M-1 27 M M 29 M M M 30 30 M M-1 M 25 27 M M M-1] [30 M M 30 M 29 29 M M M M M-1 M M-1 M M M M M M M M M M-1 M M-1 M 29 29 M M 28] [M M 30 M M-1 M 27 M M M M M M M 30 M M-1 26 M M M M M 29 30 29 29 M M M M-1] [M 28 29 M M M M M M M-1 M M-1 M M M M M 28 M-1 M M M M M M] [M-1 29 M M 29 M 30 M 29 M M M M-1 30 M M 29 M M M-1 M] [30 M 30 M M M M M M M M M M M M M M M M M M M M M M M M M M M M M] [M M M M M M M 28 29 M M 30 M M M M M M-1 M M-1 M M M-1 M M 30 28 M 28 28 M 27] [M-1 M M M 25 M M-1 M 30 M M M-1 29 27 M 27 M M M M-1 M-1 M M-1 M M-1 29 M M M M 28 M] [M M M M M 25 M M-1 M 28 M-1 M M 29 M M M 29 M 30 M M M] [M 29 M M 27 M M M 29 M M M M M M M M-1 M M M-1 28 M M 27 M M M M-1 M 30 M-1] [M M M M M M M M 27 M M M M M M M M M M M M 29 M-1 M M M M M 30 M M M] [M M-1 M M M 30 M M M M 29 30 M M M M-1 M M M M M M 28 M M M-1 M M 30 M M M] [M-1 M M M M M M M M M M M M M M M M M M M M M M M M M M M] [M M M M M-1 M M M M M M M M M M M M M 30 M M M M-1 30 M M] [29 M-1 M 29 30 28 24 26 25 M 30 26 M M M-1 M-1 30 M-1 26 M M 27 M 26 M 27 M 26 M 29 23] [M 27 30 22 M 30 30 27 22 M-1 29 M M 24 M 29 M 30 M 27 M M M 26 27 26 M M 29 24 26 M] [M 30 28 M 29 30 28 28 29 M-1 30 25 29 M M M-1 M 21 26 29 M 30 M 30 M M 24 30 M-1 M M 26] [M M M M M M M M M M M M M M 25 M 26 M M 30 M M M M M M M] [M 29 M M 29 M-1 M M M M 23 M M M M M M M M M 29 M M M-1] [M-1 M M-1 30 M M M-1 M M M M M M M M M M M M M M M M M M M M M M] [M M M-1 M 29 M M M M M M M M M M M 29 30 M M M M M M M M M M M M-1 M M] [27 M 28 29 M M 26 28 M-1 M M 26 M-1 25 22 M M M-1 27 M 28 M 23 28 29 M-1 27 M 29 23] [M*21] [28 24 30 29 M M M 24 30 M 26 M 30 M-1 26 M-1 27 24 M 27 28 M 25 25 30 M M M M]] [[M 29 M M-1 27 M 28 27 27 22 M 28 M 24 M-1 M M-1 M M M M 28 M 27 M M M-1 28 M-1 M 29 M] [27 M M 22 M M 28 M M 20 M M 24 M M 24 M M 30 28 M 25 M M M 22 M M] [28 M M M M M-1 28 29 M M M-1 30 M M M-1 25 M 29 M M-1 M M 24 28 M M 30 M 28 22] [29 27 M 27 29 M M-1 25 M 30 M 29 M-1 M M M-1 M M M 29 25 M 26 M 24 M 27] [M M M 30 28 M M M M 28 29 28 30 28 M M-1 27 M-1 M M 30 M M-1 25 27 M] [M M M 25 26 M 27 M-1 M-1 M M 27 M 27 M M 29 M M M-1 24 30 27 M M-1] [M M M M 27 26 M 30 M M M 29 M-1 M M 30 30 28 M M M-1 28 M-1 29 M M M M M-1 M M M] [M M M M M M 28 M 28 M M 28 30 28 M 30 29 M 28 M 30 M M M M 29 M M 30 29 29 M] [M M M M 30 M M 29 M-1 30 M M M M M M 29 29 M M M-1 M M 29 M-1 M-1 M M M M M 27] [M M M-1 M 30 M M M M M M M-1 24 29 M M 29 M M 29 27 M M M M M M-1 M 29 M] [29 M M M M M-1 M M M-1 M M M M M M M M M M M M M 27 M M M 29 M M M 30 M] [M M M M 24 M M M M M M M M M-1 30 M M M M M M M M M M M 29 26 M] [M-1 M M M M M M M M 30 M M M M M M M 30 M M M M M M M M M M M M M M] [M M 27 M M M M M M M M M M M M M M M M M M 28 M-1 M M M M M M M M M] [27 M 30 28 M-1 M-1 M M M M 26 22 29 M-1 30 29 20 26 24 26 23 29 M M 26 M 29] [M*29] [27 25 27 29 M M M M M 24 M 30 29 27 23 M 28 30 28 29 29 M 30 M 26 M M M M-1 27 M 28] [25 22 29 M M 28 30 29 M M 28 28 M M M 28 30 24 M 28 M 25 24 M-1 30 27 29 28 M M M-1 M] [28 30 M 21 M-1 M 24 M M M 26 M 26 M-1 M 30 M M M 29 M 30 21 M-1 M 19 M M] [28 M 30 M M 27 28 27 M 27 26 22 M M M-1 29 28 29 M M M M M M 30 29 22 M M M M 23] [30 29 24 30 M 25 M 29 M 30 M M-1 27 M M 30 30 30 M M 25 M 28 M 23 M M 30 M M M M] [23 M M M M M M 28 26 27 28 30 30 29 M M 24 27 M 30 25 M 26 25 30 M M M-1 28 27 26] [30 29 27 27 23 M M-1 28 M 26 23 28 28 21 M 27 30 28 26 M 27 M 28 M-1 M M 29] [25 30 27 M M-1 M 29 22 26 M 28 M M 28 23 24 M M M 27 M M 24 28 M M 30 M M M] [30 30 25 28 30 29 30 26 23 28 29 M M M 28 30 30 29 28 30 30 30 M M M 28 M-1 M 30 25 28] [M 30 M 27 M 27 28 M M 27 M M 27 M M M 30 M M 20 27 28 M M M 25 30 29 25] [M 26 M 29 M M M 30 29 M-1 M 25 M-1 M 24 25 27 M 29 M-1 M M-1 28 25 M 30 M-1 29 27 M M 29] [30 30 30 M M M M M M-1 22 M M M M 29 M-1 M 28 M-1 30 M M M-1 28 27 M 29 27 26 M M 26] [28 M-1 28 M M M 29 M M-1 M 28 30 M 27 27 M M 30 30 M 26 26 28 M 28 30 M M M 29 27] [M-1 M M 25 M M 28 M M 27 M M M M M 22 M M M M M 23 29 M M 26 M 29] [30 29 M M M-1 25 M M M 26 29 M M M M 27 27 M 30 M-1 27 M-1 M M M M 28 M 28 M M 28]] [[M M M M M M 26 28 M M M-1 M M-1 M-1 25 M 27 M 29 25 M M 26 25 26 M M 29 30 26 M-1 M] [30 30 M-1 M M 28 27 28 30 29 30 M-1 M M M 28 26 M M M 28 M 28 M-1 25 M-1 28 28 29 30 29 30] [30 M M M M M M M M M M M M M M M M M M M M M M] [25 27 M-1 27 28 M M 28 M M 28 28 29 M 30 M 30 25 M 26 28 M M 22 M M 26 26 29] [26 M 25 M M 27 M 27 M M 30 30 29 M 28 29 28 28 M-1 24 24 M 28 M M M 30 29 23 27 M 29] [29 M 29 29 29 M 25 27 M 29 M 27 28 29 27 M-1 M 28 27 M 30 30 M 26 M-1 M-1 30 M M-1 7] [25 30 26 M M M M M 26 M 27 M-1 30 30 M M 30 27 28 M-1 M M-1 M 26 27 28 M M M 27 M 28] [M-1 M M M M M M M 29 M M M M 29 M 29 M 30 26 M M M M 24 24 26 M-1 M M 30 30 24] [26 M-1 M-1 M-1 M M 26 M M M M 29 25 M M 28 M 29 M 27 30 29 M 29 28 28 M 27 M M M] [M M M M 30 M 27 26 29 M 30 M M 30 M M M M 30 25 24 M M-1 M M M M M M 27 M] [M M 26 M 29 M 29 26 M M M M-1 M M 25 M 28 M M M M M M 24 M-1 30 26 M M M-1 M] [26 30 27 M M M 28 M 27 M 29 M-1 26 26 M M M 29 23 28 M 28 M 25 M M M 29 25 M 29 28] [M 29 28 28 26 M M-1 M M M 23 25 M 27 M M M M-1 29 24 24 24 M M M] [M 23 30 M 28 M-1 M 29 20 M M-1 M M M 28 M M 23 27 M 27 30 M 27 M M 25 29] [27 M M M M 29 30 M M 26 24 M M 28 26 28 M M M M M-1 M M M M 29 M M-1 27 M 23 M] [M-1 29 27 27 M M-1 M M M M-1 28 30 26 30 M 27 M 30 M 28 M M M-1 26 29 27 M M-1 27] [M 28 30 M-1 M M M M M 30 29 28 M-1 M 29 M 28 29 30 28 M M 29 M M 28 28 M M M-1 M-1 M-1] [M 27 26 M M-1 27 M M M-1 29 M-1 27 M M M 29 27 M 28 27 M M 29 M M M M M 27] [M M M M M-1 29 M M M M M 24 M M M M 30 M M 28 28 M M 27 M M 24 30 M M] [M 28 30 26 M M M-1 29 M M-1 30 30 M M 27 M M 26 28 M M M M M M M-1 30 30 M-1 M M M] [M M 28 M M M M M M M 28 29 M 29 M-1 M M M 28 26 M 30 M M M] [29 M-1 M 30 M M-1 M M 29 M M-1 M 28 M 29 M M M 29 M 30 M M 29 M M M 30 M M 28] [30 M M M 30 M M M M M 29 M M-1 M M M 29 M M M M M M M M 30 M 28 M M M] [M M-1 M M M M-1 M 27 M M M-1 M M M M-1 M M M M M-1 M M M M 29 M 26 M-1 M M M 30] [M M 30 M M M M 30 M M M M M M M M M M M M M M M M M M M 29 M M M M] [M M M 29 M-1 M M M M M M M M M M-1 M M M 28 30 M M M M M-1 M M M M M M M] [30 M M 30 26 27 30 30 26 28 M 25 27 25 M-1 M 24 M M-1 25 26 M 26 M M M 25 28 28 M-1] [M M M 30 M M M M M M M M M M M M M M M M M M M M M M M M M M M M] [25 M 30 29 26 30 M 30 29 M 26 29 25 24 M 30 M-1 27 M-1 29 M M M M M-1 27 M 26 M 30 25 M-1] [M 24 30 30 M M M M-1 24 30 26 M 27 M 27 M 27 26 30 M 24 27 29 M M M 29 M-1 27 27 26]] [[M M-1 M M M 27 M 30 29 M 25 M-1 M 25 M M 30 M M 27 M M 23 M M M] [M 27 M M M 30 M M-1 29 M M-1 30 29 M M 26 26 30 M 25 M M M M 26 29 M M M M] [M M 29 M 27 M-1 M M M-1 M-1 M M M M 28 M 29 M M-1 25 M M M M M M M M-1 30 M M 29] [M 30 M M-1 29 28 M M M 30 M 29 29 26 M 28 M M M 29 M-1 M M 28 M-1] [M M 29 28 M M 30 M M M 29 M M 28 M M M M M-1 M M M M M 29 30 M M M M M M] [M 30 M M 29 M M M M 28 30 28 M M 28 M M M M M M M 29 29 M M M M M-1 M M M-1] [M M M M M M M M M M 30 M M M M M M M-1 28 M M M M M-1 M M M M M 28 M] [28 M M M M M M M 30 M M M M M M M M M M M M M M 27 M M M M M 26] [M M-1 M M M M M M M M M M M M M M M M M M M M M M M M M M M M M-1 M] [M 30 M 28 M M M M M M M M M M M M M M M M M M M M M-1 M M M M M M M] [30 30 25 M 30 29 29 29 M-1 29 M 29 24 29 26 M-1 28 M M 29 28 27 28 28 M 28 28 M M 26 29] [24 M-1 M 26 M 27 M 30 M-1 25 26 M M 26 M 24 M 29 28 M-1 26 28 M M-1 M 28 M 28 27 M-1 25 29] [M 29 M M 24 M 29 24 M-1 M M 29 M M M 30 28 29 M M 28 25 M-1 29 M M-1 27 28 24 25 M 29] [30 27 M 27 M 30 M M 30 25 26 23 M 26 21 20 M M 28 M-1 M 30 26 29 M M M] [30 M M 29 28 26 M 28 30 30 27 M 26 27 M M M 25 M M M 24 26 M M 30 M M 30 M 28 M] [28 M-1 M M M M 28 30 25 M M M-1 26 27 M-1 30 30 30 27 M-1 M-1 M 29 24 29 M M-1 M-1 M 30 28 25] [M 28 M M-1 M M 29 25 28 29 M M M M 27 M 25 M 25 29 24 M-1 M M-1 M M 29 M-1 M M M M] [22 25 M M 29 M M M 21 30 30 M 29 M M 30 30 M 27 M M M 29 M-1 29 M M M 25 30] [M M-1 M-1 M M M M M 24 29 30 30 28 23 29 M M 30 M M-1 27 M 27 M M M 30] [M M 28 M 26 29 29 26 M M M M M M 28 M M 27 30 M-1 29 M M M M M-1 26 M 29 26 M] [M 29 M M 29 M M 30 M M M M-1 M M M 25 28 29 M 28 28 25 M M M M M] [M-1 M M M M 26 M M M M M M 30 M M 28 M M M 29 29 23 30 M 29 M 30 M M 25 M-1 M] [M M M M 28 29 M M M M M M 25 M M M-1 M 27 M M M 29 M M 27 M M M M M M]] [[23 M M M M M M M M M 28 M M 25 M M M-1 M M 24 M M 26 M M M-1 29 M] [30 M M M M M 29 M M 28 M M M M 24 M M M M M-1 M 30 28 30 M 29 27 M M M 26 28] [M M 26 M M M M M M M M M M M 29 M-1 M-1 29 M M M 26 M 29 30 M 28 M M M M] [27 M M M M M 30 M 30 28 M M M M 30 M M M M-1 29 29 27 26 M M-1 M M M M M M] [M M-1 M 30 M M-1 30 M M M M M-1 29 M M M 27 M M M M-1 M M M-1 M M M M 30 M] [30 M M M M-1 M 30 28 28 M M M M M 28 M M M 26 M M M M M M M M M M M-1 M M] [M M M M M M M M M M M M M M 27 M M M M M M M M 28 M M M M] [M M M M M 30 M-1 M M-1 M M M M M 28 28 30 M M M M M M M M M M M M M M M] [25 M 30 21 25 27 28 M-1 M 24 30 M 24 M-1 M-1 M 28 M 26 23 29 M 30 M M-1 26 M 30 27] [M M M M M 28 M 28 M M 30 M-1 M M M M M 30 M M-1 26 M M 30 M M M M-1 M 25 M 29] [M M M M M 27 M 26 M M 29 M M M M M M M M M 24 M 29 30 M 28 M M M M 28 29] [28 M M M M M M-1 M 28 M M M M M M M M M M M 27 M M 24 M M M M-1 M M M] [M M M M M M M 30 26 M M M 30 M 26 M M M M M M 27 29 M 28 M M] [M-1 28 M M M M M M M 30 M M 28 M M M M M-1 M M M M M M M M M M M M M M-1] [M M M M M M M M 28 M-1 M M M 26 M M M M M M 28 M-1 M 30 M M M M M M M M] [M M M M M M M M M M M M M M M M M M M M M M 29 M M M M M M M M] [M M M M 29 M M M M-1 M M M M M M M M M M M M M M-1 M 29 M M] [M M 24 26 30 M-1 M M 27 M M M 23 M 30 M 26 26 21 30 M M-1 M 28 26 24 24 30 30 M M] [M 30 26 22 30 29 29 M 28 29 M-1 26 28 M M 29 27 30 28 22 26 M 30 M-1 M 25 26 M 28] [27 M M 30 M-1 29 M-1 M M M 28 24 23 23 27 M M M 30 28 M 28 29 M-1 29 M-1 23 M 27] [M M-1 M 24 23 27 M M M-1 M M 29 26 29 M M 25 M M-1 26 25 M M M 25 29 24 23 M M] [27 M 28 28 29 30 M-1 M 29 M-1 30 29 M M M M-1 M-1 M-1 28 28 26 29 29 M M M M-1 27 28 M 24] [M 24 28 M 28 28 28 M M-1 M M 27 M-1 29 27 M 28 30 24 26 M 29 M 26 M-1 M 29 M M M M 30] [30 26 M 30 30 M M M M-1 M-1 20 M 30 M M M 26 M 29 M-1 27 M-1 28 30 M-1 M M M 29 M 27] [M 30 M M M 30 30 26 M M 28 29 28 30 M M 29 22 27 M M 29 26 M M-1 M M M-1 30 24] [M M M M M M 30 28 28 M M 27 30 M-1 M-1 M-1 30 M 29 M M M M 29 M 29 M 29 M 28 21 M] [30 M M M M 29 25 28 M 30 M M 28 M-1 M M M-1 24 27 M M M M-1 M M M-1 M 26 30 M M 25] [M M 26 24 M-1 M M M M 23 30 M 28 M M M 28 M 30 M M M M M M-1 M-1 M-1 M M]] [[M M M M-1 29 M M M M M-1 M 27 30 M-1 29 M M M M M M 28 M 30 M 26 29 M 30 29 29 28] [30 28 M M M M M-1 M-1 21 27 M M M M-1 26 M 29 M M M 28 22 M M M M M M M] [M M M 26 M 30 M M 30 M 29 30 M 27 M 30 M M M M M M 29 M M M 30 M M M 26 M] [M M M-1 M M M-1 M-1 M M M-1 M M-1 M-1 M 25 M M 24 M M M 30 M 25 30 M M-1 M 27 M M M] [M-1 M M M 26 M M-1 M M M M M M-1 30 M M M M M 30 M M-1 M M M M M 30 M M M-1 26] [M-1 M M M M M M-1 M M M 26 M M M-1 29 M-1 M M-1 M M M M M-1 29 30 M 30 30 M M M-1 29] [M M M M M M M M M M M M M M-1 M 28 M M M M M M 30 28 M M M M M M 30 M] [M M M-1 M 29 M 28 M M M M M M M 29 M M M M M M M 30 M 30 30 M-1 M M M M] [M M M-1 M M M M M M M M M-1 M M M M M M M M M M-1 M M M M M M] [M M M M M M M-1 M 29 M M M M M M M M M M M M M M M M M-1 M-1 30 M M M M] [M 29 M 24 M-1 30 26 M-1 29 29 M 29 26 27 M M-1 M M 28 27 26 M M-1 29 29 28 25 29 29 M 26 30] [M M-1 27 23 27 M 26 M-1 M 27 M 27 27 30 25 M 28 M M 26 27 24 M M 29 M 26 M M-1 28 M 26] [30 M M M M M M M-1 M M M M M-1 M 28 M M-1 M M M 30 M-1 26 M M M M 28 M 30 M M-1] [M M M M M M M M M M M M M M M-1 28 M M 30 M M M 29 M M M M M M M M M] [M M M M M-1 M M M 27 M M-1 28 M M M M M 30 M-1 M-1 M M M M M M M] [M*27] [28 M M M M M M M M M M M M M M M M M-1 M M M M M M M M M M M M M M] [M 28 M 29 24 30 27 29 30 M 23 M-1 28 M-1 25 M 30 M-1 27 29 30 28 M 28 29 M-1 25 27 M] [25 25 M M 28 30 24 M 29 M M 27 M M M 27 27 M 29 25 28 24 M 26 M M-1 30 M-1 26 27 M 30] [30 M 25 26 M-1 M-1 M M 28 M 30 30 29 M 25 27 29 M-1 M M-1 M M M-1 27 24 M 29 M-1 30 26 M 27] [28 M 29 M-1 24 27 29 M-1 28 M 28 26 30 29 M 30 30 M 28 M 25 29 M 28 29 26 27 M M M M-1 M] [M M 27 M-1 29 M M M 29 23 22 M M M-1 M-1 M 29 26 27 M M M M M M-1 28 28 29 25 M] [27 26 26 M-1 26 24 M-1 30 M 24 M-1 M 29 25 28 M-1 M-1 M M 29 30 28 M M 30 M M] [25 M 30 M M 29 M 28 M 30 M M M 29 M M M-1 28 M M 27 28 29 M 26 29 M M 29 30 30 30] [24 M 30 28 M 27 29 29 27 27 28 M M 28 M 21 27 M M M 26 M M 29 26 26 M-1 M 25 M 30 M] [M 26 27 30 28 28 28 M 28 30 26 25 30 M M-1 M-1 27 25 M-1 M M-1 27 M-1 27 28 26 27 28 30 M M M-1] [29 30 29 27 M M M M M 25 M M-1 M 25 M 27 M 27 30 29 26 M M-1 30 29 25 30 29 29 30 29 27] [23 29 M M 30 M-1 M-1 26 M 24 30 29 M M M 29 M 24 M 27 M 28 28 M 28 25 24 M M M-1 30 28] [M 29 29 28 M-1 M M M M 24 27 M 30 29 M M-1 M M-1 26 30 26 M-1 M 28 M 25 30 M 30 M 28 27]] [[M 27 M M-1 26 M M M 26 M 29 30 27 M-1 28 27 M 29 M 30 M-1 M 22 23 23 M 28 30 25 28 M] [M 30 M 29 M 30 M 27 29 20 29 M 29 29 30 25 28 29 M M M-1 26 30 M 30 30 M M-1 M M-1 26 M-1] [M 27 29 27 M M 26 27 M M M 27 M 25 M 29 28 26 26 M M-1 M-1 29 26 M M 26 M 29 M 30 30] [M M M M 29 27 M M M-1 M M 30 26 28 M M-1 M-1 M M 26 30 26 M M M 28 M 23 23 27] [30 M M M M-1 M 30 23 M 27 29 M 30 27 M M M M-1 M 25 23 M 27 M M M 19 29 30] [M 30 M M-1 30 M-1 M M M M 28 M 29 M-1 28 30 M-1 M 28 M 25 M-1 27 M M 29 M-1 28 M-1 M-1 30 28] [M 29 M M 28 29 26 M M M-1 29 29 M 28 M 28 M-1 M 28 M-1 M M M 27 M M 30 26 27 M M 28] [M M M-1 M 29 26 M 30 M M M M M M 28 30 M M 30 28 M 30 M M 27 M M M 29 25 29 M] [M M 30 M 29 M 25 M-1 30 M-1 M M M M 24 M 30 M M 30 28 M-1 27 M M 30 M-1 M 24 M] [M M M M 26 27 28 M M M M M M M M M 26 26 30 29 M-1 M M M M M M M-1 M M M] [M M-1 M M M M M-1 M M 27 27 28 M M M-1 28 M M M M M M 24 25 M M M M 30 M M M-1] [M M M M M M M 27 M M M M 25 M M M M 28 M-1 30 M M M M 28 M M M M M M-1 M] [29 M 30 29 M 28 M M M M M M 27 M M M M M M-1 M M M M M 21 M M M M M M] [M M M M M M M M 25 M 30 30 M 30 M M M-1 M M M M M M M M M M] [M-1 M M M 26 M M 29 M M M M-1 M 30 M M M M M M M M M M M M 26 M M] [M M M M M M M M 30 M M M 30 M M M M M M M M M M M M M M M M M M] [M M M 30 M M M M M M-1 M M M M M M M M M M 28 30 M M M M M M M M M M] [30 28 28 M 30 M-1 30 21 24 30 M M-1 28 M M-1 M 27 M M M 30 25 M-1 30 M-1 M-1 27 28 26 28 30 24] [M 26 M M 26 19 M 26 22 M M 29 M M-1 24 M 30 22 30 M 24 M M 25 21 M 30 M] [24 M M 22 M-1 M M 28 M-1 30 28 M 30 M M 25 29 27 27 M 30 28 27 M 27 M-1 M 28 M M-1 28 30] [M M 26 M-1 M M 30 29 M-1 M M 27 27 25 M 25 26 30 24 M M M M-1 27 M-1 27 27 M 26 26 29 M] [M M 28 30 M 30 29 M 30 30 M M M M 27 M 25 M 29 28 23 M M-1 M 27 M 26 28 29 29 M 27] [M 27 M M 26 28 M 30 30 M 28 M 28 29 21 M M-1 30 M-1 M 25 M 26 29 26 M 26 30] [28 M 26 M M M 30 30 25 29 29 M M 28 28 24 26 M M M 30 24 M M-1 M M]] [[27 25 29 M 28 28 29 29 26 29 M-1 M M-1 26 23 28 27 27 M M 29 30 M M-1 24 30 29 M-1 M-1 M M] [25 27 29 M M-1 29 28 28 M 26 M 25 23 30 28 M M M M-1 M M M-1 M-1 M-1 M 29 29 M M M 28 26] [29 29 M 29 29 26 M-1 22 24 30 27 M-1 30 26 M-1 23 30 M 27 M M 28 M M M M M 29 30 M-1] [30 M M 27 M M M M 26 M-1 28 M 28 M 30 27 M 26 29 M 25 M 25 M M M-1 M 23 M M 29 M] [26 26 M M M 29 M 26 26 M M M-1 26 27 M-1 27 M 29 M 30 26 M 29 28 M 30 M-1 30 M 30] [30 M-1 M M 30 M M M 28 M M-1 M 28 M 29 26 29 M M M M M 26 26 21 M M 26 M M M] [28 M 25 M 29 M 29 26 M M M 30 25 M 30 30 M 29 M 27 M M 30 28 M M M 26 M 28 M M] [M M M M M 26 M-1 M M M M-1 M M M 28 29 29 M M M M 21 30 29 M M-1 M 28 M 27 M-1 M] [M 29 M-1 M M M 25 30 29 M M-1 29 M M-1 28 M 26 M-1 28 27 M 25 28 27 30 M M M-1 M-1 25 29 M-1] [28 M-1 28 M 23 28 M 26 M-1 26 27 26 28 24 M M M M M M M M M M 26 29 29 M 25 M] [30 M-1 26 M M 25 M-1 M M M M 27 29 M 29 28 M M-1 M M M M-1 29 28 30 27 27 28 M M-1 M 28] [M M-1 28 28 M M 29 M 30 M 29 M M M M M 25 M-1 26 28 M-1 24 26 M M 27 M M 23 29 M] [25 M 30 M M 26 M 28 28 M M M M-1 M M M 27 M 27 M M M 28 24 27 M M M-1 M M] [M M 23 M M M M M-1 29 M M M 29 M 28 M-1 M-1 M 26 24 27 M 27 M M M M M M 28 25 M] [M M 29 M M 30 M-1 M M M M M 27 29 M M M 28 M 29 27 27 M M 29 M M M 24 M] [M M 28 29 26 M M 26 27 M M 30 M 26 M 30 M M M M M M M-1 M-1 M M M-1 M 28 29 30 M-1] [M M M M 29 M 30 M M M M 29 28 M 30 M-1 30 M M M M 25 M M 28 M M M M M-1 M 29] [30 M-1 M 26 30 27 M M M M-1 29 M M M M M M M 30 M-1 28 M M-1 M M M-1 M-1 27 30 M M] [M-1 M-1 M M M M 29 M M M M M M M M 23 M 27 M-1 M M M M M-1 M M M M M-1 M M M] [M M M 27 M M 29 30 27 M M M 27 M 28 M-1 M M 28 M M M M 26 26 M 30 26 27 M 30] [25 M 20 29 M M M-1 M 22 28 28 M 29 M M 30 M 30 29 M M-1 30 M 30 28 M-1 M M M] [M-1 29 M M M 26 M M M M M-1 30 28 29 M 27 27 M M M M M 26 25 M M M M 29 M M M] [M M-1 M M-1 M M 28 M-1 26 M M M 30 30 M 29 26 M 30 M 29 M M 28 M 29 M-1 M 29 M-1 25 M-1] [M 27 M M M-1 30 27 M M M M 29 M M M M M M M M 28 M-1 M M M-1 29 30 M 29 M 29 29] [M 30 M-1 M M M-1 30 M 29 M M M M M-1 30 M M 26 M 29 M M 29 M 27 M-1 29 M 28 M-1 M 29] [29 M M 30 M M-1 30 M 30 M M M M M 30 30 M M M M M M 27 M M-1 30 M M M M 28 M] [M M M M M M M M 25 M-1 M M M 28 29 M-1 30 30 M M-1 30 27 M M 29 M M] [29 30 M M M M M M 30 30 M M M M M M M M M 29 M M M M M 30 M-1 M-1 M M M M]] [[M M M-1 M M M M M M M M 30 30 M M M M M M-1 M M 28 30 M M M 26 M M-1 M M-1 M] [29 M M M M M 25 M M M 30 M M M M M M M 28 28 M 30 M M M M M M M M 29] [M M M M M M 30 M M M M M 29 M M M M 29 M M M M M M M M M M 30 M M M] [M M M M M M 30 M M-1 M M M M 29 M M M M 30 M M M M M M 30 30 M M M 29 M] [M*M] [M M M M M M M M M 27 M M M M M M M M M M M M M M M M M M M M M M] [27 30 M 30 M 26 30 28 M 26 25 M 30 M 25 30 M 29 25 29 M 30 M 28 30 27 M-1 28 28 M-1 27 M] [M-1 29 M 27 29 30 25 29 M M-1 M 29 M 27 M 29 27 26 25 M-1 28 29 29 28 27 30 26 30 27 M-1 M M-1] [26 M-1 M-1 29 28 25 M-1 M M 27 M 28 M M 28 28 M M M 26 M-1 22 M 28 M 27 30 M M 27 M 29] [26 30 29 25 29 25 29 27 M M M-1 M 30 19 M-1 27 M M M M M M-1 24 M M M M 21 M 30 M-1 M] [M M-1 M 24 M-1 30 28 M M-1 M M-1 28 M 27 28 20 M M M 30 29 27 M-1 30 M 30 M M M 30 26 M] [M M M M 30 M 28 30 M 25 M-1 29 M 27 M M 29 M 29 28 29 26 M-1 28 30 M 28 M-1 24 M-1 25 M] [M M 28 M-1 M M M-1 27 28 M M M M M M 30 25 24 28 M M 28 M 28 27 M 30 M M 30 M-1 30] [M-1 M M M 29 M M 24 M M 29 M M 24 M-1 M-1 M M 22 30 26 30 30 M 29 30 30 28 29] [28 M 27 29 M M 29 M 26 M M 26 M 26 M M M M M-1 M M M M M-1 M M M M 30 M 23 M] [26 29 M M 27 M M M M-1 30 30 27 28 M-1 M M M M M 30 30 M 23 30 M M 28 M 29 M M M] [27 M 30 M-1 M M M M M M M M 28 M M M M-1 30 24 M 27 M M M 30 M M 29 M 29 M M] [29 30 M M 30 30 M M M-1 M 30 M 29 M-1 M 29 M-1 M 29 30 M-1 M 30 M M 28 M 29 M M 29 29] [30 M M 30 30 M M M 30 M M M M 27 M M-1 M-1 M M M M M-1 M M M M M 27 M M 28 M-1] [M-1 M M M 28 M M M 25 28 M M M M 23 M M M M M 27 M-1 M M M M M M M M M M] [29 M-1 M M M 30 M 30 M M M M M M M M-1 30 M 28 M M M M M-1 M M M M M M M M] [M M M M-1 M M M M 29 M M M M M-1 M M M M-1 M M 26 M M 29 M-1 M-1 M M M M 28 M] [M-1 M M M M M M 28 M M M M M M M M M M M M M M M M M M M M M M-1 M M] [M M M 30 29 28 M M M M M-1 29 30 M M M M M 29 M M 29 M M M M 24 M 28 M] [27 M M M M-1 30 M 27 29 M M-1 M M M M 26 M 29 30 M M-1 M 29 M M 29 M M M 29 M M] [M M M M-1 M M M M M M M M 30 M M 29 M 28 M M 25 M M M 30 M 30 M 30 M M M] [M-1 M M-1 M 28 M 28 M-1 30 M M M M M M M 27 M M M M-1 M M 29 M M M M-1 M 29 30 M] [M M M M M 30 30 M M M M M M M M M M M-1 M M M M M-1 M M M M M 28 M 29 M] [28 30 M M 29 M-1 M M 29 M M M M M M M 29 M M M 30 M M M M M M M M M M M] [M M M M M M M M M M M M M 30 M-1 M M M M M M M M M M M M M M M M M] [M M M M M M M M 26 M M M M M M M M M M M M M M 30 M M M M M M] [30 27 25 25 M M-1 30 27 M 26 29 M 27 M 29 29 M 27 M M 28 26 22 M M M-1 M 29 M 27 26 M]] T21"
        doJoinTest v1 v2

    testCase "big join, test 5" <| fun _ ->
        let v1 = RRBVecGen.treeReprStrToVec "[M*M]*M T22"
        let v2 = RRBVecGen.treeReprStrToVec "[M*M]*29 T8"
        doJoinTest v1 v2

    testCase "joining vectors may end up with a full node as root" <| fun _ ->
        let v1 = RRBVecGen.treeReprStrToVec "M TM"
        let v2 = RRBVecGen.treeReprStrToVec "[M*30] [M*M] T3"
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
        let vL = RRBVecGen.treeReprStrToVec "[M*M]*M-2 [M*M-1] TM-1"
        let vR = RRBVecGen.treeReprStrToVec "T1"
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
        let vL = RRBVecGen.treeReprStrToVec "[M*M]*M/2-1 T3"
        let vR = RRBVecGen.treeReprStrToVec "T3"     // tail-only, height=0
        doJoinTest vL vR
        let vR = RRBVecGen.treeReprStrToVec "M TM"   // root+tail, height=0
        doJoinTest vL vR
        let vR = RRBVecGen.treeReprStrToVec "M M TM" // height=1
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
    // testProp "Try command tests from data" <| fun (vec : RRBVector<int>) -> logger.info (eventX "Starting test with {vec}" >> setField "vec" (RRBVecGen.vecToTreeReprStr vec)); (Command.toProperty (RRBVectorMoreCommands.specFromData vec))
    // Failed case: Initial: "M T9", Actions: [pop 37; push 47; mergeR "0 T15"; mergeR "0 T11"; mergeL "0 T9"; mergeL "0 T10"; pop 48; pop 47]
    // Also: ftestPropertyWithConfig (2044959467, 296477380) "Try command tests from data"
    // Also  (498335399, 296478517) which is "[M*M]*M TM-3" with commands [push 38; rev]

    testCase "Manual test from empty" <| fun _ ->
        let mergeL = mergeL << RRBVecGen.treeReprStrToVec
        let mergeR = mergeR << RRBVecGen.treeReprStrToVec
        let actions = [
            mergeR "0 T14"; push 155; mergeR "26 30 M 28 28 23 29 30 30 26 M T24";
            mergeL "M T18"; map id ]
            // pop 72
        // TODO: This is a failure in IterLeaves() when there's an ExpandedNode in there that could have nulls in its leaf arrays. Need to fix that.
        let vec = { 0..500 } |> RRBVector.ofSeq
        let mutable current = vec
        let logVec action = ignore
        // let logVec action vec = logger.info (eventX "After {action}, vec was {vec}" >> setField "action" (action.ToString()) >> setField "vec" (RRBVecGen.vecToTreeReprStr vec))
        for action in actions do
            current <- current |> action.RunActual
            logVec action current
            RRBVectorProps.checkProperties current <| sprintf "Vector after %s" (action.ToString())

    testCase "Manual test of a NullReference bug during remove operation" <| fun _ ->
        let scanf (x : int) a = a + 1  // So that scans will produce increasing sequences
        // let mergeR = mergeR << RRBVecGen.treeReprStrToVec
        // let actions = [mergeR "M T13"; push 166; scan scanf 8; pop 157; remove 51]
        // TODO: Check if starting with the `scan` results, then doing 157 pops, is enough here
        let vec = { 0..848 } |> RRBVector.ofSeq
        let mutable current = vec
        let logVec action = ignore
        // let logVec action vec = printfn "After %O, vec was %s" action (RRBVecGen.vecToTreeReprStr vec)
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
        //   at ExpectoTemplate.RRBVectorExpectoTest+isolatedTest@1537-2.Invoke (Microsoft.FSharp.Core.Unit _arg2) [0x000cc] in <5b56e315492c3c0ba745038315e3565b>:0
        //   at Expecto.Impl+execTestAsync@878-1.Invoke (Microsoft.FSharp.Core.Unit unitVar) [0x00027] in <5a9db3dddf69a9a4a7450383ddb39d5a>:0
        //   at Microsoft.FSharp.Control.AsyncBuilderImpl+callA@522[b,a].Invoke (Microsoft.FSharp.Control.AsyncParams`1[T] args) [0x00051] in <5a7d678a904cf4daa74503838a677d5a>:0

        current <- RRBVector.append current (RRBVecGen.treeReprStrToVec "M T13")
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
        let vec = RRBVecGen.treeReprStrToVec "M T9"
        // logger.info (eventX "Starting with {vec}" >> setField "vec" (RRBVecGen.vecToTreeReprStr vec))
        let mutable current = vec
        let actions = [pop 37; push 47; mergeR <| RRBVecGen.treeReprStrToVec "0 T15"; mergeR <| RRBVecGen.treeReprStrToVec "0 T11"; mergeL <| RRBVecGen.treeReprStrToVec "0 T9"; mergeL <| RRBVecGen.treeReprStrToVec "0 T10"; pop 48; pop 47]
        let logVec action = ignore
        // let logVec action vec = logger.info (eventX "After {action}, vec was {vec}" >> setField "action" (action.ToString()) >> setField "vec" (RRBVecGen.vecToTreeReprStr vec))
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
        let vec = bigReprStr.TrimStart('\n').Replace("\n", " ") |> RRBVecGen.treeReprStrToVec
        // logger.info (eventX "Starting with {vec}" >> setField "vec" (RRBVecGen.vecToTreeReprStr vec))
        let mutable current = vec
        let mergeL = RRBVecGen.treeReprStrToVec >> RRBVectorMoreCommands.ParameterizedVecCommands.mergeL
        let mergeR = RRBVecGen.treeReprStrToVec >> RRBVectorMoreCommands.ParameterizedVecCommands.mergeR
        let actions = [ mergeL "M T19"; push 99; mergeL "0 T28"; push 80; push 127; push 17; push 30;
                        push 138; remove -109; mergeR "M T9"; push 91; push 72; insert (-90,126); push 64;
                        push 52; insert (-138,1); push 130 ]
        // let logVec action vec = logger.info (eventX "After {action}, vec was {vec}" >> setField "action" (action.ToString()) >> setField "vec" (RRBVecGen.vecToTreeReprStr vec))
        let logVec action = ignore
        for action in actions do
            current <- current |> action.RunActual
            logVec action current
            RRBVectorProps.checkProperties current <| sprintf "Vector after %s" (action.ToString())

    testCase "Another large manual test" <| fun _ ->
        let vec = RRBVecGen.treeReprStrToVec "[M*M]*M TM-3"
        // printfn "Starting with %s" (RRBVecGen.vecToTreeReprStr vec)
        let mutable current = vec
        let actions = [push 38; rev()]
        // let logVec action vec = logger.info (eventX "After {action}, vec was {vec}" >> setField "action" (action.ToString()) >> setField "vec" (RRBVecGen.vecToTreeReprStr vec))
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
    mkTest "mergeL tail-only" (RRBVector.append (RRBVecGen.treeReprStrToVec "T28"))
    mkTest "mergeR tail-only" (fun vec -> RRBVector.append vec (RRBVecGen.treeReprStrToVec "T28"))
    mkTest "mergeL sapling" (RRBVector.append (RRBVecGen.treeReprStrToVec "M T28"))
    mkTest "mergeR sapling" (fun vec -> RRBVector.append vec (RRBVecGen.treeReprStrToVec "M T28"))
    mkTest "mergeL small non-sapling" (RRBVector.append (RRBVecGen.treeReprStrToVec "M M T28"))
    mkTest "mergeR small non-sapling" (fun vec -> RRBVector.append vec (RRBVecGen.treeReprStrToVec "M M T28"))
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
let threeLevelVectorTests = mkTestSuite "Tests on three-level vector" (RRBVecGen.treeReprStrToVec "[[M*M]*M]*3 TM/2")

(* Disable for now while I test the split vector tests
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
        // logger.warn (
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
        // logger.warn (
        //     eventX "Tree {vec} passed all the checks"
        //     >> setField "vec" (sprintf "%A" v2)
        // )
        // Expect.equal vec.Length (size - Literals.blockSize - 1) <| sprintf "Vector has wrong size after pops"

    longRunningTests
    threeLevelVectorTests
    transientResidueTests
    moreTransientResidueTests
//   ]
// ignore
//   [

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

    // arrayTests
    simpleVectorTests
    manualVectorTests
    constructedVectorSplitTests
    splitJoinTests
    insertTests
    operationTests // Operational tests not yet ported to new API
    vectorTests
    nodeVecGenerationTests
    regressionTests
    mergeTests
    // apiTests

    // perfTests
  ]
*)

[<Tests>]
let tests =
  testList "Just transient splitting" [splitTransientTests]
