module ExpectoTemplate.RRBVectorMoreCommands

// https://fscheck.github.io/FsCheck/StatefulTesting.html

open Ficus.RRBArrayExtensions
open Ficus.RRBVectorNodes
open Ficus.RRBVector
open FsCheck
open Expecto

type Cmd = Command<RRBVector<int>, int []>

let vecEqual vec arr msg =
    RRBVectorProps.checkPropertiesSimple vec
    (RRBVector.toArray vec) = arr |@ msg

module ParameterizedVecCommands =
    let push n = { new Cmd()
                   with override __.RunActual vec = seq { 1 .. n } |> Seq.fold (fun vec x -> vec |> RRBVector.push x) vec
                        override __.RunModel arr = Array.append arr [| 1 .. n |]
                        override __.Post(vec, arr) = vecEqual vec arr <| sprintf "After pushing %d items, vec != arr" n
                        override __.ToString() = sprintf "push %d" n }

    let pop n = { new Cmd()
                  with override __.RunActual vec = seq { 1 .. n } |> Seq.fold (fun vec x -> vec |> RRBVector.pop) vec
                       override __.RunModel arr = arr |> Array.truncate (arr.Length - n |> max 0)
                       override __.Pre(arr) = arr.Length >= n
                       override __.Post(vec, arr) = vecEqual vec arr <| sprintf "After popping %d items, vec != arr" n
                       override __.ToString() = sprintf "pop %d" n }

    let insert (idx,item) = { new Cmd()
                            with override __.RunActual vec = vec |> RRBVector.insert idx item
                                 override __.RunModel arr = let idx' = if idx < 0 then idx + arr.Length else idx in arr |> Array.copyAndInsertAt idx' item
                                 override __.Pre(arr) = (abs idx) <= arr.Length
                                 override __.Post(vec, arr) = vecEqual vec arr <| sprintf "After inserting %d at index %d, vec != arr" item idx
                                 override __.ToString() = sprintf "insert %d %d" idx item }

    let remove idx = { new Cmd()
                       with override __.RunActual vec = vec |> RRBVector.remove idx
                            override __.RunModel arr = let idx' = if idx < 0 then idx + arr.Length else idx in arr |> Array.copyAndRemoveAt idx'
                            override __.Pre(arr) = (abs idx) <= arr.Length && idx <> arr.Length
                            override __.Post(vec, arr) = vecEqual vec arr <| sprintf "After removing item at index %d, vec != arr" idx
                            override __.ToString() = sprintf "remove %d" idx }

    let take idx = { new Cmd()
                     with override __.RunActual vec = vec |> RRBVector.take idx
                          override __.RunModel arr = let idx' = if idx < 0 then idx + arr.Length else idx in arr |> Array.truncate idx'
                          override __.Pre(arr) = (abs idx) <= arr.Length
                          override __.Post(vec, arr) = vecEqual vec arr <| sprintf "After keeping only first %d items, vec != arr" idx
                          override __.ToString() = sprintf "take %d" idx }

    let skip idx = { new Cmd()
                     with override __.RunActual vec = vec |> RRBVector.skip idx
                          override __.RunModel arr = arr |> Array.skip idx
                          override __.Pre(arr) = idx <= arr.Length
                          override __.Post(vec, arr) = vecEqual vec arr <| sprintf "After skipping %d items and keeping rest, vec != arr" idx
                          override __.ToString() = sprintf "skip %d" idx }

    let split (NonNegativeInt idx,takeOrSkip) = if takeOrSkip then take idx else skip idx

    let mergeL otherVec = { new Cmd()
                            with override __.RunActual vec = RRBVector.append otherVec vec
                                 override __.RunModel arr = Array.append (RRBVector.toArray otherVec) arr
                                 override __.Post(vec, arr) = vecEqual vec arr <| sprintf "After merging on the left, vec != arr"
                                 override __.ToString() = sprintf "mergeL %A" (RRBVecGen.vecToTreeReprStr otherVec) }

    let mergeR otherVec = { new Cmd()
                            with override __.RunActual vec = RRBVector.append vec otherVec
                                 override __.RunModel arr = Array.append arr (RRBVector.toArray otherVec)
                                 override __.Post(vec, arr) = vecEqual vec arr <| sprintf "After merging on the right, vec != arr"
                                 override __.ToString() = sprintf "mergeR %A" (RRBVecGen.vecToTreeReprStr otherVec) }

    let merge (otherVec,leftOrRight) = if leftOrRight then mergeL otherVec else mergeR otherVec

    // Generators for the above
    // TODO: Generate *non-negative* ints for indices
    let insertGen = Arb.generate<int> |> Gen.two |> Gen.map insert
    let removeGen = Arb.generate<int> |> Gen.map remove
    let pushGen = Arb.generate<NonNegativeInt> |> Gen.map (fun (NonNegativeInt n) -> push n)
    let popGen = Arb.generate<NonNegativeInt> |> Gen.map (fun (NonNegativeInt n) -> pop n)
    let splitGen = Arb.generate<NonNegativeInt * bool> |> Gen.map split
    let mergeGen = Arb.generate<RRBVector<int> * bool> |> Gen.map merge

open ParameterizedVecCommands
let specFromData data =
    { new ICommandGenerator<RRBVector<int>, int []> with
        member __.InitialActual = data
        member __.InitialModel = RRBVector.toArray data
        member __.Next model =
            let len = Array.length model
            Gen.frequency [
                400_000, insertGen
                200_000, removeGen
                200_000, pushGen
                50_000, popGen
                len, splitGen
                100_000 - len |> max 0, mergeGen
            ] }
