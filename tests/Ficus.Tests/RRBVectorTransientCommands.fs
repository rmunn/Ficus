module Ficus.Tests.RRBVectorTransientCommands

// https://fscheck.github.io/FsCheck/StatefulTesting.html

open Ficus.RRBVectorNodes
open Ficus.RRBVector
open FsCheck
open Ficus.RRBArrayExtensions
open System
open System.Threading
open Expecto.Logging
open Expecto.Logging.Message

let logger = Log.create "In-test"
(*
TODO: Write a test with the following structure:

- Start with a persistent
- Save an array copy of it to refer back to
- Turn it into a transient
- Split the transient up into many pieces
- Do a bunch of these commands to the various pieces
- After each one, make sure the original persistent is still intact and hasn't been edited
- Join the pieces together
- Do a bunch more commands to the rejoined vector
- After each one, make sure the original persistent is still intact and hasn't been edited


Another test would be:

- Make a persistent, save it and an array copy
- Turn it transient
- Feed the transient to a MailboxProcessor
- Send it a bunch of commands (insert, remove, update, push, pop)
- Each time the MailboxProcessor runs, store current ThreadId in a set
- After the test, print out (at logging level debug) the set of ThreadIds
- Check whether the set is ever greater than 1 in size, and if so, log that fact at info level (then comment out later)
*)

[<AbstractClass>]
type Op<'Actual,'Model>() =
    // Just like an FsCheck Command, but Post just returns a boolean
    ///Executes the operation on the actual object under test.
    abstract RunActual : 'Actual -> 'Actual
    ///Executes the operation on the model of the object.
    abstract RunModel : 'Model -> 'Model
    ///Precondition for execution of the operation. When this does not hold, the test continues
    ///but the operation will not be executed.
    abstract Pre : 'Model -> bool
    ///Postcondition that must hold after execution of the operation. Compares state of model and actual
    ///object and fails the property if they do not match.
    abstract Post : 'Actual * 'Model -> bool * string
    ///The default precondition is true.
    default __.Pre _ = true
    ///The default postcondition is true.
    default __.Post (_,_) = true, ""

type Cmd = Op<RRBTransientVector<int>, int[]>

let fsCheckCmdFromOp (op : Op<'Actual, 'Model>) =
    { new Command<'Actual, 'Model>()
      with override __.RunActual a = op.RunActual a
           override __.RunModel m = op.RunModel m
           override __.Pre m = op.Pre m
           override __.Post(a, m) = let bool, msg = op.Post(a, m) in bool |@ msg
           override __.ToString() = op.ToString() }
// TODO: Model should become RRBPersistentVector<int> * RRBPersistentVector<int> * int[], where the second one is the original vector (which should never change) and the third is the array of the original vector (which we constantly compare the original vector to to ensure it hasn't changed)

(*
let vecEqual (vec : RRBTransientVector<'a>) arr msg =
    if vec.Length = 0 then
        logger.infoWithBP (eventX "Length 0 BEFORE start of vecEqual, with deliberate failure before checkPropertiesSimple") |> Async.RunSynchronously
        Expecto.Tests.failtest "Deliberate failure in vecEqual with length 0"
    if vec.Length = 0 then logger.infoWithBP (eventX "Length 0 at start of vecEqual") |> Async.RunSynchronously
    let result, msg =
        try
            RRBVectorProps.checkPropertiesSimple vec
            true, msg
        with :? Expecto.AssertException as e ->
            false, msg + " failed: " + e.Message
    if result then
        if vec.Length = 0 then logger.infoWithBP (eventX "Length 0 after checking properties") |> Async.RunSynchronously
        let vecArray = RRBVector.toArray vec
        if vec.Length = 0 then logger.infoWithBP (eventX "Length 0 after toArray") |> Async.RunSynchronously
        vecArray = arr, msg
    else
        result, msg
*)

let vecEqual (vec : RRBTransientVector<'a>) arr msg =
    RRBVectorProps.checkPropertiesSimple vec
    (RRBVector.toArray vec) = arr, msg

module VecCommands =
    let push n = { new Cmd()
                   with override __.RunActual vec = { 1 .. n } |> Seq.fold (fun vec x -> vec.Push x :?> RRBTransientVector<_>) vec
                        override __.RunModel  arr = Array.append arr [| 1 .. n |]
                        override __.Post(vec, arr) = vecEqual vec arr <| sprintf "After pushing %d items, vec != arr" n
                        override __.ToString() = sprintf "push %d" n }
    let genPush = Gen.choose(1, 100) |> Gen.map push

    let pop n = { new Cmd()
                  with override __.RunActual vec =
                            // if n = 61 then
                            //     logger.warnWithBP (eventX "About to pop {n} times in vec {vec} with repr {repr}"
                            //         >> setField "n" (n)
                            //         >> setField "vec" (sprintf "%A" vec)
                            //         >> setField "repr" (sprintf "%A" (RRBVectorGen.vecToTreeReprStr vec))
                            //     ) |> Async.RunSynchronously
                            { 1 .. n } |> Seq.fold (fun vec _ -> vec.Pop() :?> RRBTransientVector<_>) vec
                       override __.RunModel  arr =
                            // if n = 61 then
                            //     logger.warnWithBP (eventX "About to pop {n} times in array of length {vec}"
                            //         >> setField "n" (n)
                            //         >> setField "vec" (sprintf "%A" arr.Length)
                            //     ) |> Async.RunSynchronously
                            arr |> Array.truncate (arr.Length - n |> max 0)
                       override __.Pre(arr) = arr.Length >= n
                       override __.Post(vec, arr) = vecEqual vec arr <| sprintf "After popping %d items, vec != arr" n
                       override __.ToString() = sprintf "pop %d" n }
    let genPop = Gen.choose(1, 100) |> Gen.map pop

    let insert (idx,item) = { new Cmd()
                            with override __.RunActual vec = let idx' = (if idx < 0 then idx + vec.Length else idx) |> min vec.Length |> max 0 in vec |> RRBVector.insert idx' item :?> RRBTransientVector<_>
                                 override __.RunModel  arr = let idx' = (if idx < 0 then idx + arr.Length else idx) |> min arr.Length |> max 0 in arr |> Array.copyAndInsertAt idx' item
                                 override __.Pre(arr) = (abs idx) <= arr.Length
                                 override __.Post(vec, arr) = vecEqual vec arr <| sprintf "After inserting %d at index %d, vec != arr" item idx
                                 override __.ToString() = sprintf "insert (%d,%d)" idx item }
    let genInsert = Gen.map2 (fun x y -> x,y) (Gen.choose(-100, 100)) (Gen.choose(1, 100)) |> Gen.map insert

    let insert5AtHead = insert (0, 5)
    let insert7InFirstLeaf = insert (3, 7)
    let insert9InTail = insert (-2, 9)

    let remove idx = { new Cmd()
                       with override __.RunActual vec = let idx' = (if idx < 0 then idx + vec.Length else idx) |> min (vec.Length - 1) |> max 0 in vec |> RRBVector.remove idx' :?> RRBTransientVector<_>
                            override __.RunModel  arr = let idx' = (if idx < 0 then idx + arr.Length else idx) |> min (arr.Length - 1) |> max 0 in arr |> Array.copyAndRemoveAt idx'
                            override __.Pre(arr) = let idx' = (if idx < 0 then idx + arr.Length else idx) |> min (arr.Length - 1) |> max 0 in arr.Length > 0 && idx' < arr.Length
                            override __.Post(vec, arr) = vecEqual vec arr <| sprintf "After removing item at index %d, vec != arr" idx
                            override __.ToString() = sprintf "remove %d" idx }
    let genRemove = Gen.choose(-100, 100) |> Gen.map remove

    let removeFromHead = remove 0
    let removeFromFirstLeaf = remove 3
    let removeFromTail = remove -2

    let slice (start, stop) =
        { new Cmd() with
            override __.RunActual vec =
                let start' = start |> Option.map (fun start -> (if start < 0 then start + vec.Length else start) |> min (vec.Length - 1) |> max 0)
                let stop' = stop |> Option.map (fun stop -> (if stop < 0 then stop + vec.Length else stop) |> min (vec.Length - 1) |> max 0)
                let start', stop' =
                    match start', stop' with
                    | Some startValue, Some stopValue ->
                        if startValue < stopValue then start', stop' else stop', start'
                    | _ -> start', stop'
                let oldLen = vec.Length
                let oldRepr = RRBVectorGen.vecToTreeReprStr vec
                // logger.warnWithBP (eventX "About to slice [{start}..{stop}] in vector of length {len} with repr {repr}"
                //     >> setField "start" (sprintf "%A" start')
                //     >> setField "stop" (sprintf "%A" stop')
                //     >> setField "len" (sprintf "%A" vec.Length)
                //     >> setField "repr" (sprintf "%A" (RRBVectorGen.vecToTreeReprStr vec))
                // ) |> Async.RunSynchronously
                let newVec = vec.GetSlice (start', stop') :?> RRBTransientVector<_>
                let newLen = newVec.Length
                let newRepr = RRBVectorGen.vecToTreeReprStr newVec
                // logger.warnWithBP (eventX "Slice [{start}..{stop}] in vector of length {len} with repr {repr} produced new vector len {newLen} with repr {newRepr}"
                //     >> setField "start" (sprintf "%A" start')
                //     >> setField "stop" (sprintf "%A" stop')
                //     >> setField "len" (oldLen)
                //     >> setField "repr" (oldRepr)
                //     >> setField "newLen" (newLen)
                //     >> setField "newRepr" (newRepr)
                // ) |> Async.RunSynchronously
                newVec
             override __.RunModel arr =
                let start' = start |> Option.map (fun start -> (if start < 0 then start + arr.Length else start) |> min (arr.Length - 1) |> max 0)
                let stop' = stop |> Option.map (fun stop -> (if stop < 0 then stop + arr.Length else stop) |> min (arr.Length - 1) |> max 0)
                let start', stop' =
                    match start', stop' with
                    | Some startValue, Some stopValue ->
                        if startValue < stopValue then start', stop' else stop', start'
                    | _ -> start', stop'
                if arr |> Array.isEmpty then arr else
                // Array.sub arr start' (stop' - start' + 1)
                // logger.warnWithBP (eventX "About to slice [{start}..{stop}] in array of length {len}"
                //     >> setField "start" (sprintf "%A" start')
                //     >> setField "stop" (sprintf "%A" stop')
                //     >> setField "len" (sprintf "%A" arr.Length)
                // ) |> Async.RunSynchronously
                let newArr =
                    match start', stop' with
                    | None, None -> arr
                    | Some a, None -> arr.[a..]
                    | None, Some b -> arr.[..b]
                    | Some a, Some b -> arr.[a..b]
                // logger.warnWithBP (eventX "Slice [{start}..{stop}] in array of length {len} produced length {newLen}"
                //     >> setField "start" (sprintf "%A" start')
                //     >> setField "stop" (sprintf "%A" stop')
                //     >> setField "len" (sprintf "%A" arr.Length)
                //     >> setField "newLen" (sprintf "%A" newArr.Length)
                // ) |> Async.RunSynchronously
                newArr
             override __.Post(vec, arr) = vecEqual vec arr <| sprintf "After slicing from %A to %A, vec != arr" start stop
             override __.ToString() = sprintf "slice (%A,%A)" start stop }

    let genSlice = Gen.frequency [ 1, Gen.constant None; 7, Gen.choose (-100,100) |> Gen.map Some] |> Gen.two |> Gen.map slice

    let logAndRun (cmd : Cmd) vec =
        logger.warn (eventX "About to run {cmd} on {repr} = {vec}"
            >> setField "cmd" (cmd.ToString())
            >> setField "repr" (sprintf "%A" <| RRBVectorGen.vecToTreeReprStr vec)
            >> setField "vec" (sprintf "%A" vec)
            )

    let split (idx : int, cmdsL : Cmd list, cmdsR : Cmd list) =
        { new Cmd() with
            override __.RunActual vec =
                let idx' = (if idx < 0 then idx + vec.Length else idx) |> min vec.Length |> max 0
                let vL, vR = vec.Split idx'
                let vL' = cmdsL |> List.fold (fun vec cmd -> if cmd.Pre (RRBVector.toArray vec) then cmd.RunActual vec else vec) (vL :?> RRBTransientVector<_>)
                let vR' = cmdsR |> List.fold (fun vec cmd -> if cmd.Pre (RRBVector.toArray vec) then cmd.RunActual vec else vec) (vR :?> RRBTransientVector<_>)
                vL'.Append vR' :?> RRBTransientVector<_>
            override __.RunModel arr =
                let idx' = (if idx < 0 then idx + arr.Length else idx) |> min arr.Length |> max 0
                let arrL, arrR = arr |> Array.splitAt idx'
                let arrL' = cmdsL |> List.fold (fun arr cmd -> if cmd.Pre arr then cmd.RunModel arr else arr) arrL
                let arrR' = cmdsR |> List.fold (fun arr cmd -> if cmd.Pre arr then cmd.RunModel arr else arr) arrR
                Array.append arrL' arrR'
            override __.Post(vec, arr) = vecEqual vec arr <| sprintf "After splitting at %d and running %A on the left and %A on the right, vec != arr" idx cmdsL cmdsR
            override __.ToString() = sprintf "split (%d,%A,%A)" idx cmdsL cmdsR }

    let cmdFrequenciesForSplit = [
        10, genInsert
        5, genRemove
        3, genPush
        2, genPop
        1, genSlice
    ]
    let genCmdsForSplit = Gen.sized (fun s -> Gen.listOfLength (s / 10 |> max 2) (Gen.frequency cmdFrequenciesForSplit))
    let genSplit = Gen.map3 (fun idx cmdsL cmdsR -> idx,cmdsL,cmdsR) (Gen.choose(-100,100)) genCmdsForSplit genCmdsForSplit |> Gen.map split

open VecCommands

let cmdsExtraSmall = [push 1; pop 1; insert5AtHead; insert7InFirstLeaf; insert9InTail; removeFromHead; removeFromFirstLeaf; removeFromTail]
let cmdsSmall = [push 1; push 4; pop 1; pop 4; insert5AtHead; insert7InFirstLeaf; insert9InTail; removeFromHead; removeFromFirstLeaf; removeFromTail]
let cmdsMedium = [push 1; push 4; push 9; pop 1; pop 4; pop 9; insert5AtHead; insert7InFirstLeaf; insert9InTail; removeFromHead; removeFromFirstLeaf; removeFromTail]
let cmdsLarge = [push 1; push 4; push 9; pop 1; pop 4; pop 9; insert5AtHead; insert7InFirstLeaf; insert9InTail; removeFromHead; removeFromFirstLeaf; removeFromTail]
let cmdsExtraLarge = [push 4; push 9; push 33; pop 4; pop 9; pop 33; insert5AtHead; insert7InFirstLeaf; insert9InTail; removeFromHead; removeFromFirstLeaf; removeFromTail]

let cmdFrequenciesForComplexOperations = (1, genSplit) :: cmdFrequenciesForSplit

type SplitTestInput = SplitTestInput of RRBVector<int> * (Cmd list)[]

let expectedSize (vec : RRBVector<'a>) =
    if vec.Length > 200 then
        vec.Length / 100 + (if vec.Length % 100 = 0 then 0 else 1)
    else
        4

let genBasicOperations (lst : Cmd list) = gen {
    // let! vec = Arb.generate<RRBVector<int>>  // TODO: Ensure only persistent vectors generated here
    // let! vec = RRBVectorGen.sizedGenVec
    let vec = RRBVector.empty<int>
    let size = expectedSize vec
    let! cmds = Gen.arrayOfLength size (Gen.listOf (Gen.elements lst))
    return SplitTestInput (vec, cmds)
}

// TODO: Add more complex operations like slicing, splitting (and running a *FEW* commands on each side before rejoining), mapping, filtering, and so on
let genComplexOperations = gen {
    // let! vec = RRBVectorGen.sizedGenVec
    let vec = RRBVector.empty<int>
    let size = expectedSize vec
    let! cmds = Gen.arrayOfLength size (Gen.listOf (Gen.frequency cmdFrequenciesForComplexOperations))
    return SplitTestInput (vec, cmds)
}

let propFromCmds (vec : RRBPersistentVector<int>) (lst : Cmd list) =
    let cmds = lst |> List.map fsCheckCmdFromOp
    // let len = vec |> RRBVector.length
    let arr = vec |> RRBVector.toArray
    { new ICommandGenerator<RRBTransientVector<int>, int[]> with
        member __.InitialActual = vec.Transient()
        member __.InitialModel = arr
        member __.Next arr =
            // let len = arr.Length
            // TODO: Use len to determine what to run or not to run, e.g. don't run slices too often on tiny vectors
            Gen.elements cmds
        }

let propFromCmdFrequencies (vec : RRBPersistentVector<int>) (lst : (int * Gen<Cmd>) list) =
    let cmds = lst
    // let len = vec |> RRBVector.length
    let arr = vec |> RRBVector.toArray
    { new ICommandGenerator<RRBTransientVector<int>, int[]> with
        member __.InitialActual = vec.Transient()
        member __.InitialModel = arr
        member __.Next arr =
            // let len = arr.Length
            // TODO: Use len to determine what to run or not to run, e.g. don't run slices too often on tiny vectors
            Gen.frequency cmds |> Gen.map fsCheckCmdFromOp
        }

let doTestL (vec : RRBVector<int>) =
    let vec = if vec |> isTransient then (vec :?> RRBTransientVector<_>).Persistent() else vec :?> RRBPersistentVector<_>
    propFromCmds vec cmdsLarge |> Command.toProperty
let doTestXL (vec : RRBVector<int>) =
    let vec = if vec |> isTransient then (vec :?> RRBTransientVector<_>).Persistent() else vec :?> RRBPersistentVector<_>
    propFromCmds vec cmdsExtraLarge |> Command.toProperty

let doComplexTest (vec : RRBVector<int>) =
    let vec = if vec |> isTransient then (vec :?> RRBTransientVector<_>).Persistent() else vec :?> RRBPersistentVector<_>
    propFromCmdFrequencies vec cmdFrequenciesForComplexOperations |> Command.toProperty
