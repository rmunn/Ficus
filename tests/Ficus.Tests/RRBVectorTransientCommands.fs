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

let checkOrigVec (vec : RRBPersistentVector<'a>) (arr : 'a[]) =
    if vec.Length <> arr.Length then
        failwithf "Original vector and its original array should be same length, but vector length was %d and array length was %d. Vec: %A and array: %A" vec.Length arr.Length vec arr
    let e = (vec |> RRBVector.toSeq).GetEnumerator()
    let maxIdx = arr.Length - 1
    for i = 0 to maxIdx do
        if not (e.MoveNext()) then
            failwithf "Unexpected end of original vector at index %d. arr.[%d] = %A" i i arr.[i]
        if e.Current <> arr.[i] then
            failwithf "Original vector was modified at index %d! This was probably caused by a thread. vec.[%d] = %A and arr.[%d] = %A" i i e.Current i arr.[i]

type SingleThreadResult =
    | Completed of int * RRBTransientVector<int>
    | Failed of int * int * RRBTransientVector<int> * int[] * Cmd option * string

type AllThreadsResult =
    | Go of AsyncReplyChannel<AllThreadsResult>
    | AllCompleted of RRBTransientVector<int>[]
    | OneFailed of int * int * RRBTransientVector<int> * int[] * Cmd option * string

type AgentCmd =
    | Cmd of Cmd
    | Finished

let completionAgent size (cts : CancellationTokenSource) (parent : MailboxProcessor<AllThreadsResult>) =
    let results = Array.zeroCreate size
    let mutable completedCount = 0
    let run (mailbox : MailboxProcessor<SingleThreadResult>) =
        let rec loop() =
            async {
                let! msg = mailbox.Receive()
                match msg with
                | Completed (position, vec) ->
                    results.[position] <- vec
                    completedCount <- completedCount + 1
                    if completedCount < size then
                        return! loop()
                    else
                        AllCompleted results |> parent.Post
                        return ()
                | Failed (position, cmdsDone, vec, arr, cmd, msg) ->
                    cts.Cancel()
                    OneFailed (position, cmdsDone, vec, arr, cmd, msg) |> parent.Post
                    return ()
            }
        loop()
    MailboxProcessor.Start run

let transientAgent token (completionAgent : MailboxProcessor<SingleThreadResult>) (position : int) (tvec : RRBTransientVector<int>) =
    let mutable arr = RRBVector.toArray tvec
    let mutable vec = tvec
    let mutable cmdsDone = 0
    let run (mailbox : MailboxProcessor<AgentCmd>) =
        let rec loop() =
            async {
                let! msg =
                    try
                        mailbox.Receive(5000)
                    with :? System.TimeoutException ->
                        let queueCount = mailbox.CurrentQueueLength
                        async {
                            return Finished }  // If we're waiting five seconds without getting anything, give up and call this thread finished
                match msg with
                | Finished ->
                    let queueCount = mailbox.CurrentQueueLength
                    Completed (position, vec) |> completionAgent.Post
                    return ()
                | Cmd cmd ->
                    let queueCount = mailbox.CurrentQueueLength
                    let shouldRun = cmd.Pre arr
                    if shouldRun then
                        arr <- cmd.RunModel arr
                        // if cmdsDone = 74 && position = 2 || cmdsDone = 44 && position = 0 then
                        //     do! logger.infoWithBP (eventX "Before {cmd}, vec was {vec}" >> setField "cmd" (sprintf "%A" cmd) >> setField "vec" (sprintf "%A" vec))
                        try
                            vec <- cmd.RunActual vec
                        with
                        | e ->
                            do! logger.infoWithBP (
                                    eventX "During {cmd} execution (vec was {vec}), error after {cmdsDone} commands in thread {pos} with message {msg}"
                                    >> setField "cmd" (sprintf "%A" cmd)
                                    >> setField "vec" (sprintf "%A" vec)
                                    >> setField "cmdsDone" cmdsDone
                                    >> setField "pos" position
                                    >> setField "msg" e
                                )
                            Failed (position, cmdsDone, vec, arr, Some cmd, e.Message) |> completionAgent.Post
                        try
                            let success, msg = cmd.Post (vec, arr)
                            if not success then
                                Failed (position, cmdsDone, vec, arr, Some cmd, msg) |> completionAgent.Post
                                // MailboxProcessors don't need to throw exceptions
                                // failwithf "Split vector number %d failed on %A with message %A; vector was %A and corresponding array was %A" position cmd msg vec arr
                                return ()
                        with
                        | :? Expecto.AssertException as e ->
                            Failed (position, cmdsDone, vec, arr, Some cmd, e.Message) |> completionAgent.Post
                            return ()
                        | e ->
                            do! logger.infoWithBP (
                                    eventX "During {cmd} post checking (vec was {vec}), error after {cmdsDone} commands in thread {pos} with message {msg}"
                                    >> setField "cmd" (sprintf "%A" cmd)
                                    >> setField "vec" (sprintf "%A" vec)
                                    >> setField "cmdsDone" cmdsDone
                                    >> setField "pos" position
                                    >> setField "msg" e.Message
                                )
                            Failed (position, cmdsDone, vec, arr, Some cmd, e.Message) |> completionAgent.Post
                    else
                        ()
                    cmdsDone <- cmdsDone + 1
                    return! loop()
            }
        loop()
    // logger.infoWithBP (
    //     eventX "Kicking off a single split thread at position {pos}"
    //     >> setField "pos" position
    // ) |> Async.RunSynchronously
    MailboxProcessor.Start(run,token)

let splitVec (tvec : RRBTransientVector<'a>) =
    let parts =
        if tvec.Length > 200 then
            tvec |> RRBVector.chunkBySize 100
        else
            tvec |> RRBVector.splitInto 4
    let s = parts |> RRBVector.toSeq
    let transients = s |> Seq.cast<RRBTransientVector<'a>>
    transients |> Seq.toArray

let expectedSize (vec : RRBVector<'a>) =
    if vec.Length > 200 then
        vec.Length / 100 + (if vec.Length % 100 = 0 then 0 else 1)
    else
        4

let startSplitTesting (vec : RRBPersistentVector<int>) (cmds : (Cmd list)[]) =
    // logger.infoWithBP (
    //     eventX "Inside startSplitTesting function with vec {vec} and cmds {cmds}"
    //     >> setField "vec" (RRBVectorGen.vecToTreeReprStr vec)
    //     >> setField "cmds" (sprintf "%A" cmds)
    // ) |> Async.RunSynchronously
    let origVec = vec
    let origArr = vec |> RRBVector.toArray
    let mutable arr = origArr |> Array.copy
    let mutable tvec = vec.Transient()
    let eq, msg = (origArr = (tvec |> RRBVector.toArray)), "Initial transient didn't equal initial persistent"
    if not eq then failwith msg
    let parts = splitVec tvec
    // logger.infoWithBP (
    //     eventX "Parts [{parts}]"
    //     >> setField "parts" (parts |> Array.map RRBVectorGen.vecToTreeReprStr |> String.concat "; ")
    // ) |> Async.RunSynchronously
    if parts.Length <> cmds.Length then failwith "Pass as many cmds as expected splits"  // TODO: Populate error msg with some data here
    let cts = new CancellationTokenSource()
    let token = cts.Token
    let mutable replyChannel = None
    let run (mailbox : MailboxProcessor<AllThreadsResult>) =
        async {
            let! msg =
                try
                    mailbox.Receive(15000)
                with :? TimeoutException ->
                    Expecto.Tests.failtest "Split test timed out waiting for initial Go command"
            match msg with
            | Go channel ->
                replyChannel <- Some channel
                let completion = mailbox |> completionAgent parts.Length cts
                let threads = parts |> Array.mapi (transientAgent cts.Token completion)
                // Send all commands to their respective threads as quickly as possible
                (cmds, threads) ||> Array.iteri2 (fun position cmdList thread ->
                    thread.Error.Add (fun e ->
                        completion.Post (Failed (position, -1, RRBTransientVector<int>.MkEmpty(), Array.empty, None, e.Message))
                    )
                    cmdList |> List.iter (Cmd >> thread.Post)
                    thread.Post Finished
                )
            | _ -> Expecto.Tests.failtest "Send Go command first before anything else"
            while mailbox.CurrentQueueLength = 0 && not (cts.IsCancellationRequested) do
                // Keep busy checking the original vector against its original array while we wait for test completion
                checkOrigVec origVec origArr
                // do! Async.Sleep 500  // Attempt to yield
            let! msg =
                try
                    mailbox.Receive(15000)
                with :? TimeoutException ->
                    Expecto.Tests.failtest "Split test timed out waiting for results from subthreads!"
            match msg with
            | Go _ ->
                Expecto.Tests.failtest "Shouldn't receive the Go message twice!"
            | AllCompleted newParts ->
                let joinedT = RRBVector.concat (newParts |> Seq.cast<RRBVector<int>>)
                let joined =
                    if joinedT |> isTransient then
                        RRBVectorProps.checkProperties joinedT "Combined transient result of split test"
                        (joinedT :?> RRBTransientVector<_>).Persistent()
                    else joinedT :?> RRBPersistentVector<_>
                RRBVectorProps.checkProperties joined "Combined persistent result of split test"
                match replyChannel with
                | None -> failwithf "Split test had no reply channel to report results!"
                | Some channel -> return channel.Reply(msg)
            | OneFailed (position, cmdsDone, vec, arr, cmd, errorMsg) ->
                match replyChannel with
                | None -> failwithf "Split test had no reply channel to report results!"
                | Some channel -> return channel.Reply(msg)
        }
    MailboxProcessor.Start run

module VecCommands =
    let push n = { new Cmd()
                   with override __.RunActual vec = { 1 .. n } |> Seq.fold (fun vec x -> vec.Push x :?> RRBTransientVector<_>) vec
                        override __.RunModel arr = Array.append arr [| 1 .. n |]
                        override __.Post(vec, arr) = vecEqual vec arr <| sprintf "After pushing %d items, vec != arr" n
                        override __.ToString() = sprintf "push %d" n }

    let pop n = { new Cmd()
                  with override __.RunActual vec = { 1 .. n } |> Seq.fold (fun vec _ -> vec.Pop() :?> RRBTransientVector<_>) vec
                       override __.RunModel arr = arr |> Array.truncate (arr.Length - n |> max 0)
                       override __.Pre(arr) = arr.Length >= n
                       override __.Post(vec, arr) = vecEqual vec arr <| sprintf "After popping %d items, vec != arr" n
                       override __.ToString() = sprintf "pop %d" n }

    let insert (idx,item) = { new Cmd()
                            with override __.RunActual vec = let idx' = if idx < 0 then idx + vec.Length else idx in vec |> RRBVector.insert idx' item :?> RRBTransientVector<_>
                                 override __.RunModel arr = let idx' = if idx < 0 then idx + arr.Length else idx in arr |> Array.copyAndInsertAt idx' item
                                 override __.Pre(arr) = (abs idx) <= arr.Length
                                 override __.Post(vec, arr) = vecEqual vec arr <| sprintf "After inserting %d at index %d, vec != arr" item idx
                                 override __.ToString() = sprintf "insert (%d,%d)" idx item }

    let insert5AtHead = insert (0, 5)
    let insert7InFirstLeaf = insert (3, 7)
    let insert9InTail = insert (-2, 9)

    let remove idx = { new Cmd()
                       with override __.RunActual vec = let idx' = if idx < 0 then idx + vec.Length else idx in vec |> RRBVector.remove idx' :?> RRBTransientVector<_>
                            override __.RunModel arr = let idx' = if idx < 0 then idx + arr.Length else idx in arr |> Array.copyAndRemoveAt idx'
                            override __.Pre(arr) = (abs idx) <= arr.Length && idx <> arr.Length
                            override __.Post(vec, arr) = vecEqual vec arr <| sprintf "After removing item at index %d, vec != arr" idx
                            override __.ToString() = sprintf "remove %d" idx }

    let removeFromHead = remove 0
    let removeFromFirstLeaf = remove 3
    let removeFromTail = remove -2

    let slice (start, stop) =
        let start, stop =
            match start, stop with
            | Some startValue, Some stopValue ->
                if startValue < stopValue then start, stop else stop, start
            | _ -> start, stop
        { new Cmd() with
            override __.RunActual vec =
                let start' = start |> Option.map (fun start -> if start < 0 then start + vec.Length else start)
                let stop' = stop |> Option.map (fun stop -> if stop < 0 then stop + vec.Length else stop)
                vec.GetSlice (start', stop') :?> RRBTransientVector<_>
             override __.RunModel arr =
                let start' = start |> Option.map (fun start -> if start < 0 then start + arr.Length else start) |> Option.defaultValue 0
                let stop' = stop |> Option.map (fun stop -> if stop < 0 then stop + arr.Length else stop) |> Option.defaultValue (arr.Length - 1)
                Array.sub arr start' (stop' - start' + 1)
             override __.Post(vec, arr) = vecEqual vec arr <| sprintf "After slicing from %A to %A, vec != arr" start stop
             override __.ToString() = sprintf "slice %A %A" start stop }

    let genSlice = Arb.generate<int option> |> Gen.two |> Gen.map slice

open VecCommands

let cmdsExtraSmall = [push 1; pop 1; insert5AtHead; insert7InFirstLeaf; insert9InTail; removeFromHead; removeFromFirstLeaf; removeFromTail]
let cmdsSmall = [push 1; push 4; pop 1; pop 4; insert5AtHead; insert7InFirstLeaf; insert9InTail; removeFromHead; removeFromFirstLeaf; removeFromTail]
let cmdsMedium = [push 1; push 4; push 9; pop 1; pop 4; pop 9; insert5AtHead; insert7InFirstLeaf; insert9InTail; removeFromHead; removeFromFirstLeaf; removeFromTail]
let cmdsLarge = [push 1; push 4; push 9; pop 1; pop 4; pop 9; insert5AtHead; insert7InFirstLeaf; insert9InTail; removeFromHead; removeFromFirstLeaf; removeFromTail]
let cmdsExtraLarge = [push 4; push 9; push 33; pop 4; pop 9; pop 33; insert5AtHead; insert7InFirstLeaf; insert9InTail; removeFromHead; removeFromFirstLeaf; removeFromTail]

type SplitTestInput = SplitTestInput of RRBVector<int> * (Cmd list)[]

let genBasicOperations (lst : Cmd list) = gen {
    // let! vec = Arb.generate<RRBVector<int>>  // TODO: Ensure only persistent vectors generated here
    let vec = RRBVector.empty<int>
    let size = expectedSize vec
    let! cmds = Gen.arrayOfLength size (Gen.listOf (Gen.elements lst))
    return SplitTestInput (vec, cmds)
}

// TODO: Add more complex operations like slicing, splitting (and running a *FEW* commands on each side before rejoining), mapping, filtering, and so on
let genComplexOperations (lst : Cmd list) = gen {
    return! genBasicOperations lst
}
