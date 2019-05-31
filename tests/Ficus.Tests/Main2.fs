module ExpectoTemplate.Main
open Expecto
open System.Reflection
open Ficus
open Ficus.RRBVectorBetterNodes
open Ficus.RRBArrayExtensions
open Ficus.RRBVector

module AssemblyInfo =

    let metaDataValue  (mda : AssemblyMetadataAttribute) = mda.Value
    let getMetaDataAttribute (assembly : Assembly) key =
        assembly.GetCustomAttributes(typedefof<AssemblyMetadataAttribute>)
                              |> Seq.cast<AssemblyMetadataAttribute>
                              |> Seq.find(fun x -> x.Key = key)

    let getReleaseDate assembly =
        "ReleaseDate"
        |> getMetaDataAttribute assembly
        |> metaDataValue

    let getGitHash assembly =
        "GitHash"
        |> getMetaDataAttribute assembly
        |> metaDataValue

// let mkArr n = [| 1..n |]

open ExpectoTemplate.RRBVectorBetterNodesExpectoTest
let mkLeaf counter n = ExpectoTemplate.RRBVectorBetterNodesExpectoTest.mkLeaf counter n :> RRBNode<int>

let debugTest() =
    let counter = mkCounter()
    // let L = [|28; 32; 29; 32; 31; 32; 32; 32; 17; 32; 26; 30; 32; 17; 32; 16; 32; 20|]
    // let R = [|19; 32; 18; 32; 22; 17; 32; 18; 32; 20|]
    // let L = Array.replicate 32 32
    // let L = [|32; 27; 32; 29; 32; 30; 32; 22; 32; 16; 32; 25; 32; 27; 32; 28; 32; 32; 17; 32; 26; 32|]
    // let R1 = [|32; 16; 32; 20; 32; 19; 32; 28; 32; 22; 19; 32; 21; 32; 22; 32; 24; 32; 25; 32; 17; 32; 28; 32; 20; 32; 22; 32; 23; 32; 32; 30|]
    // let R2 = [|32; 31; 32; 16; 32|]
    let L = [|32; 32; 32; 32; 32; 32; 32; 32; 32; 32; 32; 17|]
    let R1 = Array.replicate 32 32
    let R2 = Array.replicate 3 32
    let shift = Literals.blockSizeShift
    let nodeL =  (L |> Array.map (mkLeaf counter) |> RRBNode<int>.MkNode nullOwner shift).Expand nullOwner
    let nodeR1 = R1 |> Array.map (mkLeaf counter) |> RRBNode<int>.MkNode nullOwner shift
    let nodeR2 = R2 |> Array.map (mkLeaf counter) |> RRBNode<int>.MkNode nullOwner shift
    let nodeR = RRBNode<int>.MkNode nullOwner (shift * 2) [|nodeR1; nodeR2|]
    let origCombined = Seq.append (nodeItems shift nodeL) (nodeItems (shift * 2) nodeR) |> Array.ofSeq
    // let newL, newR = (nodeL :?> RRBFullNode<int>).ConcatNodes nullOwner shift (nodeR :?> RRBFullNode<'T>)
    // let newL, newR = (nodeL :?> RRBFullNode<int>).Rebalance2Plus1 nullOwner shift None (nodeR :?> RRBFullNode<int>)
    let newL, newR = (nodeL :?> RRBFullNode<int>).MergeTree nullOwner shift None (shift * 2) (nodeR :?> RRBFullNode<int>) false
    let arrL' = newL |> nodeItems (shift * 2)
    let arrR' =
        match newR with
        | Some nodeR -> nodeR |> nodeItems (shift * 2)
        | None -> Seq.empty
    let arrCombined = Seq.append arrL' arrR' |> Array.ofSeq
    if arrCombined = origCombined then
        printfn "Great"
    // TODO: Turn this into an individual unit test
    printfn "Done testing"

let debugSkipTest() =
    let counter = mkCounter()
    let leaves = [|32; 18|]
    // let leaves = [|19; 32; 28; 32; 20; 32; 19; 32; 23; 32; 32; 29; 32; 21; 32; 32; 32; 24; 32; 25; 32; 27; 32|]
    let shift = Literals.blockSizeShift
    let node = leaves |> Array.map (mkLeaf counter) |> RRBNode<int>.MkNode nullOwner shift
    let origItems = nodeItems shift node |> Array.ofSeq
    let idx = 1
    let expectedL, expectedR = origItems |> Array.splitAt idx
    let newL, newR = node.SplitTree nullOwner shift idx
    let actualL = nodeItems shift newL |> Array.ofSeq
    let actualR = nodeItems shift newR |> Array.ofSeq
    let actual = Array.append actualL actualR
    if actual = origItems && actualL = expectedL && actualR = expectedR then
        printfn "Great"
    else
        printfn "Failed"
    printfn "Done testing"

let debugTest2() =
    let s = seq { 1..512 }  // 515 works
    let arrSeqs = Array.createManyFromSeq s 512 Literals.blockSize
    let arrs = arrSeqs |> Array.ofSeq
    printfn "%A" arrs

let debugSplitTest() =
    let vec = seq { 1..14338 } |> RRBVector.ofSeq
    let l, r = vec |> RRBVector.splitAt 33
    let propertyFailures = RRBVectorProps.getAllPropertyResults l
    if propertyFailures.Length > 0 then
        printfn "L = %A had some property failures" l
        printfn "Failed properties: %A" propertyFailures
    let propertyFailures = RRBVectorProps.getAllPropertyResults r
    if propertyFailures.Length > 0 then
        printfn "R = %A had some property failures" r
        printfn "Failed properties: %A" propertyFailures
    let joined = RRBVector.append l r
    let propertyFailures = RRBVectorProps.getAllPropertyResults joined
    if propertyFailures.Length > 0 then
        printfn "Joined %A had some property failures" joined
        printfn "Failed properties: %A" propertyFailures
    let arrL = RRBVector.toArray l
    let arrR = RRBVector.toArray r
    let arrJoined = Array.append arrL arrR
    if arrJoined = (joined |> RRBVector.toArray) && arrJoined = [| 1..14338 |] then
        printfn "Good"
    else
        printfn "Failed"
    printfn "Done"


[<EntryPoint>]
let main argv =
    if argv |> Seq.contains ("--version") then
        let assembly =  Assembly.GetEntryAssembly()
        let name = assembly.GetName()
        let version = assembly.GetName().Version
        let releaseDate = AssemblyInfo.getReleaseDate assembly
        let githash  = AssemblyInfo.getGitHash assembly
        printfn "%s - %A - %s - %s" name.Name version releaseDate githash
    if argv |> Array.contains "--debug-vscode" then
        printfn "Debugging"
        debugSplitTest()
        0
    elif argv |> Array.contains "--stress" || argv |> Array.contains "--fscheck-only" then
        printfn "Running only FsCheck tests%s" (if argv |> Array.contains "--stress" then " for stress testing" else "")
        let noop = TestList([], Pending)
        let rec containsTests = function
            | TestCase _ -> true
            | TestList([], _) -> false
            | TestList(tests, _) -> tests |> List.exists containsTests
            | TestLabel(_, test, _) -> containsTests test
            | Test.Sequenced(_, test) -> containsTests test
        let rec filter = function
            | TestCase(AsyncFsCheck _, Pending) -> noop
            | TestCase(AsyncFsCheck _, _) as test -> test
            | TestCase(_, _) -> noop
            | TestList(tests, Pending) -> noop
            | TestList(tests, state) -> tests |> List.map filter |> (fun l -> TestList (l, state))
            | TestLabel(label, test, state) ->
                let filtered = filter test
                if filtered |> containsTests then
                    TestLabel(label, filtered, state)
                else
                    noop
            | Test.Sequenced(_, test) -> filter test
        let config = { defaultConfig with filter = filter }
        runTestsWithArgs config (argv |> Array.filter ((<>) "--fscheck-only")) RRBVectorBetterNodesExpectoTest.tests
    else
        runTestsWithArgs defaultConfig argv RRBVectorBetterNodesExpectoTest.tests
        // runTestsWithArgs defaultConfig argv experimentalTests
