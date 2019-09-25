module ExpectoTemplate.Main
open Expecto
open System.Reflection
open Ficus
open Ficus.RRBVectorNodes
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

let testProperties (vec : RRBVector<'T>) name =
    let propertyFailures = RRBVectorProps.getPropertyResults vec
    if propertyFailures.Length > 0 then
        printfn "Vector %A (called \"%s\" and with repr %s) had some property failures" vec name (RRBVecGen.vecToTreeReprStr vec)
        printfn "Failed properties: %A" propertyFailures
    else
        printfn "Looks good"

open RRBVectorMoreCommands.ParameterizedVecCommands
open Ficus.RRBVector

let isEmpty (node : Node) = node.NodeSize <= 0
// Note: do NOT call isNotTwig or isTwig on empty nodes!
let isNotTwig (node : Node) = node.Array.[0] :? Node
let isTwig (node : Node) = not (isNotTwig node)
let isRRB  (node : Node) = node :? RRBNode
let isFull (node : Node) = not (isRRB node)
let items (node : Node) = if isTwig node then node.Array else [||]
let children (node : Node) = if isNotTwig node then node.Array else [||]

let rec itemCount<'T> (node : Node) =
    if isEmpty node then 0
    elif node |> isTwig then node.Array.[0..node.NodeSize-1] |> Array.sumBy (fun leaf -> (leaf :?> 'T[]).Length)
    else node.Array.[0..node.NodeSize-1] |> Array.sumBy (fun n -> n :?> Node |> itemCount<'T>)

let inline down shift = shift - Literals.blockSizeShift

let localCheck (vec : RRBVector<'T>) =
        let rec check shift seenFullParent isLastChild (node : Node) =
            if shift <= 0 then true else
            if shift <= Literals.blockSizeShift then
                match node with
                | :? RRBNode -> isLastChild || not seenFullParent // RRBNode twigs satisfy the property without need for further checking, since their children are leaves... as long as no parent was full, or as long as they were the last child
                | _ ->
                    if node.NodeSize <= 1 then true else
                    node.Array.[..node.NodeSize - 2] |> Array.forall (fun n ->
                        (n :?> 'T[]).Length = Literals.blockSize)
            else
                match node with
                | :? RRBNode ->
                    if shift <= Literals.blockSizeShift then
                        isLastChild || not seenFullParent // RRBNode twigs satisfy the property without need for further checking, since their children are leaves... as long as no parent was full, or as long as they were the last child
                    else
                        node.Array.[0 .. node.NodeSize - 1] |> Seq.indexed |> Seq.forall (fun (i,n) -> check (down shift) seenFullParent (i = node.NodeSize - 1) (n :?> Node))
                | _ ->
                    let fullCheck (n : Node) =
                        if shift <= Literals.blockSizeShift then
                            isFull n && n.NodeSize = Literals.blockSize
                        else
                            isFull n
                    if node.NodeSize = 0 then
                        true
                    elif node.NodeSize = 1 then
                        check (down shift) seenFullParent true (node.Array.[0] :?> Node)  // If a FullNode has just one element, it doesn't matter if it has an RRB child, but its children still need to be checked.
                    else
                        node.Array.[..node.NodeSize - 2] |> Array.forall (fun n ->
                            fullCheck (n :?> Node) && check (down shift) true true (n :?> Node)
                        ) && check (down shift) true true ((Array.last node.Array) :?> Node)
        match vec with
        | :? RRBSapling<'T> as sapling -> true // Not applicable to saplings
        | :? RRBTree<'T> as tree ->
            check tree.Shift false true tree.Root
        | _ -> failwith "Unknown RRBVector subclass: property checks need to be taught about this variant"


// open ExpectoTemplate.RRBVectorMoreCommands.ParameterizedVecCommands

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
        debugBigJoinTest1()
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
        runTestsWithArgs config (argv |> Array.filter ((<>) "--fscheck-only")) RRBVectorExpectoTest.tests
    else
        runTestsWithArgs defaultConfig argv RRBVectorExpectoTest.tests
        // runTestsWithArgs defaultConfig argv experimentalTests
