module ExpectoTemplate.Main
open Expecto
open System.Reflection


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

[<EntryPoint>]
let main argv =
    if argv |> Seq.contains ("--version") then
        let assembly =  Assembly.GetEntryAssembly()
        let name = assembly.GetName()
        let version = assembly.GetName().Version
        let releaseDate = AssemblyInfo.getReleaseDate assembly
        let githash  = AssemblyInfo.getGitHash assembly
        printfn "%s - %A - %s - %s" name.Name version releaseDate githash
    if argv |> Array.contains "--stress" then
        printfn "Stress testing requested"
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
        runTestsWithArgs config argv RRBVectorExpectoTest.tests
    else
        runTestsWithArgs defaultConfig argv RRBVectorExpectoTest.tests
        // runTestsWithArgs defaultConfig argv experimentalTests
