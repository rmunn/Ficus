module ExpectoTemplate.Main
open Expecto
open System.Reflection
open Ficus
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

let debugBigTest () =
    let reprL = """
[25; 32; 30; 29; 32; 32; 23; 32; 32; 28; 32; 28; 24; 32; 32; 27; 30; 32; 32; 28; 32; 32; 29; 32; 30; 31; 32; 31]
[28; 31; 26; 32; 26; 32; 32; 32; 32; 26; 25; 28; 26; 32; 30; 32; 28; 32; 32; 25; 26; 31]
[32; 32; 29; 32; 32; 32; 32; 29; 31; 32; 32; 32; 32; 30; 28; 32; 32; 32; 32; 26; 29; 32; 32]
[29; 23; 31; 32; 32; 32; 32; 32; 32; 30; 30; 32; 32; 32; 32; 32; 27; 32; 31; 30; 32; 32; 32; 32; 32]
[32; 32; 32; 32; 32; 32; 32; 32; 32; 32; 32; 32; 32; 32; 32; 32; 27; 25; 30; 25; 32; 29; 31; 32; 23; 24; 32]
[29; 32; 26; 32; 32; 32; 29; 32; 27; 29; 30; 32; 29; 31; 32; 29; 31; 25; 32; 32; 30; 30; 30; 29; 28; 32; 32; 27; 32; 32; 29; 30]
[32; 30; 24; 26; 32; 30; 32; 32; 32; 32; 19; 25; 30; 32; 31; 32; 32; 32; 25; 19; 24; 29; 32; 32; 32]
[32; 32; 29; 32; 32; 28; 32; 32; 31; 32; 32; 31; 32; 27; 32; 32; 32; 32; 32; 32; 32; 32; 32; 32; 31; 32]
T24"""
(*
	RRBVector<length=7115,shift=10,tailOffset=7091,root=RRBNode(sizeTable=[|786; 1627; 2271; 2985; 3760; 4582; 5546; 6274; 7091|],children=[|RRBNode(sizeTable=[|31; 62; 94; 124; 156; 184; 210; 235; 263; 293; 319; 347; 374; 401; 427; 457;
  486; 515; 541; 571; 603; 632; 664; 696; 728; 760; 786|],children=(snip));
  RRBNode(sizeTable=[|25; 57; 87; 116; 148; 180; 203; 235; 267; 295; 327; 355; 379; 411; 443; 470;
  500; 532; 564; 592; 624; 656; 685; 717; 747; 778; 810; 841|],children=(snip));
  RRBNode(sizeTable=[|28; 59; 85; 117; 143; 175; 207; 239; 271; 297; 322; 350; 376; 408; 438; 470;
  498; 530; 562; 587; 613; 644|],children=(snip));
  RRBNode(sizeTable=[|32; 64; 93; 125; 157; 189; 221; 250; 281; 313; 345; 377; 409; 439; 467; 499;
  531; 563; 595; 621; 650; 682; 714|],children=(snip));
  RRBNode(sizeTable=[|29; 52; 83; 115; 147; 179; 211; 243; 275; 305; 335; 367; 399; 431; 463; 495;
  522; 554; 585; 615; 647; 679; 711; 743; 775|],children=(snip));
  RRBNode(sizeTable=[|32; 64; 96; 128; 160; 192; 224; 256; 288; 320; 352; 384; 416; 448; 480; 512;
  539; 564; 594; 619; 651; 680; 711; 743; 766; 790; 822|],children=(snip));
  RRBNode(sizeTable=[|29; 61; 87; 119; 151; 183; 212; 244; 271; 300; 330; 362; 391; 422; 454; 483;
  514; 539; 571; 603; 633; 663; 693; 722; 750; 782; 814; 841; 873; 905; 934; 964|],children=(snip));
  RRBNode(sizeTable=[|32; 62; 86; 112; 144; 174; 206; 238; 270; 302; 321; 346; 376; 408; 439; 471;
  503; 535; 560; 579; 603; 632; 664; 696; 728|],children=(snip));
  RRBNode(sizeTable=[|32; 64; 93; 125; 157; 185; 217; 249; 280; 312; 344; 375; 407; 434; 466; 498;
  530; 562; 594; 626; 658; 690; 722; 754; 785; 817|],children=(snip))|]),tail=T24>
*)
    let reprR = """
[31; 28; 32; 32; 32; 32; 30; 24; 30; 32; 32; 32; 32; 32; 32; 28; 29; 31; 32; 32; 31; 27; 32; 25; 27]
[32; 32; 29; 29; 32; 26; 32; 32; 29; 29; 32; 32; 32; 30; 24; 32; 30; 30; 25; 31; 31; 32; 29; 28]
[27; 29; 30; 26; 32; 32; 29; 32; 32; 28; 32; 32; 32; 32; 30; 32; 32; 31; 32; 32; 32; 32; 32; 32; 29; 28; 32]
[32; 32; 32; 30; 32; 32; 32; 29; 32; 27; 28; 30; 27; 28; 32; 30; 31; 32; 32; 32; 32; 32; 31; 32; 32; 32; 32; 28; 27]
[32; 32; 32; 32; 31; 32; 31; 32; 32; 32; 26; 32; 32; 32; 32; 32; 24; 32; 32; 30; 32; 32; 27]
[32; 32; 32; 31; 32; 28; 32; 32; 32; 32; 31; 31; 32; 26; 32; 32; 32; 32; 32; 32; 32]
[32; 32; 32; 32; 32; 32; 32; 32; 23; 32; 32; 28; 32; 32; 31; 32; 32; 29; 32; 32; 32; 31; 32; 32; 32; 32]
[32; 32; 32; 32; 32; 32; 32; 32; 32; 32; 32; 32; 32; 32; 32; 32; 32; 29; 32; 32; 32; 32; 32; 32; 32; 32; 32; 32; 32]
[32; 27; 32; 32; 32; 31; 32; 32; 32; 32; 31; 32; 32; 32; 32; 32; 32; 32; 32; 32; 32; 32; 32; 32; 32; 32; 32; 31; 32]
[25; 24; 20; 30; 30; 31; 32; 32; 25; 25; 21; 29; 32; 32; 32; 28; 32; 24; 23; 25; 32; 32; 32; 31; 32]
[26; 27; 31; 27; 27; 30; 29; 22; 31; 29; 28; 28; 29; 27; 22; 27; 32; 32; 27; 26; 32; 27; 31; 27]
[31; 32; 25; 25; 31; 22; 32; 29; 30; 24; 31; 32; 25; 26; 31; 26; 29; 32; 26; 30; 32; 31; 26; 32]
T23"""
(*
  RRBVector<length=10099,shift=10,tailOffset=10076,root=RRBNode(sizeTable=[|757; 1477; 2308; 3198; 3911; 4683; 5342; 6156; 7081; 8001; 8712; 9386; 10076|],children=
  [|RRBNode(sizeTable=[|31; 59; 91; 123; 155; 187; 217; 241; 271; 303; 335; 367; 399; 431; 463; 491;
  520; 551; 583; 615; 646; 673; 705; 730; 757|],children=(snip));
  RRBNode(sizeTable=[|32; 64; 93; 122; 154; 180; 212; 244; 273; 302; 334; 366; 398; 428; 452; 484;
  514; 544; 569; 600; 631; 663; 692; 720|],children=(snip));
  RRBNode(sizeTable=[|27; 56; 86; 112; 144; 176; 205; 237; 269; 297; 329; 361; 393; 425; 455; 487;
  519; 550; 582; 614; 646; 678; 710; 742; 771; 799; 831|],children=(snip));
  RRBNode(sizeTable=[|32; 64; 96; 126; 158; 190; 222; 251; 283; 310; 338; 368; 395; 423; 455; 485;
  516; 548; 580; 612; 644; 676; 707; 739; 771; 803; 835; 863; 890|],children=(snip));
  RRBNode(sizeTable=[|32; 64; 96; 128; 159; 191; 222; 254; 286; 318; 344; 376; 408; 440; 472; 504;
  528; 560; 592; 622; 654; 686; 713|],children=(snip));
  RRBNode(sizeTable=[|32; 64; 96; 127; 159; 187; 219; 251; 283; 315; 346; 377; 409; 435; 467; 499;
  531; 563; 595; 627; 659|],children=(snip));
  RRBNode(sizeTable=[|32; 64; 96; 128; 160; 192; 224; 256; 279; 311; 343; 371; 403; 435; 466; 498;
  530; 559; 591; 623; 655; 686; 718; 750; 782; 814|],children=(snip));
  RRBNode(sizeTable=[|32; 64; 96; 128; 160; 192; 224; 256; 288; 320; 352; 384; 416; 448; 480; 512;
  544; 573; 605; 637; 669; 701; 733; 765; 797; 829; 861; 893; 925|],children=(snip));
  RRBNode(sizeTable=[|32; 59; 91; 123; 155; 186; 218; 250; 282; 314; 345; 377; 409; 441; 473; 505;
  537; 569; 601; 633; 665; 697; 729; 761; 793; 825; 857; 888; 920|],children=(snip));
  RRBNode(sizeTable=[|25; 49; 69; 99; 129; 160; 192; 224; 249; 274; 295; 324; 356; 388; 420; 448;
  480; 504; 527; 552; 584; 616; 648; 679; 711|],children=(snip));
  RRBNode(sizeTable=[|26; 53; 84; 111; 138; 168; 197; 219; 250; 279; 307; 335; 364; 391; 413; 440;
  472; 504; 531; 557; 589; 616; 647; 674|],children=(snip));
  RRBNode(sizeTable=[|31; 63; 88; 113; 144; 166; 198; 227; 257; 281; 312; 344; 369; 395; 426; 452;
  481; 513; 539; 569; 601; 632; 658; 690|],children=(snip))|]),tail=(T23)>

*)
    let strL = reprL.TrimStart('\n').Replace("; ", " ").Replace("\n", " ")
    let strR = reprR.TrimStart('\n').Replace("; ", " ").Replace("\n", " ")
    let vL = strL |> RRBVecGen.treeReprStrToVec
    let vR = strR |> RRBVecGen.treeReprStrToVec


    let sL = RRBVector.toSeq vL
    let sR = RRBVector.toSeq vR
    let joined = RRBVector.append vL vR
    let joined' = RRBVector.append vR vL
    let rL = RRBVecGen.vecToTreeReprStr vL
    let rR = RRBVecGen.vecToTreeReprStr vR
    let propertyFailures = RRBVectorProps.getPropertyResults joined
    if propertyFailures.Length > 0 then
        printfn "Joined %A had some property failures" joined
        printfn "Failed properties: %A" propertyFailures
    let propertyFailures = RRBVectorProps.getPropertyResults joined'
    if propertyFailures.Length > 0 then
        printfn "Joined' %A had some property failures" joined'
        printfn "Failed properties: %A" propertyFailures

let testProperties (vec : RRBVector<'T>) name =
    let propertyFailures = RRBVectorProps.getPropertyResults vec
    if propertyFailures.Length > 0 then
        printfn "Vector %A (called \"%s\") had some property failures" vec name
        printfn "Failed properties: %A" propertyFailures
    else
        printfn "Looks good"

let debugBigTest2 () =
    // insert item at idx = split at idx + push item onto end of left + join
    let bigVecRepr = "[M M M M 30 M M M 29 M M M M M M M-1 M M 27 M M-1 M 30 M-1 29 M M M M M 30 M] [M 28 M M M-1 M-1 M M 29 M M M M 29 M M 28 M M M M M M 30 M M M M] [M 29 M 29 29 M-1 27 25 M M-1 M-1 26 M M M-1 M M-1 30 M M M M 30 26 25 M 30 22] [28 M 29 M M M 29 M 30 28 26 30 24 M 28 M M M-1 M M M 23] [M-1 M 26 25 M-1 M 29 30 M-1 M 28 M 30 M M M-1 M M-1 30 28 28 27 M M M-1 29 29] [M M 27 M M M M 30 M M M M M 30 M M 30 27 M M M M M 28 26 27 28 M M M-1 M] [28 29 M M-1 26 M-1 27 25 30 23 M 29 M M M M M M M M M M M M M M M] [M M 28 M M M 30 M M M M M M-1 M M 30 24 M M M M 28 M-1 M M M M 29 M-1 M M] [M M 30 M 30 27 29 M M-1 30 27 28 M M 29 M 29 M M-1 M M M M M] [M M M M M-1 30 M M M M M M M 28 30 M-1 M M 30 28 M M] [M M M M M M M M M M M M M-1 M M M M 23 30 25 M M M M M M M 30 M] [M M M M M M M M M M M M 30 M M M-1 M M M M M-1 M M M M M M 30 M M] [M M M M M M M M 30 28 28 M M M M M M M M M M M M M M M M M-1 M M M M] T31"
    let i = 32
    let vec = RRBVecGen.treeReprStrToVec bigVecRepr
    let vec' = vec |> RRBVector.insert i 512
    testProperties vec' (sprintf "Vector after inserting 512 at idx %d" i)
    let vL, vR = RRBVectorExpectoTest.doSplitTest vec i
    let vL' = vL |> RRBVector.push 512
    let joined = RRBVector.append vL' vR
    testProperties joined "Joined vector"
    if joined = vec' then printfn "Good" else printfn "Split + push left + joined vectors did not equal insertion into original vector"


let debugBigTest3 () =
    // starting with 2 vectors, remove idx 0 of left + join = remove idx 0 of entire
    let bigVecLRepr = "[M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M] [M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M] [M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M] [M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M] [M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M] [M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M] [M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M] [M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M] [M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M] [M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M] [M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M] [M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M] [M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M] T31"
    let bigVecRRepr = "[M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M] [M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M] [M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M] [M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M] [M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M] [M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M] [M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M] [M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M] T23"
    let vL = RRBVecGen.treeReprStrToVec bigVecLRepr
    let vR = RRBVecGen.treeReprStrToVec bigVecRRepr
    let vL' = RRBVector.remove 0 vL
    let joinedOrig = RRBVector.append vL vR
    let joined = RRBVector.append vL' vR
    testProperties joined "Joined vector"
    if joined = (RRBVector.remove 0 joinedOrig) then printfn "Good" else printfn "remove idx 0 of left + join did not equal join + remove idx 0"
    ()

let debugBigTest4 () =
        let vec = RRBVecGen.treeReprStrToVec "M-1 M T1"
        let vec' = vec.Take (Literals.blockSize * 2 - 1)
        testProperties vec' "Sliced vector"
        match vec' with
        | :? RRBSapling<int> as sapling ->
            Expect.equal sapling.TailOffset (Literals.blockSize) "Wrong tail offset"
            Expect.equal sapling.Tail.Length (Literals.blockSize - 1) "Wrong tail length"
        | :? RRBTree<int> as tree -> // Or perhaps just: failwithf "Vector after slice should be sapling, instead is %A" tree
            Expect.equal tree.TailOffset (Literals.blockSize) "Wrong tail offset"
            Expect.equal tree.Tail.Length (Literals.blockSize - 1) "Wrong tail length"

let doJoinTest v1 v2 =
    testProperties v1 "v1 in join test"
    testProperties v2 "v2 in join test"
    let s1 = RRBVector.toSeq v1
    let s2 = RRBVector.toSeq v2
    let joined = RRBVector.append v1 v2
    let joined' = RRBVector.append v2 v1
    testProperties joined "Joined vector"
    testProperties joined' "Opposite-joined vector"
    Expect.sequenceEqual (RRBVector.toSeq joined) (Seq.append s1 s2) "Joined vectors did not sequenceEqual equivalent appended seqs"
    Expect.sequenceEqual (RRBVector.toSeq joined') (Seq.append s2 s1) "Opposite-joined vectors did not sequenceEqual equivalent appended seqs"

let doSplitTest vec i =
    let repr = RRBVecGen.vecToTreeReprStr vec
    testProperties vec "Original vector"
    let vL, vR = vec |> RRBVector.split i
    testProperties vL (sprintf "Original vector was %A, split at %d\nRepr: %s\nLeft half of split" vec i repr)
    testProperties vR (sprintf "Original vector was %A, split at %d\nRepr: %s\nRight half of split" vec i repr)
    doJoinTest vL vR
    vL, vR

let debugSmallTest1 () =
    // let vecRepr = "[30 M M M 26 28 M 30 29 M M M M M M 25 M 27 M 30 M M-1 25 M 30 M 26 30 M 30 M 7] [M-1] T1"
    // Try this instead:
    // let vecRepr = "M-1 M-1 M M M 26 28 M 30 29 M M M M M M 25 M 27 M 30 M M-1 25 M 30 M 26 30 M 30 M T7"
    let vecRepr = "5 M*M-1 T7"
    // The other one was the result *after* the bad split/append
    let vec = vecRepr |> RRBVecGen.treeReprStrToVec
    let i = 32
    let vL, vR = doSplitTest vec i
    let vL', vR' =
        if vL.Length > 0 then
            RRBVector.remove 0 vL, vR
        else
            // Can't remove from an empty vector -- but in this case, we know the right vector is non-empty
            vL, RRBVector.remove 0 vR
    let joined = RRBVector.append vL' vR'
    testProperties joined "Joined vector"
    if joined = (RRBVector.remove 0 vec) then printfn "Good" else printfn "Split + remove idx 0 of left + joined vectors did not equal original vector with its idx 0 removed"


// TODO: All those debugBigTest functions need to be turned into specific Expecto tests

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
        debugSmallTest1()
        0
    elif argv |> Array.contains "--stress" then
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
