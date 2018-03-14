module ExpectoTemplate.Main
open Expecto
open System.Reflection
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


let debug () =
    let v1 = [|1..42|] |> RRBVector.ofArray
(*
    RRBSapling<length=42,shift=0,tailOffset=32,root=[|-68; -67; -34; -54; 89; 90; -70; -90; -89; 54; 34; 67; 68; 18; -2; 31; 32; -18;
    -38; -5; -4; -24; -74; -73; -40; -60; 83; 84; -76; -96; 47; 48|],tail=[|28; 61; 62; 12; -8; 25; 26; -24; -44; -11|]>
*)
    let v2 = RRBVecGen.treeReprStrToVec "[25 30 24 29] [11 9 12 9] [18 20 16 M] T14"
(*
    RRBVector<length=251,shift=10,tailOffset=237,root=RRBNode(sizeTable=[|108; 149; 237|],children=[|RRBNode(sizeTable=[|25; 55; 79; 108|],children=[|[|47; -67; -32; 52; 36; 6; -4; 42; -41; 83; -44; 84; 23; -23; 86; -38; -87;
        -27; 88; -71; -25; 84; 74; 55; 52|];
    [|49; -54; 73; -38; 48; -13; 29; 62; 63; 43; -7; -6; 27; 7; -43; -42; -9; -29;
        -11; 80; -26; -56; -16; 96; -39; 65; 33; 79; -15; 27|];
    [|93; 51; -50; -26; 37; -13; -33; 0; 4; 96; 35; 77; -83; -82; 61; 41; 42; 75;
        25; 5; 6; 39; 19; -31|];
    [|5; -40; 2; 35; 36; -14; -34; -1; 0; -50; -70; -69; -36; -56; 87; 88; -72;
        -92; 51; 52; 85; 65; 15; 16; -4; 29; 30; -20; -40|]|]);
    RRBNode(sizeTable=[|11; 20; 32; 41|],children=[|[|27; -22; -54; -12; -11; -31; 2; -48; -47; -67; -66|];
    [|95; -45; -67; -53; -73; 8; -65; 63; 40|];
    [|72; -27; -15; -14; 19; -1; -51; -50; 73; 73; 33; 59|];
    [|58; -24; 34; 76; 77; -83; 60; 40; 41|]|]);
    RRBNode(sizeTable=[|18; 38; 56; 88|],children=[|[|22; 2; 3; 36; -14; -34; -33; 0; -50; -70; -69; -36; -86; 87; 88; 68; -92;
        -76|];
    [|42; 43; 76; 26; 6; 7; -13; 20; -30; -29; -49; -16; -66; -65; -85; -84; -6;
        74; -34; 61|];
    [|83; 63; 96; 46; 47; 27; 28; 61; 11; -9; -8; 25; -25; -45; -44; 2; 82; 6|];
    [|-90; -89; -56; 87; 67; 68; 48; 81; 31; 32; 12; 45; -5; -4; 42; -41; 46; 88;
        -49; -37; -36; -56; -23; -73; -72; -92; -91; 90; -24; 41; -26; 63|]|])|]),tail=[|-56; -22; 96; 67; 7; 18; 96; -95; -35; 17; 51; -52; -6; 89|]>
*)
    let repr1 = RRBVecGen.vecToTreeReprStr v1
    let repr2 = RRBVecGen.vecToTreeReprStr v2
    printfn "%A %A" repr1 repr2

(*
    let arr = [|1..67|]
    let expected = arr
    let s = arr |> Seq.ofArray
    let actual = s |> RRBHelpers.buildTreeOfSeqWithKnownSize arr.Length
    printfn "Vector constructed was %A" actual

    let mutable updated = actual
    for i = 1 to 10 do
        updated <- updated.Insert 0 i
        // updated <- updated.Remove 5
        let propertyFailures = RRBVectorProps.getPropertyResults updated
        if propertyFailures.Length > 0 then
            printfn "Vector %A had some property failures" updated
            printfn "Failed properties: %A" propertyFailures

    let repr = RRBVecGen.vecToTreeReprStr updated
    printfn "Repr was %A" repr
    printfn "Breakpoint here"
*)
    let vec = [|1..40|] |> RRBVector.ofArray
    let i = 0
    let vL, vR = RRBVectorExpectoTest.doSplitTest vec i
    let revL = RRBVector.rev vL
    let revR = RRBVector.rev vR
    let vec' = RRBVector.append revR revL
    RRBVectorProps.checkProperties vec' "Joined vector"


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
        debugBigTest()
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
