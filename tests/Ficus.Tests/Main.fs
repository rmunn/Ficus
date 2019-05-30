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
        printfn "Vector %A (called \"%s\" and with repr %s) had some property failures" vec name (RRBVecGen.vecToTreeReprStr vec)
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

let debugJoinTest () =
        // Already a unit test called "Joining vectors where the left tree is taller than the right produces valid results"
        // let vL = RRBVecGen.treeReprStrToVec "[M*M]*3 TM/4"
        // let vR = RRBVecGen.treeReprStrToVec "M*M/2 TM"
        let vL = RRBVecGen.treeReprStrToVec "[M*M] [M*M] [M*M] [M*20] TM"
        let vR = RRBVecGen.treeReprStrToVec "M M M M 5 TM-3"
        testProperties vL "Left half of merge"
        testProperties vR "Right half of merge"
        let joined = RRBVector.append vL vR
        testProperties joined <| sprintf "Joined vector"

let debugPushTailDown () =
    let mutable vec = RRBVecGen.treeReprStrToVec "17 16 M M M M M M M M M M M M M M M M M M 17 16 M 17 16 M M M M M M M T32"   // A nearly-full vector containing a few insertion splits
    for i = 1 to 33 do
        vec <- vec.Push i
    testProperties vec "Vector after pushing"



open RRBVectorMoreCommands.ParameterizedVecCommands
open Ficus.RRBVector

let debugReallyBigTest1 () =
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
        printfn "Starting with %s" (RRBVecGen.vecToTreeReprStr vec)
        let mutable current = vec
        let mergeL = RRBVecGen.treeReprStrToVec >> RRBVectorMoreCommands.ParameterizedVecCommands.mergeL
        let mergeR = RRBVecGen.treeReprStrToVec >> RRBVectorMoreCommands.ParameterizedVecCommands.mergeR
        let actions = [mergeL "M T19"; push 99; mergeL "0 T28"; push 80; push 127; push 17; push 30;
                       push 138; remove -109; mergeR "M T9"; push 91; push 72; insert (-90,126); push 64;
                       push 52; insert (-138,1); push 130]
        let logVec action vec = printfn "After %s, vec was %s with actual structure %A" (action.ToString()) (RRBVecGen.vecToTreeReprStr vec) vec
        for action in actions do
            if action.ToString() = "remove -109" then
                printfn "Breakpoint here"
            current <- current |> action.RunActual
            logVec action current
            testProperties current <| sprintf "Vector after %s" (action.ToString())

// TODO: All those debugBigTest functions need to be turned into specific Expecto tests

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



let doJoinTest v1 v2 =
    testProperties v1 "v1 in join test"
    testProperties v2 "v2 in join test"
    let arr1 = RRBVector.toArray v1
    let arr2 = RRBVector.toArray v2
    let joined = RRBVector.append v1 v2
    let joined' = RRBVector.append v2 v1
    localCheck joined
    testProperties joined "Joined vector"
    localCheck joined'
    testProperties joined' "Opposite-joined vector"
    Expect.equal (RRBVector.toArray joined) (Seq.append arr1 arr2) "Joined vectors did not equal equivalent appended arrays"
    Expect.equal (RRBVector.toArray joined') (Seq.append arr2 arr1) "Opposite-joined vectors did not equal equivalent appended arrays"

let debugBigJoinTest1 () =
    // This one already is in Expecto, but it's failing and I'm trying to hunt down the failure
    let bigNum = 5 <<< (Literals.blockSizeShift * 3)
    let v1 = seq {0..bigNum+6} |> RRBVector.ofSeq  // Top level has 5 completely full FullNodes, tail has 7 items
    let v2 = RRBVecGen.treeReprStrToVec "[M 2 M/2*M-6 M-1 M-2 M-1 M/2-1] [M*M-6 M-1 M-3 M M M-3 M] [M-2 M-3 M-2 M-2 M-1 M-2*M-6 M] TM"

    let massiveVector = RRBVecGen.treeReprStrToVec "[[M*M]*M] [[M*M]*M] [[M*M]*M] [[M*M]*M] [[M*M]*M] [[M 9 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 16 M-1 30 M-1 15] [M M M M M M M M M M M M M M M M M M M M M M M M M M M-1 29 M M 29 M] [30 29 30 30 M-1 30 30 30 30 30 30 30 30 30 30 30 30 30 30 30 30 30 30 30 30 30 30 30 30 30 30 M]] T32"
    testProperties massiveVector "Massive vector"

    doJoinTest v1 v2

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

let debugToArray () =
    let a1 = [|1..63|]
    let a2 = [|1..65|]
    let v1 = RRBVector.ofArray a1
    let v2 = RRBVector.ofArray a2
    let a1' = v1 |> RRBVector.toArray
    let a2' = v2 |> RRBVector.toArray
    if a1 = a1' then
        printfn "Good 1"
    if a2 = a2' then
        printfn "Good 2"

// open ExpectoTemplate.RRBVectorMoreCommands.ParameterizedVecCommands
let debugNullReference () =
    let mergeL' = mergeL << RRBVecGen.treeReprStrToVec
    let mergeR' = mergeR << RRBVecGen.treeReprStrToVec
    // let actions = [push 84; mergeL' "M T4"; push 21; push 95; mergeL' "M T13"; push 27; push 73; mergeR' "M T2"; mergeR' "0 T17"; push 110; push 25; push 23; push 112; push 88; push 41; push 96; push 41; push 96; rev(); pop 118]
    let actions = [push 84; mergeL' "M T4"; push 115; mergeL' "M T13"; push 100; mergeR' "M T2"; mergeR' "0 T17"; push 632; rev(); pop 118]
    let actionsShort = [push 1063; rev(); pop 118]
    let start = RRBVector.empty
    let mutable current = start
    let logVec action vec = printfn "After %O, vec was %s" action (RRBVecGen.vecToTreeReprStr vec)
    for action in actionsShort do
        current <- current |> action.RunActual
        logVec action current
        testProperties current <| sprintf "Vector after %O" action
    printfn "All done, breakpoint here"


let debugSomethingElse () =
    let mergeL = mergeL << RRBVecGen.treeReprStrToVec
    let mergeR = mergeR << RRBVecGen.treeReprStrToVec
    let scanf a _ = a + 1  // So that scans will produce increasing sequences
    let mapf a = a  // So that map won't change the numbers
    let actions = [
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
    let actionsShort = [ map mapf; mergeR "M T3"; insert (-42,114) ] // Removed "push 66", which we'll do at the end
    let actionsEvenShorter = [ insert (-42,114) ] // Removed "push 66", which we'll do at the end
    let start = RRBVector.empty
    let startFull = RRBVecGen.treeReprStrToVec "[26 18 25 24 17 26 24 M M M M M M M M M M M M M M M M M M M] [M M M M M M M M M M M M M M M M M M M M M M M M M 17 16 M 17 25 24 M] [M M M M 17 16 M] T24"
    let startEvenShorter = { 0..1982 } |> RRBVector.ofSeq
    // [M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M] [M M M M M M M M M M M M M M M M M M M M M M M M M M M M M] T31
    // let mutable current = startFull
    let mutable current = startEvenShorter
    let logVec action vec = printfn "After %O, vec was %s" action (RRBVecGen.vecToTreeReprStr vec)
    for action in actionsEvenShorter do
        current <- current |> action.RunActual
        logVec action current
        testProperties current <| sprintf "Vector after %O" action
    for i = 1 to 65 do
        current <- current.Push i
        printfn "After pushing %d, vec was %s" i (RRBVecGen.vecToTreeReprStr current)
        testProperties current <| sprintf "Vector after pushing %d" i
    current <- current.Push 66
    printfn "After pushing %d, vec was %s" 66 (RRBVecGen.vecToTreeReprStr current)
    testProperties current <| sprintf "Vector after pushing %d" 66
    printfn "All done, breakpoint here"

let debugSomethingElse2 () =
    let mergeL = mergeL << RRBVecGen.treeReprStrToVec
    let mergeR = mergeR << RRBVecGen.treeReprStrToVec
    let scanf a _ = a + 1  // So that scans will produce increasing sequences
    let mapf a = a  // So that map won't change the numbers
    let actions = [
            push 501;
            mergeR "0 T14"; push 155; mergeR "26 30 M 28 28 23 29 30 30 26 M T24";
            mergeL "M T18"; map mapf ]
    let actionsShort = [ map mapf; mergeR "M T3"; insert (-42,114) ] // Removed "push 66", which we'll do at the end
    let actionsEvenShorter = [ insert (-42,114) ] // Removed "push 66", which we'll do at the end
    let start = RRBVector.empty
    let startFull = RRBVecGen.treeReprStrToVec "[26 18 25 24 17 26 24 M M M M M M M M M M M M M M M M M M M] [M M M M M M M M M M M M M M M M M M M M M M M M M 17 16 M 17 25 24 M] [M M M M 17 16 M] T24"
    let startEvenShorter = { 0..1982 } |> RRBVector.ofSeq
    // [M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M M] [M M M M M M M M M M M M M M M M M M M M M M M M M M M M M] T31
    // let mutable current = startFull
    let mutable current = start
    let logVec action vec = printfn "After %O, vec was %s" action (RRBVecGen.vecToTreeReprStr vec)
    for action in actions do
        current <- current |> action.RunActual
        logVec action current
        testProperties current <| sprintf "Vector after %O" action
    for i = 1 to 71 do
        current <- current.Pop()
        printfn "After popping %d times, vec was %s" i (RRBVecGen.vecToTreeReprStr current)
        testProperties current <| sprintf "Vector after popping %d times" i
        let arr = current |> RRBVector.toArray
        if arr.Length <> RRBVector.length current then
            printfn "Oops"
    current <- current.Pop()
    printfn "After popping %d times, vec was %s" 72 (RRBVecGen.vecToTreeReprStr current)
    testProperties current <| sprintf "Vector after popping %d times" 72
    let arr = current |> RRBVector.toArray
    if arr.Length <> RRBVector.length current then
        printfn "Oops"
    printfn "All done, breakpoint here"

let debugSomethingElse3() =
    let mutable current = RRBVecGen.treeReprStrToVec "[M*M]*M TM-3"
    printfn "Starting with %s" (RRBVecGen.vecToTreeReprStr current)
    for i = 1 to Literals.blockSize + 6 do
      current <- current.Push(i)
      printfn "After pushing %d times, we have %s" i (RRBVecGen.vecToTreeReprStr current)
      testProperties current <| sprintf "Vector after pushing %d times" i
    current <- current |> RRBVector.rev
    printfn "After rev(), we have %s" (RRBVecGen.vecToTreeReprStr current)
    testProperties current <| sprintf "Vector after rev()"
    printfn "All done, breakpoint here"

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
