module ExpectoTemplate.TreeParser

open FParsec

let [<Literal>] M = Ficus.Literals.blockSize

type ListItem =
    | Int of int
    | List of ListItem list

let isInt = function
    | Int _ -> true
    | List _ -> false

let isList = function
    | Int _ -> false
    | List _ -> true

type TreeRepresentation =
    { Root: ListItem
      Tail: int option }
let mkTree (root,tail) = { Root = root; Tail = tail }

let pnum = pint32 <|> (pchar 'M' >>% M)

let pdivision = pnum .>>. (pchar '/' >>. pnum) // Note no spaces allowed
                |>> (fun (a,b) -> a / b)

let pdivExpr = attempt pdivision <|> pnum

let psubtraction = pdivExpr .>>. (pchar '-' >>. pdivExpr) // *Still* no spaces allowed
                   |>> (fun (a,b) -> a - b)
let paddition    = pdivExpr .>>. (pchar '+' >>. pdivExpr) // *Still* no spaces allowed
                   |>> (fun (a,b) -> a + b)

let pexpr = attempt psubtraction <|> attempt paddition <|> pdivExpr

let pmultiplication = pchar '*' >>. pexpr


let pmultipliedExpr = pexpr .>>.? pmultiplication
                      |>> (fun (n, factor) -> List.init factor (fun _ -> n))

let pmaybeMultipliedExpr = pmultipliedExpr <|> (pexpr |>> List.singleton)
let pmixedExprs = many (pmaybeMultipliedExpr .>> spaces) |>> List.collect id
let pmixedIntList = pmixedExprs |>> List.map Int

let ptail = pchar 'T' >>. pexpr

let plist,plistImpl = createParserForwardedToRef()

let plistMult = plist .>>. pmultiplication |>> (fun (lst,n) -> List.init n (fun _ -> List lst))

let plistOrListMult = (attempt plistMult) <|> (plist |>> (List >> List.singleton))
let pmixedLists = many1 (plistOrListMult .>> spaces) |>> List.collect id
let pListContents = pmixedLists <|>
                    pmixedIntList <|>
                    (many (pexpr |>> Int  .>> spaces))

plistImpl := pchar '[' >>. spaces
                       >>. pListContents
                       .>> pchar ']'

let proot = pListContents |>> List

let pTreeRepr : Parser<_,unit> = proot .>> spaces .>>. opt ptail |>> mkTree
