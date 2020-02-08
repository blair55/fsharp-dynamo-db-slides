#load ".paket/load/netcoreapp3.1/main.group.fsx"

open System
open System.IO
open System.IO.Compression
open System.Collections.Generic
open Newtonsoft.Json
open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model

let table = "nick-test"
let client = new AmazonDynamoDBClient ()
let run f = (Async.AwaitTask >> Async.RunSynchronously) f
let srlz o = JsonConvert.SerializeObject(o, Formatting.Indented)
let prnt x = printfn "\n\n---------\n\n%s\n\n---------\n\n" x
let toDictionary m = dict m |> Dictionary<string,AttributeValue>
let rsp f g =
  try (run >> f >> srlz >> prnt) g
  with | :? AggregateException as e -> prnt e.InnerException.Message





let ``empty attribute value`` =
  new AttributeValue()
  |> srlz
  |> prnt









let ``basic put item`` =
  [ "id", new AttributeValue (S = "foo")
    "total", new AttributeValue (N = "123.45")
    "isOk", new AttributeValue (BOOL = true) ]
  |> toDictionary
  |> fun attributes ->
    new PutItemRequest (table, attributes)
    |> client.PutItemAsync
    |> rsp id










let ``basic get item`` =
  [ "id", new AttributeValue (S = "foo") ]
  |> toDictionary
  |> fun attributes ->
    new GetItemRequest (table, attributes)
    |> client.GetItemAsync
    |> rsp (fun r -> r.Item)














let ``advanced put item`` =
  [ "id", new AttributeValue (S = "foo")
    "set_of_items", new AttributeValue (SS = ResizeArray ["aaa"; "bbb"])
    "list_of_iems", new AttributeValue (L = ResizeArray [ new AttributeValue (S = "ccc")
                                                          new AttributeValue (N = "22.97")
                                                          new AttributeValue (BOOL = true) ])
    "map_of_items", new AttributeValue (M = ([ "name", new AttributeValue (S = "ddd")
                                               "size", new AttributeValue (N = "9")
                                               "locations", new AttributeValue (L = ResizeArray [ new AttributeValue (S = "xxx") ]) ]
                                             |> toDictionary)) ]
  |> toDictionary
  |> fun attributes ->
    new PutItemRequest (table, attributes)
    |> client.PutItemAsync
    |> rsp id












/// - no constuctor
/// - more than one constructor
/// - non number in N
/// - empty set
/// - set duplicates


let ``basic put item with error`` =
  [ "id", new AttributeValue (S = "foo")
    "my_attr", new AttributeValue () ]
    // "my_attr", new AttributeValue (S="abc", N="43") ]
    // "my_attr", new AttributeValue (N="not a number") ]
    // "my_attr", new AttributeValue (SS = ResizeArray []) ]
    // "my_attr", new AttributeValue (SS = ResizeArray ["aaa"; "aaa"]) ]
  |> toDictionary
  |> fun attributes ->
    new PutItemRequest (table, attributes)
    |> client.PutItemAsync
    |> rsp id










let mySet = Set.empty;;
mySet |> Set.add 5;;
mySet |> Set.add 5 |> Set.add 4;;
mySet |> Set.add 5 |> Set.add 4 |> Set.add 5;;

















/// The attribute type domain model

type Attr =
  | Attr of name:string * value:AttrValue

and AttrValue =
  | ScalarString of string
  | ScalarDecimal of decimal
  | ScalarBinary of string
  | ScalarBool of bool
  | ScalarNull
  | SetString of NonEmptyList<string>
  | SetDecimal of NonEmptyList<decimal>
  | SetBinary of NonEmptyList<string>
  | DocList of AttrValue list
  | DocMap of Attr list

and NonEmptyList<'a> =
  | NonEmptyList of h:'a * t:'a list








let toSet (NonEmptyList (h, t)) =
  Set.ofList t |> Set.add h


let toGzipMemoryStream (s:string) =
  let output = new MemoryStream ()
  use zipStream = new GZipStream (output, CompressionMode.Compress, true)
  use writer = new StreamWriter (zipStream)
  writer.Write s
  output


let rec mapAttrValue = function
  | ScalarString  s -> new AttributeValue (S = s)
  | ScalarDecimal n -> new AttributeValue (N = string n)
  | ScalarBinary  s -> new AttributeValue (B = toGzipMemoryStream s)
  | ScalarBool    b -> new AttributeValue (BOOL = b)
  | ScalarNull      -> new AttributeValue (NULL = true)
  | SetString    ss -> new AttributeValue (SS = ResizeArray (toSet ss))
  | SetDecimal   ns -> new AttributeValue (NS = ResizeArray (Seq.map string (toSet ns)))
  | SetBinary    bs -> new AttributeValue (BS = ResizeArray (Seq.map toGzipMemoryStream (toSet bs)))
  | DocList       l -> new AttributeValue (L = ResizeArray (List.map mapAttrValue l))
  | DocMap        m -> new AttributeValue (M = mapAttrsToDictionary m)

and mapAttr (Attr (name, value)) =
  name, mapAttrValue value

and mapAttrsToDictionary =
  List.map mapAttr >> dict >> Dictionary<string,AttributeValue>









let putItem tableName fields =
  use client = new AmazonDynamoDBClient()
  new PutItemRequest (tableName, mapAttrsToDictionary fields)
  |> client.PutItemAsync
  |> rsp id


let ``advanced put item using model`` =
  [ Attr ("id", ScalarString "Example")
    Attr ("Day", ScalarString "Monday")
    Attr ("UnreadEmails", ScalarDecimal 42m)
    Attr ("ItemsOnMyDesk",
      DocList
        [ ScalarString "Coffee Cup"
          ScalarString "Telephone"
          DocMap
            [ Attr ("Pens", DocMap [ Attr ("Quantity", ScalarDecimal 3m) ])
              Attr ("Pencils", DocMap [ Attr ("Quantity", ScalarDecimal 2m) ])
              Attr ("Erasers", DocMap [ Attr ("Quantity", ScalarDecimal 1m) ])
            ]
        ])
    Attr ("ASetOfNumbers",
      SetDecimal (NonEmptyList (2m, [3m; 4m; 5m])))
  ]
  |> putItem table






/// no longer can we
/// - no constuctor
/// - more than one constructor
/// - non number in N
/// - empty set
/// - set duplicates





