#load ".paket/load/netcoreapp3.1/main.group.fsx"

open System
open System.IO
open System.IO.Compression
open System.Collections.Generic
open Newtonsoft.Json
open Amazon
open Amazon.Runtime
open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model

let table = "fs-wrapper"
let credentials = new StoredProfileAWSCredentials("sandbox");
let client = new AmazonDynamoDBClient (credentials, RegionEndpoint.EUWest2)
let runSync f = (Async.AwaitTask >> Async.RunSynchronously) f
let srlz o = JsonConvert.SerializeObject(o, Formatting.Indented)
let prnt = printfn "\n\n---------\n\n%s\n\n---------\n\n"
let toDictionary m = dict m |> Dictionary<string,AttributeValue>
let rsp f g =
  try (runSync >> f >> srlz >> prnt) g
  with | :? AggregateException as e -> prnt e.InnerException.Message
       | _ as e -> prnt e.Message



/// THEME / SIZE







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
  [ "id", new AttributeValue (S = "rich")

    "set_of_items", new AttributeValue (
                      SS = ResizeArray ["aaa"; "bbb"])

    "list_of_items", new AttributeValue (
                      L = ResizeArray [ new AttributeValue (S = "ccc")
                                        new AttributeValue (N = "22.97")
                                        new AttributeValue (BOOL = true) ])

    "map_of_items", new AttributeValue (
                      M = ([ "name", new AttributeValue (S = "ddd")
                             "size", new AttributeValue (N = "9")
                             "locations",
                                new AttributeValue (
                                 L = ResizeArray [ new AttributeValue (S = "xxx") ]) ]
                           |> toDictionary)) ]
  |> toDictionary
  |> fun attributes ->
    new PutItemRequest (table, attributes)
    |> client.PutItemAsync
    |> rsp id














let ``get rich item`` =
  [ "id", new AttributeValue (S = "rich") ]
  |> toDictionary
  |> fun attributes ->
    new GetItemRequest (table, attributes)
    |> client.GetItemAsync
    |> rsp (fun r -> r.Item)




















let ``basic put item with error`` =
  [ "id", new AttributeValue (S = "fail")
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



















Set.empty |> Set.add 4
Set.empty |> Set.add 4 |> Set.add 5
Set.empty |> Set.add 4 |> Set.add 5 |> Set.add 4






















/// The attribute type domain model

type Attr =
  | Attr of name:string * value:AttrValue

and AttrValue =

  | ScalarString of String
  | ScalarInt32 of Int32
  | ScalarDecimal of Decimal
  | ScalarBinary of String
  | ScalarBool of Boolean
  | ScalarGuid of Guid
  | ScalarDate of DateTime
  | ScalarNull

  | SetString of NonEmptyList<String>
  | SetDecimal of NonEmptyList<Decimal>
  | SetInt32 of NonEmptyList<Int32>
  | SetBinary of NonEmptyList<String>

  | DocList of AttrValue list
  | DocMap of Attr list

and NonEmptyList<'a> when 'a : comparison =
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
  | ScalarGuid    g -> new AttributeValue (S = string g)
  | ScalarDate    d -> new AttributeValue (S = d.ToString("s"))
  | ScalarInt32   i -> new AttributeValue (N = string i)
  | ScalarDecimal d -> new AttributeValue (N = string d)
  | ScalarBinary  s -> new AttributeValue (B = toGzipMemoryStream s)
  | ScalarBool    b -> new AttributeValue (BOOL = b)
  | ScalarNull      -> new AttributeValue (NULL = true)
  | SetString    ss -> new AttributeValue (SS = ResizeArray (toSet ss))
  | SetDecimal   sd -> new AttributeValue (NS = ResizeArray (Seq.map string (toSet sd)))
  | SetInt32     si -> new AttributeValue (NS = ResizeArray (Seq.map string (toSet si)))
  | SetBinary    bs -> new AttributeValue (BS = ResizeArray (Seq.map toGzipMemoryStream (toSet bs)))
  | DocList       l -> new AttributeValue (L = ResizeArray (List.map mapAttrValue l))
  | DocMap        m -> new AttributeValue (M = mapAttrsToDictionary m)

and mapAttr (Attr (name, value)) =
  name, mapAttrValue value

and mapAttrsToDictionary =
  List.map mapAttr >> dict >> Dictionary<string,AttributeValue>















let putItem tableName fields =
  new PutItemRequest (tableName, mapAttrsToDictionary fields)
  |> client.PutItemAsync
  |> rsp id


let ``advanced put item using model`` =
  [ Attr ("id", ScalarString "rich item")
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
      SetDecimal (NonEmptyList (2m, [3m; 4m; 2m])))
  ]
  |> putItem table










let ``get rich item 2`` =
  [ Attr ("id", ScalarString "rich item") ]
  |> fun attrs -> new GetItemRequest (table, mapAttrsToDictionary attrs)
  |> client.GetItemAsync
  |> rsp (fun r -> r.Item)















/// <<< view item in table
/// <<< back to slides
















type Customer =
  { Id : Guid
    Email : String
    IsVerified : Boolean
    DateOfBirth : DateTime
    Balance : Decimal }





let ``put customer`` (customer:Customer) =
  [ Attr ("id", ScalarGuid customer.Id)
    Attr ("email", ScalarString customer.Email)
    Attr ("verified", ScalarBool customer.IsVerified)
    Attr ("dob", ScalarDate customer.DateOfBirth)
    Attr ("balance", ScalarDecimal customer.Balance) ]
  |> putItem table





let customerId = Guid.NewGuid()

let customer =
  { Id = customerId
    Email = "someone@tm.com"
    IsVerified = true
    DateOfBirth = new DateTime(2000,01,01)
    Balance = 3405.25m }


``put customer`` customer








let makeCustomer (d:Dictionary<string,AttributeValue>) =
  { Id          =     Guid.Parse d.["id"].S
    Email       =                d.["email"].S
    IsVerified  =                d.["verified"].BOOL
    DateOfBirth = DateTime.Parse d.["dob"].S
    Balance     =        decimal d.["balance"].N }







let ``get customer`` customerId f =
  [ Attr ("id", ScalarGuid customerId) ]
  |> mapAttrsToDictionary
  |> fun attrs -> new GetItemRequest (table, attrs)
  |> client.GetItemAsync
  |> rsp (fun r -> f r.Item)


``get customer`` customerId makeCustomer 









/// ERRORS?


// incorrect attribute name
// read wrong property
// corrupt data
// no record returned


let makeCustomerWithError (d:Dictionary<string,AttributeValue>) =
  { Id          =     Guid.Parse d.["id"].S
    Email       =                d.["imail"].S
    IsVerified  =                d.["verified"].BOOL
    DateOfBirth = DateTime.Parse d.["dob"].S
    Balance     =        decimal d.["balance"].N }



``get customer`` customerId makeCustomerWithError

// ``get customer`` (Guid.NewGuid()) makeCustomerWithError






















let toMap d =
  Seq.map (|KeyValue|) d |> Map.ofSeq

let optionToResult e = function
  | Some x -> Ok x
  | None -> Error e

let parseGuid e (s:String) =
  match Guid.TryParse s with
  | true, x -> Ok x
  | _ -> Error e

let parseDate e (s:String) =
  match DateTime.TryParse s with
  | true, x -> Ok x
  | _ -> Error e

let parseDecimal e (s:String) =
  match Decimal.TryParse s with
  | true, x -> Ok x
  | _ -> Error e



let getAttr key f =
  Map.tryFind key
  >> optionToResult (sprintf "could not find attr %s" key)
  >> Result.map f

let getAttrString key =
  getAttr key (fun (a:AttributeValue) -> a.S)
  
let getAttrBool key =
  getAttr key (fun (a:AttributeValue) -> a.BOOL)

let getAttrNumber key =
  getAttr key (fun (a:AttributeValue) -> a.N)

let getAttrDate key =
  getAttrString key
  >> Result.bind (parseDate (sprintf "could not parse %s as date" key))

let getAttrGuid key =
  getAttrString key
  >> Result.bind (parseGuid (sprintf "could not parse %s as guid" key))

let getAttrDecimal key =
  getAttrNumber key
  >> Result.bind (parseDecimal (sprintf "could not parse %s as decimal" key))






// let makeCustomerUsingResultTypes (m:Map<string,AttributeValue>) =
//   { Id          = getAttrGuid    "id" m
//     Email       = getAttrString  "email" m
//     IsVerified  = getAttrBool    "verified" m
//     DateOfBirth = getAttrDate    "dob" m
//     Balance     = getAttrDecimal "balance" m }






type AttrReader<'a> =
  AttrReader of (Map<string, AttributeValue> -> 'a)


/// "give me an attr key, and I will give a you a AttrReader that, 
///  when given an map of attributes, if successful,
///  will return the string value from the attribue"


let stringAttrReader key =
  AttrReader (getAttrString key)

let guidAttrReader =
  getAttrGuid >> AttrReader 

let dateAttrReader =
  getAttrDate >> AttrReader

let decimalAttrReader =
  getAttrDecimal >> AttrReader

let boolAttrReader =
  getAttrBool >> AttrReader






// let makeCustomerUsingResultReaders (m:Map<string,AttributeValue>) =
//   { Id          = guidAttrReader    "id"
//     Email       = stringAttrReader  "email"
//     IsVerified  = boolAttrReader    "verified"
//     DateOfBirth = dateAttrReader    "dob"
//     Balance     = decimalAttrReader "balance" }







/// Async<'a>
/// Option<'a>
/// Result<'a>

/// AttrReader<'a>












module AttrReader =

  let run (AttrReader f) m =
    f m

  let retn a =
    AttrReader (fun _ -> a)

  let map f attrReader =
    let newReader m =
      let x = run attrReader m
      f x
    AttrReader newReader

  let bind f attrReader = 
    let newReader m =
      let a = run attrReader m
      let bReader = f a
      run bReader m 
    AttrReader newReader

  let apply fReader attrReader =
    let newReader m =
      let f = run fReader m
      let a = run attrReader m
      f a
    AttrReader newReader








let buildCustomer id email verified dob balance =
  { Id = id 
    Email = email
    IsVerified = verified 
    DateOfBirth = dob 
    Balance = balance }


// let ``customer reader`` =
//   let builder = AttrReader.map buildCustomer
//   builder (guidAttrReader "id")




/// Async<'a>
/// Option<'a>
/// Result<'a>

/// AttrReader<'a>
/// AttrReaderResult<'a>










module AttrReaderResult =

  let map f =
    AttrReader.map (Result.map) f

  let retn a =
    AttrReader.retn (Result.Ok a)

  // let bind f attrReader = 
  //   let newReader m =
  //     let a = run attrReader m
  //     let bReader = f a
  //     run bReader m 
  //   AttrReader newReader


  let bind f attrReader =
    let newReader m =
      let readerResult = 
        match AttrReader.run attrReader m with
        | Ok x -> f x
        | Error err -> Error err |> AttrReader.retn
      AttrReader.run readerResult m
    AttrReader newReader


  let apply fReader attrReader =
    let newReader m =
      let f = run fReader m
      let a = run attrReader m
      f a
    AttrReader newReader



// let (<!>) = AttrReader.map
// let (>>=) = AttrReader.bind
// let (<*>) = AttrReader.apply
















type D = Dictionary<string,AttributeValue>














/// map   :          ('a -> 'b)  -> Wrapper<'a> -> Wrapper<'b>

/// apply :  Wrapper<('a -> 'b)> -> Wrapper<'a> -> Wrapper<'b>




















type Reader<'a, 'b> =
  Reader of ('a -> 'b)

module Reader =

  let run (Reader f) a =
    f a

  let map f r =
    Reader (
      fun a ->
        let b = run r a
        f b)

  let apply f r =
    Reader (
      fun a ->
        let b = run r a
        let g = run f a
        g b)












let readString key   = Reader (fun (d:D) -> d.[key].S)
let readBool   key   = Reader (fun (d:D) -> d.[key].BOOL)
let readGuid   key   = Reader (fun (d:D) -> d.[key].S |> Guid.Parse)
let readDate   key   = Reader (fun (d:D) -> d.[key].S |> DateTime.Parse)
let readNumber key f = Reader (fun (d:D) -> d.[key].N |> f)


















let (<!>) = Reader.map
let (<*>) = Reader.apply




let readCustomer =
  buildCustomer
  <!> readGuid   "id"
  <*> readString "email"
  <*> readBool   "verified"
  <*> readDate   "dob"
  <*> readNumber "balance" decimal






let r0 = buildCustomer

// let r1 = Reader.map r0

// let r2 = r1 (readGuid "id")

// let r3 = Reader.apply r2 (readString "email")

// let r4 = Reader.apply r3 (readBool "verified")

// let r5 = Reader.apply r4 (readDate "dob")

// let r6 = Reader.apply r5 (readNumber "balance" decimal)





















let getItem tableName reader fields =
  new GetItemRequest (tableName, mapAttrsToDictionary fields)
  |> client.GetItemAsync
  |> runSync
  |> fun r -> r.Item
  |> Reader.run reader


let customerId =
  Guid.NewGuid()


let ``put customer`` =
  [ Attr ("id", ScalarGuid customerId )
    Attr ("email", ScalarString "someone@tm.com")
    Attr ("verified", ScalarBool true)
    Attr ("dob", ScalarString "2000-01-01")
    Attr ("balance", ScalarDecimal 3405.25m) ]
  |> putItem table


let ``get customer`` =
  [ Attr ("id", ScalarGuid customerId) ]
  |> getItem table readCustomer

















/// OPTIONAL ATTRIBUTE


type CustomerWithPhone =
  { Id : Guid
    Email : String
    IsVerified : Boolean
    DateOfBirth : DateTime
    Balance : Decimal
    Phone : String option }

let buildCustomerWithPhone id email verified dob balance phone =
  { Id = id 
    Email = email
    IsVerified = verified 
    DateOfBirth = dob 
    Balance = balance
    Phone = phone }




let readStringOption key =
  (fun (d:D) -> if d.ContainsKey(key) then Some(d.[key].S) else None) |> Reader

let readCustomerWithPhone =
  buildCustomerWithPhone
  <!> readGuid         "id"
  <*> readString       "email"
  <*> readBool         "verified"
  <*> readDate         "dob"
  <*> readNumber       "balance" decimal
  <*> readStringOption "phone"


let ``get customer with phone`` =
  [ Attr ("id", ScalarGuid customerId) ]
  |> getItem table readCustomerWithPhone


let ``put customer with phone`` =
  [ Attr ("id", ScalarGuid customerId )
    Attr ("email", ScalarString "someone@tm.com")
    Attr ("verified", ScalarBool true)
    Attr ("dob", ScalarString "2000-01-01")
    Attr ("balance", ScalarDecimal 3405.25m)
    Attr ("phone", ScalarString "07987000333") ]
  |> putItem table









/// NESTED

type Address =
  { FirstLine : String
    SecondLine : String
    PostCode : String
    CountryCode : int }

type CustomerWithAddress =
  { Id : Guid
    Email : String
    Address : Address
    IsVerified : Boolean
    DateOfBirth : DateTime
    Balance : Decimal }
  

let buildAddress first second postcode country =
  { FirstLine = first 
    SecondLine = second
    PostCode = postcode
    CountryCode = country }


let buildCustomerWithAddress id email verified address dob balance =
  { Id = id 
    Email = email
    IsVerified = verified 
    Address = address
    DateOfBirth = dob 
    Balance = balance }


let readMap key f =
  Reader (fun (d:D) -> d.[key].M |> Reader.run f)



let readAddress =
  buildAddress
  <!> readString "first"
  <*> readString "second"
  <*> readString "postcode"
  <*> readNumber "country" int

let readCustomerWithAddress =
  buildCustomerWithAddress
  <!> readGuid    "id"
  <*> readString  "email"
  <*> readBool    "verified"
  <*> readMap     "address" readAddress
  <*> readDate    "dob"
  <*> readNumber  "balance" decimal










let ``put nested customer`` =
  [ Attr ("id", ScalarGuid customerId )
    Attr ("email", ScalarString "someone@here.com")
    Attr ("verified", ScalarBool true)
    Attr ("address",  
      DocMap
        [ Attr ("first", ScalarString "24 Church St")
          Attr ("second", ScalarString "Camden")
          Attr ("postcode", ScalarString "NW6 7WK")
          Attr ("country", ScalarInt32 44) ] )
    Attr ("dob", ScalarDate (new DateTime(2000,01,01)))
    Attr ("balance", ScalarDecimal 3405.25m) ]
  |> putItem table


let ``get nested customer`` =
  [ Attr ("id", ScalarGuid customerId) ]
  |> getItem table readCustomerWithAddress



















/// READER RESULT

module ReaderResult =

  // let retn a =
  //   Ok a |> Reader.retn

  let map f =
    Result.map f |> Reader.map

  let apply f r =
    Reader <| fun a ->
      let fa = Reader.run f a
      let fb = Reader.run r a
      match fa, fb with
      | Ok a    , Ok b     -> Ok (a b)
      | Error ea, Error eb -> Error [ ea; eb ]
      | Error e , _        -> Error [ e ]
      | _       , Error e  -> Error [ e ]



let readStringR key =
  Reader (fun (d:D) -> if d.ContainsKey(key) then Ok(d.[key].S) else Error(sprintf "missing key: %s" key))
