



#load ".paket/load/netcoreapp3.1/main.group.fsx"

open System
open System.IO
open System.IO.Compression
open System.Collections.Generic
open Newtonsoft.Json
open Amazon
open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model
open Amazon.Runtime.CredentialManagement

let table = "fs-wrapper"
let getCredentials profile =
    match (new CredentialProfileStoreChain()).TryGetAWSCredentials profile with
    | true, p -> p | _ -> failwith "Failed to load sandbox profile"
let client = new AmazonDynamoDBClient (getCredentials "sandbox", RegionEndpoint.EUWest2)
let runSync f = (Async.AwaitTask >> Async.RunSynchronously) f
let srlz o = JsonConvert.SerializeObject(o, Formatting.Indented)
let prnt = printfn "\n\n---------\n\n%s\n\n---------\n\n"
let toDictionary m = dict m |> Dictionary<string,AttributeValue>
let rsp f g =
  try (runSync >> f >> srlz >> prnt) g
  with | :? AggregateException as e
        -> prnt ("EXN: " + e.InnerException.Message)
       | _ as e -> prnt ("EXN: " + e.Message)



/// THEME / SIZE







let ``empty attribute value`` =
  new AttributeValue()
  |> srlz
  |> prnt









let ``basic put item`` =
  [ "id"   , new AttributeValue (S = "foo")
    "total", new AttributeValue (N = "123.45")
    "isOk" , new AttributeValue (BOOL = true) ]
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
                                 L = ResizeArray
                                  [ new AttributeValue (S = "xxx") ]) ]
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






/// <<<<<


















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


















Set.empty
Set.empty |> Set.add 4
Set.empty |> Set.add 4 |> Set.add 5
Set.empty |> Set.add 4 |> Set.add 5 |> Set.add 4















type NonEmptyList<'a> when 'a : comparison =
  | NonEmptyList of head:'a * tail:'a list







let toSet (NonEmptyList (head, tail)) =
  Set.ofList tail |> Set.add head















/// DOMAIN MODEL



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



















let rec mapAttrValue = function
  | ScalarString  s -> new AttributeValue (S = s)
  | ScalarGuid    g -> new AttributeValue (S = string g)
  | ScalarDate    d -> new AttributeValue (S = d.ToString("s"))
  | ScalarInt32   i -> new AttributeValue (N = string i)
  | ScalarDecimal d -> new AttributeValue (N = string d)
  | ScalarBinary  b -> new AttributeValue (B = toGzipMemoryStream b)
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



and toGzipMemoryStream (s:string) =
  let output = new MemoryStream ()
  use zipStream = new GZipStream (output, CompressionMode.Compress, true)
  use writer = new StreamWriter (zipStream)
  writer.Write s
  output














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
  ] |> putItem table










let ``get rich item 2`` =
  [ Attr ("id", ScalarString "rich item") ]
  |> fun attrs -> new GetItemRequest (table, mapAttrsToDictionary attrs)
  |> client.GetItemAsync
  |> rsp (fun r -> r.Item)















/// <<< view item in table
/// 
/// <<< back to slides
















type Customer =
  { Id : Guid
    Email : String
    IsVerified : Boolean
    DateOfBirth : DateTime
    Balance : Decimal }





let ``put customer`` (customer:Customer) =
  [ Attr ("id"      , ScalarGuid    customer.Id         )
    Attr ("email"   , ScalarString  customer.Email      )
    Attr ("verified", ScalarBool    customer.IsVerified )
    Attr ("dob"     , ScalarDate    customer.DateOfBirth)
    Attr ("balance" , ScalarDecimal customer.Balance    ) ]
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


// incorrect attribute name (imail)
// read wrong property (balance N vs S)
// corrupt dob
// no record returned



let customerIdWithErrors =
  Guid.NewGuid()

let ``put customer with errors`` =
  [ Attr ("id"       , ScalarGuid    customerIdWithErrors   )
    Attr ("email"    , ScalarString  "email@tm.com"         )
    Attr ("verified" , ScalarBool    true                   )
    Attr ("dob"      , ScalarDate   (new DateTime(2000, 1, 1)))
    // Attr ("dob"      , ScalarString  "not a date"        )
    Attr ("balance"  , ScalarDecimal 4456.3m                ) ]
  |> putItem table


let makeCustomerWithError (d:Dictionary<string,AttributeValue>) =
  { Id          =     Guid.Parse d.["id"].S
    Email       =                d.["email"].S
    IsVerified  =                d.["verified"].BOOL
    DateOfBirth = DateTime.Parse d.["dob"].S
    Balance     =        decimal d.["balance"].N }

``get customer`` customerIdWithErrors makeCustomerWithError

``get customer`` (Guid.NewGuid()) makeCustomerWithError






















let toMap d =
  Seq.map (|KeyValue|) d |> Map.ofSeq




// let find =
//   Map.tryFind





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




let getAttrString key m =
  getAttr key (fun (a:AttributeValue) -> a.S) m
  |> Result.mapError List.singleton
  

let getAttrBool key m =
  getAttr key (fun (a:AttributeValue) -> a.BOOL) m
  |> Result.mapError List.singleton
  

let getAttrNumber key m =
  getAttr key (fun (a:AttributeValue) -> a.N) m
  |> Result.mapError List.singleton


let getAttrDate key m =
  getAttrString key m
  |> Result.bind (parseDate [sprintf "could not parse %s as date" key])


let getAttrGuid key m =
  getAttrString key m
  |> Result.bind (parseGuid [sprintf "could not parse %s as guid" key])


let getAttrDecimal key m =
  getAttrNumber key m
  |> Result.bind (parseDecimal [sprintf "could not parse %s as decimal" key])






// let makeCustomerUsingResultTypes (m:Map<string,AttributeValue>) =
//   { Id          = getAttrGuid    "id" m
//     Email       = getAttrString  "email" m
//     IsVerified  = getAttrBool    "verified" m
//     DateOfBirth = getAttrDate    "dob" m
//     Balance     = getAttrDecimal "balance" m }







type AttrReader<'a> =
  AttrReader of (Map<string, AttributeValue> -> 'a)









/// " give me an attr key, I will give a you an attr reader, 
///   that, when given an attr map, will return the
///   result of trying to read & parse the attr "


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

/// AttrReader<Result<'a, 'b>>










/// retn :                   'a -> Wrapper<'a>
/// 
/// map  :           ('a -> 'b) -> Wrapper<'a> -> Wrapper<'b>
/// 
/// bind :  ('a -> Wrapper<'b>) -> Wrapper<'a> -> Wrapper<'b>
/// 
/// apply:  Wrapper<('a -> 'b)> -> Wrapper<'a> -> Wrapper<'b>










module AttrReader = begin



  /// retn : 'a -> Wrapper<'a>

  let retn a =
    AttrReader (fun _ -> a)







  /// UNWRAPPER FUNCTION

  let run (AttrReader f) m =
    f m



end





module AttrReaderResult = begin




  /// retn : 'a -> Wrapper<'a>

  let retn a =
    AttrReader.retn (Ok a)







  /// map : ('a -> 'b) -> Wrapper<'a> -> Wrapper<'b>

  let map f readerResultA =
    let newReader m =
      let resultA =
        AttrReader.run readerResultA m
      let resultB =
        match resultA with
        | Ok unwrappedA ->
          let b = f unwrappedA
          let resultB = Ok b
          resultB
        | Error e ->
          let resultC = Error e
          resultC
      resultB
    AttrReader newReader





  /// bind : ('a -> Wrapper<'b>) -> Wrapper<'a> -> Wrapper<'b>

  let bind f readerResultA =
    let newReader m =
      let readerResultB = 
        let resultA =
          AttrReader.run readerResultA m
        match resultA with
        | Ok a ->
          let readerResultB = f a
          readerResultB
        | Error e ->
          let readerResultC =
            AttrReader.retn (Error e)
          readerResultC
      let resultB =
        AttrReader.run readerResultB m
      resultB
    AttrReader newReader








  /// apply : Wrapper<('a -> 'b)> -> Wrapper<'a> -> Wrapper<'b>

  let apply f readerResultA =
    let newReader m =
      let resultF =
        AttrReader.run f m
      let resultA =
        AttrReader.run readerResultA m
      let resultB =
        match resultF, resultA with
        | Ok f    , Ok a     -> Ok (f a)
        | Error e1, Error e2 -> Error (e1 @ e2)
        | Error e1, _        -> Error e1
        | _       , Error e2 -> Error e2
      resultB
    AttrReader newReader


end













let buildCustomer id email verified dob balance =
  { Id = id 
    Email = email
    IsVerified = verified 
    DateOfBirth = dob 
    Balance = balance }
















let rb =
  buildCustomer



/// map : ('a -> 'b) -> Wrapper<'a> -> Wrapper<'b>



let r0 =
  AttrReaderResult.map buildCustomer



// let stringAttrReader2 =
//   getAttrString >> AttrReader




let r1 =
  r0 (guidAttrReader "id")









/// apply : Wrapper<('a -> 'b)> -> Wrapper<'a> -> Wrapper<'b>


let r2 =
  AttrReaderResult.apply r1
  
let r3 =
  r2 (stringAttrReader "email")








let r4 =
  AttrReaderResult.apply r3 
  
let r5 =
  r4 (boolAttrReader "verified")








let r6 =
  AttrReaderResult.apply r5
  
let r7 =
  r6 (dateAttrReader "dob")








let r8 =
  AttrReaderResult.apply r7
  
let r9 =
  r8 (decimalAttrReader "balance")












let (<!>) = AttrReaderResult.map
let (<*>) = AttrReaderResult.apply

















let readCustomer =
  buildCustomer
  <!> guidAttrReader    "id"
  <*> stringAttrReader  "email"
  <*> boolAttrReader    "verified"
  <*> dateAttrReader    "dob"
  <*> decimalAttrReader "balance"




















let getItem tableName reader fields =
  mapAttrsToDictionary fields
  |> fun attrs -> new GetItemRequest (tableName, attrs)
  |> client.GetItemAsync
  |> runSync
  |> fun r -> r.Item
  |> toMap
  |> AttrReader.run reader




let ``get customer with reader`` =
  [ Attr ("id", ScalarGuid customerId) ]
  |> getItem table readCustomer











/// ERRORS?
 



let badCustomerId2 =
  Guid.NewGuid()


let ``put bad customer`` =
  [ Attr ("id", ScalarGuid badCustomerId2 )
  // [ Attr ("id", ScalarString "bad id" )
    Attr ("email", ScalarString "bad_record@tm.com")
    Attr ("verified", ScalarBool true)
    Attr ("dob", ScalarDate (new DateTime(2000, 1, 1)))
    // Attr ("dob", ScalarString "not a date")
    Attr ("balance", ScalarDecimal 23m) ]
    // Attr ("balance_typo", ScalarDecimal 23m) ]
  |> putItem table


let ``get bad customer`` =
  [ Attr ("id", ScalarGuid badCustomerId2) ]
  // [ Attr ("id", ScalarString "bad id") ]
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






let stringOptionAttrReader key =
  AttrReader (Map.tryFind key >> Option.map (fun (a:AttributeValue) -> a.S) >> Ok)








let readCustomerWithPhone =
  buildCustomerWithPhone
  <!> guidAttrReader         "id"
  <*> stringAttrReader       "email"
  <*> boolAttrReader         "verified"
  <*> dateAttrReader         "dob"
  <*> decimalAttrReader      "balance"
  <*> stringOptionAttrReader "phone"






let customerWithPhoneId =
  Guid.NewGuid()


let ``put customer with phone`` =
  [ Attr ("id",       ScalarGuid customerWithPhoneId )
    Attr ("email",    ScalarString "someone@tm.com")
    Attr ("verified", ScalarBool true)
    Attr ("dob",      ScalarString "2000-01-01")
    Attr ("balance",  ScalarDecimal 3405.25m) 
    Attr ("phone",    ScalarString "07987000333") ]
  |> putItem table



let ``get customer with phone`` =
  [ Attr ("id", ScalarGuid customerWithPhoneId) ]
  |> getItem table readCustomerWithPhone












/// NESTED RECORD

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




// let getAttrMap key m =
//   getAttr key (fun (a:AttributeValue) -> a.M) m
//   |> Result.mapError List.singleton


let mapAttrReader key f =
  getAttr key (fun (a:AttributeValue) -> toMap a.M)
  >> Result.mapError List.singleton
  >> Result.bind (AttrReader.run f)
  |> AttrReader









let parseInt e (s:String) =
  match Int32.TryParse s with
  | true, x -> Ok x
  | _ -> Error e

let getAttrInt key m =
  getAttrNumber key m
  |> Result.bind (parseInt [sprintf "could not parse %s as integer" key])

let intAttrReader =
  getAttrInt >> AttrReader







let readAddress =
  buildAddress
  <!> stringAttrReader "first"
  <*> stringAttrReader "second"
  <*> stringAttrReader "postcode"
  <*> intAttrReader    "country"

let readCustomerWithAddress =
  buildCustomerWithAddress
  <!> guidAttrReader    "id"
  <*> stringAttrReader  "email"
  <*> boolAttrReader    "verified"
  <*> mapAttrReader     "address" readAddress
  <*> dateAttrReader    "dob"
  <*> decimalAttrReader "balance"








let nestedCustomerId =
  Guid.NewGuid()

let ``put nested customer`` =
  [ Attr ("id", ScalarGuid nestedCustomerId )
    Attr ("email", ScalarString "someone@here.com")
    Attr ("verified", ScalarBool true)
    Attr ("address",  
      DocMap
        [ Attr ("first", ScalarString "24 Church St")
          Attr ("second", ScalarString "Camden")
          Attr ("postcode", ScalarString "NW6 7WK")
          Attr ("country", ScalarInt32 44) ] )
          // Attr ("country", ScalarString "not a number") ] )
    Attr ("dob", ScalarDate (new DateTime(2000,01,01)))
    Attr ("balance", ScalarDecimal 3405.25m) ]
  |> putItem table


let ``get nested customer`` =
  [ Attr ("id", ScalarGuid customerId) ]
  // [ Attr ("id", ScalarGuid nestedCustomerId) ]
  |> getItem table readCustomerWithAddress

















