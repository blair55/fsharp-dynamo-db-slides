



module AttrReader =



  /// retn : 'a -> Wrapper<'a>

  let retn a =
    AttrReader (fun _ -> a)







  /// UNWRAPPER FUNCTION

  let run (AttrReader f) m =
    f m








  /// map : ('a -> 'b) -> Wrapper<'a> -> Wrapper<'b>

  let map f readerA =
    let newReader m =
      let unwrappedA = run readerA m
      let b = f unwrappedA
      b
    AttrReader newReader







  /// bind : ('a -> Wrapper<'b>) -> Wrapper<'a> -> Wrapper<'b>

  let bind f readerA = 
    let newReader m =
      let unwrappedA = run readerA m
      let readerB = f unwrappedA
      let unwrappedB = run readerB m 
      unwrappedB
    AttrReader newReader







  /// apply : Wrapper<('a -> 'b)> -> Wrapper<'a> -> Wrapper<'b>

  let apply f readerA =
    let newReader m =
      let unwrappedF = run f m
      let unwrappedA = run readerA m
      let b = unwrappedF unwrappedA
      b
    AttrReader newReader






