namespace DBNS {

using G = System.Collections.Generic;

/* An IndexPage is a height-balanced binary tree stored in fixed size byte array as follows:

Header | Nodes .... free space ....

Nodes are numbered from 1 to 2047, so a Node number requires 11 bits when saved.

Each Node is the same size, and holds:

Balance, Left, Right ( 3 bytes ). The first byte holds Balance ( 2 bits ) and also the top three bits of Left and Right.
ColStore Values ( fixed size, depends on IndexFileInfo ).
A Child page id ( if IsLeafPage() is false ).

When a page overflows it is divided into two, and the new child page is recorded in a parent page.

*/

class IndexPage
{
  public IndexPage( IndexFileInfo inf, byte [] data, DatabaseImp database )
  {
    if ( inf.KeyCount > 100 ) throw new System.Exception("Max number of index columns exceeded");
    Inf = inf;
    Database = database;
    bool leaf = IsLeafPage();

    ColStore = leaf ? inf.KeyCount : inf.Types.Length; // Currently these are the same.
    NodeSize = NodeOverhead + inf.KeySize() + ( leaf ? 0 : PageIdSize );
    NodeBase = FixedHeader + ( leaf ? 0 : PageIdSize ) - NodeSize;    

    MaxNode = ( PageSize - ( NodeBase + NodeSize ) ) / NodeSize;
    if ( MaxNode > 2047 ) MaxNode = 2047; // Node ids are 11 bits.

    if ( data == null ) 
      Data = new byte[ PageSize ];
    else
    {
      Data = data;
      ReadHeader();
    }
    // Console.WriteLine( "New IndexPage Count=" + Count + " ColStore=" + ColStore + " NodeSize=" + NodeSize + " MaxNode=" + MaxNode );
  }

  public const int PageSize = 0x1000; // Good possibilities are 0x1000, 0x2000 and 0x4000.
  const int NodeOverhead = 3; // Size of Balance,Left,Right in a Node ( 2 + 2 x 11 = 24 bits  needs 3 bytes ).
  const int FixedHeader = 6; // 45 bits ( 1 + 4 x 11 ) needs 6 bytes.
  protected const int PageIdSize = 6; // Number of bytes used to store a page number.

  public byte [] Data;
  public long PageId; // Page Location in file.
  public long ParentId;

  protected int Root = 0;  // Root node.
  protected int Count = 0; // Number of Records currently stored.
  int NodeAlloc = 0;       // Number of Nodes currently allocated.
  int Free = 0;            // First Free node.

  protected int NodeBase; // Header size is NodeBase + NodeSize, this is useful as Node numbers are 1-based.
  protected int NodeSize; // Number of bytes required for each node.
  int MaxNode;  // Maximum number of nodes ( constrained by PageSize ).
  int ColStore; // Number of columns stored.
  protected IndexFileInfo Inf; // Has type information for IndexFileRecords.
  protected long FirstPage; // For a non-leaf page.
  protected DatabaseImp Database;

  public bool Saved = false; // Indicates whether tree has been saved to disk.

  void Insert( IndexFile ixf, ref IndexFileRecord r )
  {
    if ( Free == 0 && NodeAlloc == MaxNode ) // Page is full, have to split it.
    {
      PageParent pp;
      if ( this == ixf.Root ) 
      {
        pp = new PageParent( Inf, null, Database );
        ixf.AllocPageId( this );
        pp.FirstPage = PageId;
        ixf.Root = pp;
        ixf.PageMap[ 0 ] = pp;
      }
      else pp = (PageParent) ixf.PageMap[ ParentId ];

      IndexPage left = Clone();
      left.PageId = PageId; 
      left.FirstPage = FirstPage;
      ixf.PageMap[ PageId ] = left;

      IndexPage right = Clone();
      ixf.AllocPageId( right );

      var div = new IndexFileRecord(); // IndexFileRecord that divides page into left/right, will be inserted into parent page.
      Divide( left, right, ref div  );
      right.FirstPage = div.Child;
      div.Child = right.PageId;
      pp.Insert( ixf, ref div );

      // Insert into either left or right depending on comparison with sv.
      ( Compare( ref r, ref div ) < 0 ? left : right ).Insert( ref r, false );

      // Console.WriteLine( "Insert Split, NodeAlloc=" + NodeAlloc + " MaxNode=" + MaxNode + "ixf.PageAlloc=" + ixf.PageAlloc );
      // Console.WriteLine( "Split div=" + div.ToString(ixf.Inf) );
      // ixf.Dump();
    }
    else Insert( ref r, false );
  }

  public int Size() { return NodeBase + NodeSize + NodeAlloc * NodeSize; }

  public virtual void LeafOp( IndexFile ixf, ref IndexFileRecord r, bool insert ) 
  {
    if ( insert ) Insert( ixf, ref r ); else Remove( ref r );
  }

  protected virtual bool IsLeafPage(){ return true; }

  protected virtual IndexPage Clone()
  {
    return new IndexPage( Inf, null, Database );
  }

  public delegate int DCompare( ref IndexFileRecord k1 );

  public virtual G.IEnumerable<IndexFileRecord> From( IndexFile ixf, DCompare seek, bool desc )
  {
    IndexFileRecord v = new IndexFileRecord();
    foreach ( int x in From( seek, desc ) ) 
    {
      GetRecord( x, ref v );
      yield return v;
    }
  }

  G.IEnumerable<int> From( DCompare seek, bool desc )
  {
    if ( desc ) foreach ( int x in Desc( Root, seek ) ) yield return x;
    else foreach ( int x in Asc( Root, seek ) ) yield return x;
  }

  void Divide( IndexPage L, IndexPage R, ref IndexFileRecord div )
  {
    var r = new IndexFileRecord();
    int count = Count / 2;
    foreach ( int x in Nodes( Root ) )
    {
      GetRecord( x, ref r );;
      if ( count > 0 )
        L.Insert( ref r, true );
      else
      {
        if ( count == 0 ) 
        {
          div = r;
          if ( IsLeafPage() ) R.Insert( ref div, true );
        }
        else 
        {
          R.Insert( ref r, true );
        }
      }
      count -= 1;
    }
  }

  // Write/Read bytes to Data array.

  protected void Set( int off, ulong x, int size ) // Saves x at Data[off] using size bytes.
  {
    for ( int i = 0; i < size; i += 1 )
    {
      Data[off + i] = (byte)x;
      x >>= 8;
    }
  }

  protected ulong Get( int off, int size ) // Extract unsigned value of size bytes from Data[off].
  {
    ulong x = 0;
    for ( int i = size-1; i >= 0; i -= 1 )
      x = ( x << 8 ) + Data[off + i];
    return x;
  }

  // Header IO.

  public void WriteHeader() // Called just before page is saved to file.
  { 
    ulong u = 
    ( IsLeafPage() ? 0UL : 1UL )
    | (((ulong)Root) << 1 )
    | (((ulong)Count) << 12 )
    | (((ulong)Free) << 23 )
    | (((ulong)NodeAlloc) << 34 );
    Set( 0, u, FixedHeader );
    if ( !IsLeafPage() ) Set( FixedHeader, (ulong)FirstPage, PageIdSize );
  }

  void ReadHeader()
  {
    ulong u = Get( 0, FixedHeader );
    Root = (int)( ( u >> 1 ) & 0x7ff );
    Count = (int)( ( u >> 12 ) & 0x7ff );
    Free = (int)( ( u >> 23 ) & 0x7ff );
    NodeAlloc = (int)( ( u >> 34 ) & 0x7ff );
    if ( !IsLeafPage() ) FirstPage = (long)Get( FixedHeader, PageIdSize );
  }

  // Node access functions.

  int GetBalance( int x )
  {
    int off = NodeBase + x * NodeSize;
    return ( Data[ off ] & 3 ) - 1; // Extract the low two bits.
  }

  void SetBalance( int x, int balance ) // balance is in range -1 .. +1
  {
    int off = NodeBase + x * NodeSize;
    Data[ off ] = (byte) ( ( balance + 1 ) | ( Data[ off ] & 0xfc ) );
  } 

  int GetLeft( int x )
  { 
    int off = NodeBase + x * NodeSize;
    return Data[ off + 1 ] | ( ( Data[ off ] & 28 ) << 6 ); // 28 = 7 << 2; adds bits 2..4 from Data[ off ]
  }

  int GetRight( int x )
  { 
    int off = NodeBase + x * NodeSize;
    return Data[ off + 2 ] | ( ( Data[ off ] & 224 ) << 3 ); // 224 = 7 << 5; adds in bits 5..7 of Data[ off ]
  }

  void SetLeft( int x, int y )
  {
    const int mask = 28; // 28 = 7 << 2
    int off = NodeBase + x * NodeSize;
    Data[ off + 1 ] = (byte) y;
    Data[ off ] = (byte) ( ( Data[ off ] & ( 255 - mask ) ) | ( ( y >> 6 ) & mask ) );
  }

  void SetRight( int x, int y )
  {
    const int mask = 224; // 224 = 7 << 5
    int off = NodeBase + x * NodeSize;
    Data[ off + 2 ] = (byte) y;
    Data[ off ] = (byte) ( ( Data[ off ] & ( 255 - mask ) ) | ( ( y >> 3 ) & mask ) );
  }

  protected virtual void SetRecord( int x, ref IndexFileRecord r )
  {
    int off = NodeOverhead + NodeBase + x * NodeSize;
    for ( int i =0 ; i < ColStore; i += 1 )
    {
      long v = r.Col[ i ].L;
      DataType t = Inf.Types[ i ];
      if ( t == DataType.String && v == 0 )
        r.Col[ i ].L = Database.EncodeString( (string)r.Col[ i ]._O );
      else if ( t == DataType.Binary && v == 0 )
        r.Col[ i ].L = Database.EncodeBinary( (byte[])r.Col[ i ]._O );
      int size = DTI.Size( t );

      ulong p = (ulong)r.Col[ i ].L;
      if ( t == DataType.Float ) p = Conv.PackFloat( p );

      Set( off, p, size );
      off += size;
    }
  }

  protected virtual void GetRecord( int x, ref IndexFileRecord r )
  { 
    if ( r.Col == null ) r.Col = new Value[ ColStore ];
    int off = NodeOverhead + NodeBase + x * NodeSize;
    for ( int i =0 ; i < ColStore; i += 1 )
    {
      DataType t = Inf.Types[ i ];
      int size = DTI.Size( t );
      ulong u = Get( off, size );
      off += size;

      if ( t == DataType.Float ) u = Conv.UnpackFloat( (uint) u );
      else if ( t == DataType.Int ) u = (ulong)(long)(int)(uint) u;
      else if ( t == DataType.Smallint ) u = (ulong)(long)(short)(ushort) u;
      else if ( t == DataType.Tinyint ) u = (ulong)(long)(sbyte)(byte) u;

      r.Col[ i ].L = (long) u;
      if ( t <= DataType.String ) r.Col[ i ]._O = Database.Decode( (long) u, t );
    }
  }

  // Record comparison - determines the order in which the Records are stored.

  public int Compare( ref IndexFileRecord r, ref IndexFileRecord y )
  {
    int cf = 0;
    for ( int i = 0; i < Inf.KeyCount; i += 1 )
    {
      cf = Util.Compare( r.Col[ i ], y.Col[ i ], Inf.Types[ i ] );
      if ( cf != 0 ) break;
    }
    return cf;
  }

  int Compare( int x, ref IndexFileRecord r )
  {
    IndexFileRecord y = new IndexFileRecord();
    GetRecord( x, ref y );  
    return Compare( ref r, ref y );
  }

  int Compare( int x, DCompare seek )
  {
    IndexFileRecord r = new IndexFileRecord();
    GetRecord( x, ref r );
    return seek( ref r );
  }

  // Debugging.

  public void CheckPage()
  {
    int count = 0;
    foreach ( int x in Nodes( Root ) )
    {
      count += 1;
      if ( count > Count ) throw new System.Exception( "Corrupt Index Page" );
    }
    if ( count != Count ) throw new System.Exception( "Corrupt Index Page" );    
  }

  public virtual void Dump( IndexFile ixf ) // For debugging.
  {
    System.Console.WriteLine( "Page Dump PageId=" + PageId + " NodeSize=" + NodeSize + " MaxNode=" + MaxNode + " Count=" + Count + " Root=" + Root );
    CheckPage();
    IndexFileRecord r = new IndexFileRecord();
    foreach ( int x in Nodes( Root ) )
    {
      GetRecord( x, ref r );
      System.Console.WriteLine( "Record=" + r.ToString(ixf.Inf) + " Node=" + x + " Left=" + GetLeft(x) + " Right=" + GetRight(x) 
       + " Balance=" + GetBalance(x) );
    }
    System.Console.WriteLine( "End Page Dump PageId=" + PageId + " NodeSize=" + NodeSize + " MaxNode=" + MaxNode + " Count=" + Count + " Root=" + Root );
  }

  // Methods to Insert, Remove records, enumerate Node Ids.

  void Insert( ref IndexFileRecord r, bool append ) // Insert IndexFileRecord into the tree ( set append true if Records are pre-sorted )
  {
    bool h;
    Root = Insert( Root, ref r, out h, append );
    Saved = false;
  }

  void Remove( ref IndexFileRecord r ) 
  {
    bool h;
    Root = Remove( Root, ref r, out h );
    Saved = false;
  }

  protected G.IEnumerable<int> Nodes( int x ) // Enumerate the Node ids ( in sorted order ).
  {
    if ( x == 0 ) yield break;
    foreach ( int a in Nodes( GetLeft(x) ) ) yield return a;
    yield return x;
    foreach ( int a in Nodes( GetRight(x) ) ) yield return a;
  }

  G.IEnumerable<int> Asc( int x, DCompare cf )
  {
    if ( x == 0 ) yield break;
    int c = Compare( x, cf );
    if ( c < 0 ) foreach ( int a in Asc( GetLeft(x), cf ) ) yield return a;
    if ( c <= 0 ) yield return x;
    foreach ( int a in Asc( GetRight(x), cf ) ) yield return a; // Is this doing extra compares when c <= 0?
  }

  G.IEnumerable<int> Desc( int x, DCompare cf )
  {
    if ( x == 0 ) yield break;
    int c = Compare( x, cf );
    if ( c > 0 ) foreach ( int a in Desc( GetRight(x), cf ) ) yield return a;
    if ( c >= 0 ) yield return x;
    foreach ( int a in Desc( GetLeft(x), cf ) ) yield return a;
  }

  protected int FindSplit( ref IndexFileRecord r ) 
  // Returns node id of the greatest IndexFileRecord less than or equal to v, or zero if no such node exists.
  {
    int x = Root;
    int result = 0;
    while ( x != 0 )
    {
      int c = Compare( x, ref r );
      if ( c < 0 )
      {
        x = GetLeft(x);
      }
      else if ( c > 0 )
      {
        result = x;
        x = GetRight(x);
      }
      else // c == 0
      {
        result = x;
        break;
      }
    }
    return result;
  }

  protected int FindSplit( DCompare compare )
  {
    int x = Root;
    int result = 0;
    IndexFileRecord r = new IndexFileRecord();
    while ( x != 0 )
    {
      GetRecord( x, ref r );
      int c = compare( ref r );
      if ( c < 0 )
      {
        x = GetLeft(x);
      }
      else if ( c > 0 )
      {
        result = x;
        x = GetRight(x);
      }
      else // c == 0
      {
        result = x;
        break;
      }
    }
    return result;
  }

  // Node Id Allocation.

  int AllocNode()
  {
    Count += 1;
    if ( Free == 0 )
    {
      NodeAlloc += 1;
      return Count;
    }
    else
    {
      int result = Free;
      Free = GetLeft( Free );
      return result;
    }
  }

  void FreeNode( int x )
  {
    SetLeft( x, Free );
    Free = x;
    Count -= 1;
  }

  // Rest is AVL height-balanced tree methods to Insert/Remove IndexFileRecords.

  // Constants for Node Balance.
  const int LeftHigher = -1, Balanced = 0, RightHigher = 1;

  int Insert( int x, ref IndexFileRecord r, out bool heightIncreased, bool append )
  {
    if ( x == 0 )
    {
      x = AllocNode();
      SetBalance( x, Balanced );
      SetLeft( x, 0 );
      SetRight( x, 0 );
      SetRecord( x, ref r );
      heightIncreased = true;
    }
    else
    {
      int c = append ? 1 : Compare( x, ref r );
      if ( c < 0 )
      {
        SetLeft( x, Insert( GetLeft(x), ref r, out heightIncreased, append ) );
        if ( heightIncreased )
        {
          int bx = GetBalance( x );
          if ( bx == Balanced )
          {
            SetBalance( x, LeftHigher );
          }
          else
          {
            heightIncreased = false;
            if ( bx == LeftHigher )
            {
              bool heightDecreased;
              return RotateRight( x, out heightDecreased );
            }
            SetBalance( x, Balanced );
          }
        }
      }
      else if ( c > 0 )
      {
        SetRight( x, Insert( GetRight(x), ref r, out heightIncreased, append ) );
        if ( heightIncreased )
        {
          int bx = GetBalance( x );
          if ( bx == Balanced )
          {
            SetBalance( x, RightHigher );
          }
          else
          {
            heightIncreased = false;
            if ( bx == RightHigher )
            {
              bool heightDecreased;
              return RotateLeft( x, out heightDecreased );
            }
            SetBalance( x, Balanced );
          }
        }
      }
      else // compare == 0, should not happen, keys should be unique with no duplicates, raise exception?
      {
        // Update( x );
        heightIncreased = false;
      }
    }
    return x;
  }

  int RotateRight( int x, out bool heightDecreased )
  {
    // Left is 2 levels higher than Right.
    heightDecreased = true;
    int z = GetLeft( x );
    int y = GetRight( z );
    int zb = GetBalance( z );
    if ( zb != RightHigher ) // Single rotation.
    {
      SetRight( z, x );
      SetLeft( x, y );
      if ( zb == Balanced ) // Can only occur when deleting Records.
      {
        SetBalance( x, LeftHigher );
        SetBalance( z, RightHigher );
        heightDecreased = false;
      }
      else // zb = LeftHigher
      {
        SetBalance( x, Balanced );
        SetBalance( z, Balanced );
      }
      return z;
    }
    else // Double rotation.
    {
      SetLeft( x, GetRight( y ) );
      SetRight( z, GetLeft( y ) );
      SetRight( y, x );
      SetLeft( y, z );
      int yb = GetBalance( y );
      if ( yb == LeftHigher )
      {
        SetBalance( x, RightHigher );
        SetBalance( z, Balanced );
      }
      else if ( yb == Balanced )
      {
        SetBalance( x, Balanced );
        SetBalance( z, Balanced );
      }
      else // yb == RightHigher
      {
        SetBalance( x, Balanced );
        SetBalance( z, LeftHigher );
      }
      SetBalance( y, Balanced );
      return y;
    }
  }

  int RotateLeft( int x, out bool heightDecreased )
  {
    // Right is 2 levels higher than Left.
    heightDecreased = true;
    int z = GetRight( x );
    int y = GetLeft( z );
    int zb = GetBalance( z );
    if ( zb != LeftHigher ) // Single rotation.
    {
      SetLeft( z, x );
      SetRight( x, y );
      if ( zb == Balanced ) // Can only occur when deleting Records.
      {
        SetBalance( x, RightHigher );
        SetBalance( z, LeftHigher );
        heightDecreased = false;
      }
      else // zb = RightHigher
      {
        SetBalance( x, Balanced );
        SetBalance( z, Balanced );
      }
      return z;
    }
    else // Double rotation
    {
      SetRight( x, GetLeft( y ) );
      SetLeft( z, GetRight( y ) );
      SetLeft( y, x );
      SetRight( y, z );
      int yb = GetBalance( y );
      if ( yb == RightHigher )
      {
        SetBalance( x, LeftHigher );
        SetBalance( z, Balanced );
      }
      else if ( yb == Balanced )
      {
        SetBalance( x, Balanced );
        SetBalance( z, Balanced );
      }
      else // yb == LeftHigher
      {
        SetBalance( x, Balanced );
        SetBalance( z, RightHigher );
      }
      SetBalance( y, Balanced );
      return y;
    }
  }

  int Remove( int x, ref IndexFileRecord r, out bool heightDecreased )
  {
    if ( x == 0 ) // key not found.
    {
      heightDecreased = false;
      return x;
    }
    int compare = Compare( x, ref r );
    if ( compare == 0 )
    {
      int deleted = x;
      if ( GetLeft( x ) == 0 )
      {
        heightDecreased = true;
        x = GetRight( x );
      }
      else if ( GetRight( x ) == 0 )
      {
        heightDecreased = true;
        x = GetLeft( x );
      }
      else
      {
        // Remove the smallest element in the right sub-tree and substitute it for x.
        int right = RemoveLeast( GetRight(deleted), out x, out heightDecreased );
        SetLeft( x, GetLeft( deleted ) );
        SetRight( x, right );
        SetBalance( x, GetBalance( deleted ) );
        if ( heightDecreased )
        {
          if ( GetBalance( x ) == LeftHigher )
          {
            x = RotateRight( x, out heightDecreased );
          }
          else if ( GetBalance(x) == RightHigher )
          {
            SetBalance( x, Balanced );
          }
          else
          {
            SetBalance( x, LeftHigher );
            heightDecreased = false;
          }
        }
      }
      FreeNode( deleted );
    }
    else if ( compare < 0 )
    {
      SetLeft( x, Remove( GetLeft(x), ref r, out heightDecreased ) );
      if ( heightDecreased )
      {
        int xb = GetBalance( x );
        if ( xb == RightHigher )
        {
          return RotateLeft( x, out heightDecreased );
        }
        if ( xb == LeftHigher )
        {
          SetBalance( x, Balanced );
        }
        else
        {
          SetBalance( x, RightHigher );
          heightDecreased = false;
        }
      }
    }
    else
    {
      SetRight( x, Remove( GetRight(x), ref r, out heightDecreased ) );
      if ( heightDecreased )
      { 
        int xb = GetBalance( x );
        if ( xb == LeftHigher )
        {
          return RotateRight( x, out heightDecreased );
        }
        if ( GetBalance( x ) == RightHigher )
        {
          SetBalance( x, Balanced );
        }
        else
        {
          SetBalance( x, LeftHigher );
          heightDecreased = false;
        }
      }
    }
    return x;
  }

  int RemoveLeast( int x, out int least, out bool heightDecreased )
  {
    if ( GetLeft(x) == 0 )
    {
      heightDecreased = true;
      least = x;
      return GetRight( x );
    }
    else
    {
      SetLeft( x, RemoveLeast( GetLeft(x), out least, out heightDecreased ) );
      if ( heightDecreased )
      {
        int xb = GetBalance( x );
        if ( xb == RightHigher )
        {
          return RotateLeft( x, out heightDecreased );
        }
        if ( xb == LeftHigher )
        {
          SetBalance( x, Balanced );
        }
        else
        {
          SetBalance( x, RightHigher );
          heightDecreased = false;
        }
      }
      return x;
    }
  }

} // end class IndexPage

class PageParent : IndexPage
{
  public PageParent( IndexFileInfo inf, byte [] data, DatabaseImp d ) : base ( inf, data, d ) {}

  public override void LeafOp( IndexFile ixf, ref IndexFileRecord r, bool insert ) 
  {
    int x = FindSplit( ref r );
    long childId = x == 0 ? FirstPage : GetChild( x );
    IndexPage cp = ixf.GetPage( childId );  
    cp.ParentId = PageId;
    cp.LeafOp( ixf, ref r, insert );  
  }

  public override G.IEnumerable<IndexFileRecord> From( IndexFile ixf, DCompare seek, bool desc )
  {
    int x = FindSplit( seek );
    long childId = x == 0 ? FirstPage : GetChild( x );
    IndexPage cp = ixf.GetPage( childId );  
    cp.ParentId = PageId;
    foreach ( IndexFileRecord r in cp.From(ixf,seek,desc) ) yield return r;
  }

  protected override bool IsLeafPage(){ return false; }

  protected override IndexPage Clone(){ return new PageParent( Inf, null, Database ); }

  protected override void SetRecord( int x, ref IndexFileRecord r )
  {
    Set( NodeBase + NodeSize - PageIdSize + x * NodeSize, (ulong)r.Child, PageIdSize );
    base.SetRecord( x, ref r );
  }

  long GetChild( int x )
  {
    return (long)Get( NodeBase + NodeSize - PageIdSize + x * NodeSize, PageIdSize );
  }

  protected override void GetRecord( int x, ref IndexFileRecord r )
  {
    r.Child = GetChild( x );
    base.GetRecord( x, ref r );
  }

  public override void Dump( IndexFile ixf ) // For debugging only
  {
    System.Console.WriteLine( "Dump Parent Page, PageId=" + PageId + " First Page=" + FirstPage + " Count=" + Count + " NodeSize=" + NodeSize );
    ixf.GetPage( FirstPage ).Dump( ixf );
    foreach ( int x in Nodes( Root ) )
    {
      IndexFileRecord v = new IndexFileRecord();
      System.Console.WriteLine( "Parent Node=" + x );
      GetRecord( x, ref v );
      System.Console.WriteLine( "Parent Key=" + v.ToString( Inf ) );
      ixf.GetPage( GetChild( x ) ).Dump( ixf );
    }
    System.Console.WriteLine( "End Dump Parent Page, PageId=" + PageId + " First Page=" + FirstPage + " Count=" + Count );
  }
} // end class PageParent

} // end namespace DBNS
