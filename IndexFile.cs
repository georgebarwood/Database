namespace DBNS {

using IO = System.IO;
using G = System.Collections.Generic;

/* An IndexFile allows random access to a large file by a particular key. 
   The keys are kept sorted in order in a B-Tree.
   Each page in the B-Tree is represented by an IndexPage object.
*/

class IndexFile
{
  public IndexFile( FullyBufferedStream f, IndexFileInfo inf, DatabaseImp d, long indexId )
  {
    F = f; Inf = inf; Database = d; IndexId = indexId;
    Initialise();
    //System.Console.WriteLine("New IndexFile " + " Root=" + Root + " PageAlloc=" + PageAlloc );
    //Dump();
  }

  public FullyBufferedStream F; // The backing file.
  public IndexFileInfo Inf; // Information about the number of keys and their datatypes ( see below for definition ).
  DatabaseImp Database; // This is used to encode String and Binary keys.
  public long IndexId;

  public IndexPage Root;
  public G.Dictionary<long,IndexPage> PageMap = new G.Dictionary<long,IndexPage>();
  public long PageAlloc; 
  bool Saved;

  public void Commit( CommitStage c )
  {
    if ( Saved ) return;

    if ( c == CommitStage.Prepare )
    {
      // Save the pages to the underlying stream.
      foreach ( G.KeyValuePair<long,IndexPage> pair in PageMap )
      {
        IndexPage p = pair.Value;
        if ( !p.Saved )
        {
          p.WriteHeader();
          F.Position = (long)p.PageId * IndexPage.PageSize;
        
          // For last page, only write the amount actually used (so file size is minimised)
          F.Write( p.Data, 0, p.PageId == PageAlloc-1 ? p.Size() : IndexPage.PageSize );
          p.Saved = true;
        }
      }
    }
    else if ( c == CommitStage.Rollback )
    {
      PageMap.Clear();
      Initialise();
    }
    else F.Commit( c );
    if ( c >= CommitStage.Flush ) Saved = true;
  }

  void Initialise()
  {
    if ( F.Length == 0 ) // New file : create Root page.
    {
      Root = new IndexPage( Inf, null, Database );
      AllocPageId( Root );
    }
    else
    {
      PageAlloc = (long) ( ( F.Length + IndexPage.PageSize - 1 ) / IndexPage.PageSize );
      Root = GetPage( 0 ); // Maybe could delay this until first use of the index ( as index may never be used at all ).
    }
    Saved = true;
  }

  public G.IEnumerable<IndexFileRecord> From( IndexPage.DCompare seek, bool desc )
  {
    foreach ( IndexFileRecord v in Root.From(this,seek,desc) ) yield return v;
  }

  public void Insert( ref IndexFileRecord r )
  {
    Root.LeafOp( this, ref r, true );
    Saved = false;
  }

  public void Delete( ref IndexFileRecord r )
  {
    Root.LeafOp( this, ref r, false );
    Saved = false;
  }

  public IndexPage GetPage( long pageId )
  {
    IndexPage p;
    PageMap.TryGetValue( pageId, out p );
    if ( p == null )
    {
      var data = ReadPage( pageId );
      p = ( 1 & data[0] ) == 0 ? new IndexPage( Inf, data, Database ) : new PageParent( Inf, data, Database );
      p.PageId = pageId;
      p.Saved = true;
      PageMap[ pageId ] = p;
      p.CheckPage();
    }
    return p;
  }

  byte[] ReadPage( long pageId ) // Reads a page from file as a byte array.
  {
    byte [] result = new byte[ IndexPage.PageSize ];
    F.Position = pageId * IndexPage.PageSize;
    int i = 0;
    while ( i < IndexPage.PageSize )
    {
      int got = F.Read( result, i, IndexPage.PageSize-i );
      if ( got == 0 ) break;
      i += got;
    }
    return result;
  }

  public void AllocPageId( IndexPage p )
  {
    p.PageId = PageAlloc++;
    PageMap[ p.PageId ] = p;    
  }

  public IndexFileRecord ExtractKey( Value[] x, long id )
  {
    int n = Inf.BaseIx.Length;
    IndexFileRecord result = new IndexFileRecord( n + 1 );
    for ( int i = 0; i < n; i += 1 ) result.Col[ i ] = x[ Inf.BaseIx[ i ] ];
    result.Col[ n ].L = id;
    return result;
  }

  public void Dump()
  {
    System.Console.WriteLine( "IndexFile Dump PageSize=" + IndexPage.PageSize + " PageAlloc=" + PageAlloc );
    Root.Dump( this );
    System.Console.WriteLine( "End IndexFile Dump, PageSize=" + IndexPage.PageSize + " PageAlloc=" + PageAlloc);
    System.Console.WriteLine();
  }

} // end class IndexFile

struct IndexFileInfo
{
  public DataType [] Types; // Types for the index columns.
  public int KeyCount;
  public int [] BaseIx; // Column indexes in base table for records in index ( used by ExtractKey ).
  public long IndexId;

  public int KeySize()
  {
    int result = 0;
    for ( int i = 0; i < KeyCount; i += 1 )
      result += DTI.Size( Types[i] );
    return result;
  }
}

struct IndexFileRecord
{
  public Value [] Col;
  public long Child;

  public IndexFileRecord( int n )
  {
    Col = new Value[ n ];
    Child = 0;
  }

  public IndexFileRecord Copy()
  {
    IndexFileRecord result = new IndexFileRecord( Col.Length );
    result.Child = Child;
    for ( int i=0; i<Col.Length; i += 1 ) result.Col[i] = Col[i];
    return result;
  }

  public string ToString( IndexFileInfo Inf ) // For debugging / testing only.
  {
    string s = "";
    for ( int i = 0 ; i < Inf.Types.Length; i+= 1 )
    {
      s += Util.ToString( Col[i], Inf.Types[i] ) + "|";
    }
    return s;
  }
}

} // end namespace DBNS