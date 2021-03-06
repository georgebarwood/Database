namespace DBNS {

using IO = System.IO;
using G = System.Collections.Generic;
using SQLNS;

class Table : TableExpression // Represents a stored database Table.
{
  public long RowCount; // Includes deleted rows.
  public int [] AllCols; // Use as 3rd parameter of Get if exact column list is not important.

  readonly DatabaseImp Database;
  readonly FullyBufferedStream DataFile; // The file in which data for the table is stored.

  byte [] Size;   // Stored size of each column
  int [] Offset;  // Offset of each column within stored row.
  readonly G.Dictionary<long,IndexFile> IxDict = new G.Dictionary<long,IndexFile>();

  int RowSize; // The size in bytes of a stored row.
  byte [] RowBuffer; // Buffer for updating a row.
  IndexInfo[] IxInfo; // Information about table indexes.
  bool Dirty; // Has the table been modified since the last commit or rollback?

  public Table ( DatabaseImp database, Schema schema, string name, ColInfo ci, long tableId )
  {
    Database = database;
    TableId = tableId;
    schema.TableDict[ name ] = this;

    IxInfo = new IndexInfo[0];
    DataFile = Database.OpenFile( FileType.Table, tableId );

    InitColumnInfo( ci );

    // System.Console.WriteLine( "Opened " + Schema + "." + name + " RowSize=" + RowSize + " RowCount=" + RowCount );
  }

  void InitColumnInfo( ColInfo ci )
  {
    CI = ci;
    int count = ci.Count;
    AllCols = Util.OneToN( count -  1 );
    Size = new byte[ count ];
    Offset = new int[ count ];
    int offset = -8; // -8 to allow for the Id column not being stored.
    for ( int i = 0; i < count; i += 1 ) 
    {
      Size[ i ] = (byte)DTI.Size( CI.Type[ i ] );  
      Offset[ i ] = offset;
      offset += Size[ i ];
    }

    RowSize = 1 + offset; // +1 is for byte that indicates whether row exists.
    RowCount = DataFile.Length / RowSize;
    RowBuffer = new byte[ RowSize ];
  }

  public override void Commit( CommitStage c )
  {
    if ( !Dirty ) return;
    DataFile.Commit( c );
    foreach ( G.KeyValuePair<long,IndexFile> p in IxDict ) p.Value.Commit( c );
    if ( c >= CommitStage.Flush ) Dirty = false;
  }

  public void CloseAndDelete() // Called as part of DROP TABLE
  {
    foreach( G.KeyValuePair<long,IndexFile> p in IxDict )
    {
      IndexFile f = p.Value;
      f.F.Close();
      Database.DeleteFile( FileType.Index, f.IndexId  );
    }
    DataFile.Close();
    Database.DeleteFile( FileType.Table, TableId );
  }

  // Basic read/write functions ( indexes not updated ).

  // Get fetchs the row identified by id from the file buffer into the Value array.
  // cols specifies a subset of the columns to be fetched ( as an optimisation ). If not important, use AllCols.
  public override bool Get( long id, Value[] row, int [] cols )
  {
    if ( id <= 0 || id > RowCount ) return false;

    DataFile.Position = (id-1) * RowSize;
    int ix; byte [] RowBuffer = DataFile.FastRead( RowSize, out ix );
    if ( RowBuffer[ix++] == 0 ) return false; // Row has been deleted

    row[ 0 ].L = id;
    for ( int c = 0; c < cols.Length; c += 1 )
    {
      int col = cols[ c ];
      DataType t = CI.Type[ col ];
      long x = Util.Get( RowBuffer, ix + Offset[ col ], Size[ col ], t ); 
      row[ col ].L = x;
      if ( t <= DataType.String ) row[ col ]._O = Database.Decode( x, t );      
    }  
    return true;
  }

  public override G.IEnumerable<bool> GetAll( Value[] row, int [] cols, EvalEnv ee )
  { 
    long n = RowCount;
    for ( long id = 1; id <= n; id += 1 )
      if ( Get( id, row, cols ) ) yield return true;  
  }

  void Save( long id, Value [] row, bool checkNew )
  { 
    DataFile.Position = (id-1) * RowSize;

    if ( row == null ) // Delete record
    {
      for ( int i = 0; i < RowSize; i += 1 ) RowBuffer[ i ] = 0;
    }
    else
    {
      RowBuffer[ 0 ] = 1;
      int ix = 1;
      for ( int i = 1; i < CI.Count; i += 1 )
      {
        DataType t = CI.Type[ i ];
        long x = row[ i ].L;
        if ( t <= DataType.String && x == 0 )
        {
          x = Database.Encode( row[ i ]._O, t );
          row[ i ].L = x;
        }
        else if ( t == DataType.Float ) x = (long)Conv.PackFloat( x );
        int size = Size[ i ];
        Util.Set( RowBuffer, ix, x, size );     
        ix += size;
      }
    }
    if ( !DataFile.Write( RowBuffer, 0, RowSize, checkNew ) ) 
    {
      throw new System.Exception( "Duplicate id" );
    }
    Dirty = true;
  }

  // Higher level operations ( indexes are updated as required ).

  public long Insert( Value [] row, int idCol )
  {
    long id;
    if ( idCol < 0 ) // Id is not in INSERT list, allocate a new id.
    {
      id = ++RowCount;
    }
    else 
    {
      id = row[ idCol ].L;
      if ( RowCount < id  ) RowCount = id;
    }
    Save( id, row, idCol >= 0 );

    // Update indexes.
    foreach ( G.KeyValuePair<long,IndexFile> p in IxDict )
    {
      IndexFile ixf = p.Value;
      IndexFileRecord ixr = ixf.ExtractKey( row, id );
      ixf.Insert( ref ixr );
    }
    return id;
  }

  public void Update( long id, Value[] dr, Value [] nr )
  {
    Save( id, nr, false );

    // Update indexes.
    foreach ( G.KeyValuePair<long,IndexFile> p in IxDict )
    {
      IndexFile ixf = p.Value;

      IndexFileRecord ixd = ixf.ExtractKey( dr, id );
      IndexFileRecord ixn = ixf.ExtractKey( nr, id );

      // Check if new and old keys are the same to save work.
      if ( ixf.Root.Compare( ref ixd, ref ixn ) != 0 )
      {
        ixf.Delete( ref ixd );
        ixf.Insert( ref ixn );
      }
    }     
  }

  public void Delete( long id, Value[] dr )
  {
    Save( id, null, false );

    // Update indexes.
    foreach ( G.KeyValuePair<long,IndexFile> p in IxDict )
    {
      IndexFile ixf = p.Value;
      IndexFileRecord ixd = ixf.ExtractKey( dr, id );
      ixf.Delete( ref ixd );
    }     
  }

  // Higher level functions.

  public long Insert( TableExpression te, int[] colIx, int idCol, EvalEnv ee )
  {
    var ins = new Inserter( this, colIx, idCol, te );
    te.FetchTo( ins, ee );
    return ins.LastIdInserted;
  }

  public void Update( int [] cols, Exp.DV [] dvs, Exp.DB where, int idCol, IdSet ids, EvalEnv ee  )
  {
    Value [] tr = new Value[ CI.Count ]; // The old row.
    Value [] nr = new Value[ CI.Count ]; // The new row.
    ee.Row = tr;

    if ( ids == null ) ids = new UpTo( RowCount );
    else ids = new IdCopy( ids ); // Need to take a copy of the id values if an index is used.

    foreach ( long id in ids.All( ee ) ) 
    if ( Get( id, tr, AllCols ) && where( ee ) )
    {
      // Initialise new row as copy of old row.
      for ( int i=0; i<nr.Length; i +=1 ) nr[ i ] = tr[ i ];

      // Update the new row.
      for ( int i=0; i < cols.Length; i += 1 ) nr[ cols [ i ] ] = dvs[ i ]( ee );

      if ( idCol >= 0 && nr[ idCol ].L != id ) // Row Id is changing.
      {
        Delete( id, tr );
        Insert( nr, idCol );
      }
      else Update( id, tr, nr );
    }
  }

  public void Delete( Exp.DB where, IdSet ids, EvalEnv ee )
  {
    Value [] tr = new Value[ CI.Count ];
    ee.Row = tr;

    if ( ids == null ) ids = new UpTo( RowCount );
    else ids = new IdCopy( ids );

    foreach ( long id in ids.All( ee ) ) 
      if ( Get( id, tr, AllCols ) && where( ee ) ) Delete( id, tr );
  }

  public int ColumnIx( string name, Exec e )
  {
    int n = CI.Count;
    for ( int i = 0; i < n; i += 1 ) if ( CI.Name[ i ] == name ) return i;
    e.Error( "Column " + name + " not found" );
    return 0;
  }

  // Functions related to Indexing.

  public void InitIndex( long indexId ) // Called during CREATE INDEX to index existing rows.
  {
    IndexFile ixf = IxDict[ indexId ];
    var rc = new RowCursor( this );
    int n = ixf.Inf.BaseIx.Length;
    IndexFileRecord ixr = new IndexFileRecord( n + 1 );
    
    for ( long id = 1; id <= RowCount; id += 1 ) if ( rc.Get(id) )
    {
      // Create the IndexFileRecord to be inserted.
      for ( int i = 0; i < n; i += 1 )ixr.Col[ i ] = rc.V[ ixf.Inf.BaseIx[ i ] ];
      ixr.Col[n].L = id; // Append the id of the row.
      ixf.Insert( ref ixr );
    }
    Dirty = true;
  }

  public override IndexFile FindIndex( int colIx ) // Finds an index to speed up a WHERE condition.
  {
    for ( int i = 0; i < IxInfo.Length; i += 1 )
      if ( IxInfo[ i ].ColIx == colIx && IxInfo[ i ].IxNum == 0 )
        return IxDict[ IxInfo[ i ].IndexId ];
    return null;
  }

  public void OpenIndexes()
  {
    OpenIndexes( Database.ReadIndexes( TableId ) );
  }

  public void OpenIndexes( IndexInfo[] info )
  {
    IxInfo = info;

    long curIndex = -1;
    var dt = new G.List<DataType>();
    var cm = new G.List<int>();

    for ( int i = 0; i <= info.Length; i += 1 )
    {
      if ( i > 0 && ( i == info.Length || info[ i ].IndexId != curIndex ) )
      {
        IndexFileInfo ci = new IndexFileInfo();
        dt.Add( DataType.Bigint ); // For id value
        ci.KeyCount = dt.Count;
        ci.Types = dt.ToArray();
        ci.BaseIx = cm.ToArray();
        ci.IndexId = curIndex;
        OpenIndex( curIndex, ci );
        dt = new G.List<DataType>();
        cm = new G.List<int>();
      }
      if ( i != info.Length )
      {
        curIndex = info[ i ].IndexId;
        int colIx = info[ i ].ColIx;
        dt.Add( CI.Type[ colIx ] );
        cm.Add( colIx );
      }
    }
  }

  public void OpenIndex( long indexId, IndexFileInfo ci )
  {
    IndexFile ixf;
    IxDict.TryGetValue( indexId, out ixf );
    if ( ixf == null )
    {
      var stream = Database.OpenFile( FileType.Index, indexId );
      IxDict[ indexId ] = new IndexFile( stream, ci, Database, indexId );
    }
  }

  public void CloseAndDeleteIndex( long indexId )
  {
    IndexFile ixf;
    IxDict.TryGetValue( indexId, out ixf );
    if ( ixf != null )
    {
      ixf.F.Close();
      IxDict.Remove( indexId );
      Database.DeleteFile( FileType.Index, indexId );
    }
  }

  // For implementation of Alter Table.
  public void AlterData( ColInfo newcols, int[] map )
  {
    // For each record, read the old data, copy the columns, write the new data.
    // Each column in the new table will either be new, in which case it gets a default value,
    // or is copied from an old column. The types do not have to match exactly, but need to have the same base type.
    // Map holds the old colId for eaach new column ( or -1 if it is a new column ).
    // Doesn't currently check for overflow.

    long [] oldRow = new long[ CI.Count ];
    long [] newRow = new long[ newcols.Count ];
    // Initialise newRow to default values.
    for ( int i = 0; i < newRow.Length; i += 1 )
      newRow[ i ] = DTI.Default( newcols.Type[ i ] ).L;

    int newRowSize = CalcRowSize( newcols );
    byte [] blank = new byte[ newRowSize ];
    RowBuffer = new byte[ newRowSize ];

    // So that old data is not over-written before it has been converted, if new row size is bigger, use descending order.
    bool desc = newRowSize > RowSize;
    long id = desc ? RowCount - 1 : 0; // Note : zero based, whereas actual id values are 1-based.
    long n = RowCount;
    while ( n > 0 )
    {
      DataFile.Position = id * RowSize;
      bool ok = AlterRead( CI.Type, oldRow );

      for ( int i = 0; i < newRow.Length; i += 1 )
      {
        int m = map[ i ];
        if ( m >= 0 ) newRow[ i ] = oldRow[ m ];
      }

      DataFile.Position = id * newRowSize;
      if ( ok ) AlterWrite( newcols.Type, newRow, newRowSize );
      else DataFile.Write( blank, 0, blank.Length );
      n -= 1;
      id = desc ? id - 1 : id + 1;
    }
    if (!desc) DataFile.SetLength( RowCount * newRowSize );
    Dirty = true;
    InitColumnInfo( newcols );
    // RowSize = newRowSize;
    // CI = newcols;
  }

  bool AlterRead( DataType [] types, long [] row ) 
  {
    int ix; byte [] RowBuffer = DataFile.FastRead( RowSize, out ix );
    if ( RowBuffer[ix++] == 0 ) return false; // Row has been deleted
    for ( int i=1; i < types.Length; i += 1 )
    {
      DataType t = types[ i ];
      int size = DTI.Size( t );
      long x = Util.Get( RowBuffer, ix, size, t );
      ix += size;
      row[ i ] = x;
    }
    return true;
  }

  void AlterWrite( DataType [] types, long [] row, int newRowSize )
  {
    RowBuffer[ 0 ] = 1;
    int ix = 1;
    for ( int i = 1; i < types.Length; i += 1 )
    {
      DataType t = types[ i ];
      long x = row[ i ];
      if ( t == DataType.Float ) x = Conv.PackFloat( x );
      int size = DTI.Size( t );
      Util.Set( RowBuffer, ix, x, size );
      ix += size;       
    }
    DataFile.Write( RowBuffer, 0, newRowSize, false );
  }

  // end Alter section

  int CalcRowSize( ColInfo c )
  {
    int result = 1; // Flag byte that indicates whether row is deleted.
    for ( int i = 1; i < c.Count; i += 1 )
      result += DTI.Size( c.Type[ i ] ); 
    return result;
  }

}  // End class Table


class RowCursor
{
  Table T;
  public Value [] V;

  public RowCursor( Table t )
  { 
    T = t; 
    V = new Value[ T.CI.Count ];
  }

  public long Insert() // Insert the Row into the underlying Table
  {
    return T.Insert( V, -1 );
  }

  public void Update( long id )
  {
    var old = new Value[ T.CI.Count ];
    T.Get( id, old, T.AllCols );
    T.Update( id, old, V );
  }

  public bool Get( long id ) // Fetch the specified Row from the underlying Table
  { 
    return T.Get( id, V, T.AllCols );
  }

} // end class RowCursor


class Inserter : ResultSet
{
  Table T;
  int [] ColIx;
  int IdCol;
  Value [] Row;
  TableExpression TE;

  public Inserter( Table t, int[] colIx, int idCol, TableExpression te )
  {
    T = t; ColIx = colIx; IdCol = idCol; TE = te;
    DataType [] types = t.CI.Type; 
    Row = new Value[ types.Length ];
    // Initialise row to default values.
    for ( int i = 0; i < types.Length; i += 1 ) Row[ i ] = DTI.Default( types[ i ] );
  }

  public override bool NewRow( Value[] row )
  {
    for ( int i=0; i < ColIx.Length; i += 1 )
      Row[ ColIx[ i ] ] = row[ i ];
    LastIdInserted = T.Insert( Row, IdCol );
    return true;
  }
} // end class Inserter

struct IndexInfo 
{
  public long IndexId;
  public int IxNum;
  public int ColIx;

  public static IndexInfo[] Single( long indexId, int cix )
  {
    var a = new IndexInfo[ 1 ];
    a[ 0 ].IndexId = indexId;
    a[ 0 ].IxNum = 0;
    a[ 0 ].ColIx = cix;
    return a;
  }    
}

} // end namespace DBNS
