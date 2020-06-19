namespace DBNS {

using IO = System.IO;
using G = System.Collections.Generic;
using SQLNS;

class Table : TableExpression // Represents a Database Table.
{
  readonly string Schema, Name;
  readonly G.Dictionary<long,IndexFile> IxDict = new G.Dictionary<long,IndexFile>();

  readonly FullyBufferedStream DF;
  int RowSize;
  byte [] RowBuffer;
  readonly DatabaseImp Db;

  public long RowCount; // Includes deleted rows.
  IndexInfo[] Ix;
  bool Dirty;

  public Table ( DatabaseImp db, Schema schema, string name, ColInfo cols, long tableId )
  {
    Db = db;
    Schema = schema.Name;
    Name = name;
    Cols = cols;
    TableId = tableId;

    schema.TableDict[ name ] = this;
    Ix = new IndexInfo[0];
    DF = Db.OpenFile( FileType.Table, tableId );

    RowSize = CalcRowSize( Cols );
    RowCount = (long)( DF.Length / RowSize );
    RowBuffer = new byte[ RowSize ];

    // System.Console.WriteLine( "Opened " + Schema + "." + name + " RowSize=" + RowSize + " RowCount=" + RowCount );
  }

  public override void Commit( CommitStage c )
  {
    if ( !Dirty ) return;
    DF.Commit( c );
    foreach ( G.KeyValuePair<long,IndexFile> p in IxDict ) p.Value.Commit( c );
    if ( c >= CommitStage.Flush ) Dirty = false;
  }

  public void CloseAndDelete() // Called as part of DROP TABLE
  {
    foreach( G.KeyValuePair<long,IndexFile> p in IxDict )
    {
      IndexFile f = p.Value;
      f.F.Close();
      Db.DeleteFile( FileType.Index, f.IndexId  );
    }
    DF.Close();
    Db.DeleteFile( FileType.Table, TableId );
  }

  // Basic read/write functions ( indexes not updated ).

  public override G.IEnumerable<bool> GetAll( Value[] row, bool [] used, EvalEnv ee )
  { 
    long n = RowCount;
    for ( long id = 1; id <= n; id += 1 )
      if ( Get( id, row, used ) ) yield return true;  
  }

  public override bool Get( long id, Value[] row, bool [] used )
  {
    if ( id <= 0 || id > RowCount ) return false;

    DF.Position = (id-1) * RowSize;
    int ix; byte [] RowBuffer = DF.FastRead( RowSize, out ix );
    if ( RowBuffer[ix++] == 0 ) return false; // Row has been deleted
    row[ 0 ].L = id;
    DataType [] types = Cols.Types;
    byte [] sizes = Cols.Sizes;
    for ( int i=1; i < types.Length; i += 1 )
    {
      int size = sizes[ i ];
      if ( used == null || used [ i ] ) // Column not skipped
      {
        DataType t = types[ i ];
        long x = (long)Util.Get( RowBuffer, ix, size, t ); 
        row[ i ].L = x;
        if ( t <= DataType.String ) row[ i ]._O =       
          t == DataType.Binary ? (object)Db.DecodeBinary( x ): (object)Db.DecodeString( x );
      }
      ix += size;
    }  
    return true;
  }

  void Save( long id, Value [] row, bool checkNew )
  { 
    DF.Position = (id-1) * RowSize;

    if ( row == null ) // Delete record
    {
      for ( int i = 0; i < RowSize; i += 1 ) RowBuffer[ i ] = 0;
    }
    else
    {
      DataType [] types = Cols.Types;
      byte [] sizes = Cols.Sizes;
      RowBuffer[ 0 ] = 1;
      int ix = 1;
      for ( int i = 1; i < types.Length; i += 1 )
      {
        DataType t = types[ i ];
        ulong x = (ulong) row[ i ].L;
        if ( t <= DataType.String && x == 0 )
        {
          object o = row[ i ]._O;
          x = ( t == DataType.Binary ? (ulong)Db.EncodeBinary( (byte[]) o ) : (ulong)Db.EncodeString( (string) o ) );
          row[ i ].L = (long) x;
        }
        else if ( t == DataType.Float ) x = Conv.PackFloat( x );
        int size = sizes[ i ];
        Util.Set( RowBuffer, ix, x, size );     
        ix += size;
      }
    }
    if ( !DF.Write( RowBuffer, 0, RowSize, checkNew ) ) 
    {
      throw new System.Exception( "Duplicate id, id=" + id + " Table=" + Schema + "." + Name );
    }
    Dirty = true;
  }

  // Higher level operations ( indexes are updated as required ).

  public long Insert( Value [] row, int idCol )
  {
    long id;
    if ( idCol < 0 )
    {
      id = ++RowCount;
    }
    else 
    {
      id = row[idCol].L;
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

  public long ExecInsert( TableExpression te, int[] colIx, int idCol, EvalEnv ee )
  {
    var ins = new Inserter( this, colIx, idCol, te );
    te.FetchTo( ins, ee );
    return ins.LastIdInserted;
  }

  public void ExecUpdate( int [] ixs, Exp.DV [] dvs, Exp where, Exp.DB w, bool [] used, int idCol, EvalEnv ee  )
  {
    Value [] tr = new Value[ Cols.Count ];
    Value [] nr = new Value[ Cols.Count ]; // The new row.
    ee.Row = tr;

    IdSet IdSet = where == null ? null : where.GetIdSet( this, ee );

    if ( IdSet == null ) IdSet = new UpTo( RowCount );
    else IdSet = new IdCopy( IdSet, ee ); // Need to take a copy of the id values if an index is used.

    foreach ( long id in IdSet.All( ee ) ) 
    if ( Get( id, tr, null ) )
    {
      for ( int i=0; i<nr.Length; i +=1 ) nr[ i ] = tr[ i ];

      if ( w( ee ) )
      {
        for ( int i=0; i < ixs.Length; i += 1 ) 
        {
          int ix = ixs[ i ];
          nr[ ix ] = dvs[ i ]( ee );   
        }
        if ( idCol >= 0 && nr[ idCol ].L != id )
        {
          Delete( id, tr );
          Insert( nr, idCol );
        }
        else Update( id, tr, nr );
      }
    }
  }

  public void ExecDelete( Exp where, Exp.DB w, bool[] used, EvalEnv ee )
  {
    Value [] tr = new Value[ Cols.Count ];
    ee.Row = tr;

    IdSet IdSet = where == null ? null : where.GetIdSet( this, ee );
    if ( IdSet == null ) IdSet = new UpTo( RowCount );
    else IdSet = new IdCopy( IdSet, ee ); // Need to take a copy of the id values, as indexes may be updated.

    foreach ( long id in IdSet.All( ee ) ) 
      if ( Get( id, tr, null ) && w( ee ) ) Delete( id, tr );
  }

  public int ColumnIx( string name, Exec e )
  {
    int n = Cols.Count;
    for ( int i = 0; i < n; i += 1 ) if ( Cols.Names[ i ] == name ) return i;
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
    for ( int i = 0; i < Ix.Length; i += 1 )
      if ( Ix[ i ].ColIx == colIx && Ix[ i ].IxNum == 0 )
        return IxDict[ Ix[ i ].IndexId ];
    return null;
  }

  public void OpenIndexes()
  {
    OpenIndexes( Db.ReadIndexes( TableId ) );
  }

  public void OpenIndexes( IndexInfo[] info )
  {
    Ix = info;

    long curIndex = -1;
    var dt = new G.List<DataType>();
    var cm = new G.List<int>();

    for ( int i=0; i<=info.Length; i += 1 )
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
        dt.Add( Cols.Types[ colIx ] );
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
      var stream = Db.OpenFile( FileType.Index, indexId );
      IxDict[ indexId ] = new IndexFile( stream, ci, Db, indexId );
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
      Db.DeleteFile( FileType.Index, indexId );
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

    long [] oldRow = new long[ Cols.Count ];
    long [] newRow = new long[ newcols.Count ];
    // Initialise newRow to default values.
    for ( int i = 0; i < newRow.Length; i += 1 )
      newRow[ i ] = DTI.Default( newcols.Types[ i ] ).L;

    int newRowSize = CalcRowSize( newcols );
    byte [] blank = new byte[ newRowSize ];
    RowBuffer = new byte[ newRowSize ];

    // So that old data is not over-written before it has been converted, if new row size is bigger, use descending order.
    bool desc = newRowSize > RowSize;
    long id = desc ? RowCount - 1 : 0; // Note : zero based, whereas actual id values are 1-based.
    long n = RowCount;
    while ( n > 0 )
    {
      DF.Position = id * RowSize;
      bool ok = AlterRead( Cols.Types, oldRow );

      for ( int i = 0; i < newRow.Length; i += 1 )
      {
        int m = map[ i ];
        if ( m >= 0 ) newRow[ i ] = oldRow[ m ];
      }

      DF.Position = id * newRowSize;
      if ( ok ) AlterWrite( newcols.Types, newRow, newRowSize );
      else DF.Write( blank, 0, blank.Length );
      n -= 1;
      id = desc ? id - 1 : id + 1;
    }
    if (!desc) DF.SetLength( RowCount * newRowSize );
    Dirty = true;
    RowSize = newRowSize;
    Cols = newcols;
  }

  bool AlterRead( DataType [] types, long [] row ) 
  {
    int ix; byte [] RowBuffer = DF.FastRead( RowSize, out ix );
    if ( RowBuffer[ix++] == 0 ) return false; // Row has been deleted
    for ( int i=1; i < types.Length; i += 1 )
    {
      DataType t = types[ i ];
      int size = DTI.Size( t );
      ulong x = Util.Get( RowBuffer, ix, size, t );
      ix += size;
      row[ i ] = (long)x;
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
      ulong x = (ulong) row[ i ];
      if ( t == DataType.Float ) x = Conv.PackFloat( x );
      int size = DTI.Size( t );
      Util.Set( RowBuffer, ix, x, size );
      ix += size;       
    }
    DF.Write( RowBuffer, 0, newRowSize, false );
  }

  // end Alter section

  int CalcRowSize( ColInfo c )
  {
    int result = 1; // Flag byte that indicates whether row is deleted.
    for ( int i = 1; i < c.Count; i += 1 )
      result += DTI.Size( c.Types[ i ] ); 
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
    V = new Value[ T.Cols.Count ];
  }

  public long Insert() // Insert the Row into the underlying Table
  {
    return T.Insert( V, -1 );
  }

  public void Update( long id )
  {
    var old = new Value[ T.Cols.Count ];
    T.Get( id, old, null );
    T.Update( id, old, V );
  }

  public bool Get( long id ) // Fetch the specified Row from the underlying Table
  { 
    return T.Get( id, V, null );
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
    DataType [] types = t.Cols.Types; 
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
