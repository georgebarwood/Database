namespace DBNS
{

using G = System.Collections.Generic;
using IO = System.IO;
using SQLNS;

enum FileType{ Table=0, Index=1, System = 3 };

class DatabaseImp : Database
{  

  public override void Sql( string sql, ResultSet rs )
  {
    SqlExec.ExecuteBatch( sql, this, rs );
    RollbackOrCommit();
  }

  public DatabaseImp( string dirName )
  {
    Name = dirName;
    Log = new Log( Name );
    Sys = GetSchema( "sys", true, null );

    {
      SysString = OpenFile(  FileType.System, 0 );
      SysStringReader = new BinaryReader( SysString );
      SysStringWriter = new BinaryWriter( SysString );

      IndexFileInfo ii = new IndexFileInfo();
      ii.KeyCount = 1;
      ii.Types = new DataType[] { DataType.String };
      var f = OpenFile( FileType.System, 1 );
      SysStringIndex = new IndexFile( f, ii, this, -1 );
    }

    {
      SysBinary = OpenFile( FileType.System, 2 );
      SysBinaryReader = new BinaryReader( SysBinary );
      SysBinaryWriter = new BinaryWriter( SysBinary );

      IndexFileInfo ii = new IndexFileInfo();
      ii.KeyCount = 1;
      ii.Types = new DataType[] { DataType.Binary };
      var f = OpenFile( FileType.System, 3 );
      SysBinaryIndex = new IndexFile( f, ii, this, -2 );
    }

    var cb = new ColBuilder();

    cb.Add( "Name", DataType.String );
    SysSchema = NewSysTable( 1, "Schema", cb.Get() );

    cb.Add( "Schema", DataType.Int );
    cb.Add( "Name",   DataType.String );
    cb.Add( "IsView", DataType.Tinyint );
    cb.Add( "Definition", DataType.String );
    SysTable = NewSysTable( 2, "Table", cb.Get() );

    cb.Add( "Table",    DataType.Int );
    cb.Add( "Name",     DataType.String );
    cb.Add( "Type",     DataType.Int );
    SysColumn = NewSysTable( 3, "Column", cb.Get() );

    cb.Add( "Table",    DataType.Int );
    cb.Add( "Name",     DataType.String );
    cb.Add( "Modified", DataType.Tinyint );
    SysIndex = NewSysTable( 4, "Index", cb.Get() );

    cb.Add( "Table",    DataType.Int );
    cb.Add( "Index",    DataType.Int );
    cb.Add( "ColId",    DataType.Int );
    SysIndexCol = NewSysTable( 5, "IndexCol", cb.Get() );

    SysColumn.OpenIndexes( IndexInfo.Single( 1, 1 ) );
    SysColumnIndex = SysColumn.FindIndex( 1 );

    SysIndexCol.OpenIndexes( IndexInfo.Single( 2, 1) );
    SysIndexColIndex = SysIndexCol.FindIndex( 1 );

    SysSchema.OpenIndexes( IndexInfo.Single( 3, 1 ) );
    SysSchemaByName = SysSchema.FindIndex( 1 );

    SysTable.OpenIndexes( IndexInfo.Single( 4, 2 ) );
    SysTableByName = SysTable.FindIndex( 2 );

    if ( SysSchema.RowCount == 0 )
    {
      IsNew = true;
      Sql( "CREATE SCHEMA sys" ); // Note these are created in TableId order.
      Sql( "CREATE TABLE sys.Schema( Name string )" );
      Sql( "CREATE TABLE sys.Table( Schema int, Name string, IsView tinyint, Definition string )" );
      Sql( "CREATE TABLE sys.Column( Table int, Name string, Type int )" );
      Sql( "CREATE TABLE sys.Index( Table int, Name string, Modified tinyint )" );
      Sql( "CREATE TABLE sys.IndexCol( Table int, Index int, ColId int )" );
      Sql( "CREATE INDEX ColumnByTable on sys.Column(Table)" );
      Sql( "CREATE INDEX IndexColByTable on sys.IndexCol(Table)" );
      Sql( "CREATE INDEX SchemaByName on sys.Schema(Name)" );
      Sql( "CREATE INDEX TableByName on sys.Table(Name)" );
      Normal = true;

      Sql( "CREATE TABLE sys.Function( Schema int, Name string, Definition string )" );
      Sql( "CREATE INDEX FunctionByName on sys.Function(Name)" );

      Sql( "CREATE TABLE sys.Procedure( Schema int, Name string, Definition string )" );
      Sql( "CREATE INDEX ProcedureByName on sys.Procedure(Name)" );
    }
    RollbackOrCommit();
    Normal = true;
  } // end DatabaseImp

  /* Private data */
  readonly string Name;
  Log Log;
  bool Rollback;
  G.HashSet<long> DeletedFiles = new G.HashSet<long>();

  readonly IO.Stream SysString, SysBinary;
  readonly BinaryReader SysStringReader, SysBinaryReader;
  readonly BinaryWriter SysStringWriter, SysBinaryWriter;

  readonly Schema Sys;
  readonly Table SysSchema, SysTable, SysColumn, SysIndex, SysIndexCol;
  readonly IndexFile SysColumnIndex, SysSchemaByName, SysTableByName, SysIndexColIndex, SysStringIndex, SysBinaryIndex;
  readonly G.Dictionary<string,Schema> SchemaDict = new G.Dictionary<string,Schema>();

  readonly bool Normal; // False during initialisation.

  /* Public methods */

  public void ExecuteSql( string sql, ResultSet rs ) // Internal version ( does not commit changes )
  {
    SqlExec.ExecuteBatch( sql, this, rs );
  }

  public Value ScalarSql( string sql )
  {
    var rs = new SingleResultSet();
    ExecuteSql( sql, rs );
    var t = rs.Table;
    if ( t.Rows.Count == 0 ) return new Value();
    return t.Rows[0][0];  
  }  

  public FullyBufferedStream OpenFile( FileType ft, long id )
  {
    long fileId = ft == FileType.System ? id : 4 + id*2 + (long)ft;
    string filename = Name + fileId;
    return new FullyBufferedStream( Log, fileId, new IO.FileStream( filename, IO.FileMode.OpenOrCreate ) );
  }

  public void DeleteFile( FileType ft, long id )
  {
    long fileId = ft == FileType.System ? id : 4 + id*2 + (long)ft;
    DeletedFiles.Add( fileId );
    Log.SetLength( fileId, 0 );
  }

  public void SetRollback()
  {
    Rollback = true;
  }

  // Schema methods Create/Drop Schema, Table, View, Function, Procedure, Index etc.

  public void CreateSchema( string schemaName, Exec e )
  {
    if ( Normal && GetSchemaId( schemaName, false, e ) >= 0 ) 
      e.Error( "Schema already exists : " + schemaName );
    var r = new RowCursor( SysSchema );
    r.V[1].O = schemaName;
    r.Insert();
  }

  public void DropSchema( string schemaName, Exec e )
  {
    // Drop all the tables, views and routines in the schema.
    Schema schema = GetSchema( schemaName, true, e );
    Sql( "EXEC sys.DropSchema(" + Util.Quote(schemaName) + ")" );
    SchemaDict.Remove( schemaName );
    ResetCache(); // Because there may be loaded views, functions or procedures that reference the dropped schema.
  }

  public void CreateTable( string schemaName, string tableName, ColInfo cols, string definition, bool isView, bool alter, Exec e )
  {
    Schema schema = GetSchema( schemaName, true, e );

    bool isView1; string definition1;
    long tableId = tableId = ReadTableId( schema, tableName, out isView1, out definition1 );

    if ( alter ) 
    {
      if ( tableId == 0 || !isView1 ) e.Error( "View does not exist" );
    }
    else if ( Normal && schema.GetCachedTable( tableName ) != null || tableId >= 0 ) 
      e.Error( "Table/View " + schemaName + "." + tableName + " already exists" );

    var r = new RowCursor( SysTable );
    r.V[1].L = schema.Id;
    r.V[2].O = tableName;
    r.V[3].L = isView ? 1 : 0;
    r.V[4].O = isView ? definition.Trim() : "";

    if ( alter ) r.Update( tableId ); else tableId = r.Insert();

    if ( isView ) ResetCache(); else SaveColumns( tableId, cols );
  }

  public void DropTable( string schemaName, string tableName, Exec e )
  {
    Table t = GetTable( schemaName, tableName, e );    
    long tid = t.TableId;
    t.CloseAndDelete();
    Sql( "EXEC sys.DropTable(" + tid + ")" );
    Schema s = GetSchema( schemaName, true, e );
    s.TableDict.Remove( tableName );
    ResetCache();
  }

  public void DropView( string schemaName, string name, Exec e )
  {
    Schema schema = GetSchema( schemaName, true, e );
    bool isView; string definition;
    long vid = ReadTableId( schema, name, out isView, out definition );
    if ( vid < 0 ) e.Error( schemaName + "." + name + " not found" );
    if ( !isView ) e.Error( schemaName + "." + name  + " is not a view" );    
    Sql( "DELETE FROM sys.Table WHERE Id = " + vid );
    ResetCache();
  }

  public Table GetTable( string schemaName, string name, Exec e )
  {
    var result = GetTableOrView( schemaName, name, e );
    if ( result is Table ) return (Table)result;
    e.Error( name + " is a view not a table" );
    return null;
  }

  public TableExpression GetTableOrView( string schemaName, string name, Exec e )
  {
    Schema schema = GetSchema( schemaName, true, e );
    TableExpression result = schema.GetCachedTable( name ); 
    if ( result != null ) return result;

    bool isView; string definition;
    long tid = ReadTableId( schema, name, out isView, out definition );
    if ( tid < 0 ) e.Error( schemaName + "." + name + " not found" );

    if ( isView )
    {
      TableExpression te = e.LoadView( definition, schemaName + "." + name );
      te.TableId = tid;
      schema.TableDict[ name ] = te;
      return te;
    }
    else
    {
      // Fetch the Column Information from the SysColumn table.
      var names = new G.List<string>(); 
      var types = new G.List<DataType>();
      names.Add( "Id" ); types.Add( DataType.Bigint ); // Add the pre-defined "id" column.

      // Use SysColumnIndex to avoid scanning the entire SysColumn table.
      var start = new LongStart( tid );
      var r = new RowCursor( SysColumn );

      foreach( IndexFileRecord ixr in SysColumnIndex.From( start.Compare, false ) )
      {
        if ( ixr.Col[0].L == tid )
        {
          r.Get( ixr.Col[1].L );
          names.Add( (string) r.V[2]._O );
          types.Add( (DataType)r.V[3].L );
        }
        else break;
      }

      Table t = new Table( this, schema, name, ColInfo.New(names,types), tid );
      t.OpenIndexes();
      return t;
    }
  }

  public void AlterTable( string schemaName, string tableName, G.List<AlterAction> alist, Exec e )
  {
    Table t = (Table) GetTable( schemaName, tableName, e );

    var names = new G.List<string>( t.Cols.Names );
    var types = new G.List<DataType>( t.Cols.Types );
    var map = new G.List<int>();
    for ( int i = 0; i < names.Count; i += 1 ) map.Add( i );

    foreach ( AlterAction aa in alist )
    {
      int ix = names.IndexOf( aa.Name );
      if ( aa.Operation != Action.Add && ix == -1 )
        e.Error( "Column " + aa.Name + " not found" );

      switch ( aa.Operation )
      {
        case Action.Add: 
          if ( ix != -1 ) e.Error( "Column " + aa.Name + " already exists" );
          names.Add( aa.Name );
          types.Add( aa.Type );
          map.Add( 0 );
          Sql( "INSERT INTO sys.Column( Table, Name, Type ) VALUES ( " + t.TableId + "," + Util.Quote(aa.Name) + "," + (int)aa.Type + ")" );
          break;
        case Action.Drop:
          names.RemoveAt( ix );
          types.RemoveAt( ix );
          map.RemoveAt( ix );
          Sql( "DELETE FROM sys.Column WHERE Table = " + t.TableId + " AND Name = " + Util.Quote(aa.Name) ); 
          Sql( "EXEC sys.DroppedColumn(" + t.TableId + "," + ix + ")" );
          break;
        case Action.ColumnRename:
          names.RemoveAt( ix );
          names.Insert( ix, aa.NewName );
          Sql( "UPDATE sys.Column SET Name = " + Util.Quote(aa.NewName) + " WHERE Table=" + t.TableId + " AND Name = " + Util.Quote(aa.Name) );
          break;
        case Action.Modify:
          if ( DTI.Base( aa.Type ) != DTI.Base( types[ ix ] ) )
            e.Error( "Modify cannot change base type" );
          if ( DTI.Scale( aa.Type ) != DTI.Scale( types[ ix ] ) )
            e.Error( "Modify cannot change scale" );
          types.RemoveAt( ix );
          types.Insert( ix, aa.Type );
          Sql( "UPDATE sys.Column SET Type = " + (int)aa.Type + " WHERE Table=" + t.TableId + " AND Name = " + Util.Quote(aa.Name) );
          Sql( "EXEC sys.ModifiedColumn(" + t.TableId + "," + ix + ")" );
          break;
      }
    }
    var newcols = ColInfo.New( names, types );
    t.AlterData( newcols, map.ToArray() );
    Sql( "EXEC sys.RecreateModifiedIndexes()" );
    t.OpenIndexes();
    ResetCache();
  }

  // Indexes.

  public void CreateIndex( string schemaName, string tableName, string indexName, string [] names, Exec e )
  {
    Table t = (Table)GetTable( schemaName, tableName, e );
    long tid = t.TableId;

    long indexId = GetIndexId( tid, indexName );
    if ( indexId != 0 ) e.Error( "Index already exists" );

    int [] colIx = new int[ names.Length ];
    for ( int i=0; i < names.Length; i +=1 )
    {
      colIx[ i ] = t.ColumnIx( names[i], e );
    }    

    // Create the index.
    {
      var r = new RowCursor( SysIndex );
      r.V[1].L = tid;
      r.V[2].O = indexName;
      indexId = r.Insert();
    }
    // Create the index columns.
    {
      var r = new RowCursor( SysIndexCol );
      r.V[1].L = tid;
      r.V[2].L = indexId;
      for ( int i = 0; i < names.Length; i += 1 )
      {
        r.V[3].L = colIx[ i ];
        r.Insert();
      }
    }

    if ( Normal ) 
    {
      t.OpenIndexes();
      t.InitIndex( indexId );
    }
  }

  public void DropIndex( string schemaName, string tableName, string indexName, Exec e )
  {
    Table t = (Table)GetTable( schemaName, tableName, e );
    long tid = t.TableId;
    long indexId = GetIndexId( tid, indexName );
    if ( indexId == 0 ) e.Error( "Index not found" );
    t.CloseAndDeleteIndex( indexId );
    Sql( "DELETE FROM sys.IndexCol WHERE Table = " + tid + " AND Index=" + indexId );
    Sql( "DELETE FROM sys.Index WHERE id=" + indexId);
    t.OpenIndexes();  
    ResetCache();  
  }

  public IndexInfo [] ReadIndexes( long tableId )
  {
    var ix = new G.List<IndexInfo>();
    var r = new RowCursor( SysIndexCol );
    long CurIx = 0;
    int IxNum = 0;

    // Use SysIndexColIndex to avoid scanning the entire SysIndexCol table.
    var start = new LongStart( tableId );
    foreach( IndexFileRecord ixr in SysIndexColIndex.From( start.Compare, false ) )
    {
      if ( ixr.Col[0].L == tableId )
      {
        r.Get( ixr.Col[1].L );
        var ii = new IndexInfo();
        ii.IndexId = r.V[2].L;
        if ( ii.IndexId != CurIx ) { IxNum = 0; CurIx = ii.IndexId; }
        ii.IxNum = IxNum++;
        ii.ColIx = (int)r.V[3].L; 
        ix.Add( ii );
      } else break;
    }
    return ix.ToArray();
  }

  // Stored procedures and functions.

  public void CreateRoutine( string schemaName, string name, string definition, bool func, bool alter, Exec e )
  {
    int schemaId = GetSchemaId( schemaName, true, e );
    name = Util.Quote( name );
    string tname = func ? "Function" : "Procedure";

    string exists = (string) ScalarSql( "SELECT Name from sys." + tname
      + " where Name=" + name + " AND Schema=" + schemaId )._O;

    if ( exists != null && !alter ) e.Error( tname + " " + name + " already exists" );
    else if ( exists == null && alter ) e.Error( tname + " " + name + " does not exist" );

    if ( alter )
    {
      Sql( "UPDATE sys." + tname + " SET Definition = " + Util.Quote(definition.Trim()) 
        + " WHERE Name = " + name + " AND Schema = " + schemaId );
    }
    else 
    {
      Sql( "INSERT INTO sys." + tname + "( Schema, Name, Definition ) VALUES ( " 
        + schemaId + "," + name + "," + Util.Quote(definition.Trim()) + ")" );
    }
    ResetCache();
  }

  public void DropRoutine( string schemaName, string name, bool func, Exec e )
  {
    int schemaId = GetSchemaId( schemaName, true, e );
    var qname = Util.Quote( name );
    string tname = func ? "Function" : "Procedure";

    string exists = (string) ScalarSql( "SELECT Name from sys." + tname
      + " where Name=" + qname + " AND Schema=" + schemaId )._O;
    if ( exists == null ) e.Error( name + " does not exist" );

    Sql( "DELETE FROM sys." + tname + " WHERE Name = " + qname + " AND Schema = " + schemaId );
    ResetCache();
  }

  public Block GetRoutine( string schemaName, string name, bool func, Exec e )
  {
    Schema schema = GetSchema( schemaName, true, e );
    string cname = name + ( func ? "F" : "P" );

    // See if routine is cached.
    Block result; if( schema.BlockDict.TryGetValue( cname, out result ) ) return result;

    string sql = (string) ScalarSql( "SELECT Definition from sys." + (func?"Function":"Procedure") 
      + " where Name=" + Util.Quote(name) + " AND Schema=" + schema.Id )._O;

    if ( sql == null ) e.Error( "Function not found " + name );

    result = SqlExec.LoadRoutine( func, sql, this, schemaName + "." + name );

    // System.Console.WriteLine( "Loaded " + schemaName + "." + name );

    schema.BlockDict[ cname ] = result;
    return result;
  }

  // RENAME

  public void RenameSchema( string sch, string sch1, Exec e )
  {
    Schema schema = GetSchema( sch, true, e );
    if ( GetSchemaId( sch1, false, e ) >= 0 ) e.Error( "Schema already exists : " + sch1 );
    Sql( "UPDATE FROM sys.Schema SET Name=" + sch1 + " WHERE Id = " + schema.Id );    
    ResetCache();    
  }

  public void RenameObject( string objtype, string sch, string name, string newsch, string newname, Exec e )
  {
    Table t = objtype == "TABLE" ? GetTable( sch, name, e ) : null;

    string error = (string)ScalarSql( "SELECT sys.RenameObject( " + Util.Quote(objtype) + "," + 
      Util.Quote(sch) + "," + Util.Quote(name) + "," + Util.Quote(newsch) + "," + Util.Quote(newname) + ")" )._O;

    if ( error != "" ) e.Error( error );
    else if ( t != null )
    {
      Schema s = GetSchema( sch, true, e );
      Schema ns = GetSchema( newsch, true, e );
      s.TableDict.Remove( name );
      ns.TableDict[ newname ] = t;
    }
    else 
    {
      ResetCache();
    }
  }

  // String and Binary handling. These are represented as a long, which is an offset into a file.
 
  public long EncodeString( string s ) 
  { 
    if ( s == "" ) return 0;
    // See if it is in SysStringIndex
    try
    {
      var start = new StringStart( s );
      foreach( IndexFileRecord ixr in SysStringIndex.From( start.Compare, false ) )
      {
        if ( (string)(ixr.Col[0]._O) == s ) return ixr.Col[0].L;
        break;
      }
    }
    catch ( System.Exception x )
    {
      SysStringIndex.Dump();
      throw x;
    }

    // Save to file.
    long sid = SysString.Length;
    SysString.Position = sid;
    SysStringWriter.Write( s );

    // Insert into the index
    IndexFileRecord r = new IndexFileRecord( 1 );
    r.Col[ 0 ].L = sid + 1;
    r.Col[ 0 ]._O = s;
    SysStringIndex.Insert( ref r );

    return sid + 1; // + 1 because zero indicates the encoding has not yet been done.
  }

  public string DecodeString( long sid ) 
  { 
    if ( sid <= 0 ) return "";
    SysString.Position = sid-1;
    string result = SysStringReader.ReadString();
    return result;
  }

  public long EncodeBinary( byte [] data ) 
  { 
    if ( data.Length == 0 ) return 0;
    // See if it is in SysBinaryIndex
    var start = new BinaryStart( data );
    foreach( IndexFileRecord ixr in SysBinaryIndex.From( start.Compare, false ) )
    {
      if ( Util.Compare( (byte[])(ixr.Col[0]._O), data ) == 0 ) return ixr.Col[0].L;
      break;
    }

    // Save to file.
    long sid = SysBinary.Length;
    SysBinary.Position = sid;
    SysBinaryWriter.WriteBytes( data );

    // Insert into the index
    IndexFileRecord r = new IndexFileRecord( 1 );
    r.Col[ 0 ].L = sid + 1;
    r.Col[ 0 ]._O = data;
    SysBinaryIndex.Insert( ref r );
    return sid + 1; // + 1 because zero means the encoding has not yet been done.
  }

  public byte[] DecodeBinary( long sid ) 
  { 
    if ( sid <= 0 ) return DTI.ZeroByte;
    SysBinary.Position = sid - 1;
    return SysBinaryReader.ReadBytes();
  }

  /* Private methods */

  void  RollbackOrCommit()
  {
    if ( Rollback )
    {
      Log.Reset();
      foreach( G.KeyValuePair<string,Schema> p in SchemaDict )
        foreach( G.KeyValuePair<string,TableExpression> q in p.Value.TableDict )
          q.Value.Rollback();
      Rollback = false;
    }
    else 
    {
      // Save indexes to underlying buffered streams ( can add more Log entries ). 
      SysStringIndex.PrepareToCommit();
      SysBinaryIndex.PrepareToCommit();
      foreach( G.KeyValuePair<string,Schema> p in SchemaDict )
        foreach( G.KeyValuePair<string,TableExpression> q in p.Value.TableDict )
          q.Value.PrepareToCommit();  
    
      if ( Log.Commit() )
      {
        SysString.Flush(); SysStringIndex.Commit();
        SysBinary.Flush(); SysBinaryIndex.Commit();

        foreach( G.KeyValuePair<string,Schema> p in SchemaDict )
          foreach( G.KeyValuePair<string,TableExpression> q in p.Value.TableDict )
            q.Value.Commit();

        foreach ( long fileId in DeletedFiles )
        {
          var f = new IO.FileInfo( Name + fileId );
          f.Delete();
        }
        Log.Reset();
      }
    }
    DeletedFiles.Clear();
  }

  void ResetCache()
  {
    // Removes all schema cached objects other than base tables ( functions, procedures, views ).
    foreach( G.KeyValuePair<string,Schema> p in SchemaDict )
    {
      Schema s = p.Value;
      s.BlockDict.Clear();

      var vlist = new G.List<string>();
      foreach( G.KeyValuePair<string,TableExpression> q in s.TableDict )
      {
        TableExpression te = q.Value;
        if ( ! ( te is Table ) )
          vlist.Add( q.Key );
      }
      foreach ( string v in vlist )
        s.TableDict.Remove( v );
    }
  }

  void SaveColumns( long tableId, ColInfo cols )
  {
    var r = new RowCursor( SysColumn );
    r.V[1].L = tableId;
    for ( int i = 0; i < cols.Count; i += 1 )
    {
      r.V[2].O = cols.Names[i];
      r.V[3].L = (long)cols.Types[i];
      r.Insert();
    }
  }

  int GetIx( ColInfo info, string name )
  {
    for ( int i = 0; i < info.Count; i += 1 )
      if ( info.Names[i] == name ) return i;
    return -1;
  }

  Schema GetSchema( string name, bool mustExist, Exec e )
  {
    Schema result;
    SchemaDict.TryGetValue( name, out result );
    if ( result != null ) return result;

    int id = ReadSchemaId( name );
    if ( id < 0 ) 
    {
      if ( mustExist) e.Error( "Schema not found: " + name );
      return null;
    }
    result = new Schema( name, id );
    SchemaDict[ name ] = result;
    return result;
  }

  int GetSchemaId( string schemaName, bool mustExist, Exec e )
  {
    Schema s = GetSchema( schemaName, mustExist, e );
    if ( s == null ) return -1;
    return s.Id;
  }

  int ReadSchemaId( string schemaName )
  {
    if ( schemaName == "sys" ) return 1;
    var start = new StringStart( schemaName );
    foreach( IndexFileRecord ixr in SysSchemaByName.From( start.Compare, false ) )
    {
      if ( (string)(ixr.Col[0]._O) == schemaName ) return (int)ixr.Col[1].L;
      break;
    }
    return -1;
  }

  long ReadTableId( Schema schema, string name, out bool isView, out string definition )
  {
    var r = new RowCursor( SysTable );
    var start = new StringStart( name );
    foreach( IndexFileRecord ixr in SysTableByName.From( start.Compare, false ) )
    {
      if ( (string)(ixr.Col[0]._O) == name )
      {
        long id = ixr.Col[1].L;
        r.Get( id );
        if ( r.V[1].L == schema.Id ) 
        {
          isView = r.V[3].L != 0;
          definition = (string)r.V[4]._O;
          return id;
        }
      }
    }
    isView = false; definition = null;
    return -1;
  }

  long GetIndexId( long tid, string indexName )
  {
    return ScalarSql( "SELECT Id FROM sys.Index WHERE Table = " + tid + " AND Name=" + Util.Quote(indexName) ).L;
  }

  void Sql( string sql ) 
  { 
    ExecuteSql(sql,null); 
  }

  Table NewSysTable( int tableId, string name, ColInfo ci )
  {
    return new Table( this, Sys, name, ci, tableId );
  }

} // end class DatabaseImp

class Schema
{
  public string Name;
  public int Id;
  public Schema( string name, int id ) { Name = name; Id = id; }
  
  public G.Dictionary<string,TableExpression> TableDict = new G.Dictionary<string,TableExpression>();
  public G.Dictionary<string,Block> BlockDict = new G.Dictionary<string,Block>();

  public TableExpression GetCachedTable( string name )
  {
    TableExpression result;
    TableDict.TryGetValue( name, out result );
    return result;
  }
}

struct ColBuilder // Helper for creating system column definitions.
{
  public G.List<string> Names;
  public G.List<DataType> Types;
  public void Add( string name, DataType type )
  {
    if ( Names == null ) 
    {
      Names = new G.List<string>();
      Types = new G.List<DataType>();
      Names.Add( "Id" );
      Types.Add( DataType.Bigint );
    }
    Names.Add( name );
    Types.Add( type );
  }
  public ColInfo Get()
  {
    ColInfo result = ColInfo.New( Names, Types );
    Names = null;   
    return result;
  }
}

} // end namespace DBNS