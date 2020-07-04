namespace SQLNS {

using G = System.Collections.Generic;
using DBNS;

/* TableExpression is complex as tables arise in different ways:

VALUES table.
Base table ( which may not initially be actually defined and present in the database when parsing a view or function  or procedure definition ).
View ( similar to Base table ).
SELECT/SET/FOR expressions ... FROM.

They are also used in different ways:

SELECT to client : SELECT Address FROM dbo.Cust
INSERT source : INSERT INTO dbo.Cust ( Address ) SELECT Address FROM dbo.AddressList
SET source : DECLARE a string SET a = Address FROM dbo.Cust where Id = k
FOR source : DECLARE a string FOR a = Address FROM dbo.Cust
Scalar sub-expression : DECLARE a string SET a = ( SELECT Address FROM dbo.Cust WHERE Id = k )
Rhs of IN expression : SELECT Address FROM dbo.Cust WHERE Id IN ( SELECT id FROM dbo.Cust WHERE Postcode = 'GL1' )

*/

abstract class TableExpression
{
  public int ColumnCount; // Number of columns in table
  public ColInfo CI; // Names and types of the columns
  public long TableId;
  public string Alias;

  public virtual DataType Type( int i ){ return CI.Type[ i ]; } // Data type of the ith column.

  public virtual TableExpression Load( SqlExec e ) { return this; } // Loads table or view definition from database.

  public virtual void CheckNames( Exec  e ){} // Checks the column names are distinct and not blank.

  public virtual void Convert( DataType [] types, Exec e ){} // Converts the expressions ready to be assigned

  public virtual void FetchTo( ResultSet rs, EvalEnv ee ){} // Fetchs the table to the specified ResultSet

  public virtual G.IEnumerable<bool> GetAll( Value[] row, int [] used, EvalEnv ee ){ yield return false; } // Iterates through the table.

  // Index optimisation.
  public virtual IndexFile FindIndex( int colIx ){ return null; }
  public virtual bool Get( long id, Value[] row, int [] used ){ return false; } // Only called if FindIndex is implemented.

  // Atomic update.  
  public virtual void Commit( CommitStage c ) { }

}

abstract class StoredResultSet : ResultSet
{
  public abstract G.IEnumerable<bool> GetStoredRows( Value[] outrow );
}

class Select : TableExpression
{
  G.List<Exp> Exps;
  Exp.DV[] Dvs;
  TableExpression TE;
  Exp Where;
  Exp.DB WhereD;
  IdSet Ids;
  OrderByExp[] Order;
  int [] Used;
  SortSpec [] SortSpec;
  GroupSpec [] GroupSpec;
  AggSpec [] AggSpec;

  public Select( G.List<Exp> exps, TableExpression te, Exp where, Exp[] group, OrderByExp[] order, bool [] used, SqlExec x )
  {
    /* There is more work to be done here, for example 2 * SUM(Total) is currently now allowed.
       Also if there is a GROUP BY, SELECT expressions cannot access fields not in the group list,
       unless thereis an enclosing aggregate function.
       Also maybe common sub-expression analysis, and perhaps constant folding, could be done?
    */ 

    Exps = exps; TE = te; Where = where; Order = order; 

    ColumnCount = exps.Count; 
    var names = new string[ ColumnCount ];
    var types = new DataType[ ColumnCount ];
    for ( int i = 0; i < ColumnCount; i += 1 )
    {
      names[ i ] = exps[ i ].Name;
      types[ i ] = exps[ i ].Type;
    }
    CI = new ColInfo( names, types );

    if ( x.ParseOnly ) return;

    Used = Util.ToList( used );

    if ( group != null )
    {
      // Compute AggSpec and GroupSpec
      var alist = new G.List<AggSpec>();
      for ( int i = 0; i < exps.Count; i += 1 )
      {
        Exp e = exps[ i ];
        AggOp op = e.GetAggOp();
        if ( op != AggOp.None )
        {
          AggSpec a = new AggSpec();
          a.ColIx = i;
          a.Type = e.Type;
          a.Op = op;
          alist.Add( a );
        }
      }
      AggSpec = alist.ToArray();

      var glist = new G.List<GroupSpec>();
      for ( int i=0; i < group.Length; i += 1 )
      {
        GroupSpec g = new GroupSpec();
        g.ColIx = Exps.Count;
        g.Type = group[ i ].Type;
        Exps.Add( group[ i ] );
        glist.Add( g );
      }
      GroupSpec = glist.ToArray();
    }

    if ( Order != null )
    {
      var sortSpec = new SortSpec[ Order.Length ]; 

      for ( int i = 0; i < Order.Length; i += 1 )
      {
        // Quite complicated as ORDER BY can use aliases or expressions.
        Exp e = Order[ i ].E;
        sortSpec[ i ].Desc = Order[ i ].Desc;

        int cix = -1;
        if ( e is ExpName )
        {
          string alias = ((ExpName)e).ColName;   
          for ( int j = 0; j < CI.Count; j += 1 )
          {
            if ( CI.Name[j] == alias )
            {
              e = Exps[ j ];
              cix = j;
              break;
            }
          }
        }
        if ( cix < 0 )
        {
          cix = Exps.Count;
          Exps.Add( e );
          e.Bind( x );   
        }     
        sortSpec[ i ].Type = e.Type;
        sortSpec[ i ].ColIx = cix;       
      }
      SortSpec = sortSpec;
    }

    Dvs = Util.GetDVList( Exps.ToArray() );

    if ( Where != null ) WhereD = Where.GetDB();

    Ids = Where == null ? null : Where.GetIdSet( TE );
    if ( Ids != null ) Ids = new IdCopy( Ids ); // Need to take a copy of the id values if an index is used.
  }

  public override void CheckNames( Exec  e )
  {
    var set = new G.HashSet<string>();
    for ( int i = 0; i < ColumnCount; i += 1 )
    {
      string name = Exps[ i ].Name;
      if ( name == "" ) e.Error( "Unnamed expression" );
      else if ( set.Contains(name) ) e.Error( "Duplicate name: " + name );
      set.Add( name );
    }
  }

  public override void Convert( DataType [] types, Exec e )
  {
    for ( int i = 0; i < types.Length; i += 1 )
    {
      DataType assignType = types[ i ];
      Exp conv = Exps[ i ].Convert( assignType );
      if ( conv == null ) e.Error( "Assign data type error" );
      Exps[ i ] = conv;
    }
    Dvs = Util.GetDVList( Exps.ToArray() ); // Not very elegant that this operation is done twice.
  }

  public override G.IEnumerable<bool> GetAll( Value[] final, int [] cols, EvalEnv e )
  {
    Value[] tr = new Value[ TE.CI.Count ];
    EvalEnv ee = new EvalEnv( e.Locals, tr, e.ResultSet );

    StoredResultSet srs = Order == null ? null : new Sorter( null, SortSpec );
    srs = GroupSpec == null ? srs : new Grouper( srs, GroupSpec, AggSpec );

    Value [] outrow = srs != null ? new Value[ Exps.Count ] : null;

    if ( srs == null )
    {
      if ( Ids != null ) 
      {
        foreach ( long id in Ids.All( ee ) ) if ( TE.Get( id, tr, Used ) )
        {
          if ( WhereD == null || WhereD( ee ) )
          {
            for ( int i = 0; i < final.Length; i += 1 ) final[ i ] = Dvs[ i ]( ee ); 
            yield return true;
          }
        }
      }
      else 
      {
        foreach ( bool ok in TE.GetAll( tr, Used, ee ) )
        if ( WhereD == null || WhereD( ee ) )
        {
          for ( int i = 0; i < final.Length; i += 1 ) final[ i ] = Dvs[ i ]( ee ); 
          yield return true;
        }
      }
    }
    else
    {
      if ( Ids != null ) 
      {
        foreach ( long id in Ids.All( ee ) ) if ( TE.Get( id, tr, Used ) )
        {
          if ( WhereD == null || WhereD( ee ) )
          {
            for ( int i = 0; i < Exps.Count; i += 1 ) outrow[ i ] = Dvs[ i ]( ee ); 
            srs.NewRow( outrow ); 
          }
        }
      }
      else 
      {
        foreach ( bool ok in TE.GetAll( tr, Used, ee ) )
        if ( WhereD == null || WhereD( ee ) )
        {
          for ( int i = 0; i < Exps.Count; i += 1 ) outrow[ i ] = Dvs[ i ]( ee );  
          srs.NewRow( outrow ); 
        }
      }

      foreach( bool b in srs.GetStoredRows( final ) ) yield return true;
    }
  }

  public override void FetchTo( ResultSet rs, EvalEnv e )
  {
    ResultSet srs = Order == null ? rs : new Sorter( rs, SortSpec );
    srs = GroupSpec == null ? srs : new Grouper( srs, GroupSpec, AggSpec );

    Value[] tr = new Value[ TE.CI.Count ];

    EvalEnv ee = new EvalEnv( e.Locals, tr, e.ResultSet );

    rs.NewTable( CI );

    Value [] outrow = new Value[ Exps.Count ]; 

    if ( Ids != null ) 
    // Fetch subset of source table using id values, send to ResultSet (if it satisfies any WHERE clause )
    {
      foreach ( long id in Ids.All( ee ) ) if ( TE.Get( id, tr, Used ) )
      {
        if ( Where == null || WhereD( ee ) )
        {
          for ( int i = 0; i < Exps.Count; i += 1 ) outrow[ i ] = Dvs[ i ]( ee );   
          if ( !srs.NewRow( outrow ) ) break;
        }
      }
    }
    else 
    // Fetch every record in source table, send it to output ( if it satisfies any WHERE clause )
    {
      foreach ( bool ok in TE.GetAll( tr, Used, ee ) )
      if ( Where == null || WhereD( ee ) )
      {
        for ( int i = 0; i < Exps.Count; i += 1 ) outrow[ i ] = Dvs[ i ]( ee );  
        if ( !srs.NewRow( outrow ) ) break;
      }
    }
    srs.EndTable();
  }
} // end class SelectTableExpression

// ********************************************************************

class ValueTable : TableExpression
{
  G.List<Exp[]> Values; 

  public ValueTable( int n, G.List<Exp[]> values )
  {
    ColumnCount = n;
    Values = values;
  }

  public override DataType Type( int i )
  {
    return Values[0][ i ].Type;
  }

  bool Convert( int i, DataType t )
  {
    if ( Type( i ) == t ) return true;
    for ( int j = 0; j < Values.Count; j += 1 )
    {
      Exp conv = Values[ j ][ i ].Convert( t );
      if ( conv == null ) return false;
      Values[ j ][ i ] = conv;
    }
    return true;
  }

  public override void Convert( DataType [] types, Exec e )
  {
    for ( int i = 0; i < types.Length; i += 1 )
    {
      if ( !Convert( i, types[ i ] ) ) 
      {
        e.Error( "Data type conversion error to " + DTI.Name( types[ i ] ) );
      }
    }
  }

  public override void FetchTo( ResultSet rs, EvalEnv ee )
  {
    Value [] row = new Value[ ColumnCount ];
    for ( int j = 0; j < Values.Count; j += 1 )
    {
      var e = Values[ j ];
      for ( int i=0; i < ColumnCount; i += 1 )
      {
        row[ i ] = e[ i ].GetDV()( ee );
      }
      if ( !rs.NewRow( row ) ) break;
    }
  }
} // end class ValueTable

// ********************************************************************

class ViewOrTable : TableExpression
{
  public string Schema, Name;
  public ViewOrTable( string schema, string name )
  {
    Schema = schema;
    Name = name;
  }

  public override TableExpression Load( SqlExec e )
  {
    return e.Db.GetTableOrView( Schema, Name, e );    
  }
} // end class ViewOrTable

// ********************************************************************

class DummyFrom : TableExpression // Used where there is a SELECT with no FROM clause.
{
  public DummyFrom()
  {
    CI = new ColInfo( new string[0], new DataType[0] );
  }
  public override G.IEnumerable<bool> GetAll( Value[] row, int [] used, EvalEnv ee )
  {
    yield return true;
  }
}

} // end namespace SQLNS