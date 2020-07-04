namespace SQLNS {

using G = System.Collections.Generic;
using DBNS;

/* Implementation of GROUP BY, SUM, COUNT etc. */

struct GroupSpec
{
  public int ColIx;
  public DataType Type;
}

enum AggOp { None, Count, Sum, Avg, Min, Max };

struct AggSpec
{
  public int ColIx;
  public DataType Type;
  public AggOp Op;
}

class Grouper : StoredResultSet, G.IEqualityComparer<Value[]>
{
  ResultSet Output;
  GroupSpec [] Group;
  AggSpec [] Agg;
  G.HashSet<Value[]> Rows;

  public Grouper( ResultSet output, GroupSpec[] group, AggSpec[] agg )
  {
    Output = output;
    Group = group;
    Agg = agg;
    Rows  = new G.HashSet<Value[]>( this );
  }

  public override bool NewRow( Value [] row )
  {
    Value [] a; // The accumulator row, initialised from the first row with a given set of group values.

    if ( Rows.TryGetValue( row, out a ) )
    for ( int i = 0; i < Agg.Length; i += 1 ) // Do the Aggregate calculation.
    {
      int cix = Agg[ i ].ColIx;
      switch( Agg[ i ].Op )
      {
        case AggOp.Count: 
          a[ cix ].L += 1; 
          break;
        case AggOp.Sum: 
          switch ( Agg[ i ].Type )
          {
            case DataType.Double: a[ cix ].D += row[ cix ].D; break;
            default: a[ cix ].L += row[ cix ].L; break;
          }
          break;  
        case AggOp.Min: 
          if ( Util.Compare( row[ cix ], a[ cix ], Agg[ i ].Type ) < 0 ) a[ cix ] = row[ cix ];
          break;    
        case AggOp.Max: 
          if ( Util.Compare( row[ cix ], a[ cix ], Agg[ i ].Type ) > 0 ) a[ cix ] = row[ cix ];
          break;     
      }
    }
    else
    {
      Rows.Add( (Value[])row.Clone() );
    }
    return true;
  }

  public override void EndTable()
  {    
    // Output the summed rows.
    foreach ( Value[] r in Rows ) 
      if ( !Output.NewRow( r ) ) break;
    Output.EndTable();
  }

  public override G.IEnumerable<bool> GetStoredRows( Value[] outrow )
  {
    foreach ( Value[] r in Rows )
    {
      for ( int i = 0; i < outrow.Length; i += 1 )
        outrow[ i ] = r[ i ];
      yield return true;
    }
  }

  public int GetHashCode( Value[] a )
  {
    int hash = 0;
    foreach ( GroupSpec s in Group )
    {
      int ix = s.ColIx;
      hash += s.Type > DataType.String ? (int)a[ix].L : Util.GetHashCode( a[ix], s.Type );
    }
    return hash;
  }

  public bool Equals( Value[] a, Value[] b )
  {
    foreach ( GroupSpec s in Group )
    {
      int ix = s.ColIx;
      if ( !Util.Equal( a[ix], b[ix], s.Type ) ) return false;
    }
    return true;
  }
} // end class Grouper

} // end namespace SQLNS