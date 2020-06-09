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

class Grouper : ResultSet, G.IEqualityComparer<Value[]>
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
    Value [] v;
    bool first = !Rows.TryGetValue( row, out v );
    if ( first )
    {
      v = (Value[])row.Clone();
      Rows.Add( v );
    }

    // Now do the Aggregate calculation.
    for ( int i = 0; i < Agg.Length; i += 1 )
    {
      int cix = Agg[i].ColIx;
      switch( Agg[i].Op )
      {
        case AggOp.Count: v[ cix ].L += 1; break;

        case AggOp.Sum: 
          if ( !first ) switch ( Agg[i].Type )
          {
            case DataType.Double: v[ cix ].D += row[ cix ].D; break;
            default: v[ cix].L += row[ cix ].L; break;
          }
          break;  
        case AggOp.Min: 
          if ( !first ) if ( Util.Compare( row[ cix ], v[ cix ], Agg[i].Type ) < 0 ) v[ cix ] = row[ cix ];
          break;    
        case AggOp.Max: 
          if ( !first ) if ( Util.Compare( row[ cix ], v[ cix ], Agg[i].Type ) > 0 ) v[ cix ] = row[ cix ];
          break;     
      }
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

  public int GetHashCode( Value[] a )
  {
    int hash = 0;
    foreach ( GroupSpec s in Group )
    {
      int ix = s.ColIx;
      hash += Util.GetHashCode( a[ix], s.Type );
    }
    return hash;
  }

  public bool Equals( Value[] a, Value[] b )
  {
    foreach ( GroupSpec s in Group )
    {
      int ix = s.ColIx;
      if ( Util.Compare( a[ix], b[ix], s.Type ) != 0 ) return false;
    }
    return true;
  }
} // end class Grouper

} // end namespace SQLNS