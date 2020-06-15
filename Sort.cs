namespace SQLNS {

using G = System.Collections.Generic;
using DBNS;

/* Implementation of ORDER BY */

struct SortSpec
{
  public int ColIx;
  public DataType Type;
  public bool Desc;
}

abstract class StoredResultSet : ResultSet
{
  public abstract G.IEnumerable<bool> GetAll( Value[] outrow );
}

class Sorter : StoredResultSet, G.IComparer<Value[]>
{
  ResultSet Output;
  SortSpec [] Spec;
  G.SortedSet<Value[]> Rows;

  public Sorter( ResultSet output, SortSpec[] s )
  {
    Output = output;
    Spec = s;
    Rows  = new G.SortedSet<Value[]>( this );
  }

  public override bool NewRow( Value [] r )
  {
    Rows.Add( (Value[])r.Clone() );
    return true;
  }

  public override void EndTable()
  {    
    // Output the sorted rows.
    foreach ( Value[] r in Rows ) 
      if ( !Output.NewRow( r ) ) break;
    Output.EndTable();
  }

  public override G.IEnumerable<bool> GetAll( Value[] outrow )
  {
    foreach ( Value[] r in Rows )
    {
      for ( int i = 0; i < outrow.Length; i += 1 )
        outrow[ i ] = r[ i ];
      yield return true;
    }
  }

  public int Compare( Value[] a, Value[] b )
  {
    foreach ( SortSpec s in Spec )
    {
      int ix = s.ColIx;
      int cf = Util.Compare( a[ix], b[ix], s.Type );
      if ( cf != 0 ) 
      {
        if ( s.Desc ) cf = - cf;
        return cf;
      }
    }
    return 1;
  }
} // end class Sorter

} // end namespace SQLNS
