namespace SQLNS {

using G = System.Collections.Generic;
using DBNS;

// Implementation of ORDER BY. The rows are stored in the Sorter by calls to NewRow.
// When all the rows have been stored, either EndTable is called to output the sorted rows,
// or GetStoredRows is called to fetch the sorted rows.

struct SortSpec
{
  public int ColIx;
  public DataType Type;
  public bool Desc;
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
    foreach ( Value[] r in Rows ) if ( !Output.NewRow( r ) ) break;
    Output.EndTable();
  }

  public override G.IEnumerable<bool> GetStoredRows( Value[] outrow )
  {
    foreach ( Value[] r in Rows )
    {
      for ( int i = 0; i < outrow.Length; i += 1 ) outrow[ i ] = r[ i ];
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
    return 1; // Not zero, that will lead to loss of rows!
  }
} // end class Sorter

} // end namespace SQLNS
