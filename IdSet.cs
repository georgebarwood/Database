namespace DBNS {
using G = System.Collections.Generic;
using SQLNS;

/*

IdSet is for optimising WHERE clauses for SELECT,UPDATE,DELETE.

When accessing a large table, we only want to access a small subset of the records.

The id values may be explicit as in 

SELECT .... FROM dbo.Cust WHERE id in ( 3,4,5 )

or they may come from an index as in

SELECT .... FROM dbo.Cust WHERE Name = 'Smith'

where an index has been defined CREATE INDEX CustByName ON dbo.Cust(Name)

*/

abstract class IdSet 
{
  public abstract G.IEnumerable<long>All( EvalEnv ee );
}

class ExpListIdSet : IdSet
{
  Exp.DL [] List;
  public ExpListIdSet( Exp[] list ) 
  { 
    List = new Exp.DL[ list.Length ];
    for ( int i = 0; i < list.Length; i += 1 )
      List[i] = list[ i ].GetDL(); 
  }

  public override G.IEnumerable<long>All( EvalEnv ee )
  { 
    for ( int i = 0; i < List.Length; i += 1 )
    {
      yield return List[ i ]( ee );
    }
  }  
}

class TableExpressionIdSet : IdSet
{
  TableExpression TE;

  public TableExpressionIdSet( TableExpression te ) 
  {
    TE = te;
  }

  public override G.IEnumerable<long>All( EvalEnv ee )
  { 
    Value [] row = new Value[1];
    foreach ( bool b in TE.GetAll( row, new int[]{ 0 }, ee ) )
      yield return row[0].L;
  }
} // end class TableExpressionIdSet
  

// IdCopy takes a copy of a set of id values, also removes any duplicates.
class IdCopy : IdSet
{
  IdSet X;

  public IdCopy( IdSet x )
  {
    X = x;
  }
  public override G.IEnumerable<long>All( EvalEnv ee )
  { 
    G.SortedSet<long> copy = new G.SortedSet<long>();
    foreach ( long id in X.All( ee ) ) copy.Add( id );
    foreach ( long id in copy ) yield return id;
  }
}

class SingleId : IdSet
{
  Exp.DL X;
  public SingleId( Exp x ){ X = x.GetDL(); }
  public override G.IEnumerable<long>All( EvalEnv ee )
  { 
    yield return X( ee );
  }
}

class UpTo : IdSet
{
  long N;
  public UpTo( long n ){ N = n; }
  public override G.IEnumerable<long>All( EvalEnv ee ){ for ( int i=1; i<=N; i+=1 ) yield return i; }
}

// Uses an index to look up a set of id values, optimises select ... from t where indexedcol in ( .... )
class Lookup : IdSet 
{
  Exp E;
  IndexFile Ix;

  public Lookup( IndexFile ix, Exp e )
  {
    E = e;
    Ix = ix;
  }

  public override G.IEnumerable<long>All( EvalEnv ee )
  { 
    var values = E.Values( ee );

    foreach ( Value v in values )
    {
      DataType t = Ix.Inf.Types[0];
      int idCol = Ix.Inf.KeyCount-1;
      var start = new ValueStart( v, t );
      foreach ( IndexFileRecord r in Ix.From( start.Compare, false ) )
      {
        if ( Util.Compare( v, r.Col[0], t ) == 0 )
        {
          yield return r.Col[ idCol ].L;
        }
        else break;
      }
    }
  } 
} // end class Lookup

class IndexFrom : IdSet
{
  IndexFile F;
  Exp.DV K;
  DataType Type;
  Token Op;
  Value V;

  public IndexFrom( IndexFile f, Exp k, Token op )
  {
    F = f; 
    K = k.GetDV(); 
    Type = k.Type;
    Op = op;
  }

  public override G.IEnumerable<long>All( EvalEnv ee )
  { 
    V = K( ee );
    foreach ( IndexFileRecord r in F.From( Compare, Op <= Token.LessEqual  ) )
    {
      if ( Op == Token.Equal && !Util.Equals( r.Col[0], V, Type ) ) yield break;
      yield return r.Col[ F.Inf.KeyCount-1 ].L;
    }
  }

  int Compare( ref IndexFileRecord r ) // This make not be quite optimal for Less and Greater.
  {
    int cf = Util.Compare( V, r.Col[0], Type );
    if ( cf == 0 )
    {
      if ( Op <= Token.LessEqual ) // Op == Token.Less || Op == Token.LessEqual
        cf = +1; // Should be -1 if Op == Less ?
      else // ( Op == Token.Greater || Op == Token.GreaterEqual || Op == Token.Equal )
        cf = -1;
    }
    return cf;    
  }  
} // end class IndexFrom

} // end namespace DBNS
