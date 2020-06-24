namespace SQLNS
{

using G = System.Collections.Generic;
using DBNS;

class EvalEnv
{
  public Value [] Locals;
  public Value [] Row;
  public ResultSet ResultSet;
  public EvalEnv( Value [] locals, Value [] row, ResultSet rs ){ Locals = locals; Row = row; ResultSet = rs; }
  public EvalEnv(){ }
}

abstract class Exp
{
  public string Name = "";
  public DataType Type;

  public virtual Exp Bind( SqlExec e ){ return this; }
  public virtual IdSet GetIdSet(  TableExpression te ) { return null; }
  public virtual bool IsConstant() { return false; } // Evaluation doesn't depend on table row.

  // Methods related to implementation of "IN".
  public virtual bool TestIn( Value x, EvalEnv e ){ return false; }
  public virtual DataType GetElementType() { return DataType.None; }

  public virtual IdSet ListIdSet(){ return null; } // Index optimisation.
  public virtual G.IEnumerable<Value> Values( EvalEnv ee ){ yield break; } // ScalarSelect implementation.

  // Methods related to implementation of aggregates.
  public virtual AggOp GetAggOp(){ return AggOp.None; }
  public virtual void BindAgg( SqlExec e ){ }

  // Optimisation of string concat
  public virtual void GetConcat( G.List<Exp> list ){ list.Add(this); }

  public Exp Convert( DataType t )
  {
    if ( t < DataType.Decimal ) t = DTI.Base( t );

    if ( Type == t ) return this;
    else if ( Type == DataType.Bigint ) return 
        t == DataType.String ? (Exp) new IntToStringExp( this )
      : t == DataType.Double ? (Exp) new IntToDoubleExp( this )
      : t >= DataType.Decimal ? (Exp) new IntToDecimalExp( this, t )
      : null;
    else if ( Type == DataType.Double ) return 
       t == DataType.String ? (Exp) new DoubleToStringExp( this )
     : t == DataType.Bigint ? (Exp) new DoubleToIntExp( this )
     : t >= DataType.Decimal ? (Exp) new DoubleToDecimalExp( this, t )
     : null;
    else if ( Type >= DataType.Decimal )
    {
      if ( t == DataType.String ) return new DecimalToStringExp( this );
      else if ( t == DataType.Double ) return new DecimalToDoubleExp( this );
      else if ( t >= DataType.Decimal )
      {
        int amount = DTI.Scale( t ) - DTI.Scale( Type );
        return amount == 0 ? (Exp) this
        : amount > 0 ? (Exp) new ExpScale( this, t, amount )
        : (Exp) new ExpScaleReduce( this, t, -amount );
      }
    }
    else if ( Type == DataType.Binary && t == DataType.String )
      return new BinaryToStringExp( this );
    else if ( Type == DataType.Bool && t == DataType.String )
      return new BoolToStringExp( this );
    else if ( Type == DataType.ScaledInt && t >= DataType.Decimal ) return this;
    return null;
  }

  public delegate Value  DV( EvalEnv e );
  public delegate bool   DB( EvalEnv e );
  public delegate long   DL( EvalEnv e );
  public delegate double DD( EvalEnv e );
  public delegate string DS( EvalEnv e );
  public delegate byte[] DX( EvalEnv e );

  // At least one of GetDV or the relevant GetD? must be implemented ( or an infinite recursion will happen ).
  public virtual DV GetDV()
  { 
    switch( DTI.Base( Type ) )
    {
      case DataType.Bool   : DB db = GetDB(); return ( ee ) => Value.New( db( ee ) );
      case DataType.Double : DD dd = GetDD(); return ( ee ) => Value.New( dd( ee ) );
      case DataType.String : DS ds = GetDS(); return ( ee ) => Value.New( ds( ee ) );
      case DataType.Binary : DX dx = GetDX(); return ( ee ) => Value.New( dx( ee ) );
      default:               DL dl = GetDL(); return ( ee ) => Value.New( dl( ee ) );
    }
  }

  public virtual DB GetDB(){ var dv = GetDV(); return ( ee ) => dv( ee ).B; }
  public virtual DL GetDL(){ var dv = GetDV(); return ( ee ) => dv( ee ).L; }
  public virtual DD GetDD(){ var dv = GetDV(); return ( ee ) => dv( ee ).D; }
  public virtual DS GetDS(){ var dv = GetDV(); return ( ee ) => (string)dv( ee )._O; }
  public virtual DX GetDX(){ var dv = GetDV(); return ( ee ) => (byte[])dv( ee )._O; }

}

abstract class UnaryExp : Exp
{
  protected Exp E;
  public override bool IsConstant()
  {
    return E.IsConstant();
  }
}

class ExpLocalVar : Exp
{
  int I;
  public ExpLocalVar( int i, DataType t ) { I = i; Type = t; }

  public override DV GetDV() { int i = I; return ( EvalEnv ee ) => ee.Locals[ i ]; }
  public override DB GetDB() { int i = I; return ( EvalEnv ee ) => ee.Locals[ i ].B; }
  public override DL GetDL() { int i = I; return ( EvalEnv ee ) => ee.Locals[ i ].L; }
  public override DS GetDS() { int i = I; return ( EvalEnv ee ) => (string)ee.Locals[ i ]._O; }
  public override DX GetDX() { int i = I; return ( EvalEnv ee ) => (byte[])ee.Locals[ i ]._O; }

  public override bool IsConstant() { return true; }
}

class ExpConstant : Exp
{
  public Value Value;
  public ExpConstant( long x, DataType t ){ Value.L = x; Type = t; }
  public ExpConstant( string x ){ Value.O = x; Type = DataType.String; }
  public ExpConstant( byte[] x ){ Value.O = x; Type = DataType.Binary; }
  public ExpConstant( bool x ){ Value.B = x; Type = DataType.Bool; }
  public override bool IsConstant() { return true; } // Evaluation doesn't depend on table row.

  public override DV GetDV() { return ( EvalEnv ee ) => Value; }
  public override DB GetDB() { return ( EvalEnv ee ) => Value.B; }
  public override DL GetDL() { return ( EvalEnv ee ) => Value.L; }
  public override DS GetDS() { return ( EvalEnv ee ) => (string)Value._O; }
  public override DX GetDX() { return ( EvalEnv ee ) => (byte[])Value._O; }
}

class ExpName : Exp
{
  public string ColName;
  public int ColIx;
  public ExpName( string name ){ ColName = name; Name = name; }

  public override Exp Bind( SqlExec e )
  {
    var ci = e.CI;
    if ( ci == null ) e.Error( "Undeclared variable " + ColName );
    for ( int i=0; i < ci.Count; i += 1 ) 
      if ( ci.Name[ i ] == ColName ) 
      { 
        e.Used[ i ] = true;
        ColIx = i; 
        Type = DTI.Base( ci.Type[ i ] );
        return this;
      }

    e.Error( "Column " + ColName + " not found" );
    return this;
  }

  public override DV GetDV() { return ( EvalEnv ee ) => ee.Row[ColIx]; }
  public override DB GetDB() { return ( EvalEnv ee ) => ee.Row[ColIx].B; }
  public override DL GetDL() { return ( EvalEnv ee ) => ee.Row[ColIx].L; }
  public override DS GetDS() { return ( EvalEnv ee ) => (string)ee.Row[ColIx]._O; }
  public override DX GetDX() { return ( EvalEnv ee ) => (byte[])ee.Row[ColIx]._O; }
} // end class ExpName


class ExpBinary : Exp
{
  Token Operator;
  Exp Left, Right;

  public ExpBinary( Token op, Exp left, Exp right ) { Operator = op; Left = left; Right = right; }

  public override DV GetDV()
  {
    DataType t = Left.Type; if ( t >= DataType.Decimal ) t = DataType.Decimal;
    switch ( t )
    {
      case DataType.Bool:
      {
        DB left = Left.GetDB();
        DB right = Right.GetDB();
        switch( Operator )
        {    
          case Token.And: return ( ee ) => Value.New( left( ee ) && right( ee ) );
          case Token.Or:  return ( ee ) => Value.New( left( ee ) || right( ee ) );
          case Token.Equal: return ( ee ) => Value.New( left( ee ) ==  right( ee ) );
          case Token.NotEqual: return ( ee ) => Value.New( left( ee ) != right( ee ) );
        }
        break;
      }
      case DataType.Bigint:
      case DataType.Decimal:
      {
        DL left = Left.GetDL();
        DL right = Right.GetDL();
        switch( Operator )
        {
          case Token.Equal:         return ( ee ) => Value.New( left( ee ) == right( ee ) );
          case Token.NotEqual:      return ( ee ) => Value.New( left( ee ) != right( ee ) );
          case Token.Greater:       return ( ee ) => Value.New( left( ee ) > right( ee ) );
          case Token.GreaterEqual:  return ( ee ) => Value.New( left( ee ) >= right( ee ) );
          case Token.Less:          return ( ee ) => Value.New( left( ee ) < right( ee ) );
          case Token.LessEqual:     return ( ee ) => Value.New( left( ee ) <= right( ee ) );
          case Token.Plus:          return ( ee ) => Value.New( left( ee ) + right( ee ) );
          case Token.Minus:         return ( ee ) => Value.New( left( ee ) - right( ee ) );
          case Token.Times:         return ( ee ) => Value.New( left( ee ) * right( ee ) );
          case Token.Divide:        return ( ee ) => Value.New( left( ee ) / right( ee ) );
          case Token.Percent:       return ( ee ) => Value.New( left( ee ) % right( ee ) );
        }
        break;
      }
      case DataType.Double:
      {
        DD left = Left.GetDD();
        DD right = Right.GetDD();
        switch( Operator )
        {
          case Token.Equal:         return ( ee ) => Value.New( left( ee ) == right( ee ) );
          case Token.NotEqual:      return ( ee ) => Value.New( left( ee ) != right( ee ) );
          case Token.Greater:       return ( ee ) => Value.New( left( ee ) > right( ee ) );
          case Token.GreaterEqual:  return ( ee ) => Value.New( left( ee ) >= right( ee ) );
          case Token.Less:          return ( ee ) => Value.New( left( ee ) < right( ee ) );
          case Token.LessEqual:     return ( ee ) => Value.New( left( ee ) <= right( ee ) );
          case Token.Plus:          return ( ee ) => Value.New( left( ee ) + right( ee ) );
          case Token.Minus:         return ( ee ) => Value.New( left( ee ) - right( ee ) );
          case Token.Times:         return ( ee ) => Value.New( left( ee ) * right( ee ) );
          case Token.Divide:        return ( ee ) => Value.New( left( ee ) / right( ee ) );
          case Token.Percent:       return ( ee ) => Value.New( left( ee ) % right( ee ) );
        }
        break;
      }
      case DataType.String:
      {
        DS left = Left.GetDS();
        DS right = Right.GetDS();
        switch( Operator )
        {
          case Token.Equal:         return ( ee ) => Value.New( left( ee ) == right( ee ) );
          case Token.NotEqual:      return ( ee ) => Value.New( left( ee ) != right( ee ) );
          case Token.Greater:       return ( ee ) => Value.New( string.Compare( left( ee ), right( ee ) ) > 0 );
          case Token.GreaterEqual:  return ( ee ) => Value.New( string.Compare( left( ee ), right( ee ) ) >= 0 );
          case Token.Less:          return ( ee ) => Value.New( string.Compare( left( ee ), right( ee ) ) < 0 );
          case Token.LessEqual:     return ( ee ) => Value.New( string.Compare( left( ee ), right( ee ) ) <= 0 );
        }
        break;
      }      
    }
    throw new System.Exception( "Unexpected operator" + this );
  }

  public override DB GetDB()
  {
    DataType t = Left.Type; if ( t >= DataType.Decimal ) t = DataType.Decimal;
    switch ( t )
    {
      case DataType.Bool:
        DB lb = Left.GetDB(), rb = Right.GetDB();
        switch( Operator )
        {
          case Token.And: return ( ee ) => lb( ee ) && rb( ee );
          case Token.Or:  return ( ee ) => lb( ee ) || rb( ee );
          case Token.Equal: return ( ee ) => lb( ee ) == rb( ee );
          case Token.NotEqual: return ( ee ) => lb( ee ) != rb( ee );
        }   
        break;    
      case DataType.Bigint:
      case DataType.Decimal:
        DL ll = Left.GetDL(), rl = Right.GetDL();
        switch( Operator )
        {
          case Token.Equal:         return ( ee ) => ll( ee ) == rl( ee );
          case Token.NotEqual:      return ( ee ) => ll( ee ) != rl( ee );
          case Token.Greater:       return ( ee ) => ll( ee ) > rl( ee );
          case Token.GreaterEqual:  return ( ee ) => ll( ee ) >= rl( ee );
          case Token.Less:          return ( ee ) => ll( ee ) < rl( ee );
          case Token.LessEqual:     return ( ee ) => ll( ee ) <= rl( ee );
        }
        break;         
      case DataType.Double:
         DD ld = Left.GetDD(), rd = Right.GetDD();
        switch( Operator )
        {
          case Token.Equal:         return ( ee ) => ld( ee ) == rd( ee );
          case Token.NotEqual:      return ( ee ) => ld( ee ) != rd( ee );
          case Token.Greater:       return ( ee ) => ld( ee ) > rd( ee );
          case Token.GreaterEqual:  return ( ee ) => ld( ee ) >= rd( ee );
          case Token.Less:          return ( ee ) => ld( ee ) < rd( ee );
          case Token.LessEqual:     return ( ee ) => ld( ee ) <= rd( ee );
        }
        break;
       
      case DataType.String:
        DS ls = Left.GetDS(), rs = Right.GetDS();
        switch( Operator )
        {
          case Token.Equal:         return ( ee ) => ls( ee ) == rs( ee );
          case Token.NotEqual:      return ( ee ) => ls( ee ) != rs( ee );
          case Token.Greater:       return ( ee ) => string.Compare( ls( ee ), rs( ee ) ) > 0;
          case Token.GreaterEqual:  return ( ee ) => string.Compare( ls( ee ), rs( ee ) ) >= 0;
          case Token.Less:          return ( ee ) => string.Compare( ls( ee ), rs( ee ) ) < 0;
          case Token.LessEqual:     return ( ee ) => string.Compare( ls( ee ), rs( ee ) ) <= 0;
        }
        break;         
    }
    return null;
  }

  public override DL GetDL()
  {
    DL left = Left.GetDL();
    DL right = Right.GetDL();
    switch( Operator )
    {
      case Token.Plus:          return ( ee ) => left( ee ) + right( ee );
      case Token.Minus:         return ( ee ) => left( ee ) - right( ee );
      case Token.Times:         return ( ee ) => left( ee ) * right( ee );
      case Token.Divide:        return ( ee ) => left( ee ) / right( ee );
      case Token.Percent:       return ( ee ) => left( ee ) % right( ee );
    }
    return null;
  }

  public override DD GetDD()
  {
    DD left = Left.GetDD();
    DD right = Right.GetDD();
    switch( Operator )
    {
      case Token.Plus:          return ( ee ) => left( ee ) + right( ee );
      case Token.Minus:         return ( ee ) => left( ee ) - right( ee );
      case Token.Times:         return ( ee ) => left( ee ) * right( ee );
      case Token.Divide:        return ( ee ) => left( ee ) / right( ee );
      case Token.Percent:       return ( ee ) => left( ee ) % right( ee );
    }
    return null;
  }

  public override Exp Bind( SqlExec e )
  {
    if ( Operator == Token.VBar )
    {
      var clist = new G.List<Exp>();
      Left.GetConcat( clist );
      Right.GetConcat( clist );
      return new ConcatExp( clist ).Bind( e );
    } 
    Left = Left.Bind( e );
    Right = Right.Bind( e );

    DataType tL = Left.Type;
    DataType tR = Right.Type;

    if ( tL == DataType.Bigint && tR == DataType.Double )
    {
      Left = new IntToDoubleExp(Left);
      tL = DataType.Double;
    }
    else if ( tR == DataType.Bigint && tL == DataType.Double )
    {
      Right = new IntToDoubleExp(Right);
      tR = DataType.Double;
    }
    else if ( tL >= DataType.Decimal || tR >= DataType.Decimal )
    {
      if ( tR == DataType.Bigint ) tR = DTI.Decimal(18,0);
      else if ( tL == DataType.Bigint ) tL = DTI.Decimal(18,0);
      else if ( tR == DataType.Float ) { Right = Right.Convert( tL ); tR = tL; }
      else if ( tL == DataType.Float ) { Left = Left.Convert( tR ); tL = tR; }

      int sL = DTI.Scale(tL);
      int sR = DTI.Scale(tR);

      if ( sL < 0 || sR < 0 ) e.Error( "Type error involving decimal" );

      switch( Operator )
      {
        case Token.Divide: tL = DTI.Decimal( 18, sL - sR ); break;
        case Token.Times:  tL = DTI.Decimal( 18, sL + sR );  break;
        case Token.Percent: break; 
        default: 
          if ( sL > sR ) { Right = new ExpScale( Right, tL, sL - sR ); tR = tL; }
          else if ( sL < sR ) { Left = new ExpScale( Left, tR, sR - sL ); tL = tR; }
          break;
      }
      tR = tL;
    }    
    if ( tL == tR )
    {
      if ( Operator <= Token.NotEqual ) 
        Type = DataType.Bool;
      else 
      {
        Type = tL;
        if ( !TokenInfo.OperatorValid( Operator, tL ) ) 
          e.Error ( "Type error " + TokenInfo.Name(Operator) + " not valid for type " + DTI.Name(tL) );
      }
    }
    else e.Error( "Binary operator datatype error");

    return this;
  }

  public override void GetConcat( G.List<Exp> list )
  { 
    if ( Operator == Token.VBar )
    {
      Left.GetConcat( list );
      Right.GetConcat( list );
    }
    else
      list.Add(this); 
  }

  public override IdSet GetIdSet( TableExpression te )
  {
    if ( Operator <= Token.Equal && Right.IsConstant() && Left is ExpName )
    {
      ExpName e = ((ExpName)Left);
      if ( e.ColName == "Id" && Operator == Token.Equal ) return new SingleId( Right );
      IndexFile ix = te.FindIndex( e.ColIx );
      if ( ix != null )  return new IndexFrom( ix, Right, Operator );
    }
    else if ( Operator <= Token.Equal && Left.IsConstant() && Right is ExpName )
    {
      ExpName e = ((ExpName)Right);
      if ( e.ColName == "Id" && Operator == Token.Equal ) return new SingleId( Left );
      IndexFile ix = te.FindIndex( e.ColIx );
      if ( ix != null ) return new IndexFrom( ix, Left, TokenInfo.Reflect( Operator ) );
    }
    else if ( Operator == Token.And )
    {
      var left = Left.GetIdSet( te );   if ( left != null )  return left;
      var right = Right.GetIdSet( te ); if ( right != null ) return right;
    }
    return null;
  }

  public override bool IsConstant() { return Left.IsConstant() && Right.IsConstant(); }

} // end class ExpBinary

class ConcatExp : Exp
{
  G.List<Exp> Exps;

  public ConcatExp( G.List<Exp> exps )
  {
    Type = DataType.String;
    Exps = exps;
  }

  public override DS GetDS()
  {
    var list = new DS[ Exps.Count ];
    for ( int i = 0; i < list.Length; i += 1 ) list[ i ] = Exps[ i ].GetDS();
    return ( e ) => DoConcat( list, e );
  }

  public string DoConcat( DS[] list, EvalEnv e )
  {
    string [] result = new string[ list.Length ];
    for ( int i = 0; i < list.Length; i += 1 )
      result[ i ] = list[ i ]( e );
    return string.Join( null, result );
  }

  public override Exp Bind( SqlExec ee )
  {
    for ( int i = 0; i < Exps.Count; i += 1 ) 
    {
      Exp x = Exps[ i ];
      x = x.Bind( ee );
      if ( x.Type != DataType.String ) x = x.Convert( DataType.String );
      Exps[ i ] = x;
    }
    return this;
  }
}

class ExpMinus : UnaryExp
{
  public ExpMinus( Exp e ) { E = e; }

  public override Exp Bind( SqlExec e )
  {
    E = E.Bind( e );
    Type = E.Type;
    if ( Type != DataType.Bigint && Type != DataType.Double && Type < DataType.Decimal )
      e.Error( "Unary minus needs numeric argument" );
    return this;
  }

  public override bool IsConstant() { return E.IsConstant(); }

  public override DL GetDL() { DL x = E.GetDL(); return ( ee ) => - x( ee ); }
  public override DD GetDD() { DD x = E.GetDD(); return ( ee ) => - x( ee ); }

} // end class ExpMinus

class ExpNot : UnaryExp
{
  public ExpNot( Exp e ) { E = e; Type = DataType.Bool;  }

  public override Exp Bind( SqlExec e )
  {
    E = E.Bind( e );
    if ( E.Type != DataType.Bool ) e.Error( "NOT needs bool argument" );
    return this;
  }

  public override DB GetDB() { DB x = E.GetDB(); return ( ee ) => !x( ee ); }
} // end class ExpNot

class OrderByExp
{
  public Exp E;
  public bool Desc;
  public OrderByExp( Exp e, bool desc ){ E = e; Desc = desc; }
}

class ExpFuncCall : Exp
{
  string Schema;
  string FuncName;
  Block B;
  Exp [] Plist;


  public ExpFuncCall( string schema, string fname, G.List<Exp> plist )
  {
    Schema = schema;
    FuncName = fname;
    Plist = plist.ToArray();
  }

  public override Exp Bind( SqlExec e  )
  {
    B = e.Db.GetRoutine( Schema, FuncName, true, e );
    Type = B.ReturnType;

    for ( int i = 0; i < Plist.Length; i += 1 )
      Plist[ i ] = Plist[ i ].Bind( e );

    if ( B.Params.Count != Plist.Length ) e.Error( "Param count error calling function " + FuncName );
    for ( int i = 0; i < Plist.Length; i += 1 )
      if ( Plist[ i ].Type != B.Params.Type[ i ] )
      {
        Exp conv = Plist[ i ].Convert( B.Params.Type[ i ] );
        if ( conv != null ) Plist[ i ] = conv;
        else e.Error( "Parameter Type Error calling function " + FuncName 
           + " required type=" + DTI.Name( B.Params.Type[ i ] ) 
           + " supplied type=" + DTI.Name( Plist[ i ].Type ) + " exp=" + Plist[ i ] );
      }

    return this;
  }

  public override DV GetDV()
  {
    var dvs = Util.GetDVList( Plist );
    return ( ee ) => B.ExecuteRoutine( ee, dvs );
  }
} // end class ExpFuncCall

class CASE : Exp
{
  public struct Part
  {
    public Exp Test;
    public Exp E;
    public Part( Exp test, Exp e ) { Test = test; E = e; }
  }

  Part [] List;

  public CASE( Part[] list ) { List = list; }

  public override Exp Bind( SqlExec e )
  {
    for ( int i = 0; i < List.Length; i += 1 ) 
    {
      if ( List[ i ].Test != null )
      {
        List[ i ].Test = List[ i ].Test.Bind( e );
        if ( List[ i ].Test.Type != DataType.Bool ) e.Error( "Case test must be Bool" );
      }
      List[ i ].E = List[ i ].E.Bind( e );
      DataType dt = List[ i ].E.Type;
      if ( i == 0 ) Type = dt;
      else if ( dt != Type ) e.Error( "Case expressions must all have same type" );
    } 
    return this;
  }

  public override DV GetDV()
  {
    var dbs = new Exp.DB[ List.Length ];
    var dvs = new Exp.DV[ List.Length ];
    for ( int i = 0; i < List.Length; i += 1 )
    {
      dbs[ i ] = List[ i ].Test == null ? null : List[ i ].Test.GetDB();
      dvs[ i ] = List[ i ].E.GetDV();
    }
    return ( ee ) => Go( ee, dbs, dvs );
  }

  Value Go( EvalEnv ee, Exp.DB[] dbs, Exp.DV[] dvs )
  {
    for ( int i = 0; i < dbs.Length; i += 1 )
      if ( dbs[ i ] == null || dbs[ i ]( ee ) ) return dvs[ i ]( ee );
    return new Value(); // Should not get here.
  }    

  public override DS GetDS()
  {
    var dbs = new Exp.DB[ List.Length ];
    var dvs = new Exp.DS[ List.Length ];
    for ( int i = 0; i < List.Length; i += 1 )
    {
      dbs[ i ] = List[ i ].Test == null ? null : List[ i ].Test.GetDB();
      dvs[ i ] = List[ i ].E.GetDS();
    }
    return ( ee ) => GoS( ee, dbs, dvs );
  }  

  string GoS( EvalEnv ee, Exp.DB[] dbs, Exp.DS[] dvs )
  {
    for ( int i = 0; i < dbs.Length; i += 1 )
      if ( dbs[ i ] == null || dbs[ i ]( ee ) ) return dvs[ i ]( ee );
    return null; // Should not get here.
  }   

} // end class CASE

class ExpList : Exp // Implements the list of expressions in an SQL conditional expression X IN ( e1, e2, e3 .... )
{
  Exp [] List;
  DV [] Dvs;

  DataType ElementType;
  public ExpList( G.List<Exp> list )
  { 
    List = list.ToArray();
    Type = DataType.None;
  }

  public override Exp Bind( SqlExec e  )
  {
    for ( int i = 0; i < List.Length; i += 1 ) 
    {
      List[ i ] = List[ i ].Bind( e );
      DataType dt = List[ i ].Type;
      if ( i == 0 ) ElementType = dt;
      else if ( dt != ElementType ) e.Error( "Tuple type error" ); // Maybe should apply Exp.Convert if possible.
    } 
    Dvs = Util.GetDVList( List );
    return this;
  }

  public override bool TestIn( Value x, EvalEnv e )
  {
    for ( int i=0; i < List.Length; i += 1 )
    {
      Value y = Dvs[ i ]( e );
      if ( Util.Equal( x, y, ElementType ) ) return true;
    }
    return false;
  }

  public override DataType GetElementType() { return ElementType; }

  public override bool IsConstant() 
  { 
    for ( int i = 0; i < List.Length; i += 1 )
      if ( !List[ i ].IsConstant() ) return false;
    return true;
  }

  public override IdSet ListIdSet()
  {
    return new ExpListIdSet( List );
  }

  public override G.IEnumerable<Value> Values( EvalEnv ee )
  {
    for ( int i = 0; i < List.Length; i += 1 )
    {
      yield return Dvs[ i ]( ee );
    }
  }

} // class ExpList

class ScalarSelect : Exp
{
  TableExpression TE;

  public ScalarSelect( TableExpression te )
  {
    TE = te;
    Type = te.Type( 0 );
  }

  public override DataType GetElementType() { return Type; }

  public override bool TestIn( Value x, EvalEnv e )
  {
    var rs = new TestInResultSet( x, Type );
    TE.FetchTo( rs, e );
    return rs.Found;
  }

  public override IdSet ListIdSet()
  {
    return new TableExpressionIdSet( TE ); 
  }

  public override G.IEnumerable<Value> Values( EvalEnv ee )
  {
    Value [] row = new Value[1];
    foreach ( bool b in TE.GetAll( row, null, ee ) )
      yield return row[ 0 ];
  }

  public override bool IsConstant() 
  {
    return true; // May need revisiting once "outer references" are implemented.
  }
} // end class ScalarSelect

class TestInResultSet : ResultSet
{
  Value X;
  DataType Type;
  public bool Found;
  public TestInResultSet ( Value x, DataType t ) { X = x; Type = t; }
  public override bool NewRow( Value [] row )
  {
    if ( Util.Equal( row[0], X, Type ) ) { Found = true; return false; }
    return true;
  }
}

class ExpIn : Exp
{
  Exp Lhs;
  Exp Rhs;

  public ExpIn( Exp lhs, Exp rhs )
  {
    Lhs = lhs;
    Rhs = rhs;
    Type = DataType.Bool;
  }

  public override Exp Bind( SqlExec e )
  {
    Lhs = Lhs.Bind( e );
    Rhs = Rhs.Bind( e );
    if ( Lhs.Type != Rhs.GetElementType() ) e.Error( "IN type mismatch" );
    return this;
  }

  public override DB GetDB()
  {
    var lhs = Lhs.GetDV();
    var rhs = Rhs;
    return ( ee ) => rhs.TestIn( lhs( ee ), ee );
  }

  public override IdSet GetIdSet( TableExpression te )
  {
    if ( Lhs is ExpName && Rhs.IsConstant() )
    {
      ExpName e = (ExpName)Lhs;
      if ( e.ColName == "Id" ) // select ... from t where id in ( .... )
      {
        return Rhs.ListIdSet();
      }
      IndexFile ix = te.FindIndex( e.ColIx );
      if ( ix != null ) // select ... from t where indexedcol in ( .... )
      {        
        return new Lookup( ix, Rhs ); // For each value in the Rhs list, lookup it's ids, and return them in the IdSet.
      }
    }
    return null;
  }
} // end class ExpIn

// Aggregates

class COUNT : Exp
{
  public COUNT() { Type = DataType.Bigint; }
  public override AggOp GetAggOp(){ return AggOp.Count; }
  public override DL GetDL()
  {
    return ( ee ) => 0;
  }
}

class ExpAgg : Exp
{
  public Exp E;
  AggOp Op;
  public ExpAgg( AggOp op, G.List<Exp> arg, SqlExec e ) 
  { 
    Op = op; 
    if ( arg.Count != 1 ) e.Error( Op + " takes one parameter" );
    E = arg[0]; 
  }

  public override DV GetDV()
  {
    var e = E.GetDV();
    return ( ee ) => e( ee );
  }

  public override void BindAgg( SqlExec e )
  { 
    E = E.Bind( e );
    Type = E.Type;
  }

  public override Exp Bind( SqlExec e )
  { 
    e.Error( "Aggregate can only be a top level SELECT" );
    return this;
  }

  public override AggOp GetAggOp(){ return Op; }
}

} // end namespace SQLNS