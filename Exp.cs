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
  public virtual Value Eval( EvalEnv e ){ return new Value(); }
  public virtual DataType Bind( SqlExec e ){ return Type; }
  public virtual IdSet GetIdSet(  TableExpression te, EvalEnv ee ) { return null; }
  public virtual bool IsConstant() { return false; } // Evaluation doesn't depend on table row ( so Eval() won't fail )
  public virtual DataType TypeCheck( SqlExec e ) { return Type; }

  // Methods related to implementation of "IN".
  public virtual bool TestIn( Value x, EvalEnv e ){ return false; }
  public virtual DataType GetElementType() { return DataType.None; }

  public virtual IdSet ListIdSet( EvalEnv e ){ return null; } // Index optimisation.
  public virtual G.IEnumerable<Value> Values( EvalEnv ee ){ yield break; } // ScalarSelect implementation.

  // Methods related to implementaiton of aggregates.
  public virtual AggOp GetAggOp(){ return AggOp.None; }
  public virtual void BindAgg( SqlExec e ){ }

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
    else if ( Type == DataType.ScaledInt && t >= DataType.Decimal ) return this;
    return null;
  }
}

class UnaryExp : Exp
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
  public override Value Eval( EvalEnv e ) 
  { 
    if ( Type <= DataType.String && e.Locals[I]._O == null )
    {
      e.Locals[I] = DTI.Default( Type );
    }
    return e.Locals[I]; 
  }
  public override bool IsConstant() { return true; }
}

class ExpConstant : Exp
{
  public Value Value;
  public ExpConstant( long x, DataType t ){ Value.L = x; Type = t; }
  public ExpConstant( string x ){ Value.O = x; Type = DataType.String; }
  public ExpConstant( byte[] x ){ Value.O = x; Type = DataType.Binary; }
  public ExpConstant( bool x ){ Value.B = x; Type = DataType.Bool; }
  public override Value Eval( EvalEnv e ){ return Value; }
  public override bool IsConstant() { return true; } // Evaluation doesn't depend on table row.
}

class ExpName : Exp
{
  public string ColName;
  public int ColIx;
  public ExpName( string name ){ ColName = name; Name = name; }

  public override DataType Bind( SqlExec e )
  {
    var ci = e.CI;
    if ( ci == null ) e.Error( "Undeclared variable " + ColName );
    for ( int i=0; i < ci.Count; i += 1 ) 
      if ( ci.Names[i] == ColName ) 
      { 
        e.Used[ i ] = true;
        ColIx = i; 
        Type = DTI.Base( ci.Types[i] );
        return Type;
      }

    // for ( int i=0; i < ci.Length; i += 1 ) System.Console.WriteLine( ci[i].Name );
    
    e.Error( "Column " + ColName + " not found" );
    return Type;
  }

  public override Value Eval( EvalEnv e ) { return e.Row[ColIx]; }

} // end class ExpName


class ExpBinary : Exp
{
  Token Operator;
  Exp Left, Right;

  public ExpBinary( Token op, Exp left, Exp right ) { Operator = op; Left = left; Right = right; }

  public override Value Eval( EvalEnv e )
  {
    Value lv = Left.Eval( e ), rv = Right.Eval( e );
    DataType t = Left.Type; if ( t >= DataType.Decimal ) t = DataType.Decimal;
    switch ( t )
    {
      case DataType.Bool:
        switch( Operator )
        {
          case Token.And: lv.B = lv.B & rv.B; break;
          case Token.Or:  lv.B = lv.B | rv.B; break;
          case Token.Equal: lv.B = lv.B == rv.B; break;
          case Token.NotEqual: lv.B = lv.B != rv.B; break;
          default: throw new System.Exception( "Unexpected boolean operator" );
        }
        break;
      case DataType.Bigint:
      case DataType.Decimal:
        switch( Operator )
        {
          case Token.Equal:         lv.B = lv.L == rv.L; break;
          case Token.NotEqual:      lv.B = lv.L != rv.L; break;
          case Token.Greater:       lv.B = lv.L > rv.L; break;
          case Token.GreaterEqual:  lv.B = lv.L >= rv.L; break; 
          case Token.Less:          lv.B = lv.L < rv.L; break;
          case Token.LessEqual:     lv.B = lv.L <= rv.L; break;
          case Token.Plus:          lv.L += rv.L; break;
          case Token.Minus:         lv.L -= rv.L; break;
          case Token.Times:         lv.L *= rv.L; break;
          case Token.Divide:        lv.L /= rv.L; break;
          case Token.Percent:       lv.L %= rv.L; break;
          default: throw new System.Exception( "Unexpected integer operator" );
        }
        break;
      case DataType.Double:
        switch( Operator )
        {
          case Token.Equal:         lv.B = lv.D == rv.D; break;
          case Token.NotEqual:      lv.B = lv.D != rv.D; break;
          case Token.Greater:       lv.B = lv.D > rv.D; break;
          case Token.GreaterEqual:  lv.B = lv.D >= rv.D; break; 
          case Token.Less:          lv.B = lv.D < rv.D; break;
          case Token.LessEqual:     lv.B = lv.D <= rv.D; break;
          case Token.Plus:          lv.D += rv.D; break;
          case Token.Minus:         lv.D -= rv.D; break;
          case Token.Times:         lv.D *= rv.D; break;
          case Token.Divide:        lv.D /= rv.D; break;
          case Token.Percent:       lv.D %= rv.D; break;
          default: throw new System.Exception( "Unexpected integer operator" );
        }
        break;
      case DataType.String:
        switch( Operator )
        {
          case Token.Equal:         lv.B = (string)lv._O == (string)rv._O; break;
          case Token.NotEqual:      lv.B = string.Compare( (string)lv._O, (string)rv._O ) != 0; break;
          case Token.Greater:       lv.B = string.Compare( (string)lv._O, (string)rv._O ) > 0; break;
          case Token.GreaterEqual:  lv.B = string.Compare( (string)lv._O, (string)rv._O ) >= 0; break;
          case Token.Less:          lv.B = string.Compare( (string)lv._O, (string)rv._O ) < 0; break;
          case Token.LessEqual:     lv.B = string.Compare( (string)lv._O, (string)rv._O ) <= 0; break;
          case Token.VBar:
          case Token.Plus:          lv.O = (string)lv._O + (string)rv._O; break;
          default: throw new System.Exception( "Unexpected string operator" );
        }
        break;
      default: throw new System.Exception("Unexpected type " + Left.Type + " exp=" + this );
    }
    return lv;
  }

  public override DataType Bind( SqlExec e )
  {
    Left.Bind( e );
    Right.Bind( e );
    return TypeCheck( e );
  }

  public override DataType TypeCheck( SqlExec e )
  {
    DataType tL = Left.TypeCheck( e );
    DataType tR = Right.TypeCheck( e );

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
    else if ( Operator != Token.VBar && ( tL >= DataType.Decimal || tR >= DataType.Decimal ) )
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
          e.Error ( "Type error " + TokenInfo.Name(Operator) + " not valid for type " + tL );
      }
    }
    else if ( tL == DataType.String && Operator == Token.VBar )
    {
      Type = DataType.String;
      switch ( tR )
      {
        case DataType.Bigint:   Right = new IntToStringExp( Right );  break;
        case DataType.Double:   Right = new DoubleToStringExp( Right ); break;
        case DataType.Binary:   Right = new BinaryToStringExp( Right ); break;
        case DataType.Bool:     Right = new BoolToStringExp( Right ); break;
        default: 
          if ( tR >= DataType.Decimal ) Right = new DecimalToStringExp( Right ); 
          else e.Error( "Vbar error"); // Should not get here
          break;
      }
    }
    else e.Error( "Binary operator datatype error");
    return Type;
  }

  public override IdSet GetIdSet( TableExpression te, EvalEnv ee )
  {
    if ( Operator <= Token.Equal && Right.IsConstant() && Left is ExpName )
    {
      ExpName e = ((ExpName)Left);
      if ( e.ColName == "Id" && Operator == Token.Equal ) return new SingleId( Right ); // Ought to also cater for inequalities.
      IndexFile ix = te.FindIndex( e.ColIx );
      if ( ix != null ) 
      {
        // Console.WriteLine( "Index found " + ToString() );
        return new IndexFrom( ix, Right, Operator );
      }
    }

    if ( Operator == Token.And )
    {
      var left = Left.GetIdSet( te, ee );
      if ( left != null ) return left;
      var right = Right.GetIdSet( te ,ee );
      if ( right != null ) return right;
    }

    // Console.WriteLine( "Index not found " + ToString() );
    return null;
  }

  public override bool IsConstant() { return Left.IsConstant() && Right.IsConstant(); }

} // end class ExpBinary

class ExpScale : UnaryExp
{
  long Amount;
  public ExpScale( Exp e, DataType t, int amount )
  {
    E = e; 
    Amount = (long)Util.PowerTen( amount );
    Type = t;
  }
  public override Value Eval( EvalEnv e )
  {
    Value v = E.Eval( e );
    v.L = v.L * Amount;
    return v;
  }
}

class ExpScaleReduce : UnaryExp
{
  long Amount;
  public ExpScaleReduce( Exp e, DataType t, int amount )
  {
    E = e; 
    Amount = (long)Util.PowerTen( amount );
    Type = t;
  }
  public override Value Eval( EvalEnv e )
  {
    Value v = E.Eval( e );
    v.L = v.L / Amount;
    return v;
  }
}

class ExpMinus : UnaryExp
{
  public ExpMinus( Exp e )
  {
    E = e;
  }

  public override DataType Bind( SqlExec e )
  {
    Type = E.Bind( e );
    if ( Type != DataType.Bigint && Type != DataType.Double && Type < DataType.Decimal )
      e.Error( "Unary minus needs numeric argument" );
    return Type;
  }

  public override Value Eval( EvalEnv e )
  {
    Value v = E.Eval( e );
    if ( E.Type == DataType.Double ) v.D = - v.D; else v.L = - v.L;
    return v;
  }

  public override bool IsConstant() { return E.IsConstant(); }

} // end class ExpMinus

class ExpNot : UnaryExp
{
  public ExpNot( Exp e )
  {
    E = e;
    Type = DataType.Bool;
  }

  public override DataType Bind( SqlExec e )
  {
    if ( E.Bind( e ) != DataType.Bool )
      e.Error( "NOT needs bool argument" );
    return Type;
  }

  public override Value Eval( EvalEnv e )
  {
    Value v = E.Eval( e );
    v.B = ! v.B;
    return v;
  }
} // end class ExpNot

// Conversions

class IntToStringExp : UnaryExp
{
  public IntToStringExp( Exp e )
  { 
    E = e;
    Type = DataType.String;
  }

  public override Value Eval( EvalEnv e )
  {
    Value v = E.Eval( e );
    v.O = v.L.ToString();
    return v;
  }  
}

class DecimalToStringExp : UnaryExp
{
  public DecimalToStringExp( Exp e )
  { 
    E = e;
    Type = DataType.String;
  }

  public override Value Eval( EvalEnv e )
  {
    Value v = E.Eval( e );
    decimal d = v.L;
    int scale = DTI.Scale( E.Type );
    d = d / Util.PowerTen( scale );
    v.O = d.ToString( "F" + scale, System.Globalization.CultureInfo.InvariantCulture );
    return v;
  }  
}

class DecimalToDoubleExp : UnaryExp
{
  public DecimalToDoubleExp( Exp e )
  { 
    E = e;
    Type = DataType.Double;
  }

  public override Value Eval( EvalEnv e )
  {
    Value v = E.Eval( e );
    v.D = ((double)v.L) / Util.PowerTen( DTI.Scale( E.Type ) );
    return v;
  }  
}

class IntToDoubleExp : UnaryExp
{
  public IntToDoubleExp( Exp e )
  { 
    E = e;
    Type = DataType.Double;
  }

  public override Value Eval( EvalEnv e )
  {
    Value v = E.Eval( e );
    v.D = v.L;
    return v;
  }  
}

class DoubleToIntExp : UnaryExp
{
  public DoubleToIntExp( Exp e )
  { 
    E = e;
    Type = DataType.Bigint;
  }

  public override Value Eval( EvalEnv e )
  {
    Value v = E.Eval( e );
    v.L = (long)v.D;
    return v;
  }  
}

class DoubleToDecimalExp : UnaryExp
{
  public DoubleToDecimalExp( Exp e, DataType t )
  { 
    E = e;
    Type = t;
  }

  public override Value Eval( EvalEnv e )
  {
    Value v = E.Eval( e );
    v.L = (long) ( v.D * Util.PowerTen( DTI.Scale( Type ) ) );
    return v;
  }  
}

class IntToDecimalExp : UnaryExp
{
  public IntToDecimalExp( Exp e, DataType t )
  { 
    E = e;
    Type = t;
  }

  public override Value Eval( EvalEnv e )
  {
    Value v = E.Eval( e );
    v.L = (long) ( v.L * (long)Util.PowerTen( DTI.Scale( Type ) ) );
    return v;
  }  
}

class DoubleToStringExp : UnaryExp
{
  public DoubleToStringExp( Exp e )
  { 
    E = e;
    Type = DataType.String;
  }

  public override Value Eval( EvalEnv e )
  {
    Value v = E.Eval( e );
    v.O = v.D.ToString();
    return v;
  }  
}

class BinaryToStringExp : UnaryExp
{
  public BinaryToStringExp( Exp e )
  { 
    E = e;
    Type = DataType.String;
  }

  public override Value Eval( EvalEnv e )
  {
    Value v = E.Eval( e );
    v.O = Util.ToString( (byte[])v._O );
    return v;
  }  
}

class BoolToStringExp : UnaryExp
{
  public BoolToStringExp( Exp e )
  { 
    E = e;
    Type = DataType.String;
  }

  public override Value Eval( EvalEnv e )
  {
    Value v = E.Eval( e );
    v.O = v.B.ToString();
    return v;
  }  
}

// end conversions

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

  public override Value Eval( EvalEnv e )
  {
    return B.ExecuteFunctionCall( e, Plist );
  }

  public override DataType Bind( SqlExec e  )
  {
    B = e.Db.GetRoutine( Schema, FuncName, true, e );
    Type = B.ReturnType;

    for ( int i = 0; i < Plist.Length; i += 1 )
      Plist[ i ].Bind( e );

    return TypeCheck( e );
  }

  public override DataType TypeCheck( SqlExec e )
  {
    if ( B.Params.Count != Plist.Length ) e.Error( "Param count error calling function " + FuncName );
    for ( int i = 0; i < Plist.Length; i += 1 )
      if ( Plist[i].Type != B.Params.Types[i] )
      {
        Exp conv = Plist[i].Convert( B.Params.Types[i] );
        if ( conv != null ) Plist[i] = conv;
        else e.Error( "Parameter Type Error calling function " + FuncName + " required type=" + B.Params.Types[i] + " supplied type=" +
          DTI.Name( Plist[i].Type ) + " exp=" + Plist[i] );
      }
    return Type;
  }
} // end class ExpFuncCall

class CASE : Exp
{
  public struct Part
  {
    public Exp Test;
    public Exp E;
    public Part( Exp test, Exp e )
    {
      Test = test;
      E = e;
    }
  }

  Part [] List;

  public CASE( Part[] list )
  {
    List = list;
  }

  public override DataType Bind( SqlExec e )
  {
    for ( int i = 0; i < List.Length; i += 1 ) 
    {
      Exp test = List[i].Test;
      if ( test != null && test.Bind( e )!= DataType.Bool ) e.Error( "Case test must be Bool" );
      DataType dt = List[i].E.Bind( e );
      if ( i == 0 ) Type = dt;
      else if ( dt != Type ) e.Error( "Case expressions must all have same type" );
    } 
    return Type;
  }

  public override Value Eval( EvalEnv e )
  {
    for ( int i = 0; i < List.Length; i += 1 ) 
    {
      Exp test = List[i].Test;
      if ( test == null || test.Eval( e ).B ) return List[i].E.Eval( e );
    }
    return new Value(); // Should not get here.
  }
} // end class CASE

class ExpList : Exp // Implements the list of expressions in an SQL conditional expression X IN ( e1, e2, e3 .... )
{
  Exp [] List;
  DataType ElementType;
  public ExpList( G.List<Exp> list )
  { 
    List = list.ToArray();
    Type = DataType.None;
  }

  public override DataType Bind( SqlExec e  )
  {
    for ( int i = 0; i < List.Length; i += 1 ) 
    {
      DataType dt = List[i].Bind( e );
      if ( i == 0 ) ElementType = dt;
      else if ( dt != ElementType ) e.Error( "Tuple type error" ); // Maybe should apply Exp.Convert if possible.
    } 
    return Type;
  }

  public override bool TestIn( Value x, EvalEnv e )
  {
    for ( int i=0; i < List.Length; i += 1 )
    {
      Value y = List[i].Eval( e );
      if ( Util.Equal( x, y, ElementType ) ) return true;
    }
    return false;
  }

  public override DataType GetElementType() { return ElementType; }

  public override bool IsConstant() 
  { 
    for ( int i = 0; i < List.Length; i += 1 )
      if ( !List[i].IsConstant() ) return false;
    return true;
  }

  public override IdSet ListIdSet( EvalEnv e )
  {
    return new ExpListIdSet( List, e );
  }

  public override G.IEnumerable<Value> Values( EvalEnv ee )
  {
    for ( int i = 0; i < List.Length; i += 1 )
    {
      yield return List[i].Eval( ee );
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

  public override IdSet ListIdSet( EvalEnv ee )
  {
    return new TableExpressionIdSet( TE, ee );
  }

  public override G.IEnumerable<Value> Values( EvalEnv ee )
  {
    var rs = new SingleResultSet();
    TE.FetchTo( rs, ee  );
    var rows = rs.Table.Rows;
    for ( int i = 0; i < rows.Count; i += 1 )
      yield return rows[i][0];
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

  public override DataType Bind( SqlExec e )
  {
    Lhs.Bind( e );
    Rhs.Bind( e );
    if ( Lhs.Type != Rhs.GetElementType() ) e.Error( "IN type mismatch" );
    return Type;
  }

  public override Value Eval( EvalEnv e )
  {
    Value lhs = Lhs.Eval( e );
    Value result = new Value();
    result.B = Rhs.TestIn( lhs, e );
    return result;    
  }

  public override IdSet GetIdSet( TableExpression te, EvalEnv ee )
  {
    if ( Lhs is ExpName && Rhs.IsConstant() )
    {
      ExpName e = (ExpName)Lhs;
      if ( e.ColName == "Id" ) // select ... from t where id in ( .... )
      {
        return Rhs.ListIdSet( ee );
      }
      IndexFile ix = te.FindIndex( e.ColIx );
      if ( ix != null ) // select ... from t where indexedcol in ( .... )
      {        
        return new Lookup( ix, Rhs.Values( ee ) ); // For each value in the Rhs list, lookup it's ids, and return them in the IdSet.
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

  public override Value Eval( EvalEnv e )
  { 
    return E.Eval( e );
  }

  public override void BindAgg( SqlExec e )
  { 
    Type = E.Bind( e );
  }

  public override DataType Bind( SqlExec e )
  { 
    e.Error( "Aggregate can only be a top level SELECT" );
    return DataType.None;
  }

  public override AggOp GetAggOp(){ return Op; }
}

} // end namespace SQLNS