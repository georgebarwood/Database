namespace SQLNS
{

using G = System.Collections.Generic;
using DBNS;

class ExpScale : UnaryExp
{
  long Amount;
  public ExpScale( Exp e, DataType t, int amount )
  {
    E = e; 
    Amount = (long)Util.PowerTen( amount );
    Type = t;
  }

  public override DL GetDL()
  {
    DL x = E.GetDL();
    return ( ee ) => x( ee ) * Amount;
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

  public override DL GetDL()
  {
    DL x = E.GetDL();
    return ( ee ) => x( ee ) / Amount;
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

  public override bool IsConstant() { return E.IsConstant(); }

  public override DL GetDL()
  {
    DL x = E.GetDL();
    return ( ee ) => - x( ee );
  }

  public override DD GetDD()
  {
    DD x = E.GetDD();
    return ( ee ) => - x( ee );
  }

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

  public override DB GetDB()
  {
    DB x = E.GetDB();
    return ( ee ) => !x( ee );
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

  public override DS GetDS()
  {
    DL x = E.GetDL();
    return ( ee ) => x( ee ).ToString();
  }
}

class DecimalToStringExp : UnaryExp
{
  public DecimalToStringExp( Exp e )
  { 
    E = e;
    Type = DataType.String;
  }

  public override DS GetDS()
  {
    DL x = E.GetDL();
    DataType t = E.Type;
    return ( ee ) => D2S( x( ee ), t );
  }

  public static string D2S( long v, DataType t )
  {
    decimal d = v;
    int scale = DTI.Scale( t );
    d = d / Util.PowerTen( scale );
    return d.ToString( "F" + scale, System.Globalization.CultureInfo.InvariantCulture );
  }
}

class DecimalToDoubleExp : UnaryExp
{
  public DecimalToDoubleExp( Exp e )
  { 
    E = e;
    Type = DataType.Double;
  }

  public override DD GetDD()
  {
    DL x = E.GetDL();
    double p10 = Util.PowerTen( DTI.Scale( E.Type ) );
    return ( ee ) => (double)x(ee) / p10;
  }
}

class IntToDoubleExp : UnaryExp
{
  public IntToDoubleExp( Exp e )
  { 
    E = e;
    Type = DataType.Double;
  }

  public override DD GetDD()
  {
    DL x = E.GetDL();
    return ( ee ) => (double)x(ee);
  }
}

class DoubleToIntExp : UnaryExp
{
  public DoubleToIntExp( Exp e )
  { 
    E = e;
    Type = DataType.Bigint;
  }

  public override DL GetDL()
  {
    DD x = E.GetDD();
    return ( ee ) => (long)x(ee);
  }
}

class DoubleToDecimalExp : UnaryExp
{
  public DoubleToDecimalExp( Exp e, DataType t )
  { 
    E = e;
    Type = t;
  }

  public override DL GetDL()
  {
    DD x = E.GetDD();
    ulong p10 = Util.PowerTen( DTI.Scale( Type ) );
    return ( ee ) => (long)( x(ee) * p10 );
  }
}

class IntToDecimalExp : UnaryExp
{
  public IntToDecimalExp( Exp e, DataType t )
  { 
    E = e;
    Type = t;
  }

  public override DL GetDL()
  {
    DL x = E.GetDL();
    long p10 = (long)Util.PowerTen( DTI.Scale( Type ) );
    return ( ee ) => x(ee) * p10;
  }
}

class DoubleToStringExp : UnaryExp
{
  public DoubleToStringExp( Exp e )
  { 
    E = e;
    Type = DataType.String;
  }

  public override DS GetDS()
  {
    DD x = E.GetDD();
    return ( ee ) => x(ee).ToString();
  } 
}

class BinaryToStringExp : UnaryExp
{
  public BinaryToStringExp( Exp e )
  { 
    E = e;
    Type = DataType.String;
  }

  public override DS GetDS()
  {
    DX x = E.GetDX();
    return ( ee ) => Util.ToString( x(ee) );
  } 
}

class BoolToStringExp : UnaryExp
{
  public BoolToStringExp( Exp e )
  { 
    E = e;
    Type = DataType.String;
  }

  public override DS GetDS()
  {
    DB x = E.GetDB();
    return ( ee ) => x(ee).ToString();
  } 
}

} // end namespace SQLNS
