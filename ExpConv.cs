namespace SQLNS
{

using G = System.Collections.Generic;
using DBNS;

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
    return ( ee ) => (long)x( ee );
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
    return ( ee ) => (double)x( ee ) / p10;
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
    return ( ee ) => (double)x( ee );
  }
}

// ToDecimal

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
    return ( ee ) => x( ee ) * p10;
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
    return ( ee ) => (long)( x( ee ) * p10 );
  }
}

// ToString

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
    return ( ee ) => x( ee ).ToString();
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
    return ( ee ) => Util.ToString( x( ee ) );
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
    return ( ee ) => x( ee ).ToString();
  } 
}

// Decimal scaling

class ExpScale : UnaryExp
{
  int Amount;
  public ExpScale( Exp e, DataType t, int amount )
  {
    E = e; 
    Amount = amount;
    Type = t;
  }

  public override DL GetDL()
  {
    long p10 = (long)Util.PowerTen( Amount );
    DL x = E.GetDL();
    return ( ee ) => x( ee ) * p10;
  }
}

class ExpScaleReduce : UnaryExp
{
  int Amount;
  public ExpScaleReduce( Exp e, DataType t, int amount )
  {
    E = e; 
    Amount = amount;
    Type = t;
  }

  public override DL GetDL()
  {
    long p10 = (long)Util.PowerTen( Amount );
    DL x = E.GetDL();
    return ( ee ) => x( ee ) / p10;
  }
}

} // end namespace SQLNS
