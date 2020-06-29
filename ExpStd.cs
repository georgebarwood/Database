namespace SQLNS
{

using G = System.Collections.Generic;
using DBNS;

// Standard functions.

abstract class StdExp : Exp
{
  protected Exp [] Arg;
  DataType [] Types;
  
  public StdExp( G.List<Exp>arg, DataType type, DataType [] types, Exec e )
  {
    Arg = arg.ToArray();
    Type = type;
    Types = types;    
    if ( Arg.Length != Types.Length ) e.Error( this + " takes " + Types.Length + " argument(s)" );
  }

  public override void Bind( SqlExec e )
  {
    for ( int i = 0; i < Types.Length; i += 1 )
    {
      Arg[ i ].Bind( e );
      DataType t = Arg[ i ].Type;
      if ( t != Types[ i ] ) 
        e.Error( this + " parameter type error, arg " + i + " expected " 
         + DTI.Name( Types[ i ] ) + " actual " + DTI.Name( t ) );
    }
  }
}   
class EXCEPTION : Exp
{
  public EXCEPTION( G.List<Exp> parms, Exec e )
  {
    if ( parms.Count != 0 ) e.Error( "EXCEPTION takes no parameters" );
    Type = DataType.String;
  }

  public override DS GetDS()
  {
    return ( ee ) => GetException( ee );
  }

  string GetException( EvalEnv ee )
  {
    var ex = ee.ResultSet.Exception;
    string result = 
      ex == null ? ""
      : ( ex is UserException || ex is Exception ) ? ex.Message
      : ex.ToString(); // for full debug info.
    ee.ResultSet.Exception = null;
    return result;
  }
} // end class EXCEPTION

class REPLACE : StdExp
{
  public REPLACE( G.List<Exp>args, Exec e ) : base( args, DataType.String, 
    new DataType[]{ DataType.String, DataType.String, DataType.String }, e ){}

  public override DS GetDS()
  {
    var s = Arg[0].GetDS();
    var pat = Arg[1].GetDS();
    var sub = Arg[2].GetDS();
    return ( ee ) => s( ee ).Replace( pat( ee ), sub( ee ) );
  }

} // end class REPLACE

class SUBSTRING : StdExp
{
  public SUBSTRING( G.List<Exp>args, Exec e ) : base( args, DataType.String, 
    new DataType[]{ DataType.String, DataType.Bigint, DataType.Bigint }, e ){}

  public override DS GetDS()
  {
    var a0 = Arg[0].GetDS();
    var a1 = Arg[1].GetDL();
    var a2 = Arg[2].GetDL();
    return ( ee ) => DoSub( a0( ee ), (int)a1( ee ), (int)a2( ee ) );
  }

  public string DoSub( string s, int start, int len )
  {
    if ( start < 1 ) start = 1;
    if ( start > s.Length ) start = s.Length;
    if ( len < 0 ) len = 0;
    if ( len > s.Length - (start-1) ) len = s.Length - (start-1);
    return s.Substring( start-1, len );
  }

} // end class SUBSTRING

class LEN : UnaryExp
{
  DataType SType;
  public LEN( G.List<Exp>args, Exec e )
  {
    if ( args.Count != 1 ) e.Error( this + "takes one argument" );
    E = args[0];
    Type = DataType.Bigint;
  }

  public override void Bind( SqlExec e )
  {
    E.Bind( e );
    SType = E.Type;
    if ( SType != DataType.String && SType != DataType.Binary ) e.Error( "LEN argument must be string or binary" );
  }   

  public override DL GetDL()
  {
    if ( SType == DataType.String )
    {
      var a0 = E.GetDS();
      return ( ee ) => a0( ee ).Length;
    }
    else
    {
      var a0 = E.GetDX();
      return ( ee ) => a0( ee ).Length;
    }
  }
} // end class LEN

class PARSEINT : StdExp
{
  public PARSEINT( G.List<Exp>args, Exec e ) : base( args, DataType.Bigint, new DataType[]{ DataType.String }, e ){}

  public override DL GetDL()
  {
    var a = Arg[0].GetDS();
    return ( ee ) => DoParse( a( ee ) );
  }

  long DoParse( string s )
  {
    try
    {
      return long.Parse( s );
    }
    catch ( System.Exception )
    {
      throw new System.Exception( "Cannot convert '" + s + "' to integer" );
    }
  }
} // end class PARSEINT

class PARSEDECIMAL : StdExp
{
  public PARSEDECIMAL( G.List<Exp>args, Exec e ) : base( args, DataType.ScaledInt, new DataType[]{ DataType.String, DataType.Bigint }, e ){}

  public override DL GetDL()
  {
    var a = Arg[0].GetDS();
    var t = Arg[1].GetDL();
    return ( ee ) => DoParse( a( ee ), (DataType)t( ee ) );
  }

  long DoParse( string s, DataType t )
  {
    try
    {
      return (long) ( decimal.Parse( s ) * Util.PowerTen( DTI.Scale(t) ) );
    }
    catch ( System.Exception )
    {
      throw new System.Exception( "Cannot convert '" + s + "' to decimal" );
    }
  }
} // end class PARSEDECIMAL

class PARSEDOUBLE : StdExp
{
  public PARSEDOUBLE( G.List<Exp>args, Exec e ) : base( args, DataType.Double, new DataType[]{ DataType.String }, e ){}

  public override DD GetDD()
  {
    var a = Arg[0].GetDS();
    return ( ee ) => DoParse( a( ee ) );
  }

  double DoParse( string s )
  {
    try
    {
      return double.Parse( s );
    }
    catch ( System.Exception )
    {
      throw new System.Exception( "Cannot convert '" + s + "' to double" );
    }
  }
} // end class PARSEDOUBLE

// Functions which access the global state : LASTID, EXCEPTION, ARG, FILEATTR, FILECONTENT.

class LASTID : Exp
{
  public LASTID( G.List<Exp> parms, Exec e )
  {
    if ( parms.Count != 0 ) e.Error( "LASTID takes no parameters" );
    Type = DataType.Bigint;
  }

  public override DL GetDL()
  {
    return ( ee ) => ee.ResultSet.LastIdInserted;
  }
} // end class LASTID

class GLOBAL : StdExp
{
  public GLOBAL( G.List<Exp>args, Exec e ) : base( args, DataType.Bigint, 
    new DataType[]{ DataType.Bigint }, e ){}

  public override DL GetDL()
  {
    var k = Arg[0].GetDL();
    return ( ee ) => ee.ResultSet.Global( (int)k( ee ) );
  }
} // end class GLOBAL

class ARG : StdExp
{
  public ARG( G.List<Exp>args, Exec e ) : base( args, DataType.String, 
    new DataType[]{ DataType.Bigint, DataType.String }, e ){}

  public override DS GetDS()
  {
    var k = Arg[0].GetDL();
    var n = Arg[1].GetDS();
    return ( ee ) => ee.ResultSet.Arg( (int)k( ee ), n( ee ) );
  }
} // end class ARG

class ARGNAME : StdExp
{
  public ARGNAME( G.List<Exp>args, Exec e ) : base( args, DataType.String, 
    new DataType[]{ DataType.Bigint, DataType.Bigint }, e ){}

  public override DS GetDS()
  {
    var k = Arg[0].GetDL();
    var x = Arg[1].GetDL();
    return ( ee ) => ee.ResultSet.ArgName( (int)k( ee ), (int)x( ee ) );
  }
} // end class ARGNAME

class FILEATTR : StdExp
{
  public FILEATTR( G.List<Exp>args, Exec e ) : base( args, DataType.String, 
    new DataType[]{ DataType.Bigint, DataType.Bigint }, e ){}

  public override DS GetDS()
  {
    var x = Arg[0].GetDL();
    var k = Arg[1].GetDL();
    return ( ee ) => ee.ResultSet.FileAttr( (int)x( ee ), (int)k( ee ) );
  }
} // end class FILEATTR

class FILECONTENT : StdExp
{
  public FILECONTENT( G.List<Exp>args, Exec e ) : base( args, DataType.Binary, 
    new DataType[]{ DataType.Bigint }, e ){}

  public override DX GetDX()
  {
    var x = Arg[0].GetDL();
    return ( ee ) => ee.ResultSet.FileContent( (int)x( ee ) );
  }
} // end class FILECONTENT

} // end namespace SQLNS
