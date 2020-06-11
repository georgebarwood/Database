namespace SQLNS
{

using G = System.Collections.Generic;
using DBNS;

// Standard functions.

class StdExp : Exp
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

  public override DataType Bind( SqlExec e )
  {
    for ( int i = 0; i < Types.Length; i += 1 )
    {
      DataType t = Arg[i].Bind( e );
      if ( t != Types[ i ] ) 
        e.Error( this + " parameter type error, arg " + i + " expected " 
         + DTI.Name( Types[ i ] ) + " actual " + DTI.Name( t ) );
    }
    return Type;
  }
}   
class EXCEPTION : Exp
{
  public EXCEPTION( G.List<Exp> parms, Exec e )
  {
    if ( parms.Count != 0 ) e.Error( "EXCEPTION takes no parameters" );
    Type = DataType.String;
  }

  public override Value Eval( EvalEnv e  )
  {
    Value result = new Value();
    var ex = e.ResultSet.Exception;
    result.O = ex == null ? "" : ex.ToString(); // .Message or .ToString() for full debug info.
    e.ResultSet.Exception = null;
    return result;
  }
} // end class EXCEPTION

class REPLACE : StdExp
{
  public REPLACE( G.List<Exp>args, Exec e ) : base( args, DataType.String, 
    new DataType[]{ DataType.String, DataType.String, DataType.String }, e ){}

  public override Value Eval( EvalEnv e )
  {
    Value s = Arg[0].Eval( e );
    string pat = (string)( Arg[1].Eval( e )._O );
    string sub = (string)( Arg[2].Eval( e )._O );
    s.O = ((string)s._O).Replace( pat, sub );
    return s; 
  }

} // end class REPLACE

class SUBSTRING : StdExp
{
  public SUBSTRING( G.List<Exp>args, Exec e ) : base( args, DataType.String, 
    new DataType[]{ DataType.String, DataType.Bigint, DataType.Bigint }, e ){}

  public override Value Eval( EvalEnv e )
  {
    Value s = Arg[0].Eval( e );
    string x = (string)s._O;
    int start = (int) Arg[1].Eval( e ).L - 1;
    int len = (int) Arg[2].Eval( e ).L;

    if ( start < 0 ) start = 0;
    if ( start > x.Length ) start = x.Length;
    if ( len < 0 ) len = 0; // Maybe should raise exception
    if ( len > x.Length - start ) len = x.Length - start;

    s.O = ((string)s._O).Substring( start, len );

    return s; 
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

  public override DataType Bind( SqlExec e )
  {
    SType = E.Bind( e );
    if ( SType != DataType.String && SType != DataType.Binary ) e.Error( "LEN argument must be string or binary" );
    return Type;
  }   

  public override Value Eval( EvalEnv e )
  {
    object o = E.Eval( e )._O;
    Value result = new Value();
    result.L = SType == DataType.String ? ((string)o).Length : ((byte[])o).Length;
    return result;
  }
} // end class LEN

class PARSEINT : StdExp
{
  public PARSEINT( G.List<Exp>args, Exec e ) : base( args, DataType.Bigint, new DataType[]{ DataType.String }, e ){}

  public override Value Eval( EvalEnv e )
  {
    string s = (string)Arg[0].Eval( e )._O;
    try
    {
      Value result = new Value();
      result.L = long.Parse( s );
      return result;
    }
    catch ( System.Exception )
    {
      throw new System.Exception( "Error converting [" + s + "] to integer" );
    }
  }
} // end class PARSEINT

class PARSEDECIMAL : StdExp
{
  public PARSEDECIMAL( G.List<Exp>args, Exec e ) : base( args, DataType.ScaledInt, new DataType[]{ DataType.String, DataType.Bigint }, e ){}

  public override Value Eval( EvalEnv e )
  {
    string s = (string)Arg[0].Eval( e )._O;
    DataType t = (DataType)Arg[1].Eval( e ).L;
    try
    {
      Value result = new Value();
      result.L = (long) ( decimal.Parse( s ) * Util.PowerTen( DTI.Scale(t) ) );
      return result;
    }
    catch ( System.Exception )
    {
      throw new System.Exception( "Error converting [" + s + "] to decimal" );
    }
  }
} // end class PARSEDECIMAL

class PARSEDOUBLE : StdExp
{
  public PARSEDOUBLE( G.List<Exp>args, Exec e ) : base( args, DataType.Double, new DataType[]{ DataType.String }, e ){}

  public override Value Eval( EvalEnv e )
  {
    string s = (string)Arg[0].Eval( e )._O;
    try
    {
      Value result = new Value();
      result.D = double.Parse( s );
      return result;
    }
    catch ( System.Exception )
    {
      throw new System.Exception( "Error converting [" + s + "] to double" );
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

  public override Value Eval( EvalEnv e  )
  {
    Value result = new Value();
    result.L = e.ResultSet.LastIdInserted;
    return result;
  }
} // end class LASTID

class ARG : StdExp
{
  public ARG( G.List<Exp>args, Exec e ) : base( args, DataType.String, 
    new DataType[]{ DataType.Bigint, DataType.String }, e ){}

  public override Value Eval( EvalEnv e  )
  {
    int kind = (int)Arg[0].Eval( e ).L;
    string name = (string)Arg[1].Eval( e )._O;
    Value result = new Value();
    result.O = e.ResultSet.Arg( kind, name );
    return result;
  }
} // end class ARG

class FILEATTR : StdExp
{
  public FILEATTR( G.List<Exp>args, Exec e ) : base( args, DataType.String, 
    new DataType[]{ DataType.Bigint, DataType.Bigint }, e ){}

  public override Value Eval( EvalEnv e )
  {
    Value result = new Value();
    int ix = (int)Arg[0].Eval( e ).L;
    int kind = (int)Arg[1].Eval( e ).L;
    result.O = e.ResultSet.FileAttr( ix, kind );
    return result;
  }
} // end class FILEATTR

class FILECONTENT : StdExp
{
  public FILECONTENT( G.List<Exp>args, Exec e ) : base( args, DataType.Binary, 
    new DataType[]{ DataType.Bigint }, e ){}

  public override Value Eval( EvalEnv e )
  {
    Value result = new Value();
    int ix = (int)Arg[0].Eval( e ).L;
    result.O = e.ResultSet.FileContent( ix );
    return result;
  }
} // end class FILECONTENT

} // end namespace SQLNS
