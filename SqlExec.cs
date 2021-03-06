namespace SQLNS
{

using G = System.Collections.Generic;
using DBNS;

abstract class Exec // This allows for alternates to SQL in principle ( although none are anticipated ).
{
  public abstract void Error( string Message );
}

class SqlExec : Exec // Parses and Executes ( Interprets ) SQL.
{
  public static void ExecuteBatch( string sql, DatabaseImp d, ResultSet rs )
  {
    // System.Console.WriteLine("ExecuteBatch: " + sql );
    new SqlExec( sql, d, null ).Batch( rs );
  }

  public static Block LoadRoutine( bool func, string source, string name, DatabaseImp d )
  {
    // System.Console.WriteLine("LoadRoutine: " + name + " = " + source );
    SqlExec e = new SqlExec( source, d, name );
    e.B = new Block( d, func );
    e.B.Params = e.RoutineDef( func, out e.B.ReturnType );
    e.B.Complete();
    return e.B;
  }

  public static TableExpression LoadView( string source, string name, DatabaseImp d )
  {
    SqlExec e = new SqlExec( source, d, name );
    e.B = new Block( d, false );
    return e.ViewDef();
  }

  public override void Error( string error )
  {
    throw new Exception( error, ObjectName, SourceLine, SourceColumn,
      Source.Substring( TokenStart, SourceIx - TokenStart ), Source, T );
  }

  public DatabaseImp Db;
  public ColInfo CI; // Associated with current table scope.
  public bool [] Used; // Which columns in CI have been referenced in an expression.
  public Block B;

  // Rest is private

  string Source; // The source SQL
  string ObjectName; // The name of the view or routine being parsed ( null if batch statements are being parsed ).

  int SourceIx = -1; // Index of current character in Source
  int SourceLine = 1; // Current line number
  int SourceColumn = 0;  // Current column number

  char CC;      // Current character in Source as it is being parsed.
  Token T;      // Current token.
  int TokenStart; // Position in source of start of current token.

  string NS,TS; // Current name token : TS is uppercase copy of NS.
  long DecimalInt, DecimalFrac; // Details of decimal token
  int DecimalScale;

  public bool ParseOnly; // True when parsing CREATE FUNCTION or CREATE PROCEDURE.
  bool DynScope;  // When parsing SELECT or WHERE clauses, suppresses name lookup/type checking.
    
  int BreakId = -1; // Break label id for current WHILE or FOR statement.

  SqlExec( string sql, DatabaseImp db, string objectName )
  {
    Source = sql;
    Db = db;
    ObjectName = objectName;
    ReadChar();
    ReadToken(); 
  }

  void Batch( ResultSet rs ) 
  { 
    B = new Block( Db, false );
    do
    {
      while ( T != Token.Eof && !Test("GO") ) Statement(); 
      B.CheckLabelsDefined( this );
      B.Complete();
      B.ExecuteBatch( rs );
      B.Init();
    } while ( T != Token.Eof );
  }

  void Add( System.Action a )
  { 
    if ( !ParseOnly ) B.AddStatement( a ); 
  }

  public void Bind( Exp[] e )
  {
    if ( e != null ) for ( int i = 0; i < e.Length; i += 1 ) 
    {
      e[ i ].Bind( this );
    }
  }

  // ****************** Token parsing

  char ReadChar()
  {
    char cc;
    SourceIx += 1;
    if ( SourceIx >= Source.Length )
    {
      SourceIx = Source.Length;
      cc = '\0';
    }
    else
    {
      cc = Source[ SourceIx ];
      if ( cc == '\n' ) { SourceColumn = 0; SourceLine += 1; } else SourceColumn += 1;
    }
    CC = cc;
    return cc;
  }

  void ReadToken()
  {
    char cc = CC;
    SkipSpace:
    while ( cc == ' ' || cc == '\n' || cc == '\r' ) cc = ReadChar();
    TokenStart = SourceIx;
    {
      char sc = cc; 
      cc = ReadChar();
      switch( sc )
      {
        case 'A': case 'B': case 'C': case 'D': case 'E': case 'F': case 'G': case 'H': case 'I': case 'J': case 'K': case 'L': case 'M':
        case 'N': case 'O': case 'P': case 'Q': case 'R': case 'S': case 'T': case 'U': case 'V': case 'W': case 'X': case 'Y': case 'Z':
        case 'a': case 'b': case 'c': case 'd': case 'e': case 'f': case 'g': case 'h': case 'i': case 'j': case 'k': case 'l': case 'm':
        case 'n': case 'o': case 'p': case 'q': case 'r': case 's': case 't': case 'u': case 'v': case 'w': case 'x': case 'y': case 'z':
        case '@':
        {
          T = Token.Name;
          while ( cc >= 'A' && cc <= 'Z' || cc >= 'a' && cc <= 'z' || cc == '@' ) cc = ReadChar();
          NS = Source.Substring( TokenStart, SourceIx - TokenStart );
          TS = NS.ToUpper();
          break;
        }
        case '0': case '1': case '2': case '3': case '4': case '5': case '6': case '7': case '8': case '9':
        {
          T = Token.Number;
          char fc = Source[ TokenStart ];
          if ( fc == '0' && cc == 'x' )
          {
            cc = ReadChar();
            T = Token.Hex;
            while ( cc >= '0' && cc <= '9' || cc >= 'A' && cc <= 'F' || cc >= 'a' && cc <= 'f') cc = ReadChar();
          }
          else
          {
            while ( cc >= '0' && cc <= '9' ) cc = ReadChar();  
            int part1 = SourceIx;
            DecimalInt = long.Parse( Source.Substring( TokenStart, part1 - TokenStart ) );
            if ( cc == '.' && T == Token.Number )
            {
              T = Token.Decimal;
              cc = ReadChar();
              while ( cc >= '0' && cc <= '9' ) cc = ReadChar();   
              DecimalScale = SourceIx - ( part1 + 1 );
              DecimalFrac = long.Parse( Source.Substring( part1 + 1, DecimalScale ) );
            }
            else 
            {
              DecimalScale = 0;
              DecimalFrac = 0;
            }
          }
          NS = Source.Substring( TokenStart, SourceIx - TokenStart );
          break;
        }
        case '[':   
        { 
          T = Token.Name;
          int start = SourceIx; NS = "";
          while ( cc != '\0' )
          {
            if ( cc == ']' )
            {
              cc = ReadChar();
              if ( cc != ']' ) break;
              NS = NS + Source.Substring( start, SourceIx-start-1 );
              start = SourceIx;
            }
            cc = ReadChar();
          }
          NS = NS + Source.Substring( start, SourceIx-start-1 );
          TS = NS.ToUpper();
          break;
        }
        case '\'' :
        {
          T = Token.String;
          int start = SourceIx; NS = "";
          while ( cc != '\0' )
          {
            if ( cc == '\'' )
            {
              cc = ReadChar();
              if ( cc != '\'' ) break;
              NS = NS + Source.Substring( start, SourceIx-start-1 );
              start = SourceIx;
            }
            cc = ReadChar();
          }
          NS = NS + Source.Substring( start, SourceIx-start-1 );
          break;
        }
        case '-' : T = Token.Minus; 
          if ( cc == '-' ) // Skip single line comment.
          {
            while ( cc != '\n' && cc != '\0' ) cc = ReadChar();
            goto SkipSpace;
          }
          break;
        case '/' : T = Token.Divide;
          if ( cc == '*' )  // Skip comment.
          {           
            cc = ReadChar();
            char prevchar = 'X';
            while ( ( cc != '/' || prevchar != '*' ) && cc != '\0' )
            {
              prevchar = cc;
              cc = ReadChar();
            }
            cc = ReadChar();
            goto SkipSpace;
          }
          break;
        case '>' : 
          T = Token.Greater; 
          if ( cc == '=' ) { T = Token.GreaterEqual; cc = ReadChar(); } 
          break;
        case '<' : 
          T = Token.Less; 
          if ( cc == '=' ) { T = Token.LessEqual; cc = ReadChar(); } 
          else if ( cc == '>' ) { T = Token.NotEqual; cc = ReadChar(); } 
          break;
        case '!' : 
          T = Token.Exclamation; 
          if ( cc == '=' ) { T = Token.NotEqual; cc = ReadChar(); }
          break;
        case '(' : T = Token.LBra; break;
        case ')' : T = Token.RBra; break;
        case '|' : T = Token.VBar; break;
        case ',' : T = Token.Comma; break;
        case '.' : T = Token.Dot; break;
        case '=' : T = Token.Equal; break;
        case '+' : T = Token.Plus; break;
        case ':' : T = Token.Colon; break;
        case '*' : T = Token.Times; break;
        case '%' : T = Token.Percent; break;
        case '\0' : T = Token.Eof; break;
        default: T = Token.Unknown; Error( "Unrecognised character" ); break;
      }
    }
  }

  // ****************** Help functions for parsing.

  DataType GetExactDataType( string tname )
  {
    switch ( tname )
    {
      case "int"      : return DataType.Int;
      case "string"   : return DataType.String;
      case "binary"   : return DataType.Binary;
      case "tinyint"  : return DataType.Tinyint;
      case "smallint" : return DataType.Smallint;
      case "bigint"   : return DataType.Bigint;
      case "float"    : return DataType.Float;
      case "double"   : return DataType.Double;
      case "bool"     : return DataType.Bool;
      case "decimal"  :
         int p = 18; int s = 0;
         if ( Test( Token.LBra ) )
         {
           p = ReadInt();
           if ( p < 1 ) Error( "Minimum precision is 1" );
           if ( p > 18 ) Error( "Maxiumum decimal precision is 18" );
           if ( Test( Token.Comma) ) s = ReadInt();
           if ( s < 0 ) Error( "Scale cannot be negative" );
           if ( s > p ) Error( "Scale cannot be greater than precision" );
           Read( Token.RBra );
         }
         return DTI.Decimal( p, s );
      default: Error( "Datatype expected" ); return DataType.None;
    }
  }

  DataType GetDataType( string s )
  {
    return DTI.Base( GetExactDataType( s ) );
  }

  Token GetOperator( out int prec )
  {
    Token result = T;    
    if ( result >= Token.Name )
    {
      if ( result == Token.Name )
      {
        if ( TS == "AND" ) result = Token.And;
        else if ( TS == "OR" ) result = Token.Or;
        else if ( TS == "IN" ) result = Token.In;
        else { prec = -1; return result; }
      }
      else { prec = -1; return result; }
    }
    prec = TokenInfo.Precedence[(int)result];
    return result;
  }

  string Name()
  {
    if ( T != Token.Name ) Error ("Name expected");
    string result = NS;
    ReadToken();
    return result;
  }

  bool Test( Token t )
  {
    if ( T != t ) return false;
    ReadToken();
    return true;
  }

  int ReadInt()
  {
    if ( T != Token.Number ) Error( "Number expected" );
    int result = int.Parse(NS);
    ReadToken();
    return result;
  }

  void Read( Token t )
  {
    if ( T != t ) Error ( "Expected \"" + TokenInfo.Name( t ) + "\"" );
    ReadToken();
  }

  bool Test( string t )
  {
    if ( T != Token.Name || TS != t ) return false;
    ReadToken();
    return true;
  }

  void Read( string ts )
  {
    if ( T != Token.Name || TS != ts ) Error( "Expected " + ts );
    ReadToken();
  }

  // End Help functions for parsing.

  // ****************** Expression parsing

  Exp NameExp( bool AggAllowed )
  {
    Exp result = null;
    string name = NS;
    ReadToken();
    if ( Test(Token.Dot) )
    {
      string fname = Name();
      var parms = new G.List<Exp>();
      Read( Token.LBra );
      if ( T != Token.RBra) do
      {
        parms.Add( Exp() );
      } while ( Test( Token.Comma ) );
      Read( Token.RBra );
      result = new ExpFuncCall( name, fname, parms );
    }
    else if ( Test( Token.LBra ) )
    {
      var parms = new G.List<Exp>();
      if ( T != Token.RBra) do
      {
        parms.Add( Exp() );
      } while ( Test( Token.Comma ) );
      Read( Token.RBra );
      if ( AggAllowed && name == "COUNT" )
      {
        if ( parms.Count > 0 ) Error( "COUNT does have any parameters" );
        result = new COUNT();
      }
      else if ( AggAllowed && name == "SUM" ) result = new ExpAgg( AggOp.Sum, parms, this );
      else if ( AggAllowed && name == "MIN" ) result = new ExpAgg( AggOp.Min, parms, this );
      else if ( AggAllowed && name == "MAX" ) result = new ExpAgg( AggOp.Max, parms, this );
      else if ( name == "PARSEINT" ) result = new PARSEINT( parms, this );
      else if ( name == "PARSEDOUBLE" ) result = new PARSEDOUBLE( parms, this );
      else if ( name == "PARSEDECIMAL" ) result = new PARSEDECIMAL( parms, this );
      else if ( name == "LEN" ) result = new LEN( parms, this );
      else if ( name == "REPLACE" ) result = new REPLACE( parms, this );  
      else if ( name == "SUBSTRING" ) result = new SUBSTRING( parms, this );
      else if ( name == "EXCEPTION" ) result = new EXCEPTION( parms, this );
      else if ( name == "LASTID" ) result = new LASTID( parms, this );
      else if ( name == "GLOBAL" ) result = new GLOBAL( parms, this );   
      else if ( name == "ARG" ) result = new ARG( parms, this );   
      else if ( name == "ARGNAME" ) result = new ARGNAME( parms, this );    
      else if ( name == "FILEATTR" ) result = new FILEATTR( parms, this );
      else if ( name == "FILECONTENT" ) result = new FILECONTENT( parms, this );
      else Error( "Unknown function : " + name );
    }
    else if ( name == "true" ) result = new ExpConstant(true);
    else if ( name == "false" ) result = new ExpConstant(false);
    else
    {
      int i = B.Lookup( name );
      if ( i < 0 )
      {
        if ( DynScope )
          result = new ExpName( name );
        else 
          Error( "Undeclared local : " + name ); 
      }
      else result = new ExpLocalVar( i, B.LocalTypeList[ i ], name );
    }
    return result;
  }

  Exp Primary( bool AggAllowed )
  {
    Exp result = null;
    if ( T == Token.Name )
    {
      result = 
        Test( "CASE" ) ? Case() 
        : Test( "NOT" ) ? new ExpNot( Exp(10) ) // Not sure about precedence here.
        : NameExp( AggAllowed );
    }
    else if ( Test( Token.LBra ) )
    {
      if ( Test( "SELECT" ) )
        result = ScalarSelect();
      else
      {
        result = Exp();
        if ( Test( Token.Comma ) ) // Operand of IN e.g. X IN ( 1,2,3 )
        {
          var list = new G.List<Exp>();
          list.Add( result );
          do
          {
            list.Add( Exp() );
          } while ( Test( Token.Comma ) );
          result = new ExpList( list );
        }
      }
      Read( Token.RBra );
    }
    else if ( T == Token.String )
    {
      result = new ExpConstant( NS );
      ReadToken();
    }
    else if ( T == Token.Number || T == Token.Decimal )
    {      
      long value = DecimalInt;
      if ( DecimalScale > 0 ) value = value * (long)Util.PowerTen( DecimalScale ) + DecimalFrac;
      result = new ExpConstant( value, DecimalScale > 0 ? DTI.Decimal( 18, DecimalScale ) : DataType.Bigint );
      ReadToken();
    }
    else if ( T == Token.Hex )
    {
      if ( ( NS.Length & 1 ) != 0 ) Error( "Hex literal must have even number of characters" );
      result = new ExpConstant( Util.ParseHex(NS) );
      ReadToken();
    }
    else if ( Test( Token.Minus ) )
    {
      result = new ExpMinus( Exp( 30 ) );
    }
    else Error( "Expression expected" );
    return result;
  }

  Exp ExpOrAgg()
  {
    Exp result = Exp( Primary( true ), 0 );
    if ( !ParseOnly && !DynScope ) result.Bind( this );
    return result;
  }

  Exp Exp()
  {
    return Exp(0);
  }

  Exp Exp( int prec )
  {
    Exp result = Exp( Primary( false ), prec );
    if ( !ParseOnly && !DynScope ) result.Bind( this );
    return result;
  }
  
  Exp Exp( Exp lhs, int precedence )
  {
    int prec_t; Token t = GetOperator( out prec_t );
    while ( prec_t >= precedence )
    {
      int prec_op = prec_t; Token op = t;
      ReadToken();
      Exp rhs = Primary( false );
      t = GetOperator( out prec_t );
      while ( prec_t > prec_op /* or t is right-associative and prec_t == prec_op */ )
      {
        rhs = Exp( rhs, prec_t );
        t = GetOperator( out prec_t );
      }
      lhs = BinaryOp( op, lhs, rhs );
    }
    return lhs;
  }

  Exp BinaryOp( Token op, Exp lhs, Exp rhs )
  {
    if ( op == Token.In )
      return new ExpIn( lhs, rhs );
    else
      return new ExpBinary( op, lhs, rhs );
  }

  Exp Case()
  {
    var list = new G.List<CASE.Part>();
    while ( Test( "WHEN" ) )
    {
      Exp test = Exp(); Read( "THEN" ); Exp e = Exp();
      list.Add( new CASE.Part( test, e ) );          
    }
    if ( list.Count == 0 ) Error( "Empty Case Expression" );
    Read( "ELSE" );
    list.Add( new CASE.Part( null, Exp() ) );
    Read( "END" );
    return new CASE( list.ToArray() );
  }

  Exp ScalarSelect()
  {
    TableExpression te = Expressions( null );
    if ( te.ColumnCount != 1 ) Error ( "Scalar select must have one column" );
    return new ScalarSelect( te );    
  }

  // ****************** Table expression parsing

  TableExpression InsertExpression()
  {
    if ( Test( "VALUES" ) ) return Values();
    else if ( Test( "SELECT") ) return Expressions( null );
    else Error( "VALUES or SELECT expected" );
    return null;
  }

  TableExpression Values()
  {
    var values = new G.List<Exp[]>();
    DataType [] types = null;
    while ( true )
    {
      Read( Token.LBra );
      var v = new G.List<Exp>();;
      while ( true )
      {
        v.Add( Exp() );
        if ( Test( Token.RBra ) ) break;
        if ( T != Token.Comma ) Error( "Comma or closing bracket expected" );
        ReadToken();
      }
      if ( types == null )
      {
        types = new DataType[ v.Count ];
        for ( int i = 0; i < v.Count; i += 1 ) types[ i ] = v[ i ].Type;
      }
      else
      {
        if ( types.Length != v.Count ) Error( "Inconsistent number of values" );
      }
      values.Add( v.ToArray() );
      if  ( !Test( Token.Comma ) && T != Token.LBra ) break; // The comma between multiple VALUES is optional.           
    }  
    return new ValueTable( types.Length, values );
  }

  TableExpression Expressions( G.List<int> assigns )
  {
    // assigns has the indexes of local variables being assigned in a SET or FOR statement.
    bool save = DynScope; DynScope = true; // Suppresses Binding of expressions until table is known.
    var exps = new G.List<Exp>();
    do
    {
      if ( assigns != null )
      {
        var name = Name();
        int i = B.Lookup( name );
        if ( i < 0 ) Error( "Undeclared local variable : " + name );
        Read( Token.Equal ); 
        if ( assigns.Contains( i ) ) Error( "Duplicated local name in SET or FOR" );       
        assigns.Add( i );
      }
      Exp exp = ExpOrAgg();
      if ( Test( "AS" ) ) exp.Name = Name();
      exps.Add( exp );
    } while ( Test( Token.Comma ) );

    TableExpression te = Test( "FROM" ) ? PrimaryTableExp() : new DummyFrom();

    if ( ObjectName == null ) te.CheckNames( this );

    Exp where = Test( "WHERE" ) ? Exp() : null;

    Exp[] group = null;
    if ( Test( "GROUP" ) )
    {
      var list = new G.List<Exp>();
      Read( "BY" );
      do
      {
        Exp exp = Exp();
        list.Add( exp );
      } while ( Test( Token.Comma ) );
      group = list.ToArray();
    }
    OrderByExp[] order = OrderBy();
    DynScope = save;

    TableExpression result;

    if ( !ParseOnly )
    {
      var save1 = Used; var save2 = CI;
      te = te.Load( this );
      CI = te.CI;
      Used = new bool[ CI.Count ]; // Bitmap of columns that are referenced by any expression.

      for ( int i=0; i<exps.Count; i+=1 ) 
      {
        if ( exps[ i ].GetAggOp() != AggOp.None )
        {
          if ( group == null ) 
          {
            group = new Exp[0];
          }
          exps[ i ].BindAgg( this );
        }
        else 
        { 
          exps[ i ].Bind( this );
        }
      }

      if ( where != null )
      {
        where.Bind( this );
        if ( where.Type != DataType.Bool ) Error( "WHERE expression must be boolean" );
      }
      
      Bind( group );

      result = new Select( exps, te, where, group, order, Used, this );
      
      if ( assigns != null ) // Convert the Rhs of each assign to be compatible with the Lhs.
      {
        var types = new DataType[ assigns.Count ];
        for ( int i = 0; i < assigns.Count; i += 1 )
          types[ i ] = B.LocalTypeList[ assigns[ i ] ];
        result.Convert( types, this );
      }

      Used = save1; CI = save2;
    }
    else result = new Select( exps, te, where, group, order, null, this ); // Potentially used by CheckNames
    return result;
  }

  ViewOrTable TableName()
  {
    if ( T == Token.Name )
    {
      var n1 = NS;
      ReadToken();
      if ( T == Token.Dot )
      {
        ReadToken();
        if ( T == Token.Name )
          return new ViewOrTable( n1, Name() );
      }
    }
    Error( "Table or view name expected" );
    return null;
  }

  TableExpression PrimaryTableExp()
  {
    if ( T == Token.Name ) return TableName();
    else if ( Test( Token.LBra ) )
    {
      Read( "SELECT" );
      TableExpression te = Expressions( null );
      Read( Token.RBra );
      if ( Test("AS") ) te.Alias = Name();
      return te;
    }
    Error( "Table expected" );
    return null;
  }

  OrderByExp [] OrderBy()
  {
    if ( Test( "ORDER" ) )
    {
      var list = new G.List<OrderByExp>();
      Read("BY");
      do
      {
        list.Add( new OrderByExp( Exp(), Test("DESC") ) );
      } while ( Test( Token.Comma) );
      return list.ToArray();
    }
    return null;
  }

  // ****************** Statement parsing

  TableExpression Select( bool exec )
  {
    var te = Expressions( null );
    var b = B;
    if ( exec ) Add( () => b.Select( te ) );
    return te;
  }

  void Set()
  {
    var locals = new G.List<int>();
    var te = Expressions( locals );
    var la = locals.ToArray();
    var b = B;
    Add( () => b.Set( te, la ) );
  }

  void Insert()
  {
    Read( "INTO" );
    string schema = Name();
    Read( Token.Dot );
    string tableName = Name();
    Read( Token.LBra );
    var names = new G.List<string>();
    while ( true )
    {
      string name = Name();
      if ( names.Contains( name ) ) Error( "Duplicate name in insert list" );
      names.Add( name );
      if ( Test( Token.RBra ) ) break;
      if ( T != Token.Comma ) Error( "Comma or closing bracket expected" );
      ReadToken();
    }

    TableExpression src = InsertExpression();  
    if ( src.ColumnCount != names.Count ) Error( "Insert count mismatch" );

    if ( !ParseOnly )
    {
      var t = Db.GetTable( schema, tableName, this );

      int[] colIx = new int[names.Count];
      int idCol = -1;

      var types = new DataType[ names.Count ];
      for ( int i=0; i < names.Count; i += 1 ) 
      {
        int ci = t.ColumnIx( names[ i ], this );
        if ( ci == 0 ) idCol = i;
        colIx[ i ] = ci;
        types[ i ] = t.CI.Type[ ci ];
      }
      src.Convert( types, this );
      var b = B;
      Add( () => b.Insert( t, src, colIx, idCol ) );
    }
  }

  struct Assign // Is this really needed now?
  {
    public ExpName Lhs;
    public Exp Rhs;
    public Assign( string name, Exp rhs ) { Lhs = new ExpName(name); Rhs = rhs; }
  }

  void Update()
  {
    bool save = DynScope; DynScope = true;
    var te = TableName();
    Read( "SET" );
    var alist = new G.List<Assign>();
    do
    {
      var name = Name();
      Read( Token.Equal );
      var exp = Exp();
      alist.Add( new Assign( name, exp ) );
    } while ( Test( Token.Comma ) );
    var a = alist.ToArray();
    var where = Test( "WHERE" ) ? Exp() : null;
    if ( where == null ) Error( "UPDATE must have a WHERE" );
    DynScope = save;

    if ( !ParseOnly )
    {
      Table t = Db.GetTable( te.Schema, te.Name, this );

      var save1 = Used; var save2 = CI;
      Used = new bool[ t.CI.Count ]; // Bitmap of columns that are referenced by any expression.
      CI = t.CI;      

      int idCol = -1;
      for ( int i=0; i < a.Length; i += 1 ) 
      {        
        a[ i ].Lhs.Bind( this );
        a[ i ].Rhs.Bind( this );

        if ( a[ i ].Lhs.Type != a[ i ].Rhs.Type )
        {
          Exp conv = a[ i ].Rhs.Convert( a[ i ].Lhs.Type );
          if ( conv == null ) Error( "Update type mismatch" );
          else a[ i ].Rhs = conv;
        }
        if ( a[ i ].Lhs.ColIx == 0 ) idCol = i;
      }
      if ( where != null )
      {
        where.Bind( this );
        if ( where.Type != DataType.Bool ) Error( "WHERE expression must be boolean" );
      }
      var whereDb = where.GetDB();
      var dvs = new Exp.DV[ a.Length ];
      var ixs = new int[ a.Length ];
      for ( int i = 0; i < a.Length; i += 1 )
      {
        ixs[ i ] = a[ i ].Lhs.ColIx;
        dvs[ i ] = a[ i ].Rhs.GetDV();
      }

      var ids = where.GetIdSet( t );
      Add( () => t.Update( ixs, dvs, whereDb, idCol, ids, B ) ); 

      Used = save1; CI = save2; 
    }
  }

  void Delete()
  {
    bool save = DynScope; DynScope = true;
    Read( "FROM" );
    var te = TableName();
    Exp where = Test( "WHERE" ) ? Exp() : null;
    if ( where == null ) Error( "DELETE must have a WHERE" );
    DynScope = save;

    if ( !ParseOnly )
    {
      Table t = Db.GetTable( te.Schema, te.Name, this );
      var save1 = Used; var save2 = CI;
      Used = new bool[ t.CI.Count ]; // Bitmap of columns that are referenced by any expression.
      CI = t.CI;

      if ( where != null )
      {
        where.Bind( this );
        if ( where.Type != DataType.Bool ) Error( "WHERE expression must be boolean" );
      }
      var whereDb = where.GetDB();
      var ids = where.GetIdSet( t );

      Add( () => t.Delete( whereDb, ids, B ) );

      Used = save1; CI = save2;
    }
  }

  void Execute()
  {
    Read( Token.LBra );
    var exp = Exp();
    Read( Token.RBra );
    if ( !ParseOnly )
    {
      exp.Bind( this );
      if ( exp.Type != DataType.String ) Error( "Argument of EXECUTE must be a string" );
      var ds = exp.GetDS();
      var b = B;
      Add( () => b.Execute( ds ) );
    }
  }

  void Exec()
  {
    string name = Name();
    string schema = null;
    if ( Test( Token.Dot ) )
    {
      schema = name;
      name = Name();
    }
    Read( Token.LBra );
    var parms = new G.List<Exp>();

    if ( !Test( Token.RBra ) )
    {
      parms.Add( Exp() );
      while ( Test( Token.Comma ) ) parms.Add( Exp() );
      Read( Token.RBra );
    }

    if ( schema != null )
    {
      if ( !ParseOnly )
      {
        var b = Db.GetRoutine( schema, name, false, this );

        // Check parameter types.
        if ( b.Params.Count != parms.Count ) Error( "Param count error calling " + name + "." + name );
        for ( int i = 0;  i < parms.Count; i += 1 )
          if ( parms[ i ].Type != b.Params.Type[ i ] ) 
            Error( "Parameter Type Error calling procedure " + name );


        var pdv = Util.GetDVList( parms.ToArray() );
        var caller = B;
        Add( () => b.ExecuteRoutine( caller, pdv ) ); 
      }   
    }
    else if ( name == "SETMODE" )
    {
      if ( parms.Count != 1 ) Error ( "SETMODE takes one param" );
      if ( !ParseOnly )
      {
        parms[ 0 ].Bind( this );
        if ( parms[ 0 ].Type != DataType.Bigint ) Error( "SETMODE param error" );
        var dl = parms[0].GetDL();
        var b = B;
        Add( () => b.SetMode( dl ) );
      }
    }
    else Error( "Unrecognised procedure" );
  }

  void For()
  {
    var locals = new G.List<int>();
    TableExpression te = Expressions( locals );

    var b = B;
    int forid = b.GetForId();
    var la = locals.ToArray();
    Add( () => b.InitFor( forid, te, la ) );
    int start = b.GetStatementId();
    int breakid = b.GetJumpId();
    Add( () => b.For( forid, breakid ) );
    
    int save = BreakId;
    BreakId = breakid;
    Statement();
    BreakId = save;

    Add( () => b.JumpBack( start ) );
    b.SetJump( breakid );
  }

  // ****************** Create Statements

  void CreateTable()
  {
    string schema = Name();
    Read( Token.Dot );
    string tableName = Name();
    int sourceStart = SourceIx-1;
    Read( Token.LBra );
    var names = new G.List<string>();
    var types = new G.List<DataType>();
    while ( true )
    {
      var name = Name();
      if ( names.Contains( name ) ) Error ( "Duplicate column name" );
      names.Add( name );
      types.Add( GetExactDataType( Name() ) );
      if ( Test( Token.RBra ) ) break;
      if ( T != Token.Comma ) Error( "Comma or closing bracket expected" );
      ReadToken();
    }
    string source = Source.Substring( sourceStart, TokenStart - sourceStart );
    var ci = ColInfo.New( names, types );
    Add( () => Db.CreateTable( schema, tableName, ci, source, false, false, this ) );
  }

  void CreateView( bool alter )
  {
    string schema = Name();
    Read( Token.Dot );
    string viewName = Name();
    Read( "AS" );
    int sourceStart = TokenStart;
    Read( "SELECT" );
    var save = ParseOnly;
    ParseOnly = true;
    var se = Select( false );
    ParseOnly = save;
    se.CheckNames( this );
    string source = Source.Substring( sourceStart, TokenStart - sourceStart );
    Add( () => Db.CreateTable( schema, viewName, se.CI, source, true, alter, this ) );
  }

  TableExpression ViewDef()
  {
    Read( "SELECT" );
    return Select( false );
  }

  void CreateRoutine( bool isFunc, bool alter )
  {
    string schema = Name();
    Read( Token.Dot );
    string routineName = Name();
    int sourceStart = SourceIx-1;

    Block save1 = B; bool save2 = ParseOnly; 
    B = new Block( B.Db, isFunc ); ParseOnly = true;
    DataType retType; var parms = RoutineDef( isFunc, out retType );
    B = save1; ParseOnly = save2;

    string source = Source.Substring( sourceStart, TokenStart - sourceStart );
    Add( () => Db.CreateRoutine( schema, routineName, source, isFunc, alter, this ) );
  }

  ColInfo RoutineDef( bool func, out DataType retType )
  {
    var names = new G.List<string>();
    var types = new G.List<DataType>();

    Read( Token.LBra );
    while ( T == Token.Name )
    {
      string name = Name();
      DataType type = GetDataType( Name() );
      names.Add( name ); 
      types.Add( type );
      B.Declare( name, type );
      if ( T == Token.RBra ) break;      
      if ( T != Token.Comma ) Error( "Comma or closing bracket expected" );
      ReadToken();
    }
    Read( Token.RBra );
    if ( func ) 
    { 
      Read( "RETURNS" );
      retType = GetDataType( Name() );
    } else retType = DataType.None;
    Read( "AS" );
    Read( "BEGIN" );
    Begin();
    B.CheckLabelsDefined( this );
    return ColInfo.New( names, types );
  }

  void CreateIndex()
  { 
    string indexname = Name();
    Read( "ON" );
    string schema = Name();
    Read( Token.Dot );
    string tableName = Name();
    Read( Token.LBra );
    var names = new G.List<string>();
    while ( true )
    {
      names.Add( Name() );
      if ( Test( Token.RBra ) ) break;
      if ( T != Token.Comma ) Error( "Comma or closing bracket expected" );
      ReadToken();
    }
    Add( () => Db.CreateIndex( schema, tableName, indexname, names.ToArray(), this ) );
  }

  void Create()
  {
    if ( Test( "FUNCTION" ) ) CreateRoutine( true, false );      
    else if ( Test( "PROCEDURE" ) ) CreateRoutine( false, false );
    else if ( Test( "TABLE" ) ) CreateTable();
    else if ( Test( "VIEW" ) ) CreateView( false );
    else if ( Test ("SCHEMA" ) )
    {
      string name = Name();
      Add( () => Db.CreateSchema( name, this ) );
    }
    else if ( Test ("INDEX" ) ) CreateIndex();
    else Error( "Unknown keyword" );
  }

  void Alter()
  {
    if ( Test( "TABLE" ) ) AlterTable();
    else if ( Test( "VIEW" ) ) CreateView( true );
    else if ( Test( "FUNCTION" ) ) CreateRoutine( true, true );      
    else if ( Test( "PROCEDURE" ) ) CreateRoutine( false, true );
    else Error ("ALTER : TABLE,VIEW.. expected");
  }

  void Drop() 
  {
    if ( Test( "TABLE" ) )
    {
      var s = Name();
      Read( Token.Dot );
      var n = Name();
      Add ( () => Db.DropTable( s, n, this ) );
    }
    else if ( Test( "VIEW" ) )
    {
      var s = Name();
      Read( Token.Dot );
      var n = Name();
      Add ( () => Db.DropView( s, n, this ) );
    }
    else if ( Test( "INDEX" ) )
    {
      var ix = Name();
      Read( "ON" );
      var s = Name();
      Read( Token.Dot );
      var n = Name();
      Add( () => Db.DropIndex( s, n, ix, this ) );
    }
    else if ( Test( "FUNCTION" ) )
    {
      var s = Name();
      Read( Token.Dot );
      var n = Name();
      Add( () => Db.DropRoutine( s, n, true, this ) );
    }  
    else if ( Test( "PROCEDURE" ) )
    {
      var s = Name();
      Read( Token.Dot );
      var n = Name();
      Add( () => Db.DropRoutine( s, n, false, this ) );
    }      
    else if ( Test( "SCHEMA" ) )
    {
      var s = Name();
      Add( () => Db.DropSchema( s, this ) );
    }
    else Error( "DROP : TABLE,VIEW.. expected" );
  }

  void Rename()
  {
    string objtype = TS;
    if ( Test("SCHEMA") )
    {
      var name = Name();
      Read( "TO" );
      var newname = Name();
      Add( () => Db.RenameSchema( name, newname, this) );
    }
    else if ( Test("TABLE") | Test("VIEW") | Test("PROCEDURE") | Test ("FUNCTION") )
    {
      var sch = Name();
      Read( Token.Dot );
      var name = Name();
      Read( "TO" );
      var sch1 = Name();
      Read( Token.Dot );
      var name1 = Name();
      Add( () => Db.RenameObject( objtype, sch, name, sch1, name1, this ) );
    }
    else Error( "RENAME : TABLE,VIEW.. expected" );
  }


  void AlterTable()
  {
    string schema = Name();
    Read( Token.Dot );
    string tableName = Name();
    
    var list = new G.List<AlterAction>();
    var action = new AlterAction();
   
    do
    {
      if ( Test("ADD" ) )
      {
        action.Operation = Action.Add;
        action.Name = Name();
        action.Type = GetExactDataType( Name() );
      }
      else if ( Test("DROP" ) )
      {
        action.Operation = Action.Drop;
        action.Name = Name();
      }
      else if ( Test("RENAME") )
      {
        action.Operation = Action.ColumnRename;
        action.Name = Name();
        Read( "TO" );
        action.NewName = Name();
      }
      else if ( Test("MODIFY" ) )
      {
        action.Operation = Action.Modify;
        action.Name = Name();
        action.Type = GetExactDataType( Name() );          
      }
      else break;
      list.Add( action );
    } while ( Test( Token.Comma ) );
    Add( () => Db.AlterTable( schema, tableName, list, this ) );
  }

  void Throw()
  {
    var msg = Exp();
    if ( !ParseOnly )
    {
      msg.Bind( this );
      if ( msg.Type != DataType.String ) Error( "THROW type error" );
      Exp.DS m = msg.GetDS();
      var b = B;
      Add( () => b.Throw( m ) );
    }
  }    

  // Other statements.

  void Declare()
  {
    do
    {
      var name = Name();
      if ( B.Lookup( name ) >= 0 ) Error( "Duplicate declation of " + name );
      var type = GetDataType( Name() );
      B.Declare( name, type );
    } while ( Test( Token.Comma ) );
  }

  void While()
  {
    var exp = Exp();
    var b = B;
    
    int start = B.GetStatementId();
    int breakid = B.GetJumpId();
    if (!ParseOnly)
    {
      exp.Bind( this );
      if ( exp.Type != DataType.Bool ) Error( "WHILE expression must be boolean" );
      var db = exp.GetDB();
      Add( () => b.If( db, breakid ) );
    }
    
    int save = BreakId;
    BreakId = breakid;
    Statement();
    BreakId = save;
    Add( () => b.JumpBack( start ) );
    b.SetJump( breakid );
  }

  void If()
  {
    var exp = Exp();
    var b = B;

    int falseid = b.GetJumpId();
    if (!ParseOnly)
    {
      exp.Bind( this );
      if ( exp.Type != DataType.Bool ) Error( "IF expression must be boolean" );
      var db = exp.GetDB();
      Add( () => b.If( db, falseid ) );
    }

    Statement();

    if ( Test("ELSE") ) 
    {
      int endid = b.GetJumpId();
      Add( () => b.Goto( endid ) ); // Skip over the else clause
      b.SetJump( falseid );
      Statement();
      b.SetJump( endid );
    }  
    else b.SetJump( falseid );  
  }

  void Goto()
  {
    string label = Name();
    Add( B.GetGoto( label ) );
  }

  void Break()
  {
    int breakId = BreakId; // Need to take a copy of current value.
    if ( breakId < 0 ) Error( "No enclosing loop for break" );
    var b = B;
    Add( () => b.Goto( breakId ) );
  }

  void Return()
  {
    var b = B;
    Exp e = b.IsFunc ? Exp() : null;
    if ( !ParseOnly )
    {
      if ( e != null ) e.Bind( this );
      if ( e != null && B.ReturnType != e.Type ) 
      {
        e = e.Convert( B.ReturnType );
        if ( e == null ) 
        {
          Error( "Return type error" );   
        }     
      }
      var dv = e == null ? null : e.GetDV();
      Add( () => b.Return( dv ) );
    }
  }

  void Begin() 
  { 
    while ( !Test( "END" ) ) Statement(); 
  }

  void Statement()
  {
    if ( T == Token.Name ) 
    {
      string ts = TS;
      ReadToken();
      if ( Test( Token.Colon ) )
      {
        if ( B.SetLabel( ts ) ) Error( "Label " + ts + " already defined" );
      }
      else switch ( ts )
      {  
        case "ALTER":   Alter(); break;
        case "BEGIN":   Begin(); break;
        case "BREAK":   Break(); break;
        case "CREATE":  Create(); break;
        case "DROP":    Drop(); break;
        case "DECLARE": Declare(); break;
        case "DELETE":  Delete(); break;
        case "EXEC":    Exec(); break;
        case "EXECUTE": Execute(); break;
        case "FOR":     For(); break;
        case "GOTO":    Goto(); break;
        case "IF":      If(); break;
        case "INSERT":  Insert(); break;
        case "RENAME":  Rename(); break;
        case "RETURN":  Return(); break;
        case "SELECT":  Select( true ); break;
        case "SET":     Set(); break;
        case "THROW":   Throw(); break;
        case "UPDATE":  Update(); break;
        case "WHILE":   While(); break;
        default: Error( "Statement keyword expected" ); break;
      }
    }
    else 
    {
      Error("Statement keyword expected");
    }
  }
} // end class SqlExec

class UserException : System.Exception
{
  public UserException( string s ) : base( s ) {}
}

class Exception : System.Exception
{
/* These might be useful in future, if there was interface to retrieve the fields from SQL.
  public string ObjectName;
  public int Line, Col;
  public string Error;
  public string Src;
*/
  public Exception( string error, string name, int line, int col, string token, string src, Token t ) 
  : base 
  ( 
    error 
    + ( name != null ? " in " + name : "" )
    + " at Line " + line + " Col " + col 
    + ( token != "" ? " Token=" + token : "" )
    + @" source=
" + src  
  )
  {
/*
    ObjectName = name;
    Line = line;
    Col = col;
    Error = error;
    Src = src;
*/
  }
}

} // end namespace SQLNS
