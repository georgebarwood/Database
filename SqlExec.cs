namespace SQLNS
{

using G = System.Collections.Generic;
using DBNS;

abstract class Exec // This allows for alternates to SQL in principle ( although none are anticipated ).
{
  public abstract void Error( string Message );
  public abstract TableExpression LoadView( string source, string viewname );
}

class SqlExec : Exec // Parses and Executes ( Interprets ) SQL.
{
  public static void ExecuteBatch( string sql, DatabaseImp d, ResultSet rs )
  {
    // System.Console.WriteLine("ExecuteBatch: " + sql );
    new SqlExec( sql, d, null ).Batch( rs );
  }

  public static Block LoadRoutine( bool func, string sql, DatabaseImp d, string name )
  {
    SqlExec e = new SqlExec( sql, d, name );
    e.B = new Block( d, func );
    e.B.Params = e.RoutineDef( func, out e.B.ReturnType );
    return e.B;
  }

  public override TableExpression LoadView( string source, string viewname )
  {
    SqlExec e = new SqlExec( source, Db, viewname );
    e.B = new Block( Db, false );
    return e.ViewDef();
  }

  public override void Error( string error )
  {
    throw new Exception( RoutineName, SourceLine, SourceColumn, error,
      Source.Substring( TokenStart, TokenStop - TokenStart ), Source, T );
  }

  public DatabaseImp Db;
  public ColInfo CI; // Associated with current table scope.
  public bool [] Used; // Which columns in CI have been referenced in an expression.
  public Block B;

  // Rest is private

  string Source; // The source SQL
  string RoutineName; // The name of the routine being parsed ( null if batch statements are being parsed ).

  int SourceIx = -1; // Index of current character in Source
  int SourceLine = 1; // Current line number
  int SourceColumn = 0;  // Current column number

  char CC;      // Current character in Source as it is being parsed.
  Token T;      // Current token.
  int TokenStart, TokenStop; // Position in source of start and end of current token.

  string NS,TS; // Current name token : TS is uppercase copy of NS.
  long DecimalInt, DecimalFrac; // Details of decimal token
  int DecimalScale;

  public bool ParseOnly; // True when parsing CREATE FUNCTION or CREATE PROCEDURE.
  bool DynScope;  // When parsing SELECT or WHERE clauses, suppresses name lookup/type checking.
    
  int BreakId = -1; // Break label id for current WHILE or FOR statement.

  SqlExec( string sql, DatabaseImp db, string routineName )
  {
    Source = sql;
    Db = db;
    RoutineName = routineName;
    ReadChar();
    ReadToken(); 
  }

  void Batch( ResultSet rs ) 
  { 
    B = new Block( Db, false );
    while ( T != Token.Eof ) Statement(); 
    B.AllocLocalValues( this );
    B.ExecuteStatements( rs );
  }

  void Add( System.Action a )
  { 
    if ( !ParseOnly ) B.AddStatement( a ); 
  }

  // ****************** Token parsing

  void ReadChar()
  {
    SourceIx += 1;
    CC = ( SourceIx >= Source.Length ) ? '\0' : Source[ SourceIx ];
    if ( CC == '\n' ) { SourceColumn = 0; SourceLine += 1; } else SourceColumn += 1;
  }

  void ReadToken()
  {
    SkipSpace:
    while ( CC == ' ' || CC == '\n' || CC == '\r' ) ReadChar();
    TokenStart = SourceIx;
    if ( CC >= 'A' && CC <= 'Z' || CC >= 'a' && CC <= 'z' || CC == '@' )
    {
      T = Token.Name;
      ReadChar();
      while ( CC >= 'A' && CC <= 'Z' || CC >= 'a' && CC <= 'z' || CC == '@' ) ReadChar();
      TokenStop = SourceIx;
      NS = Source.Substring( TokenStart, TokenStop - TokenStart );
      TS = NS.ToUpper();
    }
    else if ( CC >= '0' && CC <= '9' )
    {
      char fc = CC;
      T = Token.Number;
      ReadChar();
      if ( fc == '0' && CC == 'x' )
      {
        ReadChar();
        T = Token.Hex;
        while ( CC >= '0' && CC <= '9' || CC >= 'A' && CC <= 'F' || CC >= 'a' && CC <= 'f') ReadChar();
      }
      else
      {
        while ( CC >= '0' && CC <= '9' ) ReadChar();  
        int part1 = SourceIx;
        DecimalInt = long.Parse( Source.Substring( TokenStart, part1 - TokenStart ) );
        if ( CC == '.' && T == Token.Number )
        {
          T = Token.Decimal;
          ReadChar();
          while ( CC >= '0' && CC <= '9' ) ReadChar();   
          DecimalScale = SourceIx - ( part1 + 1 );
          DecimalFrac = long.Parse( Source.Substring( part1 + 1, DecimalScale ) );
        }
        else 
        {
          DecimalScale = 0;
          DecimalFrac = 0;
        }
      }
      TokenStop = SourceIx;
      NS = Source.Substring( TokenStart, TokenStop - TokenStart );
    }
    else if ( CC == '[' )
    {
      T = Token.Name;
      ReadChar();
      int start = SourceIx; NS = "";
      while ( CC != '\0' )
      {
        if ( CC == ']' )
        {
          ReadChar();
          if ( CC != ']' ) break;
          NS = NS + Source.Substring( start, SourceIx-start-1 );
          start = SourceIx;
        }
        ReadChar();
      }
      TokenStop = SourceIx;
      NS = NS + Source.Substring( start, SourceIx-start-1 );
      TS = NS.ToUpper();
    }   
    else if ( CC == '\'' )
    {
      T = Token.String;
      ReadChar();
      int start = SourceIx; NS = "";
      while ( CC != '\0' )
      {
        if ( CC == '\'' )
        {
          ReadChar();
          if ( CC != '\'' ) break;
          NS = NS + Source.Substring( start, SourceIx-start-1 );
          start = SourceIx;
        }
        ReadChar();
      }
      TokenStop = SourceIx;
      NS = NS + Source.Substring( start, SourceIx-start-1 );
    }
    else
    {
      if ( CC == '(' ) T = Token.LBra;
      else if ( CC == ')' ) T = Token.RBra;
      else if ( CC == ',' ) T = Token.Comma;
      else if ( CC == '.' ) T = Token.Dot;
      else if ( CC == ':' ) T = Token.Colon;
      else if ( CC == '>' ) T = Token.Greater;
      else if ( CC == '<' ) T = Token.Less;
      else if ( CC == '+' ) T = Token.Plus;
      else if ( CC == '|' ) T = Token.VBar;
      else if ( CC == '-' ) T = Token.Minus;
      else if ( CC == '*' ) T = Token.Times;
      else if ( CC == '/' ) T = Token.Divide;
      else if ( CC == '%' ) T = Token.Percent;
      else if ( CC == '=' ) T = Token.Equal;
      else if ( CC == '!' ) T = Token.Exclamation;
      else if ( CC == '\0' ) { T = Token.Eof; TokenStop = SourceIx; return; }
      else { ReadChar(); TokenStop = SourceIx; T = Token.Unknown; Error( "Unrecognised character" ); }

      ReadChar();
      // The processing below could be more efficient if moved into sections above.
      if ( CC == '=' && ( T == Token.Greater || T == Token.Less || T == Token.Exclamation ) )
      {
        T = T == Token.Greater ? Token.GreaterEqual 
          : T == Token.Exclamation ? Token.NotEqual
          : Token.LessEqual;
        ReadChar();
      }
      else if ( CC == '>' && T == Token.Less )
      {
        T = Token.NotEqual;
        ReadChar();
      } 
      else if ( T == Token.Divide && CC == '*' )
      {
        // Skip comment
        ReadChar();
        char prevchar = 'X';
        while ( ( CC != '/' || prevchar != '*' ) && CC != '\0' )
        {
          prevchar = CC;
          ReadChar();
        }
        ReadChar();
        goto SkipSpace;
      }
      else if ( T == Token.Minus && CC == '-' )
      {
        while ( CC != '\n' && CC != '\0' ) ReadChar();
        goto SkipSpace;
      }
      TokenStop = SourceIx;
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

  void Read( string t )
  {
    if ( T != Token.Name || TS != t ) Error( "Expected " + t );
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
      else result = new ExpLocalVar( i, B.LocalType[i] );
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
        for ( int i = 0; i < v.Count; i += 1 ) types[ i ] = v[i].Type;
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

  TableExpression Expressions( G.List<int> locals )
  {
    // locals has the indexes of local variables being assigned in a SET or FOR statement.
    bool save = DynScope; DynScope = true; // Suppresses Binding of expressions until table is known.
    var exps = new G.List<Exp>();
    do
    {
      if ( locals != null )
      {
        var name = Name();
        int i = B.Lookup( name );
        if ( i < 0 ) Error( "Undeclared local variable : " + name );
        Read( Token.Equal ); 
        if ( locals.Contains( i ) ) Error( "Duplicated local name in SET or FOR" );       
        locals.Add( i );
      }
      Exp exp = ExpOrAgg();
      if ( Test( "AS" ) ) exp.Name = Name();
      exps.Add( exp );
    } while ( Test( Token.Comma ) );

    TableExpression te = Test( "FROM" ) ? PrimaryTableExp() : new DummyFrom();

    te.CheckNames( this );

    Exp where = Test( "WHERE" ) ? Exp() : null;

    Exp[] group = null;
    if ( Test( "GROUP" ) )
    {
      var list = new G.List<Exp>();
      Read( "BY" );
      do
      {
        Exp exp = Exp();
        if ( Test( "AS" ) ) exp.Name = Name();
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
      CI = te.Cols;
      Used = new bool[ CI.Count ]; // Bitmap of columns that are referenced by any expression.

      for ( int i=0; i<exps.Count; i+=1 ) 
      {
        if ( exps[i].GetAggOp() != AggOp.None )
        {
          if ( group == null ) 
          {
            if ( i != 0 ) Error( "All exps in aggregate select must be aggregate functions" );
            group = new Exp[0];
            System.Console.WriteLine("Auto-group");
          }
          exps[i].BindAgg( this );
        }
        else 
        { 
          if ( group != null ) Error( "All exps in aggregate select must be aggregate functions" );
          exps[i].Bind( this );
        }
      }

      if ( where != null && where.Bind( this ) != DataType.Bool ) 
        Error( "WHERE expression must be boolean" );
      
      if ( group != null ) 
      {
        for ( int i=0; i<group.Length; i+=1 ) 
        {
          group[i].Bind( this );
          exps.Add( group[i] );
        }
      }

      result = new Select( exps, te, where, group, order, Used, this );
      
      if ( locals != null ) 
      {
        var types = new DataType[ locals.Count ];
        for ( int i = 0; i < locals.Count; i += 1 )
          types[i] = B.LocalType[ locals[i] ];
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
    if ( exec ) Add( () => B.ExecuteSelect( te, null ) );
    return te;
  }

  void Set()
  {
    var locals = new G.List<int>();
    var te = Expressions( locals );
    Add( () => B.ExecuteSelect( te, locals.ToArray() ) );
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
        int ci = t.ColumnIx( names[i], this );
        if ( ci == 0 ) idCol = i;
        colIx[i] = ci;
        types[i] = t.Cols.Types[ ci ];
      }
      src.Convert( types, this );
      Add( () => B.ExecInsert( t, src, colIx, idCol ) );
    }
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
      Used = new bool[ t.Cols.Count ]; // Bitmap of columns that are referenced by any expression.
      CI = t.Cols;      

      int idCol = -1;
      for ( int i=0; i < a.Length; i += 1 ) 
      {        
        if ( a[i].Lhs.Bind( this ) != a[i].Rhs.Bind( this ) )
        {
          Exp conv = a[i].Rhs.Convert( a[i].Lhs.Type );
          if ( conv == null ) Error( "Update type mismatch" );
          else a[i].Rhs = conv;
        }
        if ( a[i].Lhs.ColIx == 0 ) idCol = i;
      }
      if ( where != null && where.Bind( this ) != DataType.Bool ) 
        Error( "WHERE expression must be boolean" );

      var used = Used; // Need to take a copy
      var w = where.GetDB();
      var dvs = new Exp.DV[ a.Length ];
      var ixs = new int[ a.Length ];
      for ( int i = 0; i < a.Length; i += 1 )
      {
        ixs[ i ] = a[ i ].Lhs.ColIx;
        dvs[ i ] = a[ i ].Rhs.GetDV();
      }

      Add( () => t.ExecUpdate( ixs, dvs, where, w, used, idCol, B ) ); 

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
      Used = new bool[ t.Cols.Count ]; // Bitmap of columns that are referenced by any expression.
      CI = t.Cols;

      if ( where != null && where.Bind( this ) != DataType.Bool ) 
        Error( "WHERE expression must be boolean" );
      var used = Used; // Need to take a copy.
      var w = where.GetDB();
      Add( () => t.ExecDelete( where, w, used, B ) );

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
      if ( exp.Type != DataType.String ) Error( "Argument of EXECUTE must be a string" );
      var ds = exp.GetDS();
      Add( () => B.Execute( ds ) );
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
          if ( parms[i].Type != b.Params.Types[i] ) 
            Error( "Parameter Type Error calling procedure " + name );


        var pdv = new Exp.DV[ parms.Count ];
        for ( int i = 0; i < parms.Count; i += 1 ) pdv[ i ] = parms[ i ].GetDV();

        Add( () => B.ExecProcedure( b, pdv ) ); 
      }   
    }
    else if ( name == "SETMODE" )
    {
      if ( !ParseOnly && ( parms.Count != 1 || parms[0].Bind( this ) != DataType.Bigint ) )
        Error( "SETMODE param error" );
      var dl = parms[0].GetDL();
      Add( () => B.SetMode( dl ) );
    }
    else Error( "Unrecognised procedure" );
  }

  void For()
  {
    var locals = new G.List<int>();
    TableExpression te = Expressions( locals );

    int forid = B.GetForId();
    Add( () => B.InitFor( forid, te, locals.ToArray() ) );
    int start = B.GetHere();
    int breakid = B.GetJumpId();
    Add( () => B.ExecuteFor( forid, breakid ) );
    
    int save = BreakId;
    BreakId = breakid;
    Statement();
    BreakId = save;

    Add( () => B.JumpBack( start ) );
    B.SetJump( breakid );
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
    Add( () => Db.CreateTable( schema, tableName, ColInfo.New( names, types ), source, false, false, this ) );
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
    Add( () => Db.CreateTable( schema, viewName, se.Cols, source, true, alter, this ) );
  }

  TableExpression ViewDef()
  {
    Read( "SELECT" );
    return Select( false );
  }

  void CreateRoutine( bool func, bool alter )
  {
    string schema = Name();
    Read( Token.Dot );
    string funcName = Name();
    int sourceStart = SourceIx-1;

    Block save1 = B; bool save2 = ParseOnly; 
    B = new Block( B.Db, func ); ParseOnly = true;
    DataType retType; var parms = RoutineDef( func, out retType );
    B = save1; ParseOnly = save2;

    string source = Source.Substring( sourceStart, TokenStart - sourceStart );
    Add( () => Db.CreateRoutine( schema, funcName, source, func, alter, this ) );
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
    Block();
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
    string objtype = NS;
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
      else if ( Test("MODIFY" ) ) // DataType change, e.g. Int => BitInt
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
    if ( exp.TypeCheck( this ) != DataType.Bool ) Error( "WHILE expression must be boolean" );
    
    int start = B.GetHere();
    int breakid = B.GetJumpId();
    if (!ParseOnly)
    {
      var db = exp.GetDB();
      Add( () => B.ExecuteIf( db, breakid ) );
    }
    
    int save = BreakId;
    BreakId = breakid;
    Statement();
    BreakId = save;

    Add( () => B.JumpBack( start ) );
    B.SetJump( breakid );
  }

  void If()
  {
    var exp = Exp();
    if ( !ParseOnly && exp.TypeCheck( this ) != DataType.Bool ) Error( "IF expression must be boolean" );

    int falseid = B.GetJumpId();
    if (!ParseOnly)
    {
      var db = exp.GetDB();
      Add( () => B.ExecuteIf( db, falseid ) );
    }

    Statement();

    if ( Test("ELSE") ) 
    {
      int endid = B.GetJumpId();
      Add( () => B.ExecuteGoto( endid ) ); // Skip over the else clause
      B.SetJump( falseid );
      Statement();
      B.SetJump( endid );
    }  
    else B.SetJump( falseid );  
  }

  void Goto()
  {
    string label = Name();
    Add( B.Goto( label ) );
  }

  void LabelStatement()
  {
    string label = Name();
    Read( Token.Colon );
    if ( B.SetLabel( label ) ) Error( "Label " + label + " already defined" );
  }

  void Break()
  {
    int breakId = BreakId; // Need to take a copy of current value.
    if ( breakId < 0 ) Error( "No enclosing loop for break" );
    Add( () => B.ExecuteGoto( breakId ) );
  }

  void Return()
  {
    Exp e = B.IsFunc ? Exp() : null;
    if ( !ParseOnly )
    {
      if ( e != null && B.ReturnType != e.Type ) 
      {
        e = e.Convert( B.ReturnType );
        if ( e == null ) Error( "Return type error" );        
      }
      var dv = e == null ? null : e.GetDV();
      Add( () => B.ExecuteReturn( dv ) );
    }
  }

  void Block() { while ( !Test( "END" ) ) Statement(); }

  void Statement()
  {
    if ( T == Token.Name ) 
    {
      if ( Test( "SELECT" ) ) Select( true );
      else if ( Test( "INSERT" ) ) Insert();
      else if ( Test( "UPDATE" ) ) Update();
      else if ( Test( "DELETE" ) ) Delete();
      else if ( Test( "EXEC" ) ) Exec();
      else if ( Test( "EXECUTE" ) ) Execute();
      else if ( Test( "FOR" ) ) For();
      else if ( Test( "DECLARE" ) ) Declare();
      else if ( Test( "SET" ) ) Set();
      else if ( Test( "WHILE" ) ) While();
      else if ( Test( "IF" ) ) If();
      else if ( Test( "BEGIN" ) ) Block();
      else if ( Test( "GOTO" ) ) Goto();
      else if ( Test( "BREAK" ) ) Break();
      else if ( Test( "RETURN" ) ) Return();
      else if ( Test( "CREATE" ) ) Create();
      else if ( Test( "ALTER" ) ) Alter();
      else if ( Test( "DROP" ) ) Drop();
      else if ( Test( "RENAME" ) ) Rename();
      else LabelStatement();
    }
    else 
    {
      Error("Statement expected");
    }
  }
} // end class SqlExec

class Exception : System.Exception
{
  public string Routine;
  public int Line, Col;
  public string Error;
  public string Src;
  public Exception( string routine, int line, int col, string error, string token, string src, Token t ) 
  : base 
  ( 
    error 
    + ( routine != null ? " in " + routine : "" )
    + ( t == Token.Eof ? "" : " at Line " + line + " Col " + col + " Token=" + token 
    // + "(" + t + ")"
    )
    + " source=" + src // May help when debugging dynamic SQL    
  )
  {
    Routine = routine;
    Line = line;
    Col = col;
    Error = error;
    Src = src;
  }
}

} // end namespace SQLNS
