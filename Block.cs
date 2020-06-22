namespace SQLNS
{

using G = System.Collections.Generic;
using DBNS;

class Block : EvalEnv // Result of compiling a batch of statements or a routine (stored function or procedure) definition.
{
  public Block( DatabaseImp d, bool isFunc ) { Db = d; IsFunc = isFunc; Init(); }

  public readonly DatabaseImp Db;
  public readonly bool IsFunc;

  // Execution fields.
  System.Action[] Statements; // List of statements to be executed.
  int [] Jumps; // Resolution of the ith jumpid.
  int NextStatement; // Index into Statements, can be assigned to change execution control flow.
  Value FunctionResult;

  // Type information.
  public ColInfo Params; // Types of routine parameters.
  public DataType ReturnType; // Function return type.
  DataType [] LocalTypes; // Types of local variables.

  // Compilation lists and maps.
  G.List<System.Action> StatementList; // For building Statements.
  G.List<int> JumpList; // For building Jumps.
  int LabelUndefined = 0; // Number of labels awaiting definition.  
  G.Dictionary<string,int> VarMap; // Lookup dictionary for local variables.
  G.Dictionary<string,int> LabelMap; // Lookup dictionary for local labels.
  public G.List<DataType> LocalTypeList; // Type of the ith local variable.

  void ExecuteStatements( ResultSet rs ) // Statement execution loop.
  {
    ResultSet = rs;
    NextStatement = 0;
    while ( NextStatement < Statements.Length ) Statements[ NextStatement++ ]();
  }

  public void Init()
  {
    Statements = null;
    Jumps = null;
    LocalTypes = null;

    StatementList = new G.List<System.Action>();
    JumpList = new G.List<int>();
    LocalTypeList = new G.List<DataType>();
    VarMap = new G.Dictionary<string,int>();
    LabelMap = new G.Dictionary<string,int>();
  }

  public void Complete()
  {
    Statements = StatementList.ToArray();
    Jumps = JumpList.ToArray();
    LocalTypes = LocalTypeList.ToArray();

    StatementList = null;
    JumpList = null;
    LocalTypeList = null;
    VarMap = null;
    LabelMap = null;
  }

  // Statement preparation ( parse phase ).

  public void AddStatement( System.Action a ) { StatementList.Add( a ); }

  public void Declare( string varname, DataType type ) // Declare a local variable.
  {
    VarMap[ varname ] = LocalTypeList.Count;
    LocalTypeList.Add( type );
  }  

  public int Lookup( string varname ) // Gets the number of a local variable, -1 if not declared.
  { int result; return VarMap.TryGetValue( varname, out result ) ? result : -1; }

  public int LookupJumpId( string label ) // Gets jumpid for a label.
  { int result; return LabelMap.TryGetValue( label, out result ) ? result : -1; }

  public int GetJumpId() { int jumpid = JumpList.Count; JumpList.Add( -1 ); return jumpid; }

  public int GetStatementId() { return StatementList.Count; }

  public void SetJump( int jumpid ) { JumpList[ jumpid ] = GetStatementId(); }

  public bool SetLabel( string name ) // returns true if label already defined ( an error ).
  {
    int i = GetStatementId();
    int jumpid = LookupJumpId( name );
    if ( jumpid < 0 )
    {
      jumpid = JumpList.Count;
      LabelMap[ name ] = jumpid;
      JumpList.Add( i );
    }
    else 
    {
      if ( Jumps[ jumpid ] >= 0 ) return true;
      JumpList[ jumpid ] = i;
      LabelUndefined -= 1;
    }
    return false;
  }

  public void CheckLabelsDefined( Exec e )
  {
    if ( LabelUndefined != 0 ) e.Error( "Undefined Goto Label" );
  }

  public int GetForId()
  {
    int forid = LocalTypeList.Count;
    LocalTypeList.Add( DataType.None );
    return forid;
  }

  public System.Action GetGoto( string name )
  {
    int jumpid = LookupJumpId( name );
    if ( jumpid < 0 )
    {
      LabelMap[ name ] = GetJumpId();
      LabelUndefined += 1;
      return () => Goto( jumpid );
    }
    else return () => JumpBack( Jumps[ jumpid ] );
  }

  // Statement execution.

  Value [] InitLocals()
  {
    int n = LocalTypes.Length;
    var result = new Value[ n ];
    for ( int i = 0; i < n; i += 1 ) result[ i ] = DTI.Default( LocalTypes[ i ] );
    return result;
  }

  public void ExecuteBatch( ResultSet rs )
  {
    Locals = InitLocals();
    ExecuteStatements( rs );
  }

  public Value ExecuteRoutine( EvalEnv e, Exp.DV [] parms )
  {
    // Allocate the local variables.
    var locals = InitLocals();

    // Evaluate the parameters to be passed, saving them in the newly allocated local variables.
    for ( int i = 0; i < parms.Length; i += 1 ) locals[ i ] = parms[ i ]( e );

    // Save local state.
    var save1 = Locals; var save2 = NextStatement;

    Locals = locals;
    ExecuteStatements( e.ResultSet );

    // Restore local state.
    NextStatement = save2; Locals = save1;

    return FunctionResult;
  }

  public void Execute( Exp.DS e )
  {
    string s = e( this );
    try
    {
      Db.ExecuteSql( s, ResultSet );    
    }
    catch ( System.Exception exception )
    {
      Db.SetRollback();
      ResultSet.Exception = exception;
    }
  }

  public void Goto( int jumpId )
  {
    NextStatement = Jumps[ jumpId ];
  }

  public void Return( Exp.DV e )
  {
    if ( IsFunc ) FunctionResult = e( this );
    NextStatement = Statements.Length;
  }

  public void If( Exp.DB test, int jumpid )
  {
    if ( !test( this ) ) NextStatement = Jumps[ jumpid ];
  }

  public void JumpBack( int statementId ) { NextStatement = statementId; }

  public void Select( TableExpression te ) { te.FetchTo( ResultSet, this ); }

  public void Set( TableExpression te, int [] assigns )
  {
    te.FetchTo( new SetResultSet( assigns, this ), this );
  }

  public void InitFor( int forid, TableExpression te, int[] assigns )
  {
    Locals[ forid ]._O = new For( te, assigns, this );
  }

  public void For( int forid, int breakid )
  {
    For f = (For) Locals[ forid ]._O;
    if ( f == null || !f.Fetch() ) NextStatement = Jumps[ breakid ];
  }

  public void Insert( Table t, TableExpression te, int[] colIx, int idCol )
  {
    long lastId = t.Insert( te, colIx, idCol, this );
    if ( ResultSet != null ) ResultSet.LastIdInserted = lastId;
  }

  public void SetMode( Exp.DL e )
  {
    ResultSet.SetMode( e( this ) );
  }

} // end class Block


class SetResultSet : ResultSet // Implements SET assignment of local variables.
{
  int [] Assigns;
  Block B;

  public SetResultSet( int [] assigns, Block b )
  {
    Assigns = assigns;
    B = b;
  }

  public override bool NewRow( Value [] r )
  {
    for ( int i=0; i < Assigns.Length; i += 1 ) B.Locals[ Assigns[ i ] ] = r[ i ];
    return false; // Only one row is assigned.
  }  
} // end class SetResultSet

class For // Implements FOR statement.
{
  G.List<Value[]> Rows;
  int [] Assigns;
  Value[] Locals; 
  int Fetched;

  public For( TableExpression te, int[] assigns, Block b )
  {
    Fetched = 0;
    Assigns = assigns;
    Locals = b.Locals;
    var rs = new SingleResultSet();
    te.FetchTo( rs, b );
    Rows = rs.Table.Rows;
  }

  public bool Fetch()
  {
    if ( Fetched >= Rows.Count ) return false;
    Value [] r = Rows[ Fetched ++ ];
    for ( int i = 0; i < Assigns.Length; i += 1 )
      Locals[ Assigns[ i ] ] = r[ i ];
    return true;
  }

} // end class For

} // end namespace SQLNS
