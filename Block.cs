namespace SQLNS
{

using G = System.Collections.Generic;
using DBNS;

class Block : EvalEnv // Result of compiling a batch of statements or a routine (stored function or procedure) definition.
{
  public Block( DatabaseImp d, bool isFunc ) { Db = d; IsFunc = isFunc; }

  public readonly DatabaseImp Db;
  public readonly bool IsFunc;

  public ColInfo Params;
  public DataType ReturnType;
  public G.List<DataType> LocalType = new G.List<DataType>(); // Type of the ith local variable.

  int NextStatement; // Index into Statements, can be assigned to change execution control flow.
  G.List<System.Action> Statements = new G.List<System.Action>(); // List of statements to be executed.
  Value FunctionResult;

  // Lookup dictionaries for local variables.
  G.Dictionary<string,int> VarMap = new G.Dictionary<string,int>();

  // Lookup dictionary for local labels.
  G.Dictionary<string,int> LabelMap = new G.Dictionary<string,int>();
  G.List<int> Jump = new G.List<int>(); // The resolution of the ith jumpid.
  int JumpUndefined = 0; // Number of jump labels awaiting definition.


  // Statement execution loop.
  public void ExecuteStatements( ResultSet rs )
  {
    ResultSet = rs;
    NextStatement = 0;
    while ( NextStatement < Statements.Count ) Statements[ NextStatement++ ]();
  }

  // Statement preparation ( parse phase ).

  public void AddStatement( System.Action a ) { Statements.Add( a ); }

  public void Declare( string varname, DataType type ) // Declare a local variable.
  {
    VarMap[ varname ] = LocalType.Count;
    LocalType.Add( type );
  }  

  public int Lookup( string varname ) // Gets the number of a local variable, -1 if not declared.
  { int result; return VarMap.TryGetValue( varname, out result ) ? result : -1; }

  public int LookupJumpId( string label ) // Gets jumpid for a label.
  { int result; return LabelMap.TryGetValue( label, out result ) ? result : -1; }

  public int GetJumpId() { int jumpid = Jump.Count; Jump.Add( -1 ); return jumpid; }

  public int GetStatementId() { return Statements.Count; }

  public void SetJump( int jumpid ) { Jump[ jumpid ] = Statements.Count; }

  public bool SetLabel( string name ) // returns true if label already defined ( an error ).
  {
    int i = Statements.Count;
    int jumpid = LookupJumpId( name );
    if ( jumpid < 0 )
    {
      jumpid = Jump.Count;
      LabelMap[ name ] = jumpid;
      Jump.Add( i );
    }
    else 
    {
      if ( Jump[ jumpid ] >= 0 ) return true;
      Jump[ jumpid ] = i;
      JumpUndefined -= 1;
    }
    return false;
  }

  public void CheckLabelsDefined( Exec e )
  {
    if ( JumpUndefined != 0 ) e.Error( "Undefined Goto Label" );
  }

  public int GetForId()
  {
    int forid = LocalType.Count;
    LocalType.Add( DataType.None );
    return forid;
  }

  public System.Action GetGoto( string name )
  {
    int jumpid = LookupJumpId( name );
    if ( jumpid < 0 )
    {
      LabelMap[ name ] = GetJumpId();
      JumpUndefined += 1;
      return () => Goto( jumpid );
    }
    else return () => JumpBack( Jump[ jumpid ] );
  }

  // Statement execution.

  public void AllocLocalValues( Exec e )
  {
    CheckLabelsDefined( e );
    Locals = InitLocals();
  }

  public Value [] InitLocals()
  {
    int n = LocalType.Count;
    var result = new Value[ n ];
    for ( int i = 0; i < n; i += 1 ) result[ i ] = DTI.Default( LocalType[ i ] );
    return result;
  }

  public Value ExecuteRoutine( EvalEnv e, Exp.DV [] parms )
  {
    // Allocate the local variables for the called function.
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
    NextStatement = Jump[ jumpId ];
  }

  public void Return( Exp.DV e )
  {
    if ( IsFunc ) FunctionResult = e( this );
    NextStatement = Statements.Count;
  }

  public void If( Exp.DB test, int jumpid )
  {
    if ( !test( this ) ) NextStatement = Jump[ jumpid ];
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
    if ( f == null || !f.Fetch() ) NextStatement = Jump[ breakid ];
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
  Value[] LocalValues; 
  int Fetched;

  public For( TableExpression te, int[] assigns, Block b )
  {
    Fetched = 0;
    Assigns = assigns;
    LocalValues = b.Locals;
    var rs = new SingleResultSet();
    te.FetchTo( rs, b );
    Rows = rs.Table.Rows;
  }

  public bool Fetch()
  {
    if ( Fetched >= Rows.Count ) return false;
    Value [] r = Rows[ Fetched ++ ];
    for ( int i = 0; i < Assigns.Length; i += 1 )
      LocalValues[ Assigns[ i ] ] = r[ i ];
    return true;
  }

} // end class For

} // end namespace SQLNS
