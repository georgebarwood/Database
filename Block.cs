/* Plan for generating code.

A block ( batch, stored function or procedure ) will have an associated class and method.

Implementation of SET assignment, IF, WHILE statements, local variables is all straight-forward.

How to implement something like

SELECT exp1, exp2 ..... FROM <table> WHERE <condition involving local variables> ?

If the result is grouped/sorted, introduce Sorter or Grouper ResultSet to do sorting/grouping.

In general we need an iterator to fetch the rows.

So first call is to GetAll, which returns the iterator.

Start of loop:

Call "MoveNext" on interator, and exit loop if it returns false.

MoveNext returns a byte[] buffer and an offset.

Next generate code to push the WHERE condition on the stack ( may reference the buffer and local variables ).

If WHERE condition is true, evaluate the SELECT expressions, pushing them on the stack.

Call the ResultSet output method with a variable number of parameters.

Jump back to start of loop.

Exit from the loop, call the ResultSet EndTable method.

*/


namespace SQLNS
{

using G = System.Collections.Generic;
using DBNS;

class Block : EvalEnv // Represents a batch of statements or a routine (stored function or procedure) definition.
{
  public Block( DatabaseImp d, bool isFunc ) { Db = d; IsFunc = isFunc; Init(); }

  public readonly DatabaseImp Db;
  public readonly bool IsFunc;

  // Run-time fields.
  System.Action[] Statements; // List of statements to be executed.
  int [] Jumps; // Resolution of the ith jumpid.
  int NextStatement; // Index into Statements, can be assigned to change execution control flow.
  Value FunctionResult; // Holds result if block is a function.

  // Type information.
  public ColInfo Params; // Types of routine parameters.
  public DataType ReturnType; // Function return type.
  DataType [] LocalTypes; // Types of local variables.

  // Compile-time lists and maps.
  G.List<System.Action> StatementList; // For building Statements.
  G.List<int> JumpList; // For building Jumps.
  public G.List<DataType> LocalTypeList; // For building LocalTypes.
  G.Dictionary<string,int> VarMap; // Lookup dictionary for local variables.
  G.Dictionary<string,int> LabelMap; // Lookup dictionary for local labels.
  int LabelUndefined = 0; // Number of labels awaiting definition.

  // Statement preparation ( compile phase ).

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

  public void AddStatement( System.Action a ) { StatementList.Add( a ); }

  public void Declare( string varname, DataType type ) // Declare a local variable.
  {
    VarMap[ varname ] = LocalTypeList.Count;
    LocalTypeList.Add( type );
  }  

  public int Lookup( string varname ) // Gets the number of a local variable, -1 if not declared.
  { 
    int result; return VarMap.TryGetValue( varname, out result ) ? result : -1; 
  }

  public int LookupJumpId( string label ) // Gets jumpid for a label.
  { 
    int result; return LabelMap.TryGetValue( label, out result ) ? result : -1; 
  }

  public int GetJumpId() 
  { 
    int jumpid = JumpList.Count; JumpList.Add( -1 ); 
    return jumpid; 
  }

  public int GetStatementId() 
  { 
    return StatementList.Count; 
  }

  public void SetJump( int jumpid ) 
  { 
    JumpList[ jumpid ] = GetStatementId(); 
  }

  public bool SetLabel( string name ) // returns true if label already defined ( an error ).
  {
    int sid = GetStatementId();
    int jumpid = LookupJumpId( name );
    if ( jumpid < 0 )
    {
      jumpid = JumpList.Count;
      LabelMap[ name ] = jumpid;
      JumpList.Add( sid );
    }
    else 
    {
      if ( JumpList[ jumpid ] >= 0 ) return true;
      JumpList[ jumpid ] = sid;
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
      jumpid = GetJumpId();
      LabelMap[ name ] = jumpid;
      LabelUndefined += 1;
      return () => Goto( jumpid );
    }
    else 
    {
      int sid = JumpList[ jumpid ];
      return () => JumpBack( sid );
    }
  }

  // Statement execution.

  Value [] InitLocals()
  {
    int n = LocalTypes.Length;
    var result = new Value[ n ];
    for ( int i = 0; i < n; i += 1 ) result[ i ] = DTI.Default( LocalTypes[ i ] );
    return result;
  }

  void ExecuteStatements( ResultSet rs ) // Statement execution loop.
  {
    ResultSet = rs;
    NextStatement = 0;
    while ( NextStatement < Statements.Length ) Statements[ NextStatement++ ]();
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

    if ( IsFunc )
      ExecuteStatements( e.ResultSet );
    else 
    try
    {
      ExecuteStatements( e.ResultSet );
    }
    catch ( System.Exception exception )
    {
      Db.SetRollback();
      ResultSet.Exception = exception;
    }

    // Restore local state.
    NextStatement = save2; Locals = save1;

    return FunctionResult;
  }

  public void Execute( Exp.DS e ) // Execute a string expression.
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

  public void Throw( Exp.DS e )
  {
    throw new UserException( e ( this ) );
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

  public void JumpBack( int statementId ) 
  { 
    NextStatement = statementId; 
  }

  public void Select( TableExpression te ) 
  { 
    te.FetchTo( ResultSet, this ); 
  }

  public void Set( TableExpression te, int [] assigns )
  {
    te.FetchTo( new SetResultSet( assigns, this ), this );
  }

  public void InitFor( int forid, TableExpression te, int[] assigns )
  {
    Locals[ forid ]._O = new ForState( te, assigns, this );
  }

  public void For( int forid, int breakid )
  {
    ForState f = (ForState) Locals[ forid ]._O;
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

  class ForState // Implements FOR statement.
  {
    G.IEnumerator<bool> Cursor;
    Value[] Row;
    int [] Assigns;
    Value[] Locals; 

    public ForState( TableExpression te, int[] assigns, Block b )
    {
      Assigns = assigns;
      Locals = b.Locals;
      Row = new Value[ te.ColumnCount ];
      Cursor = te.GetAll( Row, null, b ).GetEnumerator();
    }

    public bool Fetch()
    {
      if ( !Cursor.MoveNext() ) return false;
      for ( int i = 0; i < Assigns.Length; i += 1 )
        Locals[ Assigns[ i ] ] = Row[ i ];
      return true;
    }

  } // end class ForState

} // end class Block

} // end namespace SQLNS
