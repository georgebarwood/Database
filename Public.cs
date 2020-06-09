/* Public interface */

namespace DBNS
{

using G = System.Collections.Generic;
using IOS = System.Runtime.InteropServices;

public abstract class Database
{
  public static Database GetDatabase( string dirName ) { return new DatabaseImp( dirName );  }
  public abstract void Sql( string sql, ResultSet rs );
  public bool IsNew; // Database has just been created.
}

/* ResultSet holds or processes the results of the execution of a batch of SQL statements, which is a list of tables.
   A SELECT statement outputs a new table to the ResultSet by calling NewRow repeatedly.
   Note: if the row is saved, a copy needs to be taken, e.g. (Value[])row.Clone().
*/

public abstract class ResultSet
{
  public virtual  void NewTable( ColInfo ci ){} // Called for each table selected, has information about the columns ( maybe should also provide table name? ).
  public abstract bool NewRow( Value [] row ); // Called for each selected row. If result is false, sending is aborted ( no more rows are sent ).
  public virtual  void EndTable(){} // Called when all rows have been sent ( or sending is aborted ).

  /* As well as accepting SELECT results, ResultSet is also be used to pass parameters in, via the pre-defined functions ARG,FILEATTR,FILECONTENT. */
  public virtual string Arg( int kind, string name ){ return ""; }
  public virtual string ArgName( int kind, int ix ){ return ""; } /* Can be used to obtain names of unknown fields */

  /* FileAttr and FileContent give access to uploaded files. */
  public virtual string FileAttr( int ix, int kind /*0=Name,1=ContentType,2=Filename*/ ){ return ""; }
  public virtual byte [] FileContent ( int ix ){ return new byte[0]; }

  public System.Exception Exception;
  public long LastIdInserted;
}

// Note: Decimal(p,s) is encoded as Decimal + 16 * p + 1024 * s, where 0 < p <= 18 and 0 < s <= p ( 4 bits, 6 bits, 6 bits ).
public enum DataType : ushort { None=0, Binary=1, String=2, Bigint=3, Double=4, Int=5, Float=6, Smallint=7, Tinyint=8, Bool=9, ScaledInt=10, Decimal=15 };

public class ColInfo
{
  public readonly int Count;
  public readonly string [] Names;
  public readonly DataType [] Types;

  public ColInfo( G.List<string> names, G.List<DataType> types )
  {
    Names = names.ToArray();
    Types = types.ToArray();
    Count = Names.Length;
  }
  public ColInfo( string [] names, DataType[] types )
  {
    Names = names; 
    Types = types; 
    Count = Names.Length;
  }
}

// Value holds an arbitrary SQL value. The comments show which field holds each DataType.

[IOS.StructLayout(IOS.LayoutKind.Explicit)]
public struct Value
{
  [IOS.FieldOffset(0)] public bool B;     // Bool
  [IOS.FieldOffset(0)] public long L;     // Tinyint, Smallint, Int, Bigint
  [IOS.FieldOffset(0)] public double D;   // Float, Double
  [IOS.FieldOffset(8)] public object _O;  // Binary, String ( L holds an encoding, computed when the value is saved to disk )

  public object O { set { _O = value; L = 0; } } // Encoding needs to be be set to zero when _O is assigned.

  public bool Equal( Value x, DataType t )
  {
    return 
      t == DataType.Bigint ? L == x.L
    : t == DataType.String ? (string)_O == (string)x._O
    : t == DataType.Double ? D == x.D
    : t == DataType.Bool   ? B == x.B
    : t == DataType.Binary ? Util.Compare( (byte[])_O, (byte[])x._O ) == 0
    : false; // Shouldn't get here.
  }

  public string ToString( DataType t ) // Only for debugging.
  {
    t = DTI.Base( t );
    return 
        t == DataType.Bigint ? L.ToString()
      : t == DataType.String ? "'" + (string)_O + "'"
      : t == DataType.Double ? D.ToString()
      : t == DataType.Bool ? B.ToString()
      : t == DataType.Binary ? Util.ToString( (byte[])_O )
      : "??" + t + "??"; // Shouldn't get here.
  }

} // end struct Value

} // end namespace DBNS

/* Guide to implementation source files:

Public.cs = this file : public interface.
WebServer.cs = main program, http web server ( example client ).
Init.cs = SQL initialisation script.

SQL-independent ( namespace DBNS )
================================
Database.cs = implements Database.
Log.cs = log file to ensure atomic updates.
Stream.cs = fully buffered stream for Rollback/Commit.
Table.cs = implementation of TABLE.
IndexFile.cs, IndexPage.cs = implementation of INDEX.

SQL-specific ( namespace SQLNS )
================================
SqlExec.cs = parsing and execution of SQL statements.
Block.cs = list of statements for execution.
Exp.cs, ExpStd.Exp = scalar expressions.
TableExp.cs = table-valued expressions.
Group.cs = implementation of GROUP BY.
Sort.cs = implementation of ORDER BY.
IdSet.cs = optimisation of WHERE.
Util.cs = various utility classes.

*/
