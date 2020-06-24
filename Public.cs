/* Public interface */

namespace DBNS
{

using G = System.Collections.Generic;
using IOS = System.Runtime.InteropServices;

public abstract class Database
{
  public static Database GetDatabase( string dirName ) { return new DatabaseImp( dirName );  }
  public abstract void Sql( string sql, ResultSet rs ); // The main entry point : execute the SQL string.
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

  // As well as accepting SELECT results, ResultSet is also used to access http parameters in, via the pre-defined functions ARG,FILEATTR,FILECONTENT.
  // kind values : AbsPath = 0, QueryString = 1, FormString = 2, Cookie = 3; */
  public virtual string Arg( int kind, string name ){ return null; }
  public virtual string ArgName( int kind, int ix ){ return null; } // Can be used to obtain names of unknown fields.

  // FileAttr and FileContent give access to http uploaded files.
  public virtual string FileAttr( int ix, int kind /*0=Name,1=ContentType,2=Filename*/ ){ return null; }
  public virtual byte [] FileContent ( int ix ){ return null; }

  // SetMode controls how SELECT results are processed. 0 = normal, 1 = HTML table display. See WebResultSet.NewRow.
  public virtual void SetMode( long mode ){}

  public System.Exception Exception;
  public long LastIdInserted;
}

// Note: Decimal(p,s) is encoded as Decimal + 16 * p + 1024 * s, where 0 < p <= 18 and 0 < s <= p ( 4 bits, 6 bits, 6 bits ).
public enum DataType : ushort { None=0, Binary=1, String=2, Bigint=3, Double=4, Int=5, Float=6, Smallint=7, Tinyint=8, Bool=9, ScaledInt=10, Decimal=15 };

public class ColInfo
{
  public readonly int Count;
  public readonly string [] Name;
  public readonly DataType [] Type;

  public ColInfo( string [] name, DataType[] type )
  {
    Name = name; 
    Type = type; 
    Count = type.Length;
  }

  public static ColInfo New( G.List<string> names, G.List<DataType> types )
  {
    return new ColInfo( names.ToArray(), types.ToArray() );
  }
}

// Value holds an arbitrary SQL value. The comments show which field holds each DataType.

[IOS.StructLayout(IOS.LayoutKind.Explicit)]
public struct Value
{
  [IOS.FieldOffset(0)] public bool B;     // Bool
  [IOS.FieldOffset(0)] public long L;     // Tinyint, Smallint, Int, Bigint, Decimal
  [IOS.FieldOffset(0)] public double D;   // Float, Double
  [IOS.FieldOffset(8)] public object _O;  // Binary, String ( L holds an encoding, computed when the value is saved to disk )

  public object O { set { _O = value; L = 0; } } // Encoding needs to be be set to zero when _O is assigned.

  public static Value New( bool b ){ return new Value{ B = b }; }
  public static Value New( long l ){ return new Value{ L = l }; }
  public static Value New( double d ){ return new Value{ D = d }; }
  public static Value New( string s ){ return new Value{ _O = s }; }
  public static Value New( byte [] x ){ return new Value{ _O = x }; }

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
Util.cs = various utility classes.

SQL-specific ( namespace SQLNS )
================================
SqlExec.cs = parsing and execution of SQL statements.
Block.cs = list of statements for execution.
Exp.cs, ExpStd.cs, ExpConv.cs = scalar expressions.
TableExp.cs = table-valued expressions.
Group.cs = implementation of GROUP BY.
Sort.cs = implementation of ORDER BY.
IdSet.cs = optimisation of WHERE.

*/
