namespace DBNS
{

// Various enumerated types, structures and utility classes.

using G = System.Collections.Generic;
using IO = System.IO;
using IOS = System.Runtime.InteropServices;

enum Action { Add, Drop, ColumnRename, Modify };

struct AlterAction
{
  public Action Operation;
  public string Name, NewName;
  public DataType Type;
}

enum Token { 
  Less, LessEqual, GreaterEqual, Greater, Equal, NotEqual, In, /* Note: order is significant */
  Plus, Minus, Times, Divide, Percent, VBar,
  And, Or,
  Name, Number, Decimal, Hex, String, LBra, RBra, Comma, Colon, Dot, Exclamation, Unknown, Eof }

class TokenInfo
{
  public static int[] Precedence = new int[] 
  {
    10, 10, 10, 10, 10, 10, 10,
    20, 20, 30, 30, 30, 15
,
    8, 5
  };

  static string[] NameData = new string[]
  {
    "<", "<=", ">=", ">", "=", "!=", "IN",
    "+", "-", "*", "/", "%", "|",
    "AND", "OR", 
    "Name", "Number", "Decimal", "Hex", "String", "(", ")", ",", ":", ".", "!", "?", "End of File"
  };

  public static string Name( Token t ) { return NameData[ (int) t ]; }

  public static bool OperatorValid( Token op, DataType t )
  {
    if ( t >= DataType.Decimal ) t = DataType.Decimal;
    switch ( t )
    {
      case DataType.String: return op == Token.Plus || op <= Token.NotEqual || op == Token.VBar ;
      case DataType.Binary: return op == Token.Plus || op <= Token.NotEqual;
      case DataType.Bigint: 
      case DataType.Double:
      case DataType.Decimal:
        return op <= Token.NotEqual || op >= Token.Plus && op <= Token.Percent;
      case DataType.Bool:
        return op == Token.And || op == Token.Or || op == Token.Equal || op == Token.NotEqual;
   }
   return false;
  }
}

class DTI // "Data Type Info"
{
  /* None, Binary, String, Bigint, Double, Int, Float, Smallint, Tinyint, Bool, ScaledInt  */

  static int[] SizeData = new int[]{ 0,8,8,8,8, 4, 4, 2, 1, 1 };

  static string[] Names = new string[]{ "none", "binary", "string", "bigint", "double", "int", "float", "smallint", "tinyint", "bool", "scaledint" };

  static DataType[] BaseData = new DataType[]{ DataType.None, DataType.Binary, DataType.String, DataType.Bigint, DataType.Double, 
     DataType.Bigint, DataType.Double, DataType.Bigint, DataType.Bigint, DataType.Bool, DataType.ScaledInt };

  static byte[] DecimalByteSize = new byte[]{ 0, 1, 1, 2, 2, 3, 3, 4, 4, 4, 5, 5, 6, 6, 6, 7, 7, 8, 8 };

  public static byte[] ZeroByte = new byte[0];

  public static Value Default( DataType t )
  {
    Value result = new Value();
    switch (t)
    {
      case DataType.Binary: result.O = ZeroByte; break;
      case DataType.String: result.O = ""; break;
      case DataType.Float: case DataType.Double: result.D = 0; break;
    }     
    return result;
  }

  public static DataType Base( DataType t ) 
  { 
    if ( t < DataType.Decimal ) return BaseData[(int)t]; 
    DataType result = Decimal( 18, Scale( t ) );
    return result;
  }

  public static int Size( DataType t )
  { 
    if ( t <= DataType.Decimal ) return SizeData[ (int) t ];
    int p = ( (int)t >> 4 ) % 64;
    return DecimalByteSize[ p ];
  }

  public static int Scale( DataType t )
  {
    if ( t >= DataType.Decimal ) return ((int)t) / 1024;
    return -1;
  }

  public static int Precision( DataType t )
  {
    return ( ((int)t) / 16 ) % 64;
  }

  public static DataType Decimal( int p, int s )
  {
    return (DataType)( (int)DataType.Decimal + p * 16 + s * 1024 );
  }

  public static string Name( DataType t )
  {
    if ( t < DataType.Decimal ) return Names[ (int)t ];
    return "decimal(" + Precision(t) + "," + Scale(t) + ")";
  }
}

class Util
{
  public static ulong PowerTen( int scale )
  {
    ulong result = 1;
    while ( scale -- > 0 ) result *= 10;
    return result;
  }

  public static string Quote( string s )
  {
    return "'" + s.Replace( "'", "''" ) + "'";
  }

  public static int GetHashCode( Value a, DataType t )
  {
    switch( t )
    {
      case DataType.String: return ((string)a._O).GetHashCode();
      case DataType.Binary: return GetHashCode((byte[])a._O);
      default: return (int)a.L;
    }
  }

  public static int Compare( Value a, Value b, DataType t ) // Not used yet?
  {
    switch ( t ) 
    { 
      case DataType.Binary: return Util.Compare( (byte[])a._O, (byte[])b._O ); 
      case DataType.String: return string.Compare( (string)a._O, (string)b._O ); 
      case DataType.Float:
      case DataType.Double: return (a.D).CompareTo( b.D );
      default: return (a.L).CompareTo( b.L );
    }
  }

  public static int GetHashCode( byte [] a )
  {
    int hash = a.Length;
    for ( int i = 0; i < a.Length; i += 1 ) hash += a[i];
    return hash;
  }

  public static int Compare( byte[] a, byte[] b )
  {
    if ( a.Length != b.Length ) 
      return a.Length > b.Length ? +1 : -1;
    else for ( int i=0; i < a.Length; i += 1 )
    {
      if ( a[i] != b[i] ) return a[i] > b[i] ? +1 : -1;
    }
    return 0;
  }

  public static string Hex = "0123456789abcdef";

  public static string ToString( byte[] b )
  {
    var sb = new System.Text.StringBuilder();
    sb.Append( "0x" );
    for ( int i = 0; i < b.Length; i += 1 )
    {
      sb.Append( Hex[ b[i] / 16 ] );
      sb.Append( Hex[ b[i] % 16 ] );
    }
    return sb.ToString();
  }

  static byte GetHex( char c )
  {
    if ( c >= '0' && c <= '9' ) return (byte) ( c - '0' );
    if ( c >= 'a' && c <= 'f' ) return (byte) ( 10 + ( c - 'a' ) );
    if ( c >= 'A' && c <= 'F' ) return (byte) ( 10 + ( c - 'A' ) );
    throw new System.Exception( "Unexpected character in hex constant" );
  }

  public static byte[] ParseHex( string s )
  {
    byte [] result = new byte[ (s.Length-2) / 2 ];
    for ( int i=0; i < result.Length; i += 1 )
    {
      result[i] = (byte) ( GetHex( s[i*2+2] ) * 16 + GetHex( s[i*2+3] ) );
    }
    return result;
  }

  public static void Set( byte[] data, int off, ulong x, int size ) // Saves x at data[off] using size bytes.
  {
    for ( int i = 0; i < size; i += 1 )
    {
      data[off + i] = (byte)x;
      x >>= 8;
    }
  }

  public static ulong Get( byte[] data, int off, int size, DataType t ) // Extract unsigned value of size bytes from data[off].
  {
    ulong x = 0;
    for ( int i = size-1; i >= 0; i -= 1 )
      x = ( x << 8 ) + data[off + i];

    if ( size < 8 )
    {
      if  ( t == DataType.Float ) x = Conv.UnpackFloat( (uint)x );
      else if ( t != DataType.Bool ) 
      {
        ulong signBit = 1UL << (size*8 - 1 );
        if ( ( signBit & x ) != 0 )
        {
          x = x + ( 0xffffffffffffffffUL << ( size * 8 ) );
        }
      }
    }
    return x;
  }
}

class ValueStart
{
  Value K;
  DataType T;
  public ValueStart( Value k, DataType t ) { K = k; T = t; }
  public int Compare( ref IndexFileRecord r )
  {
    int cf = Util.Compare( K, r.Col[0], T );
    return cf == 0 ? -1 : cf;
  }
}

class LongStart
{
  long K;
  public LongStart( long k ) { K = k; }
  public int Compare( ref IndexFileRecord r )
  {
    int cf = K <= r.Col[0].L ? -1 : +1;
    return cf == 0 ? -1 : cf;
  }
}

class StringStart
{
  string K;
  public StringStart( string k ) { K = k; }
  public int Compare( ref IndexFileRecord r )
  {
    int cf = string.Compare( K, (string)r.Col[0]._O );
    return cf == 0 ? -1 : cf;
  }
}

class BinaryStart
{
  byte [] K;
  public BinaryStart( byte [] k ) { K = k; }
  public int Compare( ref IndexFileRecord r )
  {

    int cf = Util.Compare( K, (byte[])r.Col[0]._O );
    return cf == 0 ? -1 : cf;
  }
}

// Extensions to IO.BinaryReader, IO.BinaryWriter allows byte arrays to be written/read.

class BinaryWriter : IO.BinaryWriter
{
  public BinaryWriter( IO.Stream s ) : base( s ) {}
  public void WriteBytes( byte[] b )
  {
    Write7BitEncodedInt( b.Length );
    Write( b, 0, b.Length );
  }
}

class BinaryReader : IO.BinaryReader 
{
  public BinaryReader( IO.Stream s ) : base( s ) {}
  public byte [] ReadBytes()
  {
    int n = Read7BitEncodedInt();
    byte [] result = new byte[ n ];
    int i = 0;
    while ( i < n )
    {
      int got = Read( result, i, n-i );
      if ( got == 0 ) break;
      i += got;
    }
    return result;
  }
}

[IOS.StructLayout(IOS.LayoutKind.Explicit)]
public struct Conv
{
  [IOS.FieldOffset(0)] public uint U;
  [IOS.FieldOffset(0)] public float F;
  [IOS.FieldOffset(0)] public double D;
  [IOS.FieldOffset(0)] public ulong UL;

  public static ulong UnpackFloat( uint u )
  {
    Conv c = new Conv();
    c.U = u;
    c.D = c.F;
    return c.UL;
  }

  public static uint PackFloat( ulong x )
  {
    Conv c = new Conv();
    c.UL = x;
    c.F = (float)c.D;
    return c.U;
  }
}

class StoredTable
{
  public ColInfo Info;
  public StoredTable( ColInfo info ){ Info = info; }
  public G.List<Value[]> Rows = new G.List<Value[]>();
}

class SingleResultSet : ResultSet // Only holds a single table ( used internally ).
{
  public StoredTable Table;

  public override void NewTable( ColInfo info )
  {
    Table = new StoredTable( info );
  }

  public override bool NewRow( Value [] row )
  {
    Table.Rows.Add( (Value[]) ( row.Clone() ) );
    return true;
  }
} // end class SingleResultSet

} // end namespace DBNS
