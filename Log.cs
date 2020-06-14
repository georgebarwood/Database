namespace DBNS {

using IO = System.IO;
using G = System.Collections.Generic;

/* 
  Log accumulates updates to a set of numbered files, which are saved to a log file before the files are actually written.
  This ensures that either all of the updates are applied or none, and the files are not left in an inconsistent state.
  On startup, the log file is examined, and if it is not empty, the updates are applied ( this will be unusual ).

  A Log entry is fileId (64 bits), fileOffset (64 bits), size (32 bits) and size bytes of data.
*/

class Log
{
  IO.FileStream LF;
  IO.BinaryWriter LFW;

  public Log( string directory )
  {
    LF = new IO.FileStream( directory + "log", IO.FileMode.OpenOrCreate );
    if ( LF.Length != 0 ) // Read the log file, applying the changes to the specified files.
    {
      byte [] b = new byte[ 0x1000 ];
      var files = new G.Dictionary<long,IO.Stream>();
      
      LF.Position = 0;
      var fr = new IO.BinaryReader( LF );
      while ( LF.Position < LF.Length )
      {
        long fileId = fr.ReadInt64();
        long fileOffset = fr.ReadInt64();
        int size = fr.ReadInt32();
        IO.Stream f;
        if ( !files.TryGetValue( fileId, out f ) )
        {
          f = new IO.FileStream( directory + fileId, IO.FileMode.OpenOrCreate );
          files[ fileId ] = f;
        }
        if ( size > 0 )
        {
          if ( size > b.Length ) b = new byte[ size ];
          LF.Read( b, 0, size );
          f.Position = fileOffset;
          f.Write( b, 0, size );
        }
        else f.SetLength( fileOffset );
      }
      foreach( G.KeyValuePair<long,IO.Stream> p in files )
      {
        IO.Stream f = p.Value;
        long length = f.Length;
        f.Close();
        if ( length == 0 ) 
        {
          var fi = new IO.FileInfo( directory + p.Key );
          fi.Delete();
        }
      }
      Reset();
    }
    LFW = new IO.BinaryWriter( LF );
  }

  public void LogWrite( long fileId, long fileOffset, byte [] buffer, int offset, int size )
  {
    if ( size > 0 )
    {
      LFW.Write( fileId );
      LFW.Write( fileOffset );
      LFW.Write( size );
      LFW.Write( buffer, offset, size );
    }    
  }

  public void SetLength( long fileId, long fileOffset ) // If fileOffset = 0, the file will be deleted.
  {
    int size = 0;
    LFW.Write( fileId );
    LFW.Write( fileOffset );
    LFW.Write( size );
  }

  public bool Commit()
  {
    if ( LF.Length == 0 ) return false;
    LF.Flush();
    return true;
  }

  public void Reset()
  {
    LF.SetLength( 0 );
    LF.Position = 0;
    LF.Flush();
  }

} // end class Log

} // end namespace DBNS
