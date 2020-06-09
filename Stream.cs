namespace DBNS {

using IO = System.IO;
using G = System.Collections.Generic;

class FullyBufferedStream : IO.Stream
{
  Log Log;
  long FileId;

  public FullyBufferedStream ( Log log, long fileId, IO.Stream f ) 
  { 
    Log = log;
    FileId = fileId;
    F = f; 
    Len = F.Length;
    Pos = 0;
    ReadAvail = 0;
    WriteAvail = 0;
  }

  // Note : Flush() must be called to write changes to the underlying stream, alternatively Rollback() may be called to discard changes.

  // Buffer size.
  const int BufferShift = 12;
  const int BufferSize = 1 << BufferShift;
  const int BufferMask = BufferSize - 1;

  byte [] CurBuffer; // The current buffer.
  int CurIndex;      // Index into current buffer ( equal to Pos & BufferMask ).
  int WriteAvail;    // Number of bytes available for writing in CurBuffer ( from CurIndex ).
  int ReadAvail;     // Number of bytes available for reading in CurBuffer ( from CurIndex ).

  long Pos; // Current position in the file
  long Len; // File length

  readonly IO.Stream F; // Underlying stream.
  readonly G.Dictionary<long,byte[]> Buffers = new G.Dictionary<long,byte[]>();
  readonly G.SortedSet<long> UnsavedPageNums = new G.SortedSet<long>(); // Could used HashSet, but this can reduce seeks.

  bool CurBufferSetUnsaved; // Current buffer has been marked as unsaved ( has been written )

  public override int Read( byte[] b, int off, int n )
  { 
    int request = n;
    while ( n > 0 )
    {
      int got = n > ReadAvail ? ReadAvail : n;
      if ( got > 0 )
      {
        for ( int i = 0; i < got; i += 1 ) b[ off + i ] = CurBuffer[ CurIndex + i ];
        off += got;
        n -= got;
        Pos += got;
        CurIndex += got;
        ReadAvail -= got;
      }
      else if ( Pos < Len )
      {
        CurBuffer = GetBuffer( Pos >> BufferShift );
        CurIndex = (int) ( Pos & BufferMask );
        ReadAvail = BufferSize - CurIndex;
        if ( ReadAvail > Len - Pos ) ReadAvail = (int)( Len - Pos );
        WriteAvail = 0;
        CurBufferSetUnsaved = false;
      }
      else break;
    }
    return request - n;
  }

  public void Rollback()
  {
    Buffers.Clear();
    UnsavedPageNums.Clear();
    CurBufferSetUnsaved = false;
    ReadAvail = 0;
    WriteAvail = 0;      
  }

  public override void Close()
  {
    Rollback();
    F.Close();
  }

  public override void Flush()
  {
    foreach ( long bufferNum in UnsavedPageNums )
    {
      byte [] b = GetBuffer( bufferNum );
      long ppos = bufferNum << BufferShift;
      long n = Len - ppos;
      if ( n > BufferSize ) n = BufferSize;
      if ( F.Position != ppos ) F.Seek( ppos, 0 );
      F.Write( b, 0, (int)n );
    }
    UnsavedPageNums.Clear();
    CurBufferSetUnsaved = false;
    F.SetLength( Len );
    F.Flush();
  }

  public bool Write( byte[] b, int off, int n, bool checkFirstByteZero )
  {
    Log.LogWrite( FileId, Pos, b, off, n );

    if ( Pos + n > Len ) Len = Pos + n;

    while ( n > 0 )
    {
      int got = n > WriteAvail ? WriteAvail : n;
      if ( got > 0 )
      {
        if ( checkFirstByteZero )
        {
          if ( CurBuffer[ CurIndex ] != 0 ) return false;
          checkFirstByteZero = false;
        }
        if ( !CurBufferSetUnsaved )
        {
          UnsavedPageNums.Add( Pos >> BufferShift );
          CurBufferSetUnsaved = true;
        }
        for ( int i = 0; i < got; i += 1 ) CurBuffer[ CurIndex + i ] = b[ off + i ];
        off += got;
        n -= got;
        Pos += got;
        CurIndex += got;
        WriteAvail -= got;
      }
      else
      {
        CurBuffer = GetBuffer( Pos >> BufferShift );
        CurIndex = (int) ( Pos & BufferMask );
        WriteAvail = BufferSize - CurIndex;
        ReadAvail = 0;
        CurBufferSetUnsaved = false;
      }
    }
    return true;
  }

  public override void Write( byte[] b, int off, int n )
  {
    Log.LogWrite( FileId, Pos, b, off, n );

    if ( Pos + n > Len ) Len = Pos + n;

    while ( n > 0 )
    {
      int got = n > WriteAvail ? WriteAvail : n;
      if ( got > 0 )
      {
        if ( !CurBufferSetUnsaved )
        {
          UnsavedPageNums.Add( Pos >> BufferShift );
          CurBufferSetUnsaved = true;
        }
        for ( int i = 0; i < got; i += 1 ) CurBuffer[ CurIndex + i ] = b[ off + i ];
        off += got;
        n -= got;
        Pos += got;
        CurIndex += got;
        WriteAvail -= got;
      }
      else
      {
        CurBuffer = GetBuffer( Pos >> BufferShift );
        CurIndex = (int) ( Pos & BufferMask );
        WriteAvail = BufferSize - CurIndex;
        ReadAvail = 0;
        CurBufferSetUnsaved = false;
      }
    }
  }

  byte [] GetBuffer( long bufferNum )
  {
    byte [] result;
    if ( Buffers.TryGetValue( bufferNum, out result ) ) return result;
    result = new byte[ BufferSize ];
    Buffers[ bufferNum ] = result;
    long ppos = bufferNum << BufferShift;
    if ( F.Position != ppos ) F.Seek( ppos, 0 );
    int i = 0;
    while ( i < BufferSize )
    {
      int got = F.Read( result, i, BufferSize - i );
      if ( got == 0 ) break;
      i += got;
    }
    return result;
  }

  public override long Seek( long to, System.IO.SeekOrigin how )
  { 
    long newpos;
    if ( how == System.IO.SeekOrigin.Begin )
      newpos = to;
    else if ( how == System.IO.SeekOrigin.End )
      newpos = Len + to;
    else // how == System.IO.SeekOrigin.Current
      newpos = Pos + to;
    if ( Pos != newpos )
    {
      Pos = newpos;
      WriteAvail = 0;
      ReadAvail = 0; // Could optimise seeks within current page.
    }
    return newpos;
  }

  public override void SetLength( long x )
  {
    Log.SetLength( FileId, x );
    Len = x;
    ReadAvail = 0;
    WriteAvail = 0;
  }

  public override bool CanRead { get{ return true; } }
  public override bool CanWrite { get{ return true; } }
  public override bool CanSeek { get{ return true; } }
  public override long Length { get{ return Len; } }
  public override long Position { get{ return Pos; } set{ Seek(value,0); } }

  // FastRead can be fast because if possible not bytes are copied, instead the underlying buffer (CurBuffer) and an index into it is returned.
  // To be fast, n should be significantly smaller than BufferSize, and Seek must be used infrequently.
  public byte[] FastRead( int n, out int ix )
  {
    if ( ReadAvail >= n ) // Common case.
    {
      ix = CurIndex;
      CurIndex += n;
      Pos += n;
      ReadAvail -= n;
      return CurBuffer;
    }
    else // This should only happen rarely ( but will always happen after a Seek ).
    {
      byte [] result = new byte[ n ];
      Read( result, 0, n );
      ix = 0;
      return result;
    }
  }  

/* Optional overrides ( if ReadByte and WriteByte are used )

  public override int ReadByte()
  {
    if ( ReadAvail == 0 )
    {
      if ( Pos == Len ) return -1;
      CurBuffer = GetBuffer( Pos >> BufferShift );
      CurIndex = (int) ( Pos & BufferMask );
      ReadAvail = BufferSize - CurIndex;    
      if ( ReadAvail > Len - Pos ) ReadAvail = (int)( Len - Pos );
      CurBufferSetUnsaved = false;
      WriteAvail = 0;
    }
    Pos += 1;
    ReadAvail -= 1;
    return CurBuffer[ CurIndex++ ];
  }

  public override void WriteByte( byte b )
  {
    if ( Pos + 1 > Len ) Len = Pos + 1;
    if ( WriteAvail == 0 )
    {
      CurBuffer = GetBuffer( Pos >> BufferShift );
      CurIndex = (int) ( Pos & BufferMask );
      WriteAvail = BufferSize - CurIndex;
      ReadAvail = 0;
      CurBufferSetUnsaved = false;
    }
    if ( !CurBufferSetUnsaved )
    {
      Dirty.Add( Pos >> BufferShift );
      CurBufferSetUnsaved = true;
    }
    CurBuffer[ CurIndex++ ] = b;
    Pos += 1;
    WriteAvail -= 1;
  }
*/

} // end class FullyBufferedStream

} // end namespace DBNS