namespace DBNS {

using IO = System.IO;
using G = System.Collections.Generic;

class FullyBufferedStream : IO.Stream
{
  // Flush() must be called to write changes to the underlying stream, alternatively Rollback() may be called to discard changes.

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

  readonly Log Log;
  readonly long FileId;
  readonly IO.Stream F; // Underlying stream.
  readonly G.Dictionary<long,byte[]> Buffers = new G.Dictionary<long,byte[]>();
  readonly G.SortedSet<long> UnsavedPageNums = new G.SortedSet<long>();

  // Buffer size constants.
  const int BufferShift = 12;
  const int BufferSize = 1 << BufferShift;

  byte [] CurBuffer; // The current buffer.
  long CurBufferNum = -1;  // The page number of the current buffer.
  int CurIndex;      // Index into current buffer ( equal to Pos & (BufferSize - 1) ).
  int WriteAvail;    // Number of bytes available for writing in CurBuffer ( from CurIndex ).
  int ReadAvail;     // Number of bytes available for reading in CurBuffer ( from CurIndex ).

  long Len; // File length
  long Pos; // Current position in the file

  bool UnsavedAdded; // Current buffer has been added to UnsavedPageNums.

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
        DoSeek( true );
      }
      else break;
    }
    return request - n;
  }

  // Instead of copying bytes, if possible Fastread returns the underlying buffer and an index into it.
  public byte[] FastRead( int n, out int ix )
  {
    if ( ReadAvail == 0 ) DoSeek( true );
    if ( ReadAvail >= n )
    {
      ix = CurIndex;
      CurIndex += n;
      Pos += n;
      ReadAvail -= n;
      return CurBuffer;
    }
    else
    {
      byte [] result = new byte[ n ];
      Read( result, 0, n );
      ix = 0;
      return result;
    }
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
        if ( !UnsavedAdded )
        {
          UnsavedPageNums.Add( Pos >> BufferShift );
          UnsavedAdded = true;
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
        DoSeek( false );
      }
    }
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
        if ( !UnsavedAdded )
        {
          UnsavedPageNums.Add( Pos >> BufferShift );
          UnsavedAdded = true;
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
        DoSeek( false );
      }
    }
    return true;
  }

  public void Rollback()
  {
    Buffers.Clear();
    UnsavedPageNums.Clear();
    UnsavedAdded = false;
    ReadAvail = 0;
    WriteAvail = 0;   
    CurBufferNum = -1;
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
    UnsavedAdded = false;
    F.SetLength( Len );
    F.Flush();
  }

  public override void Close()
  {
    Rollback();
    F.Close();
  }

  void DoSeek( bool read )
  {
    if ( CurBufferNum != ( Pos >> BufferShift ) )
    {
      CurBufferNum = Pos >> BufferShift;
      CurBuffer = GetBuffer( CurBufferNum );
    }

    CurIndex = (int) ( Pos & ( BufferSize - 1 ) );
    UnsavedAdded = false;
    if ( read )
    {
      ReadAvail = BufferSize - CurIndex;
      if ( ReadAvail > Len - Pos ) ReadAvail = (int)( Len - Pos );
      WriteAvail = 0;
    }
    else
    {
      WriteAvail = BufferSize - CurIndex;
      ReadAvail = 0;
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
      ReadAvail = 0;
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

/* Optional overrides ( if ReadByte and WriteByte are used ) */

  public override int ReadByte()
  {
    if ( ReadAvail == 0 ) DoSeek( true );
    Pos += 1;
    ReadAvail -= 1;
    return CurBuffer[ CurIndex++ ];
  }

  public override void WriteByte( byte b )
  {
    if ( Pos + 1 > Len ) Len = Pos + 1;
    if ( WriteAvail == 0 ) DoSeek( false );
    if ( !UnsavedAdded )
    {
      UnsavedPageNums.Add( Pos >> BufferShift );
      UnsavedAdded = true;
    }
    CurBuffer[ CurIndex++ ] = b;
    Pos += 1;
    WriteAvail -= 1;
  }

} // end class FullyBufferedStream

} // end namespace DBNS