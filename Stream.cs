namespace DBNS {

using IO = System.IO;
using G = System.Collections.Generic;

class FullyBufferedStream : IO.Stream
{
  // Implementation of IO.Stream in which all writes are buffered, and buffers are kept for every page access ( unless Rollback is called ).
  // Writes are recorded in the supplied log file, which ensures atomic updates.
  // Flush() must be called to write changes to the underlying stream, alternatively Rollback() may be called to discard changes.

  public FullyBufferedStream ( Log log, long fileId, IO.Stream f ) 
  { 
    Log = log;
    FileId = fileId;
    BaseStream = f; 
    Len = BaseStream.Length;
    Pos = 0;
    ReadAvail = 0;
    WriteAvail = 0;
  }

  readonly Log Log; // Log file to ensure atomic updates.
  readonly long FileId; // For Log file.
  readonly IO.Stream BaseStream; // Underlying stream.

  long Len; // File length
  long Pos; // Current position in the file

  readonly G.Dictionary<long,byte[]> Buffers = new G.Dictionary<long,byte[]>();
  readonly G.SortedSet<long> UnsavedPageNums = new G.SortedSet<long>();

  // Buffer size constants.
  const int BufferShift = 12; // Log base 2 of BufferSize.
  const int BufferSize = 1 << BufferShift;

  byte [] CurBuffer; // The current buffer.
  long CurBufferNum = -1;  // The page number of the current buffer.
  int CurIndex;      // Index into current buffer ( equal to Pos % BufferSize ).
  int WriteAvail;    // Number of bytes available for writing in CurBuffer ( from CurIndex ).
  int ReadAvail;     // Number of bytes available for reading in CurBuffer ( from CurIndex ).

  bool UnsavedAdded; // Current buffer has been added to UnsavedPageNums.

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
      else if ( Pos < Len ) DoSeek( true );
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
          UnsavedPageNums.Add( CurBufferNum );
          UnsavedAdded = true;
        }
        for ( int i = 0; i < got; i += 1 ) CurBuffer[ CurIndex + i ] = b[ off + i ];
        off += got;
        n -= got;
        Pos += got;
        CurIndex += got;
        WriteAvail -= got;
      }
      else DoSeek( false );
    }
  }

  // Version of Write which checks the first byte written is zero.
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
          UnsavedPageNums.Add( CurBufferNum );
          UnsavedAdded = true;
        }
        for ( int i = 0; i < got; i += 1 ) CurBuffer[ CurIndex + i ] = b[ off + i ];
        off += got;
        n -= got;
        Pos += got;
        CurIndex += got;
        WriteAvail -= got;
      }
      else DoSeek( false );
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
      long pos = bufferNum << BufferShift;
      long n = Len - pos;
      if ( n > BufferSize ) n = BufferSize;
      if ( BaseStream.Position != pos ) BaseStream.Position = pos;
      BaseStream.Write( b, 0, (int)n );
    }
    UnsavedPageNums.Clear();
    UnsavedAdded = false;
    BaseStream.SetLength( Len );
    BaseStream.Flush();
  }

  public override void Close()
  {
    Rollback();
    BaseStream.Close();
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

  void DoSeek( bool read )
  {
    if ( CurBufferNum != ( Pos >> BufferShift ) )
    {
      CurBufferNum = Pos >> BufferShift;
      CurBuffer = GetBuffer( CurBufferNum );
      UnsavedAdded = false;
    }

    CurIndex = (int) ( Pos & ( BufferSize - 1 ) );
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
    long pos = bufferNum << BufferShift;
    if ( BaseStream.Position != pos ) BaseStream.Position = pos;
    int i = 0;
    while ( i < BufferSize )
    {
      int got = BaseStream.Read( result, i, BufferSize - i );
      if ( got == 0 ) break;
      i += got;
    }
    return result;
  }

} // end class FullyBufferedStream

} // end namespace DBNS