using G = System.Collections.Generic;

public class WebServer
{
  readonly System.Net.HttpListener listener;
  readonly DBNS.Database Database;

  public WebServer( )
  {
    Database = DBNS.Database.GetDatabase( @"C:\Databasefiles\Test\" );  

    if ( Database.IsNew ) Init.Go( Database ); // Creates initial tables, stored procedures etc.

    listener = new System.Net.HttpListener( );
    //listener.Prefixes.Add( "http://localhost:8080/" ); 

    // Note: to listen to internet addresses, program will need to run with admin privilege
    // Or configuration will be needed. e.g. run CMD as Administrator, then type:
    // netsh http add urlacl url=http://+:8080/ user=GEORGE-DELL\pc
    listener.Prefixes.Add( "http://+:8080/" );

    listener.Start( );   
  }

  void HandleRequest( System.Net.HttpListenerContext ctx )
  {
    try
    {
      var outStream = new System.IO.MemoryStream( 0x1000 );
      var wrs = new WebResultSet( ctx, outStream );
      var stopWatch = new System.Diagnostics.Stopwatch(); 
      stopWatch.Start();
      lock( Database ) Database.Sql( "EXEC web.Main()", wrs );
      stopWatch.Stop();
      System.Console.WriteLine( "Time (ms)=" + stopWatch.ElapsedTicks / (System.Diagnostics.Stopwatch.Frequency/1E3 ) );
      wrs.SendEmails();
      try
      {
        outStream.Position = 0;
        outStream.CopyTo( ctx.Response.OutputStream );
      } catch ( System.Exception ) {} // Exceptions due to client closing connection etc. are normal.
    }
    catch ( System.Exception e ) { System.Console.WriteLine( "Exception: " + e ); }
    finally { ctx.Response.OutputStream.Close(); }
  }
  
  class Request
  {
    public WebServer Ws;
    public System.Net.HttpListenerContext Ctx;
    public Request( WebServer ws )
    {
      Ws = ws; 
      Ctx = ws.listener.GetContext();
    }
  }

  static void HandleRequestStart( System.Object obj )
  {
    Request rq = (Request) obj;
    rq.Ws.HandleRequest( rq.Ctx );
  }

  static void WebServerStart( System.Object obj )
  {
    WebServer ws = ( WebServer ) obj;
    System.Console.WriteLine( "Database Webserver running, press any key to quit." );
    while ( ws.listener.IsListening )
    {  
      System.Threading.ThreadPool.QueueUserWorkItem( HandleRequestStart, new Request( ws ) );
    }
  }

  public void Run( )
  {
    System.Threading.ThreadPool.QueueUserWorkItem( WebServerStart, this );
  }

  public void Stop( )
  {
    listener.Stop( );
    listener.Close( );
  }

 } // end class WebServer

////////////////////////////////////////////////////////////////////////////////////////////

class Program
{
  static void Main( string[] args )
  {
    try
    {
      WebServer ws = new WebServer( );
      ws.Run( );
      System.Console.ReadKey( );
      ws.Stop( );
    }
    catch( System.Exception e )
    {
      System.Console.WriteLine("Init error: " + e );
      System.Console.ReadKey( );
    }
  }
} // end class Program


////////////////////////////////////////////////////////////////////////////////////////////

/* WebResultSet handles SELECT output and also makes http request information available to SQL code */

class WebResultSet : DBNS.ResultSet
{
  System.Net.HttpListenerContext Ctx;
  System.IO.MemoryStream OutStream;

  System.Collections.Specialized.NameValueCollection Form;
  FormFile [] Files;
  G.List<string>CookieNames;

  int Mode;
  DBNS.ColInfo CI;
  System.DateTime Now;
  bool GotNow;

  G.List<Email> EmailList; // Emails to send.

  struct Email
  {
    public string From;
    public string To;
    public string Subject;
    public string Body;
    public string Server;
    public int Port;
    public string Username;
    public string Password;
  }

  public WebResultSet( System.Net.HttpListenerContext ctx, System.IO.MemoryStream outStream )
  {
    Ctx = ctx;
    OutStream = outStream;

    if ( ctx.Request.HasEntityBody )
    {
      if ( ctx.Request.ContentType == "application/x-www-form-urlencoded" )
      {
        string input = null;
        using ( System.IO.StreamReader reader = new System.IO.StreamReader( ctx.Request.InputStream ) )
        {
          input = reader.ReadToEnd( );
        }
        Form = System.Web.HttpUtility.ParseQueryString( input );
      }
      else /* Assume multipart/form-data */
      {
        Files = ParseMultipart( ctx.Request );
      }
    }

    CookieNames = new G.List<string>();
    foreach ( string k in ctx.Request.Cookies ) CookieNames.Add( k );
  }

  public override bool NewRow( DBNS.Value [] row )
  {
    if ( Mode == 0 )
    {
      object v = row[0]._O;
      if ( v is string ) PutUtf8( (string)v );
      else
      {
        int code = (int) row[0].L;
        v = row[1]._O;

        if ( code == 10 ) Ctx.Response.ContentType = (string)v;
        else if ( code == 11 ) PutBytes( (byte[]) v );
        else if ( code == 14 ) Ctx.Response.StatusCode = (int)row[1].L;
        else if ( code == 15 ) Ctx.Response.Redirect( (string) v );
        else if ( code == 16 )
        {
          System.Net.Cookie ck = new System.Net.Cookie( (string)v, row[2].S );
          string expires = row[3].S;
          if ( expires != "" ) ck.Expires = System.DateTime.Parse( expires );
          Ctx.Response.Cookies.Add( ck );
        }
        else if ( code == 20 ) SaveEmail( row );
      }
    }
    else if ( Mode == 1 ) // HTML table mode
    {
      PutUtf8( "<tr>" );
      for ( int i = 0; i < CI.Count; i += 1 )
      {
        var type = CI.Type[ i ];
        PutUtf8( type == DBNS.DataType.String ? "<td>" : "<td align=right>" );
        PutUtf8( DBNS.Util.ToHtml( row[ i ], type ) );
      }
    }
    return true;
  }

  public override void NewTable( DBNS.ColInfo ci )
  {
    CI = ci;
    if ( Mode == 1 )
    {
      PutUtf8( "<table><tr>" );
      for ( int i = 0; i < ci.Count; i += 1 )
        PutUtf8( "<th>" + ci.Name[ i ] );
    }
  }

  public override void EndTable()
  {
    if ( Mode == 1 && CI != null )
    {
      PutUtf8( "</table>" );
      CI = null;
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  void SaveEmail( DBNS.Value [] row )
  {
    Email em;
    em.From = row[1].S;
    em.To = row[2].S;
    em.Subject = row[3].S;
    em.Body = row[4].S;
    em.Server = row[5].S;
    em.Port = (int)row[6].L;
    em.Username = row[7].S;
    em.Password = row[8].S;
    if ( EmailList == null ) EmailList = new G.List<Email>();
    EmailList.Add( em );
  }

  public void SendEmails()
  {
    if ( EmailList == null ) return;
    foreach ( Email em in EmailList )
    {
      var mail = new System.Net.Mail.MailMessage();
      var server = new System.Net.Mail.SmtpClient( em.Server );

      mail.From = new System.Net.Mail.MailAddress( em.From );
      mail.To.Add( em.To );
      mail.Subject = em.Subject;
      mail.Body = em.Body;

      server.Port = em.Port;
      server.UseDefaultCredentials = false; 
      server.Credentials = new System.Net.NetworkCredential(em.Username, em.Password);
      server.EnableSsl = true;
      server.DeliveryMethod = System.Net.Mail.SmtpDeliveryMethod.Network;
      try
      {
        server.Send(mail);
      } catch ( System.Exception e ) 
      {
        System.Console.WriteLine( "Exception sending email " + e.ToString() );
      }
    }
  }

  void PutBytes( byte[] b )
  {
    OutStream.Write( b, 0, b.Length );
  }

  byte [] EncBuffer = new byte[512];

  void PutUtf8( string s )
  {
    int slen = s.Length;
    int need = System.Text.Encoding.UTF8.GetMaxByteCount( slen );
    if ( need > EncBuffer.Length ) EncBuffer = GetBuf( need );
    int nb = System.Text.Encoding.UTF8.GetBytes( s, 0, slen, EncBuffer, 0 );
    OutStream.Write( EncBuffer, 0, nb );
  }

  static byte[] GetBuf( int need )
  { 
    int n = 512; 
    while ( n < need ) n *= 2; 
    return new byte[n]; 
  }

  public override void SetMode( long mode )
  {
    EndTable();
    Mode = (int)mode;
  }

  /////////////////// Functions to access global information
  public const int GTicks = 0;

  public override long Global( int kind )
  {
    switch( kind )
    {
      case GTicks:
       if ( !GotNow) { Now = System.DateTime.UtcNow; GotNow = true; }
       return Now.Ticks; // 10 million ticks in a second.
    }
    return 0;
  }

  /////////////////// Functions to make http request information available to SQL code

  public const int AbsPath = 0, QueryString = 1, FormString = 2, Cookie = 3;

  public override string Arg( int kind, string name )
  {
    string a = null;
    switch ( kind )
    {
      case AbsPath: a = System.Net.WebUtility.UrlDecode( Ctx.Request.Url.AbsolutePath ); break;
      case QueryString: a = Ctx.Request.QueryString[ name ]; break;
      case FormString: a = Form == null ? null : Form[ name ]; break;
      case Cookie: a = Ctx.Request.Cookies[ name ].Value; break;
    }
    return a == null ? "" : a;
  }

  public override string ArgName( int kind, int ix )
  {
    string a = "";
    switch ( kind )
    {
      case QueryString: 
        var c = Ctx.Request.QueryString;
        if ( ix < c.Count ) a = c.GetKey( ix ); 
        break;
      case FormString: 
        if ( Form != null && ix < Form.Count )  
          a = Form.GetKey( ix );
        break;
      case Cookie:
        if ( ix < CookieNames.Count ) a = CookieNames[ix]; 
        break;        
    }
    return a == null ? "" : a;
  }

  public override byte [] FileContent ( int ix )
  {
    return Files[ ix ].Content;
  }

  public override string FileAttr( int ix, int kind )
  {
    if ( Files == null || ix >= Files.Length ) return "";
    switch( kind )
    {
      case 0: return Files[ ix ].Name;
      case 1: return Files[ ix ].ContentType;
      case 2: return Files[ ix ].Filename;
    }
    return "";
  }

  // Rest relates to parsing multipart request body for file upload.

  struct FormFile
  {
    public string Name;
    public string ContentType;
    public string Filename;
    public byte [] Content;
    public FormFile( string name, string ct, string fn, byte [] b )
    {
      Name = name; ContentType = ct; Filename = fn; Content = b; 
    }
  }

  FormFile[] ParseMultipart( System.Net.HttpListenerRequest rq )
  {
    /* Typical multipart body would be:
    ------WebKitFormBoundaryVXXOTFUWdfGpOcFK
    Content-Disposition: form-data; name="f1"; filename="test.txt"
    Content-Type: text/plain

    Hello there

    ------WebKitFormBoundaryVXXOTFUWdfGpOcFK
    Content-Disposition: form-data; name="submit"

    Upload
    ------WebKitFormBoundaryVXXOTFUWdfGpOcFK--
    */ 

    var flist = new G.List<FormFile>();

    byte[] data = ToByteArray( rq.InputStream );
    System.Text.Encoding encoding = System.Text.Encoding.UTF8; // Not entirely clear what encoding should be used for headers.
    int pos = 0; /* Index into data */
    while ( true )
    {
      int headerLength = IndexOf( data, encoding.GetBytes( "\r\n\r\n" ), pos ) - pos + 4;

      if ( headerLength < 4 ) break;
      string headers = encoding.GetString( data, pos, headerLength );
      pos += headerLength;

      // The first header line is the delimiter
      string delimiter = headers.Substring( 0, headers.IndexOf( "\r\n" ) );

      // Extract atrtributes from header
      string contentType = Look( @"(?<=Content\-Type:)(.*?)(?=\r\n)", headers );
      string name = Look( @"(?<= name\=\"")(.*?)(?=\"")", headers );
      string filename = Look( @"(?<=filename\=\"")(.*?)(?=\"")", headers );

      // Get the content length 
      byte[] delimiterBytes = encoding.GetBytes( "\r\n" + delimiter );
      int contentLength = IndexOf( data, delimiterBytes, pos ) - pos;

      if ( contentLength < 0 ) break;     
   
      // Extract the content from data
      byte[] content = new byte[ contentLength ];
      System.Buffer.BlockCopy( data, pos, content, 0, contentLength );
      pos += contentLength + delimiterBytes.Length;

      flist.Add( new FormFile( name, contentType, filename, content ) );  
    }
    return flist.ToArray();
  }

  string Look( string pat, string headers )
  {
    System.Text.RegularExpressions.Regex re = new System.Text.RegularExpressions.Regex( pat );
    System.Text.RegularExpressions.Match m = re.Match( headers );
    return m.Success ? m.Value.Trim( ) : "";
  }

  int IndexOf( byte[] b, byte[] pat, int p )
  {
     while( true )
    {
      p = System.Array.IndexOf( b, pat[0], p );
      if ( p < 0 || p+pat.Length > b.Length ) return -1;
      int matched = 1;
      while ( true )
      {
        if ( matched == pat.Length ) return p;
        if ( b[p+matched] != pat[matched] ) break;
        matched += 1;
      }
      p += 1;
    }
  }

  byte[] ToByteArray( System.IO.Stream stream )
  {
    byte[] buffer = new byte[1024];
    using ( System.IO.MemoryStream ms = new System.IO.MemoryStream( ) )
    {
      while ( true )
      {
        int read = stream.Read( buffer, 0, buffer.Length );
        if ( read <= 0 ) return ms.ToArray( );
        ms.Write( buffer, 0, read );
      }
    }
  }
} // end class WebResultSet
