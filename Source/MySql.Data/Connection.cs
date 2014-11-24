// Copyright © 2004, 2014, Oracle and/or its affiliates. All rights reserved.
//
// MySQL Connector/NET is licensed under the terms of the GPLv2
// <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most 
// MySQL Connectors. There are special exceptions to the terms and 
// conditions of the GPLv2 as it is applied to this software, see the 
// FLOSS License Exception
// <http://www.mysql.com/about/legal/licensing/foss-exception.html>.
//
// This program is free software; you can redistribute it and/or modify 
// it under the terms of the GNU General Public License as published 
// by the Free Software Foundation; version 2 of the License.
//
// This program is distributed in the hope that it will be useful, but 
// WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY 
// or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License 
// for more details.
//
// You should have received a copy of the GNU General Public License along 
// with this program; if not, write to the Free Software Foundation, Inc., 
// 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

using System;
using System.ComponentModel;
using System.Drawing.Design;
using MySql.Data.Common;
using MySql.Data.MySqlClient.Properties;
#if !RT
using System.Data;
using System.Data.Common;
#endif
#if !CF
#endif
#if !CF && !RT
using System.Transactions;
using IsolationLevel = System.Data.IsolationLevel;
#endif
#if !CF
using MySql.Data.MySqlClient.Replication;

#endif
#if NET_40_OR_GREATER
using System.Threading.Tasks;
using System.Threading;
#endif

namespace MySql.Data.MySqlClient {
    /// <include file='docs/MySqlConnection.xml' path='docs/ClassSummary/*'/>
    public sealed partial class MySqlConnection : IDisposable {
        internal ConnectionState ConnectionState;
        internal Driver Driver;
        internal bool HasBeenOpen;
        private SchemaProvider _schemaProvider;
#if !CF
#endif
#if !CF && !RT
        private ExceptionInterceptor _exceptionInterceptor;
        internal CommandInterceptor CommandInterceptor;
#endif
        private bool _isKillQueryConnection;
        private string _database;
        private int _commandTimeout;

        /// <include file='docs/MySqlConnection.xml' path='docs/InfoMessage/*'/>
        public event MySqlInfoMessageEventHandler InfoMessage;

        private static readonly Cache<string, MySqlConnectionStringBuilder> ConnectionStringCache =
            new Cache<string, MySqlConnectionStringBuilder>( 0, 25 );

        /// <include file='docs/MySqlConnection.xml' path='docs/DefaultCtor/*'/>
        public MySqlConnection() {
            //TODO: add event data to StateChange docs
            Settings = new MySqlConnectionStringBuilder();
            _database = String.Empty;
        }

        /// <include file='docs/MySqlConnection.xml' path='docs/Ctor1/*'/>
        public MySqlConnection( string connectionString ) : this() {
            ConnectionString = connectionString;
        }

        #region Destructor
        ~MySqlConnection() {
#if !RT
            Dispose( false );
#else
      Dispose();
#endif
        }
        #endregion

        #region Interal Methods & Properties
#if !CF
        internal PerformanceMonitor PerfMonitor { get; private set; }
#endif

        internal ProcedureCache ProcedureCache { get; private set; }

        internal MySqlConnectionStringBuilder Settings { get; private set; }

        internal MySqlDataReader Reader {
            get {
                return Driver?.Reader;
            }
            set {
                Driver.Reader = value;
                IsInUse = Driver.Reader != null;
            }
        }

        internal void OnInfoMessage( MySqlInfoMessageEventArgs args ) {
            InfoMessage?.Invoke( this, args );
        }

        internal bool SoftClosed => ( State == ConnectionState.Closed ) && Driver?.CurrentTransaction != null;

        internal bool IsInUse { get; set; }
        #endregion

        #region Properties
        /// <summary>
        /// Returns the id of the server thread this connection is executing on
        /// </summary>
        [Browsable( false )]
        public int ServerThread => Driver.ThreadId;

        /// <summary>
        /// Gets the name of the MySQL server to which to connect.
        /// </summary>
        [Browsable( true )]
        public override string DataSource => Settings.Server;

        /// <include file='docs/MySqlConnection.xml' path='docs/ConnectionTimeout/*'/>
        [Browsable( true )]
        public override int ConnectionTimeout => (int) Settings.ConnectionTimeout;

        /// <include file='docs/MySqlConnection.xml' path='docs/Database/*'/>
        [Browsable( true )]
        public override string Database => _database;

        /// <summary>
        /// Indicates if this connection should use compression when communicating with the server.
        /// </summary>
        [Browsable( false )]
        public bool UseCompression => Settings.UseCompression;

        /// <include file='docs/MySqlConnection.xml' path='docs/State/*'/>
        [Browsable( false )]
        public override ConnectionState State => ConnectionState;

        /// <include file='docs/MySqlConnection.xml' path='docs/ServerVersion/*'/>
        [Browsable( false )]
        public override string ServerVersion => Driver.Version.ToString();

        /// <include file='docs/MySqlConnection.xml' path='docs/ConnectionString/*'/>
        [Editor( "MySql.Data.MySqlClient.Design.ConnectionStringTypeEditor,MySqlClient.Design", typeof( UITypeEditor ) )]
        [Browsable( true )]
        [Category( "Data" )]
        [Description( "Information used to connect to a DataSource, such as 'Server=xxx;UserId=yyy;Password=zzz;Database=dbdb'." )]
        public override string ConnectionString {
            get {
                // Always return exactly what the user set.
                // Security-sensitive information may be removed.
                return Settings.GetConnectionString( !HasBeenOpen || Settings.PersistSecurityInfo );
            }
            set {
                if ( State != ConnectionState.Closed )
                    Throw(
                        new MySqlException(
                            "Not allowed to change the 'ConnectionString' property while the connection (state=" + State + ")." ) );

                MySqlConnectionStringBuilder newSettings;
                lock ( ConnectionStringCache ) {
                    if ( value == null ) newSettings = new MySqlConnectionStringBuilder();
                    else {
                        newSettings = ConnectionStringCache[ value ];
                        if ( null == newSettings ) {
                            newSettings = new MySqlConnectionStringBuilder( value );
                            ConnectionStringCache.Add( value, newSettings );
                        }
                    }
                }

                Settings = newSettings;

                if ( !String.IsNullOrEmpty(Settings.Database) ) _database = Settings.Database;

                if ( Driver != null ) Driver.Settings = newSettings;
            }
        }

#if !CF && !__MonoCS__ && !RT

        protected override DbProviderFactory DbProviderFactory => MySqlClientFactory.Instance;

#endif

        public bool IsPasswordExpired => Driver.IsPasswordExpired;
        #endregion

        partial void AssertPermissions();

        #region Transactions
#if !MONO && !CF && !RT
        /// <summary>
        /// Enlists in the specified transaction. 
        /// </summary>
        /// <param name="transaction">
        /// A reference to an existing <see cref="System.Transactions.Transaction"/> in which to enlist.
        /// </param>
        public override void EnlistTransaction( Transaction transaction ) {
            // enlisting in the null transaction is a noop
            if ( transaction == null ) return;

            // guard against trying to enlist in more than one transaction
            if ( Driver.CurrentTransaction != null ) {
                if ( Driver.CurrentTransaction.BaseTransaction == transaction ) return;

                Throw( new MySqlException( "Already enlisted" ) );
            }

            // now see if we need to swap out drivers.  We would need to do this since
            // we have to make sure all ops for a given transaction are done on the
            // same physical connection.
            var existingDriver = DriverTransactionManager.GetDriverInTransaction( transaction );
            if ( existingDriver != null ) {
                // we can't allow more than one driver to contribute to the same connection
                if ( existingDriver.IsInActiveUse ) Throw( new NotSupportedException( Resources.MultipleConnectionsInTransactionNotSupported ) );

                // there is an existing driver and it's not being currently used.
                // now we need to see if it is using the same connection string
                var text1 = existingDriver.Settings.ConnectionString;
                var text2 = Settings.ConnectionString;
                if ( String.Compare(text1, text2, StringComparison.OrdinalIgnoreCase) != 0 ) Throw( new NotSupportedException( Resources.MultipleConnectionsInTransactionNotSupported ) );

                // close existing driver
                // set this new driver as our existing driver
                CloseFully();
                Driver = existingDriver;
            }

            if ( Driver.CurrentTransaction == null ) {
                var t = new MySqlPromotableTransaction( this, transaction );
                if ( !transaction.EnlistPromotableSinglePhase( t ) ) Throw( new NotSupportedException( Resources.DistributedTxnNotSupported ) );

                Driver.CurrentTransaction = t;
                DriverTransactionManager.SetDriverInTransaction( Driver );
                Driver.IsInActiveUse = true;
            }
        }
#endif

        /// <include file='docs/MySqlConnection.xml' path='docs/BeginTransaction/*'/>
        public new MySqlTransaction BeginTransaction() {
            return BeginTransaction( IsolationLevel.RepeatableRead );
        }

        /// <include file='docs/MySqlConnection.xml' path='docs/BeginTransaction1/*'/>
        public new MySqlTransaction BeginTransaction( IsolationLevel iso ) {
            //TODO: check note in help
            if ( State != ConnectionState.Open ) Throw( new InvalidOperationException( Resources.ConnectionNotOpen ) );

            // First check to see if we are in a current transaction
            if ( Driver.HasStatus( ServerStatusFlags.InTransaction ) ) Throw( new InvalidOperationException( Resources.NoNestedTransactions ) );

            var t = new MySqlTransaction( this, iso );

            var cmd = new MySqlCommand( "", this ) { CommandText = "SET SESSION TRANSACTION ISOLATION LEVEL " };

            switch ( iso ) {
                case IsolationLevel.ReadCommitted:
                    cmd.CommandText += "READ COMMITTED";
                    break;
                case IsolationLevel.ReadUncommitted:
                    cmd.CommandText += "READ UNCOMMITTED";
                    break;
                case IsolationLevel.RepeatableRead:
                    cmd.CommandText += "REPEATABLE READ";
                    break;
                case IsolationLevel.Serializable:
                    cmd.CommandText += "SERIALIZABLE";
                    break;
                case IsolationLevel.Chaos:
                    Throw( new NotSupportedException( Resources.ChaosNotSupported ) );
                    break;
                case IsolationLevel.Snapshot:
                    Throw( new NotSupportedException( Resources.SnapshotNotSupported ) );
                    break;
            }

            cmd.ExecuteNonQuery();

            cmd.CommandText = "BEGIN";
            cmd.ExecuteNonQuery();

            return t;
        }
        #endregion

        /// <include file='docs/MySqlConnection.xml' path='docs/ChangeDatabase/*'/>
        public override void ChangeDatabase( string databaseName ) {
            if ( String.IsNullOrEmpty( databaseName ) ) Throw( new ArgumentException( Resources.ParameterIsInvalid, "databaseName" ) );

            if ( State != ConnectionState.Open ) Throw( new InvalidOperationException( Resources.ConnectionNotOpen ) );

            // This lock  prevents promotable transaction rollback to run
            // in parallel
            lock ( Driver ) {
#if !CF && !RT
                if ( Transaction.Current != null
                     && Transaction.Current.TransactionInformation.Status == TransactionStatus.Aborted ) Throw( new TransactionAbortedException() );
#endif
                // We use default command timeout for SetDatabase
                using ( new CommandTimer( this, (int) Settings.DefaultCommandTimeout ) ) Driver.SetDatabase( databaseName );
            }
            _database = databaseName;
        }

        internal void SetState( ConnectionState newConnectionState, bool broadcast ) {
            if ( newConnectionState == ConnectionState
                 && !broadcast ) return;
            var oldConnectionState = ConnectionState;
            ConnectionState = newConnectionState;
            if ( broadcast ) OnStateChange( new StateChangeEventArgs( oldConnectionState, ConnectionState ) );
        }

        /// <summary>
        /// Ping
        /// </summary>
        /// <returns></returns>
        public bool Ping() {
            if ( Reader != null ) Throw( new MySqlException( Resources.DataReaderOpen ) );
            if ( Driver != null
                 && Driver.Ping() ) return true;
            Driver = null;
            SetState( ConnectionState.Closed, true );
            return false;
        }

        /// <include file='docs/MySqlConnection.xml' path='docs/Open/*'/>
        public override void Open() {
            if ( State == ConnectionState.Open ) Throw( new InvalidOperationException( Resources.ConnectionAlreadyOpen ) );

#if !CF && !RT
            // start up our interceptors
            _exceptionInterceptor = new ExceptionInterceptor( this );
            CommandInterceptor = new CommandInterceptor( this );
#endif

            SetState( ConnectionState.Connecting, true );

            AssertPermissions();

#if !CF && !RT
            // if we are auto enlisting in a current transaction, then we will be
            // treating the connection as pooled
            if ( Settings.AutoEnlist
                 && Transaction.Current != null ) {
                Driver = DriverTransactionManager.GetDriverInTransaction( Transaction.Current );
                if ( Driver != null
                     && ( Driver.IsInActiveUse || !Driver.Settings.EquivalentTo( Settings ) ) ) Throw( new NotSupportedException( Resources.MultipleConnectionsInTransactionNotSupported ) );
            }
#endif

            try {
                var currentSettings = Settings;
#if !CF

                // Load balancing 
                if ( ReplicationManager.IsReplicationGroup( Settings.Server ) )
                    if ( Driver == null ) ReplicationManager.GetNewConnection( Settings.Server, false, this );
                    else currentSettings = Driver.Settings;
#endif

                if ( Settings.Pooling ) {
                    var pool = MySqlPoolManager.GetPool( currentSettings );
                    if ( Driver == null
                         || !Driver.IsOpen ) Driver = pool.GetConnection();
                    ProcedureCache = pool.ProcedureCache;
                }
                else {
                    if ( Driver == null
                         || !Driver.IsOpen ) Driver = Driver.Create( currentSettings );
                    ProcedureCache = new ProcedureCache( (int) Settings.ProcedureCacheSize );
                }
            }
            catch ( Exception ) {
                SetState( ConnectionState.Closed, true );
                throw;
            }

            // if the user is using old syntax, let them know
            if ( Driver.Settings.UseOldSyntax ) MySqlTrace.LogWarning( ServerThread, "You are using old syntax that will be removed in future versions" );

            SetState( ConnectionState.Open, false );
            Driver.Configure( this );

            if ( !( Driver.SupportsPasswordExpiration && Driver.IsPasswordExpired ) )
                if ( !string.IsNullOrEmpty( Settings.Database ) ) ChangeDatabase( Settings.Database );

            // setup our schema provider
            _schemaProvider = new IsSchemaProvider( this );

#if !CF
            PerfMonitor = new PerformanceMonitor( this );
#endif

            // if we are opening up inside a current transaction, then autoenlist
            // TODO: control this with a connection string option
#if !MONO && !CF && !RT
            if ( Transaction.Current != null
                 && Settings.AutoEnlist ) EnlistTransaction( Transaction.Current );
#endif

            HasBeenOpen = true;
            SetState( ConnectionState.Open, true );
        }

        /// <include file='docs/MySqlConnection.xml' path='docs/CreateCommand/*'/>
        public new MySqlCommand CreateCommand() {
            // Return a new instance of a command object.
            return new MySqlCommand { Connection = this };
        }

        /// <summary>
        /// Creates a new MySqlConnection object with the exact same ConnectionString value
        /// </summary>
        /// <returns>A cloned MySqlConnection object</returns>
        public object Clone() {
            var clone = new MySqlConnection();
            var connectionString = Settings.ConnectionString;
            if ( connectionString != null ) clone.ConnectionString = connectionString;
            return clone;
        }

        internal void Abort() {
            try {
                Driver.Close();
            }
            catch ( Exception ex ) {
                MySqlTrace.LogWarning(
                    ServerThread,
                    String.Concat( "Error occurred aborting the connection. Exception was: ", ex.Message ) );
            }
            finally {
                IsInUse = false;
            }
            SetState( ConnectionState.Closed, true );
        }

        internal void CloseFully() {
            if ( Settings.Pooling
                 && Driver.IsOpen ) {
                // if we are in a transaction, roll it back
                if ( Driver.HasStatus( ServerStatusFlags.InTransaction ) ) {
                    var t = new MySqlTransaction( this, IsolationLevel.Unspecified );
                    t.Rollback();
                }

                MySqlPoolManager.ReleaseConnection( Driver );
            }
            else Driver.Close();
            Driver = null;
        }

        /// <include file='docs/MySqlConnection.xml' path='docs/Close/*'/>
        public override void Close() {
            if ( Driver != null ) Driver.IsPasswordExpired = false;

            if ( State == ConnectionState.Closed ) return;

            Reader?.Close();

            // if the reader was opened with CloseConnection then driver
            // will be null on the second time through
            if ( Driver != null )
#if !CF && !RT
                if ( Driver.CurrentTransaction == null )
#endif
                    CloseFully();
#if !CF && !RT
                else Driver.IsInActiveUse = false;
#endif

            SetState( ConnectionState.Closed, true );
        }

        internal string CurrentDatabase() {
            if ( !string.IsNullOrEmpty( Database ) ) return Database;
            var cmd = new MySqlCommand( "SELECT database()", this );
            return cmd.ExecuteScalar().ToString();
        }

        internal void HandleTimeoutOrThreadAbort( Exception ex ) {
            var isFatal = false;

            if ( _isKillQueryConnection ) {
                // Special connection started to cancel a query.
                // Abort will prevent recursive connection spawning
                Abort();
                if ( ex is TimeoutException ) Throw( new MySqlException( Resources.Timeout, true, ex ) );
                else return;
            }

            try {
                // Do a fast cancel.The reason behind small values for connection
                // and command timeout is that we do not want user to wait longer
                // after command has already expired.
                // Microsoft's SqlClient seems to be using 5 seconds timeouts 
                // here as well.
                // Read the  error packet with "interrupted" message.
                CancelQuery( 5 );
                Driver.ResetTimeout( 5000 );
                if ( Reader != null ) {
                    Reader.Close();
                    Reader = null;
                }
            }
            catch ( Exception ex2 ) {
                MySqlTrace.LogWarning( ServerThread, "Could not kill query, " + " aborting connection. Exception was " + ex2.Message );
                Abort();
                isFatal = true;
            }
            if ( ex is TimeoutException ) Throw( new MySqlException( Resources.Timeout, isFatal, ex ) );
        }

        public void CancelQuery( int timeout ) {
            var cb = new MySqlConnectionStringBuilder( Settings.ConnectionString ) {
                Pooling = false,
                AutoEnlist = false,
                ConnectionTimeout = (uint) timeout
            };

            using ( var c = new MySqlConnection( cb.ConnectionString ) ) {
                c._isKillQueryConnection = true;
                c.Open();
                var commandText = "KILL QUERY " + ServerThread;
                var cmd = new MySqlCommand( commandText, c ) { CommandTimeout = timeout };
                cmd.ExecuteNonQuery();
            }
        }

        #region Routines for timeout support.

        // Problem description:
        // Sometimes, ExecuteReader is called recursively. This is the case if
        // command behaviors are used and we issue "set sql_select_limit" 
        // before and after command. This is also the case with prepared 
        // statements , where we set session variables. In these situations, we 
        // have to prevent  recursive ExecuteReader calls from overwriting 
        // timeouts set by the top level command.

        // To solve the problem, SetCommandTimeout() and ClearCommandTimeout() are 
        // introduced . Query timeout here is  "sticky", that is once set with 
        // SetCommandTimeout, it only be overwritten after ClearCommandTimeout 
        // (SetCommandTimeout would return false if it timeout has not been 
        // cleared).

        // The proposed usage pattern of there routines is following: 
        // When timed operations starts, issue SetCommandTimeout(). When it 
        // finishes, issue ClearCommandTimeout(), but _only_ if call to 
        // SetCommandTimeout() was successful.

        /// <summary>
        /// Sets query timeout. If timeout has been set prior and not
        /// yet cleared ClearCommandTimeout(), it has no effect.
        /// </summary>
        /// <param name="value">timeout in seconds</param>
        /// <returns>true if </returns>
        internal bool SetCommandTimeout( int value ) {
            if ( !HasBeenOpen )
                // Connection timeout is handled by driver
                return false;

            if ( _commandTimeout != 0 )
                // someone is trying to set a timeout while command is already
                // running. It could be for example recursive call to ExecuteReader
                // Ignore the request, as only top-level (non-recursive commands)
                // can set timeouts.
                return false;

            if ( Driver == null ) return false;

            _commandTimeout = value;
            Driver.ResetTimeout( _commandTimeout * 1000 );
            return true;
        }

        /// <summary>
        /// Clears query timeout, allowing next SetCommandTimeout() to succeed.
        /// </summary>
        internal void ClearCommandTimeout() {
            if ( !HasBeenOpen ) return;
            _commandTimeout = 0;
            Driver?.ResetTimeout( 0 );
        }
        #endregion

        public MySqlSchemaCollection GetSchemaCollection( string collectionName, string[] restrictionValues ) {
            if ( collectionName == null ) collectionName = SchemaProvider.MetaCollection;

            var restrictions = _schemaProvider.CleanRestrictions( restrictionValues );
            var c = _schemaProvider.GetSchema( collectionName, restrictions );
            return c;
        }

        #region Pool Routines
        /// <include file='docs/MySqlConnection.xml' path='docs/ClearPool/*'/>
        public static void ClearPool( MySqlConnection connection ) {
            MySqlPoolManager.ClearPool( connection.Settings );
        }

        /// <include file='docs/MySqlConnection.xml' path='docs/ClearAllPools/*'/>
        public static void ClearAllPools() {
            MySqlPoolManager.ClearAllPools();
        }
        #endregion

        internal void Throw( Exception ex ) {
#if !CF && !RT
            if ( _exceptionInterceptor == null ) throw ex;
            _exceptionInterceptor.Throw( ex );
#else
      throw ex;
#endif
        }

#if !RT
        public void Dispose() {
            Dispose( true );
            GC.SuppressFinalize( this );
        }
#else
    public void Dispose()
    {
      if (State == ConnectionState.Open)
        Close();

      GC.SuppressFinalize(this);
    }
#endif

#if NET_40_OR_GREATER
    #region Async
    /// <summary>
    /// Async version of BeginTransaction
    /// </summary>
    /// <returns>An object representing the new transaction.</returns>
    public Task<MySqlTransaction> BeginTransactionAsync()
    {
      return BeginTransactionAsync(IsolationLevel.RepeatableRead, CancellationToken.None);
    }

    public Task<MySqlTransaction> BeginTransactionAsync(CancellationToken cancellationToken)
    {
      return BeginTransactionAsync(IsolationLevel.RepeatableRead, cancellationToken);
    }

    /// <summary>
    /// Async version of BeginTransaction
    /// </summary>
    /// <param name="iso">The isolation level under which the transaction should run. </param>
    /// <returns>An object representing the new transaction.</returns>
    public Task<MySqlTransaction> BeginTransactionAsync(IsolationLevel iso)
    {
      return BeginTransactionAsync(iso, CancellationToken.None);
    }

    public Task<MySqlTransaction> BeginTransactionAsync(IsolationLevel iso, CancellationToken cancellationToken)
    {
      var result = new TaskCompletionSource<MySqlTransaction>();
      if (cancellationToken == CancellationToken.None || !cancellationToken.IsCancellationRequested)
      {
        try
        {
          MySqlTransaction tranResult = BeginTransaction(iso);
          result.SetResult(tranResult);
        }
        catch (Exception ex)
        {
          result.SetException(ex);
        }
      }
      else
      {
        result.SetCanceled();
      }

      return result.Task;
    }

    public Task ChangeDataBaseAsync(string databaseName)
    {
      return ChangeDataBaseAsync(databaseName, CancellationToken.None);
    }

    /// <summary>
    /// Async version of ChangeDataBase
    /// </summary>
    /// <param name="databaseName">The name of the database to use.</param>
    /// <returns></returns>
    public Task ChangeDataBaseAsync(string databaseName, CancellationToken cancellationToken)
    {
      var result = new TaskCompletionSource<bool>();
      if (cancellationToken == CancellationToken.None || !cancellationToken.IsCancellationRequested)
      {
        try
        {
          ChangeDatabase(databaseName);
          result.SetResult(true);
        }
        catch (Exception ex)
        {
          result.SetException(ex);
        }
      }
      return result.Task;
    }

    ///// <summary>
    ///// Async version of Open
    ///// </summary>
    ///// <returns></returns>
    //public Task OpenAsync()
    //{
    //  return Task.Run(() =>
    //  {
    //    Open();
    //  });
    //}

    /// <summary>
    /// Async version of Close
    /// </summary>
    /// <returns></returns>
    public Task CloseAsync()
    {
      return CloseAsync(CancellationToken.None);
    }

    public Task CloseAsync(CancellationToken cancellationToken)
    {
      var result = new TaskCompletionSource<bool>();
      if (cancellationToken == CancellationToken.None || !cancellationToken.IsCancellationRequested)
      {
        try
        {
          Close();
          result.SetResult(true);
        }
        catch (Exception ex)
        {
          result.SetException(ex);
        }
      }
      else 
      {
        result.SetCanceled();
      }
      return result.Task;
    }

    /// <summary>
    /// Async version of ClearPool
    /// </summary>
    /// <param name="connection">The connection associated with the pool to be cleared.</param>
    /// <returns></returns>
    public Task ClearPoolAsync(MySqlConnection connection)
    {
      return ClearPoolAsync(connection, CancellationToken.None);
    }

    public Task ClearPoolAsync(MySqlConnection connection, CancellationToken cancellationToken)
    {
      var result = new TaskCompletionSource<bool>();
      if (cancellationToken == CancellationToken.None || !cancellationToken.IsCancellationRequested)
      {
        try
        {
          ClearPool(connection);
          result.SetResult(true);
        }
        catch (Exception ex)
        {
          result.SetException(ex);
        }
      }
      else
      {
        result.SetCanceled();
      }
      return result.Task;
    }

    /// <summary>
    /// Async version of ClearAllPools
    /// </summary>
    /// <returns></returns>
    public Task ClearAllPoolsAsync()
    {
      return ClearAllPoolsAsync(CancellationToken.None);
    }

    public Task ClearAllPoolsAsync(CancellationToken cancellationToken)
    {
      var result = new TaskCompletionSource<bool>();
      if (cancellationToken == CancellationToken.None || !cancellationToken.IsCancellationRequested)
      {
        try
        {
          ClearAllPools();
          result.SetResult(true);
        }
        catch (Exception ex)
        {
          result.SetException(ex);
        }
      }
      else
      {
        result.SetCanceled();
      }
      return result.Task;
    }
    /// <summary>
    /// Async version of GetSchemaCollection
    /// </summary>
    /// <param name="collectionName">Name of the collection</param>
    /// <param name="restrictionValues">Values to restrict</param>
    /// <returns>A schema collection</returns>
    public Task<MySqlSchemaCollection> GetSchemaCollectionAsync(string collectionName, string[] restrictionValues)
    {
      return GetSchemaCollectionAsync(collectionName, restrictionValues, CancellationToken.None);
    }

    public Task<MySqlSchemaCollection> GetSchemaCollectionAsync(string collectionName, string[] restrictionValues, CancellationToken cancellationToken)
    {
      var result = new TaskCompletionSource<MySqlSchemaCollection>();
      if (cancellationToken == CancellationToken.None || !cancellationToken.IsCancellationRequested)
      {
        try
        {
          var schema = GetSchemaCollection(collectionName, restrictionValues);
          result.SetResult(schema);
        }
        catch (Exception ex)
        {
          result.SetException(ex);
        }
      }
      else
      {
        result.SetCanceled();
      }
      return result.Task;
    }
    #endregion
#endif
    }

    /// <summary>
    /// Represents the method that will handle the <see cref="MySqlConnection.InfoMessage"/> event of a 
    /// <see cref="MySqlConnection"/>.
    /// </summary>
    public delegate void MySqlInfoMessageEventHandler( object sender, MySqlInfoMessageEventArgs args );

    /// <summary>
    /// Provides data for the InfoMessage event. This class cannot be inherited.
    /// </summary>
    public class MySqlInfoMessageEventArgs : EventArgs {
        /// <summary>
        /// 
        /// </summary>
        public MySqlError[] Errors;
    }

    /// <summary>
    /// IDisposable wrapper around SetCommandTimeout and ClearCommandTimeout
    /// functionality
    /// </summary>
    internal class CommandTimer : IDisposable {
        private bool _timeoutSet;
        private MySqlConnection _connection;

        public CommandTimer( MySqlConnection connection, int timeout ) {
            _connection = connection;
            if ( connection != null ) _timeoutSet = connection.SetCommandTimeout( timeout );
        }

        #region IDisposable Members
        public void Dispose() {
            if ( _timeoutSet ) {
                _timeoutSet = false;
                _connection.ClearCommandTimeout();
                _connection = null;
            }
        }
        #endregion
    }
}