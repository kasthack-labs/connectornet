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
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Text.RegularExpressions;
using System.Threading;
using System.Transactions;
using MySql.Data.Constants;
using MySql.Data.MySqlClient.Properties;
using System.Drawing.Design;
#if !RT
using System.Data;
#endif
#if !CF
using MySql.Data.MySqlClient.Replication;

#endif

namespace MySql.Data.MySqlClient {
    /// <include file='docs/mysqlcommand.xml' path='docs/ClassSummary/*'/> 
    public sealed partial class MySqlCommand : ICloneable, IDisposable {
        private MySqlConnection _connection;
        private string _cmdText;
        private CommandType _cmdType;
        private long _updatedRowCount;
        private IAsyncResult _asyncResult;
        internal long lastInsertedId;
        private PreparableStatement _statement;
        private int _commandTimeout;
        private bool _resetSqlSelect;
        private string _batchableCommandText;
        private CommandTimer _commandTimer;
        private bool _useDefaultTimeout;
        private bool _internallyCreated;

        /// <include file='docs/mysqlcommand.xml' path='docs/ctor1/*'/>
        public MySqlCommand() {
            _cmdType = CommandType.Text;
            Parameters = new MySqlParameterCollection( this );
            _cmdText = String.Empty;
            _useDefaultTimeout = true;
            Constructor();
        }

        partial void Constructor();

        /// <include file='docs/mysqlcommand.xml' path='docs/ctor2/*'/>
        public MySqlCommand( string cmdText ) : this() {
            CommandText = cmdText;
        }

        /// <include file='docs/mysqlcommand.xml' path='docs/ctor3/*'/>
        public MySqlCommand( string cmdText, MySqlConnection connection ) : this( cmdText ) {
            Connection = connection;
        }

        /// <include file='docs/mysqlcommand.xml' path='docs/ctor4/*'/>
        public MySqlCommand( string cmdText, MySqlConnection connection, MySqlTransaction transaction ) : this( cmdText, connection ) {
            Transaction = transaction;
        }

        #region Destructor
#if !RT
        ~MySqlCommand() { Dispose( false ); }
#else
    ~MySqlCommand()
    {
      this.Dispose();
    }
#endif
        #endregion

        #region Properties
        /// <include file='docs/mysqlcommand.xml' path='docs/LastInseredId/*'/>
        [Browsable( false )]
        public long LastInsertedId => lastInsertedId;

        /// <include file='docs/mysqlcommand.xml' path='docs/CommandText/*'/>
        [Category( "Data" )]
        [Description( "Command text to execute" )]
        [Editor( "MySql.Data.Common.Design.SqlCommandTextEditor,MySqlClient.Design", typeof(UITypeEditor))]
        public override string CommandText {
            get {
                return _cmdText;
            }
            set {
                _cmdText = value ?? string.Empty;
                _statement = null;
                _batchableCommandText = null;
                if ( _cmdText == null
                     || !_cmdText.EndsWith( "DEFAULT VALUES", StringComparison.OrdinalIgnoreCase ) ) return;
                _cmdText = _cmdText.Substring( 0, _cmdText.Length - 14 );
                _cmdText = _cmdText + "() VALUES ()";
            }
        }

        /// <include file='docs/mysqlcommand.xml' path='docs/CommandTimeout/*'/>
        [Category( "Misc" )]
        [Description( "Time to wait for command to execute" )]
        [DefaultValue( 30 )]
        public override int CommandTimeout {
            get {
                return _useDefaultTimeout ? 30 : _commandTimeout;
            }
            set {
                if ( _commandTimeout < 0 ) Throw( new ArgumentException( "Command timeout must not be negative" ) );

                // Timeout in milliseconds should not exceed maximum for 32 bit
                // signed integer (~24 days), because underlying driver (and streams)
                // use milliseconds expressed ints for timeout values.
                // Hence, truncate the value.
                var timeout = Math.Min( value, Int32.MaxValue / 1000 );
                if ( timeout != value )
                    MySqlTrace.LogWarning(
                        _connection.ServerThread,
                        "Command timeout value too large (" + value + " seconds). Changed to max. possible value (" + timeout + " seconds)" );
                _commandTimeout = timeout;
                _useDefaultTimeout = false;
            }
        }

        /// <include file='docs/mysqlcommand.xml' path='docs/CommandType/*'/>
        [Category( "Data" )]
        public override CommandType CommandType {
            get {
                return _cmdType;
            }
            set {
                _cmdType = value;
            }
        }

        /// <include file='docs/mysqlcommand.xml' path='docs/IsPrepared/*'/>
        [Browsable( false )]
        public bool IsPrepared => _statement != null && _statement.IsPrepared;

        /// <include file='docs/mysqlcommand.xml' path='docs/Connection/*'/>
        [Category( "Behavior" )]
        [Description( "Connection used by the command" )]
        public new MySqlConnection Connection {
            get {
                return _connection;
            }
            set {
                /*
        * The connection is associated with the transaction
        * so set the transaction object to return a null reference if the connection 
        * is reset.
        */
                if ( _connection != value ) Transaction = null;

                _connection = value;

                // if the user has not already set the command timeout, then
                // take the default from the connection
                if ( _connection != null ) {
                    if ( _useDefaultTimeout ) {
                        _commandTimeout = (int) _connection.Settings.DefaultCommandTimeout;
                        _useDefaultTimeout = false;
                    }

                    EnableCaching = _connection.Settings.TableCaching;
                    CacheAge = _connection.Settings.DefaultTableCacheAge;
                }
            }
        }

        /// <include file='docs/mysqlcommand.xml' path='docs/Parameters/*'/>
        [Category( "Data" )]
        [Description( "The parameters collection" )]
        [DesignerSerializationVisibility( DesignerSerializationVisibility.Content )]
        public new MySqlParameterCollection Parameters { get; }

        /// <include file='docs/mysqlcommand.xml' path='docs/Transaction/*'/>
        [Browsable( false )]
        public new MySqlTransaction Transaction { get; set; }

        public bool EnableCaching { get; set; }

        public int CacheAge { get; set; }

        internal List<MySqlCommand> Batch { get; private set; }

        internal bool Canceled { get; private set; }

        internal string BatchableCommandText => _batchableCommandText;

        internal bool InternallyCreated {
            get {
                return _internallyCreated;
            }
            set {
                _internallyCreated = value;
            }
        }
        #endregion

        #region Methods
        /// <summary>
        /// Attempts to cancel the execution of a currently active command
        /// </summary>
        /// <remarks>
        /// Cancelling a currently active query only works with MySQL versions 5.0.0 and higher.
        /// </remarks>
        public override void Cancel() {
            _connection.CancelQuery( _connection.ConnectionTimeout );
            Canceled = true;
        }

        /// <summary>
        /// Creates a new instance of a <see cref="MySqlParameter"/> object.
        /// </summary>
        /// <remarks>
        /// This method is a strongly-typed version of <see cref="IDbCommand.CreateParameter"/>.
        /// </remarks>
        /// <returns>A <see cref="MySqlParameter"/> object.</returns>
        /// 
        public new MySqlParameter CreateParameter() => (MySqlParameter) CreateDbParameter();

        /// <summary>
        /// Check the connection to make sure
        ///		- it is open
        ///		- it is not currently being used by a reader
        ///		- and we have the right version of MySQL for the requested command type
        /// </summary>
        private void CheckState() {
            // There must be a valid and open connection.
            if ( _connection == null ) Throw( new InvalidOperationException( "Connection must be valid and open." ) );

            if ( _connection.State != ConnectionState.Open
                 && !_connection.SoftClosed ) Throw( new InvalidOperationException( "Connection must be valid and open." ) );

            // Data readers have to be closed first
            if ( _connection.IsInUse
                 && !_internallyCreated )
                Throw(
                    new MySqlException( "There is already an open DataReader associated with this Connection which must be closed first." ) );
        }

        /// <include file='docs/mysqlcommand.xml' path='docs/ExecuteNonQuery/*'/>
        public override int ExecuteNonQuery() {
            var records = -1;

#if !CF && !RT
            // give our interceptors a shot at it first
            if ( _connection != null
                 && _connection.CommandInterceptor != null
                 && _connection.CommandInterceptor.ExecuteNonQuery( CommandText, ref records ) ) return records;
#endif

            // ok, none of our interceptors handled this so we default
            using ( var reader = ExecuteReader() ) {
                reader.Close();
                return reader.RecordsAffected;
            }
        }

        internal void ClearCommandTimer() {
            _commandTimer?.Dispose();
            _commandTimer = null;
        }

        internal void Close( MySqlDataReader reader ) {
            _statement?.Close( reader );
            ResetSqlSelectLimit();
            if ( _statement != null ) _connection?.Driver?.CloseQuery( _connection, _statement.StatementId );
            ClearCommandTimer();
        }

        /// <summary>
        /// Reset reader to null, to avoid "There is already an open data reader"
        /// on the next ExecuteReader(). Used in error handling scenarios.
        /// </summary>
        private void ResetReader() {
            _connection?.Reader?.Close();
            if ( _connection != null ) _connection.Reader = null;
        }

        /// <summary>
        /// Reset SQL_SELECT_LIMIT that could have been modified by CommandBehavior.
        /// </summary>
        internal void ResetSqlSelectLimit() {
            // if we are supposed to reset the sql select limit, do that here
            if ( !_resetSqlSelect ) return;
            _resetSqlSelect = false;
            new MySqlCommand( "SET SQL_SELECT_LIMIT=DEFAULT", _connection ) { _internallyCreated = true }.ExecuteNonQuery();
        }

        /// <include file='docs/mysqlcommand.xml' path='docs/ExecuteReader/*'/>
        public new MySqlDataReader ExecuteReader() => ExecuteReader( CommandBehavior.Default );

        /// <include file='docs/mysqlcommand.xml' path='docs/ExecuteReader1/*'/>
        public new MySqlDataReader ExecuteReader( CommandBehavior behavior ) {
#if !CF && !RT
            // give our interceptors a shot at it first
            MySqlDataReader interceptedReader = null;

            var executeReader = _connection?.CommandInterceptor?.ExecuteReader( CommandText, behavior, ref interceptedReader );
            if ( executeReader!=null && executeReader.Value ) return interceptedReader;
#endif

            // interceptors didn't handle this so we fall through
            var success = false;
            CheckState();
            var driver = _connection.Driver;

            _cmdText = _cmdText.Trim();
            if ( String.IsNullOrEmpty( _cmdText ) ) Throw( new InvalidOperationException( Resources.CommandTextNotInitialized ) );

            var sql = _cmdText.Trim( ';' );

#if !CF
            // Load balancing getting a new connection
            if ( _connection.HasBeenOpen
                 && !driver.HasStatus( ServerStatusFlags.InTransaction ) ) ReplicationManager.GetNewConnection( _connection.Settings.Server, !IsReadOnlyCommand( sql ), _connection );
#endif

            lock ( driver ) {
                // We have to recheck that there is no reader, after we got the lock
                if ( _connection.Reader != null ) Throw( new MySqlException( Resources.DataReaderOpen ) );

#if !CF && !RT
                var curTrans = System.Transactions.Transaction.Current;

                if ( curTrans != null ) {
                    var inRollback = false;
                    if ( driver.CurrentTransaction != null ) inRollback = driver.CurrentTransaction.InRollback;
                    if ( !inRollback ) {
                        var status = TransactionStatus.InDoubt;
                        try {
                            // in some cases (during state transitions) this throws
                            // an exception. Ignore exceptions, we're only interested 
                            // whether transaction was aborted or not.
                            status = curTrans.TransactionInformation.Status;
                        }
                        catch ( TransactionException ) {}
                        if ( status == TransactionStatus.Aborted ) Throw( new TransactionAbortedException() );
                    }
                }
#endif
                _commandTimer = new CommandTimer( _connection, CommandTimeout );

                lastInsertedId = -1;

                switch ( CommandType ) {
                    case CommandType.TableDirect:
                        sql = "SELECT * FROM " + sql;
                        break;
                    case CommandType.Text:
                        if ( sql.InvariantIndexOf(" ") == -1 ) if ( AddCallStatement( sql ) ) sql = "call " + sql;
                        break;
                }

                // if we are on a replicated connection, we are only allow readonly statements
                if ( _connection.Settings.Replication
                     && !InternallyCreated ) EnsureCommandIsReadOnly( sql );

                if ( _statement == null
                     || !_statement.IsPrepared )
                    _statement = CommandType == CommandType.StoredProcedure ? new StoredProcedure( this, sql ) : new PreparableStatement( this, sql );

                // stored procs are the only statement type that need do anything during resolve
                _statement.Resolve( false );

                // Now that we have completed our resolve step, we can handle our
                // command behaviors
                HandleCommandBehaviors( behavior );

                _updatedRowCount = -1;
                try {
                    var reader = new MySqlDataReader( this, _statement, behavior );
                    _connection.Reader = reader;
                    Canceled = false;
                    // execute the statement
                    _statement.Execute();
                    // wait for data to return
                    reader.NextResult();
                    success = true;
                    return reader;
                }
                catch ( TimeoutException tex ) {
                    _connection.HandleTimeoutOrThreadAbort( tex );
                    throw; //unreached
                }
                catch ( ThreadAbortException taex ) {
                    _connection.HandleTimeoutOrThreadAbort( taex );
                    throw;
                }
                catch ( IOException ioex ) {
                    _connection.Abort(); // Closes connection without returning it to the pool
                    throw new MySqlException( Resources.FatalErrorDuringExecute, ioex );
                }
                catch ( MySqlException ex ) {
                    if ( ex.InnerException is TimeoutException ) throw; // already handled

                    try {
                        ResetReader();
                        ResetSqlSelectLimit();
                    }
                    catch ( Exception ) {
                        // Reset SqlLimit did not work, connection is hosed.
                        Connection.Abort();
                        throw new MySqlException( ex.Message, true, ex );
                    }

                    // if we caught an exception because of a cancel, then just return null
                    if ( ex.IsQueryAborted ) return null;
                    if ( ex.IsFatal ) Connection.Close();
                    if ( ex.Number == 0 ) throw new MySqlException( Resources.FatalErrorDuringExecute, ex );
                    throw;
                }
                finally {
                    if ( _connection != null ) {
                        if ( _connection.Reader == null )
                            // Something went seriously wrong,  and reader would not
                            // be able to clear timeout on closing.
                            // So we clear timeout here.
                            ClearCommandTimer();
                        if ( !success )
                            // ExecuteReader failed.Close Reader and set to null to 
                            // prevent subsequent errors with DataReaderOpen
                            ResetReader();
                    }
                }
            }
        }

        private void EnsureCommandIsReadOnly( string sql ) {
            sql = StringUtility.InvariantToLower( sql );
            if ( !sql.StartsWith( "select" )
                 && !sql.StartsWith( "show" ) ) Throw( new MySqlException( Resources.ReplicatedConnectionsAllowOnlyReadonlyStatements ) );
            if ( sql.EndsWith( "for update" )
                 || sql.EndsWith( "lock in share mode" ) ) Throw( new MySqlException( Resources.ReplicatedConnectionsAllowOnlyReadonlyStatements ) );
        }

        private bool IsReadOnlyCommand( string sql ) {
            sql = sql.ToLower();
            return ( sql.StartsWith( "select" ) || sql.StartsWith( "show" ) )
                   && !( sql.EndsWith( "for update" ) || sql.EndsWith( "lock in share mode" ) );
        }

        /// <include file='docs/mysqlcommand.xml' path='docs/ExecuteScalar/*'/>
        public override object ExecuteScalar() {
            lastInsertedId = -1;
            object val = null;

#if !CF && !RT
            // give our interceptors a shot at it first
            if ( _connection != null
                 && _connection.CommandInterceptor.ExecuteScalar( CommandText, ref val ) ) return val;
#endif

            using ( var reader = ExecuteReader() ) if ( reader.Read() ) val = reader.GetValue( 0 );

            return val;
        }

        private void HandleCommandBehaviors( CommandBehavior behavior ) {
            if ( ( behavior & CommandBehavior.SchemaOnly ) != 0 ) {
                new MySqlCommand( "SET SQL_SELECT_LIMIT=0", _connection ).ExecuteNonQuery();
                _resetSqlSelect = true;
            }
            else if ( ( behavior & CommandBehavior.SingleRow ) != 0 ) {
                new MySqlCommand( "SET SQL_SELECT_LIMIT=1", _connection ).ExecuteNonQuery();
                _resetSqlSelect = true;
            }
        }

        /// <include file='docs/mysqlcommand.xml' path='docs/Prepare2/*'/>
        private void Prepare( int cursorPageSize ) {
            using ( new CommandTimer( Connection, CommandTimeout ) ) {
                // if the length of the command text is zero, then just return
                var psSql = CommandText;
                if ( psSql == null
                     || psSql.Trim().Length == 0 ) return;

                if ( CommandType == CommandType.StoredProcedure ) _statement = new StoredProcedure( this, CommandText );
                else _statement = new PreparableStatement( this, CommandText );

                _statement.Resolve( true );
                _statement.Prepare();
            }
        }

        /// <include file='docs/mysqlcommand.xml' path='docs/Prepare/*'/>
        public override void Prepare() {
            if ( _connection == null ) Throw( new InvalidOperationException( "The connection property has not been set." ) );
            if ( _connection.State != ConnectionState.Open ) Throw( new InvalidOperationException( "The connection is not open." ) );
            if ( _connection.Settings.IgnorePrepare ) return;

            Prepare( 0 );
        }
        #endregion

        #region Async Methods
        internal delegate object AsyncDelegate( int type, CommandBehavior behavior );

        internal AsyncDelegate Caller;
        internal Exception ThrownException;

        internal object AsyncExecuteWrapper( int type, CommandBehavior behavior ) {
            ThrownException = null;
            try {
                if ( type == 1 ) return ExecuteReader( behavior );
                return ExecuteNonQuery();
            }
            catch ( Exception ex ) {
                ThrownException = ex;
            }
            return null;
        }

        /// <summary>
        /// Initiates the asynchronous execution of the SQL statement or stored procedure 
        /// that is described by this <see cref="MySqlCommand"/>, and retrieves one or more 
        /// result sets from the server. 
        /// </summary>
        /// <returns>An <see cref="IAsyncResult"/> that can be used to poll, wait for results, 
        /// or both; this value is also needed when invoking EndExecuteReader, 
        /// which returns a <see cref="MySqlDataReader"/> instance that can be used to retrieve 
        /// the returned rows. </returns>
        public IAsyncResult BeginExecuteReader() => BeginExecuteReader( CommandBehavior.Default );

        /// <summary>
        /// Initiates the asynchronous execution of the SQL statement or stored procedure 
        /// that is described by this <see cref="MySqlCommand"/> using one of the 
        /// <b>CommandBehavior</b> values. 
        /// </summary>
        /// <param name="behavior">One of the <see cref="CommandBehavior"/> values, indicating 
        /// options for statement execution and data retrieval.</param>
        /// <returns>An <see cref="IAsyncResult"/> that can be used to poll, wait for results, 
        /// or both; this value is also needed when invoking EndExecuteReader, 
        /// which returns a <see cref="MySqlDataReader"/> instance that can be used to retrieve 
        /// the returned rows. </returns>
        public IAsyncResult BeginExecuteReader( CommandBehavior behavior ) {
            if ( Caller != null ) Throw( new MySqlException( Resources.UnableToStartSecondAsyncOp ) );

            Caller = AsyncExecuteWrapper;
            _asyncResult = Caller.BeginInvoke( 1, behavior, null, null );
            return _asyncResult;
        }

        /// <summary>
        /// Finishes asynchronous execution of a SQL statement, returning the requested 
        /// <see cref="MySqlDataReader"/>.
        /// </summary>
        /// <param name="result">The <see cref="IAsyncResult"/> returned by the call to 
        /// <see cref="BeginExecuteReader()"/>.</param>
        /// <returns>A <b>MySqlDataReader</b> object that can be used to retrieve the requested rows. </returns>
        public MySqlDataReader EndExecuteReader( IAsyncResult result ) {
            result.AsyncWaitHandle.WaitOne();
            var c = Caller;
            Caller = null;
            if ( ThrownException != null ) throw ThrownException;
            return (MySqlDataReader) c.EndInvoke( result );
        }

        /// <summary>
        /// Initiates the asynchronous execution of the SQL statement or stored procedure 
        /// that is described by this <see cref="MySqlCommand"/>. 
        /// </summary>
        /// <param name="callback">
        /// An <see cref="AsyncCallback"/> delegate that is invoked when the command's 
        /// execution has completed. Pass a null reference (<b>Nothing</b> in Visual Basic) 
        /// to indicate that no callback is required.</param>
        /// <param name="stateObject">A user-defined state object that is passed to the 
        /// callback procedure. Retrieve this object from within the callback procedure 
        /// using the <see cref="IAsyncResult.AsyncState"/> property.</param>
        /// <returns>An <see cref="IAsyncResult"/> that can be used to poll or wait for results, 
        /// or both; this value is also needed when invoking <see cref="EndExecuteNonQuery"/>, 
        /// which returns the number of affected rows. </returns>
        public IAsyncResult BeginExecuteNonQuery( AsyncCallback callback, object stateObject ) {
            if ( Caller != null ) Throw( new MySqlException( Resources.UnableToStartSecondAsyncOp ) );

            Caller = AsyncExecuteWrapper;
            _asyncResult = Caller.BeginInvoke( 2, CommandBehavior.Default, callback, stateObject );
            return _asyncResult;
        }

        /// <summary>
        /// Initiates the asynchronous execution of the SQL statement or stored procedure 
        /// that is described by this <see cref="MySqlCommand"/>. 
        /// </summary>
        /// <returns>An <see cref="IAsyncResult"/> that can be used to poll or wait for results, 
        /// or both; this value is also needed when invoking <see cref="EndExecuteNonQuery"/>, 
        /// which returns the number of affected rows. </returns>
        public IAsyncResult BeginExecuteNonQuery() {
            if ( Caller != null ) Throw( new MySqlException( Resources.UnableToStartSecondAsyncOp ) );

            Caller = AsyncExecuteWrapper;
            _asyncResult = Caller.BeginInvoke( 2, CommandBehavior.Default, null, null );
            return _asyncResult;
        }

        /// <summary>
        /// Finishes asynchronous execution of a SQL statement. 
        /// </summary>
        /// <param name="asyncResult">The <see cref="IAsyncResult"/> returned by the call 
        /// to <see cref="BeginExecuteNonQuery()"/>.</param>
        /// <returns></returns>
        public int EndExecuteNonQuery( IAsyncResult asyncResult ) {
            asyncResult.AsyncWaitHandle.WaitOne();
            var c = Caller;
            Caller = null;
            if ( ThrownException != null ) throw ThrownException;
            return (int) c.EndInvoke( asyncResult );
        }
        #endregion

        #region Private Methods
        /*		private ArrayList PrepareSqlBuffers(string sql)
                {
                    ArrayList buffers = new ArrayList();
                    MySqlStreamWriter writer = new MySqlStreamWriter(new MemoryStream(), connection.Encoding);
                    writer.Version = connection.driver.Version;

                    // if we are executing as a stored procedure, then we need to add the call
                    // keyword.
                    if (CommandType == CommandType.StoredProcedure)
                    {
                        if (storedProcedure == null)
                            storedProcedure = new StoredProcedure(this);
                        sql = storedProcedure.Prepare( CommandText );
                    }

                    // tokenize the SQL
                    sql = sql.TrimStart(';').TrimEnd(';');
                    ArrayList tokens = TokenizeSql( sql );

                    foreach (string token in tokens)
                    {
                        if (token.Trim().Length == 0) continue;
                        if (token == ";" && ! connection.driver.SupportsBatch)
                        {
                            MemoryStream ms = (MemoryStream)writer.Stream;
                            if (ms.Length > 0)
                                buffers.Add( ms );

                            writer = new MySqlStreamWriter(new MemoryStream(), connection.Encoding);
                            writer.Version = connection.driver.Version;
                            continue;
                        }
                        else if (token[0] == parameters.ParameterMarker) 
                        {
                            if (SerializeParameter(writer, token)) continue;
                        }

                        // our fall through case is to write the token to the byte stream
                        writer.WriteStringNoNull(token);
                    }

                    // capture any buffer that is left over
                    MemoryStream mStream = (MemoryStream)writer.Stream;
                    if (mStream.Length > 0)
                        buffers.Add( mStream );

                    return buffers;
                }*/

        internal long EstimatedSize() {
            long size = CommandText.Length;
            foreach ( MySqlParameter parameter in Parameters ) size += parameter.EstimatedSize();
            return size;
        }

        /// <summary>
        /// Verifies if a query is valid even if it has not spaces or is a stored procedure call
        /// </summary>
        /// <param name="query">Query to validate</param>
        /// <returns>If it is necessary to add call statement</returns>
        private bool AddCallStatement( string query ) {
            /*PATTERN MATCHES
       * SELECT`user`FROM`mysql`.`user`;, select(left('test',1));, do(1);, commit, rollback, use, begin, end, use`sakila`;, select`test`;, select'1'=1;, SET@test='test';
       */
            var pattern = @"^|COMMIT|ROLLBACK|BEGIN|END|DO\S+|SELECT\S+[FROM|\S+]|USE?\S+|SET\S+";
            var regex = new Regex( pattern, RegexOptions.IgnoreCase );
            return !( regex.Matches( query ).Count > 0 );
        }
        #endregion

        #region ICloneable
        /// <summary>
        /// Creates a clone of this MySqlCommand object.  CommandText, Connection, and Transaction properties
        /// are included as well as the entire parameter list.
        /// </summary>
        /// <returns>The cloned MySqlCommand object</returns>
        public MySqlCommand Clone() {
            var clone = new MySqlCommand( _cmdText, _connection, Transaction ) {
                CommandType = CommandType,
                _commandTimeout = _commandTimeout,
                _useDefaultTimeout = _useDefaultTimeout,
                _batchableCommandText = _batchableCommandText,
                EnableCaching = EnableCaching,
                CacheAge = CacheAge
            };
            PartialClone( clone );

            foreach ( MySqlParameter p in Parameters ) clone.Parameters.Add( p.Clone() );
            return clone;
        }

        partial void PartialClone( MySqlCommand clone );

        object ICloneable.Clone() => Clone();
        #endregion

        #region Batching support
        internal void AddToBatch( MySqlCommand command ) {
            if ( Batch == null ) Batch = new List<MySqlCommand>();
            Batch.Add( command );
        }

        internal string GetCommandTextForBatching() {
            if ( _batchableCommandText != null ) return _batchableCommandText;
            if ( String.Compare( CommandText.Substring( 0, 6 ), "INSERT", StringComparison.OrdinalIgnoreCase ) == 0 ) {
                var cmd = new MySqlCommand( "SELECT @@sql_mode", Connection );
                var sqlMode = cmd.ExecuteScalar().ToString().InvariantToUpper();
                var tokenizer = new MySqlTokenizer( CommandText ) {
                    AnsiQuotes = sqlMode.InvariantIndexOf( "ANSI_QUOTES") != -1,
                    BackslashEscapes = sqlMode.InvariantIndexOf( "NO_BACKSLASH_ESCAPES" ) == -1
                };
                var token = tokenizer.NextToken().InvariantToLower() ;
                while ( token != null ) {
                    if ( token.InvariantToUpper() == "VALUES"
                         && !tokenizer.Quoted ) {
                        token = tokenizer.NextToken();
                        Debug.Assert( token == "(" );

                        // find matching right paren, and ensure that parens 
                        // are balanced.
                        var openParenCount = 1;
                        while ( token != null ) {
                            _batchableCommandText += token;
                            token = tokenizer.NextToken();

                            switch ( token ) {
                                case "(":
                                    openParenCount++;
                                    break;
                                case ")":
                                    openParenCount--;
                                    break;
                            }

                            if ( openParenCount == 0 ) break;
                        }

                        if ( token != null ) _batchableCommandText += token;
                        token = tokenizer.NextToken();
                        if ( token != null
                             && ( token == "," || StringUtility.InvariantToUpper( token ) == "ON" ) ) {
                            _batchableCommandText = null;
                            break;
                        }
                    }
                    token = tokenizer.NextToken();
                }
            }
            // Otherwise use the command verbatim
            else _batchableCommandText = CommandText;

            return _batchableCommandText;
        }
        #endregion

        // This method is used to throw all exceptions from this class.  
        private void Throw( Exception ex ) {
            _connection?.Throw( ex );
            throw ex;
        }

#if !RT
        public void Dispose() {
            Dispose( true );
            GC.SuppressFinalize( this );
        }

        protected override void Dispose( bool disposing ) {
            if ( _statement != null
                 && _statement.IsPrepared ) _statement.CloseStatement();

            base.Dispose( disposing );
        }
#else
    public void Dispose()
    {
      GC.SuppressFinalize(this);
    }
#endif
    }
}