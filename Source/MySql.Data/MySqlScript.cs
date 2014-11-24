// Copyright � 2004, 2014, Oracle and/or its affiliates. All rights reserved.
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
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using MySql.Data.MySqlClient.Properties;
#if NET_40_OR_GREATER
using System.Threading.Tasks;
using System.Threading;

#endif

namespace MySql.Data.MySqlClient {
    /// <summary>
    /// Provides a class capable of executing a SQL script containing
    /// multiple SQL statements including CREATE PROCEDURE statements
    /// that require changing the delimiter
    /// </summary>
    public class MySqlScript {
        public event MySqlStatementExecutedEventHandler StatementExecuted;
        public event MySqlScriptErrorEventHandler Error;
        public event EventHandler ScriptCompleted;

        #region Constructors
        /// <summary>
        /// Initializes a new instance of the 
        /// <see cref="MySqlScript"/> class.
        /// </summary>
        public MySqlScript() {
            Delimiter = ";";
        }

        /// <summary>
        /// Initializes a new instance of the 
        /// <see cref="MySqlScript"/> class.
        /// </summary>
        /// <param name="connection">The connection.</param>
        public MySqlScript( MySqlConnection connection ) : this() {
            Connection = connection;
        }

        /// <summary>
        /// Initializes a new instance of the 
        /// <see cref="MySqlScript"/> class.
        /// </summary>
        /// <param name="query">The query.</param>
        public MySqlScript( string query ) : this() {
            Query = query;
        }

        /// <summary>
        /// Initializes a new instance of the 
        /// <see cref="MySqlScript"/> class.
        /// </summary>
        /// <param name="connection">The connection.</param>
        /// <param name="query">The query.</param>
        public MySqlScript( MySqlConnection connection, string query ) : this() {
            Connection = connection;
            Query = query;
        }
        #endregion

        #region Properties
        /// <summary>
        /// Gets or sets the connection.
        /// </summary>
        /// <value>The connection.</value>
        public MySqlConnection Connection { get; set; }

        /// <summary>
        /// Gets or sets the query.
        /// </summary>
        /// <value>The query.</value>
        public string Query { get; set; }

        /// <summary>
        /// Gets or sets the delimiter.
        /// </summary>
        /// <value>The delimiter.</value>
        public string Delimiter { get; set; }
        #endregion

        #region Public Methods
        /// <summary>
        /// Executes this instance.
        /// </summary>
        /// <returns>The number of statements executed as part of the script.</returns>
        public int Execute() {
            var openedConnection = false;

            if ( Connection == null ) throw new InvalidOperationException( Resources.ConnectionNotSet );
            if ( string.IsNullOrEmpty( Query ) ) return 0;

            // next we open up the connetion if it is not already open
            if ( Connection.State != ConnectionState.Open ) {
                openedConnection = true;
                Connection.Open();
            }

            // since we don't allow setting of parameters on a script we can 
            // therefore safely allow the use of user variables.  no one should be using
            // this connection while we are using it so we can temporarily tell it
            // to allow the use of user variables
            var allowUserVars = Connection.Settings.AllowUserVariables;
            Connection.Settings.AllowUserVariables = true;

            try {
                var mode = Connection.Driver.Property( "sql_mode" );
                mode = mode.InvariantToUpper();
                var ansiQuotes = mode.InvariantIndexOf( "ANSI_QUOTES" ) != -1;
                var noBackslashEscapes = mode.InvariantIndexOf( "NO_BACKSLASH_ESCAPES" ) != -1;

                // first we break the query up into smaller queries
                var statements = BreakIntoStatements( ansiQuotes, noBackslashEscapes );

                var count = 0;
                var cmd = new MySqlCommand( null, Connection );
                foreach ( var statement in statements.Where( statement => !String.IsNullOrEmpty( statement.Text ) ) ) {
                    cmd.CommandText = statement.Text;
                    try {
                        cmd.ExecuteNonQuery();
                        count++;
                        OnQueryExecuted( statement );
                    }
                    catch ( Exception ex ) {
                        if ( Error == null ) throw;
                        if ( !OnScriptError( ex ) ) break;
                    }
                }
                OnScriptCompleted();
                return count;
            }
            finally {
                Connection.Settings.AllowUserVariables = allowUserVars;
                if ( openedConnection ) Connection.Close();
            }
        }
        #endregion

        private void OnQueryExecuted( ScriptStatement statement ) {
            StatementExecuted?.Invoke( this, new MySqlScriptEventArgs { Statement = statement } );
        }

        private void OnScriptCompleted() { ScriptCompleted?.Invoke( this, EventArgs.Empty ); }

        private bool OnScriptError( Exception ex ) {
            if ( Error == null ) return false;
            var args = new MySqlScriptErrorEventArgs( ex );
            Error( this, args );
            return args.Ignore;
        }

        private List<int> BreakScriptIntoLines() {
            var lineNumbers = new List<int>();

            var sr = new StringReader( Query );
            var line = sr.ReadLine();
            var pos = 0;
            while ( line != null ) {
                lineNumbers.Add( pos );
                pos += line.Length;
                line = sr.ReadLine();
            }
            return lineNumbers;
        }

        private static int FindLineNumber( int position, List<int> lineNumbers ) {
            var i = 0;
            while ( i < lineNumbers.Count
                    && position < lineNumbers[ i ] ) i++;
            return i;
        }

        private List<ScriptStatement> BreakIntoStatements( bool ansiQuotes, bool noBackslashEscapes ) {
            var currentDelimiter = Delimiter;
            var startPos = 0;
            var statements = new List<ScriptStatement>();
            var lineNumbers = BreakScriptIntoLines();
            var tokenizer = new MySqlTokenizer( Query ) { AnsiQuotes = ansiQuotes, BackslashEscapes = !noBackslashEscapes };

            var token = tokenizer.NextToken();
            while ( token != null ) {
                if ( !tokenizer.Quoted )
                    if ( token.InvariantToLower() == "delimiter" ) {
                        tokenizer.NextToken();
                        AdjustDelimiterEnd( tokenizer );
                        currentDelimiter = Query.Substring( tokenizer.StartIndex, tokenizer.StopIndex - tokenizer.StartIndex ).Trim();
                        startPos = tokenizer.StopIndex;
                    }
                    else {
                        // this handles the case where our tokenizer reads part of the
                        // delimiter
                        if ( currentDelimiter.StartsWith( token, StringComparison.OrdinalIgnoreCase ) )
                            if ( ( tokenizer.StartIndex + currentDelimiter.Length ) <= Query.Length )
                                if ( Query.Substring( tokenizer.StartIndex, currentDelimiter.Length ) == currentDelimiter ) {
                                    token = currentDelimiter;
                                    tokenizer.Position = tokenizer.StartIndex + currentDelimiter.Length;
                                    tokenizer.StopIndex = tokenizer.Position;
                                }

                        var delimiterPos = token.IndexOf( currentDelimiter, StringComparison.OrdinalIgnoreCase );
                        if ( delimiterPos != -1 ) {
                            var endPos = tokenizer.StopIndex - token.Length + delimiterPos;
                            if ( tokenizer.StopIndex == Query.Length - 1 ) endPos++;
                            var currentQuery = Query.Substring( startPos, endPos - startPos );
                            var statement = new ScriptStatement {
                                Text = currentQuery.Trim(),
                                Line = FindLineNumber( startPos, lineNumbers )
                            };
                            statement.Position = startPos - lineNumbers[ statement.Line ];
                            statements.Add( statement );
                            startPos = endPos + currentDelimiter.Length;
                        }
                    }
                token = tokenizer.NextToken();
            }

            // now clean up the last statement
            if ( startPos >= Query.Length - 1 ) return statements;
            var sqlLeftOver = Query.Substring( startPos ).Trim();
            if ( !String.IsNullOrEmpty( sqlLeftOver ) ) {
                var statement = new ScriptStatement { Text = sqlLeftOver, Line = FindLineNumber( startPos, lineNumbers ) };
                statement.Position = startPos - lineNumbers[ statement.Line ];
                statements.Add( statement );
            }
            return statements;
        }

        private void AdjustDelimiterEnd( MySqlTokenizer tokenizer ) {
            if ( tokenizer.StopIndex < Query.Length ) {
                var pos = tokenizer.StopIndex;
                var c = Query[ pos ];

                while ( !Char.IsWhiteSpace( c )
                        && pos < ( Query.Length - 1 ) ) c = Query[ ++pos ];
                tokenizer.StopIndex = pos;
                tokenizer.Position = pos;
            }
        }

#if NET_40_OR_GREATER

        #region Async
        /// <summary>
        /// Async version of Execute
        /// </summary>
        /// <returns>The number of statements executed as part of the script inside.</returns>
        public Task<int> ExecuteAsync() => ExecuteAsync( CancellationToken.None );

        public Task<int> ExecuteAsync( CancellationToken cancellationToken ) {
            var result = new TaskCompletionSource<int>();
            if ( cancellationToken == CancellationToken.None
                 || !cancellationToken.IsCancellationRequested )
                try {
                    var executeResult = Execute();
                    result.SetResult( executeResult );
                }
                catch ( Exception ex ) {
                    result.SetException( ex );
                }
            else result.SetCanceled();
            return result.Task;
        }
        #endregion

#endif
    }

    /// <summary>
    /// 
    /// </summary>
    public delegate void MySqlStatementExecutedEventHandler( object sender, MySqlScriptEventArgs args );

    /// <summary>
    /// 
    /// </summary>
    public delegate void MySqlScriptErrorEventHandler( object sender, MySqlScriptErrorEventArgs args );

    /// <summary>
    /// 
    /// </summary>
    public class MySqlScriptEventArgs : EventArgs {
        private ScriptStatement _statement;

        internal ScriptStatement Statement {
            set {
                _statement = value;
            }
        }

        /// <summary>
        /// Gets the statement text.
        /// </summary>
        /// <value>The statement text.</value>
        public string StatementText => _statement.Text;

        /// <summary>
        /// Gets the line.
        /// </summary>
        /// <value>The line.</value>
        public int Line => _statement.Line;

        /// <summary>
        /// Gets the position.
        /// </summary>
        /// <value>The position.</value>
        public int Position => _statement.Position;
    }

    /// <summary>
    /// 
    /// </summary>
    public class MySqlScriptErrorEventArgs : MySqlScriptEventArgs {
        /// <summary>
        /// Initializes a new instance of the <see cref="MySqlScriptErrorEventArgs"/> class.
        /// </summary>
        /// <param name="exception">The exception.</param>
        public MySqlScriptErrorEventArgs( Exception exception ) {
            Exception = exception;
        }

        /// <summary>
        /// Gets the exception.
        /// </summary>
        /// <value>The exception.</value>
        public Exception Exception { get; }

        /// <summary>
        /// Gets or sets a value indicating whether this <see cref="MySqlScriptErrorEventArgs"/> is ignore.
        /// </summary>
        /// <value><c>true</c> if ignore; otherwise, <c>false</c>.</value>
        public bool Ignore { get; set; }
    }

    internal struct ScriptStatement {
        public string Text;
        public int Line;
        public int Position;
    }
}