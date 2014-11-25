using System;
using System.Configuration;
using System.Data;
using MySql.Data.MySqlClient;

namespace MySqlDataTest {
    internal class Program {
        private static void Main( string[] args ) {
            Console.WriteLine( "Starting up" );
            try {
                using ( var conn = new MySqlConnection( ConfigurationManager.ConnectionStrings[ "deft" ].ConnectionString ) ) {
                    Console.WriteLine( "Current state: {0}",conn.State );
                    Console.WriteLine( "Connecting..." );
                    conn.Open();
                    Console.WriteLine( "Current state: {0}", conn.State );
                    using ( var cmd = conn.CreateCommand() ) {
                        cmd.CommandText = "show tables;";
                        Console.WriteLine( "Executing query..." );
                        using ( var resp = cmd.ExecuteReader() ) {
                            Console.WriteLine( "It works! Has rows? {0}", resp.HasRows );
                            var rs = resp.ResultSet.Fields;
                            foreach ( var field in rs ) {
                                Console.WriteLine( "Column {0}, ordinal={1}", field.ColumnName,field.OriginalColumnName );
                            }
                            while ( ( resp.Read() ) ) {
                                Console.WriteLine( resp.GetString( 0 ) );
                            } ;
                            Console.WriteLine( "Bzz...." );
                        }
                    }
                }
                Console.WriteLine( "Successfully completed test" );
            }
            catch ( Exception ex) {
                Console.WriteLine( ex.Message );
                Console.WriteLine( ex.StackTrace );
            }
            finally {
                Console.ReadLine();
            }
        }
    }
}