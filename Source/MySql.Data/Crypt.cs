// Copyright (c) 2004-2008 MySQL AB, 2008-2009 Sun Microsystems, Inc.,
// 2013 Oracle and/or its affiliates. All rights reserved.
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
using System.Security.Cryptography;
using MySql.Data.MySqlClient.Properties;
using System.Text;

namespace MySql.Data.MySqlClient {
    /// <summary>
    /// Summary description for Crypt.
    /// </summary>
    internal class Crypt {
        /// <summary>
        /// Simple XOR scramble
        /// </summary>
        /// <param name="from">Source array</param>
        /// <param name="fromIndex">Index inside source array</param>
        /// <param name="to">Destination array</param>
        /// <param name="toIndex">Index inside destination array</param>
        /// <param name="password">Password used to xor the bits</param>
        /// <param name="length">Number of bytes to scramble</param>
        private static void XorScramble( byte[] from, int fromIndex, byte[] to, int toIndex, byte[] password, int length ) {
            // make sure we were called properly
            if ( fromIndex < 0
                 || fromIndex >= from.Length ) throw new ArgumentException( Resources.IndexMustBeValid, "fromIndex" );
            if ( ( fromIndex + length ) > from.Length ) throw new ArgumentException( Resources.FromAndLengthTooBig, "fromIndex" );
            if ( from == null ) throw new ArgumentException( Resources.BufferCannotBeNull, "from" );
            if ( to == null ) throw new ArgumentException( Resources.BufferCannotBeNull, "to" );
            if ( toIndex < 0
                 || toIndex >= to.Length ) throw new ArgumentException( Resources.IndexMustBeValid, "toIndex" );
            if ( ( toIndex + length ) > to.Length ) throw new ArgumentException( Resources.IndexAndLengthTooBig, "toIndex" );
            if ( password == null
                 || password.Length < length ) throw new ArgumentException( Resources.PasswordMustHaveLegalChars, "password" );
            if ( length < 0 ) throw new ArgumentException( Resources.ParameterCannotBeNegative, "count" );

            // now perform the work
            for ( var i = 0; i < length; i++ ) to[ toIndex++ ] = (byte) ( from[ fromIndex++ ] ^ password[ i ] );
        }

        /// <summary>
        /// Returns a byte array containing the proper encryption of the 
        /// given password/seed according to the new 4.1.1 authentication scheme.
        /// </summary>
        /// <param name="password"></param>
        /// <param name="seed"></param>
        /// <returns></returns>
        public static byte[] Get411Password( string password, string seed ) {
            // if we have no password, then we just return 2 zero bytes
            if ( password.Length == 0 ) return new byte[1];

            var sha = new SHA1CryptoServiceProvider();

            var firstHash = sha.ComputeHash( Encoding.Default.GetBytes( password ) );
            var secondHash = sha.ComputeHash( firstHash );
            var seedBytes = Encoding.Default.GetBytes( seed );

            var input = new byte[seedBytes.Length + secondHash.Length];
            Array.Copy( seedBytes, 0, input, 0, seedBytes.Length );
            Array.Copy( secondHash, 0, input, seedBytes.Length, secondHash.Length );
            var thirdHash = sha.ComputeHash( input );

            var finalHash = new byte[thirdHash.Length + 1];
            finalHash[ 0 ] = 0x14;
            Array.Copy( thirdHash, 0, finalHash, 1, thirdHash.Length );

            for ( var i = 1; i < finalHash.Length; i++ ) finalHash[ i ] = (byte) ( finalHash[ i ] ^ firstHash[ i - 1 ] );
            return finalHash;
            //byte[] buffer = new byte[finalHash.Length - 1];
            //Array.Copy(finalHash, 1, buffer, 0, finalHash.Length - 1);
            //return buffer;
        }

        private static double Rand( ref long seed1, ref long seed2, long max ) {
            seed1 = ( seed1 * 3 ) + seed2;
            seed1 %= max;
            seed2 = ( seed1 + seed2 + 33 ) % max;
            return ( seed1 / (double) max );
        }

        /// <summary>
        /// Encrypts a password using the MySql encryption scheme
        /// </summary>
        /// <param name="password">The password to encrypt</param>
        /// <param name="seed">The encryption seed the server gave us</param>
        /// <param name="newVer">Indicates if we should use the old or new encryption scheme</param>
        /// <returns></returns>
        public static String EncryptPassword( String password, String seed, bool newVer ) {
            var max = 0x3fffffff;
            if ( !newVer ) max = 0x01FFFFFF;
            if ( string.IsNullOrEmpty( password ) ) return password;

            var hashSeed = Hash( seed );
            var hashPass = Hash( password );

            var seed1 = ( hashSeed[ 0 ] ^ hashPass[ 0 ] ) % max;
            var seed2 = ( hashSeed[ 1 ] ^ hashPass[ 1 ] ) % max;
            if ( !newVer ) seed2 = seed1 / 2;

            var scrambled = new char[seed.Length];
            for ( var x = 0; x < seed.Length; x++ ) {
                var r = Rand( ref seed1, ref seed2, max );
                scrambled[ x ] = (char) ( Math.Floor( r * 31 ) + 64 );
            }

            if ( !newVer ) return new string( scrambled );
            /* Make it harder to break */
            var extra = (char) Math.Floor( Rand( ref seed1, ref seed2, max ) * 31 );
            for ( var x = 0; x < scrambled.Length; x++ ) scrambled[ x ] ^= extra;

            return new string( scrambled );
        }

        /// <summary>
        /// Hashes a password using the algorithm from Monty's code.
        /// The first element in the return is the result of the "old" hash.
        /// The second element is the rest of the "new" hash.
        /// </summary>
        /// <param name="p">Password to be hashed</param>
        /// <returns>Two element array containing the hashed values</returns>
        private static long[] Hash( String p ) {
            var val1 = 1345345333;
            var val2 = 0x12345671;
            var inc = 7;

            for ( var i = 0; i < p.Length; i++ ) {
                if ( p[ i ] == ' '
                     || p[ i ] == '\t' ) continue;
                var temp = ( 0xff & i );
                val1 ^= ( ( ( val1 & 63 ) + inc ) * temp ) + ( val1 << 8 );
                val2 += ( val2 << 8 ) ^ val1;
                inc += temp;
            }

            var hash = new long[2];
            hash[ 0 ] = val1 & 0x7fffffff;
            hash[ 1 ] = val2 & 0x7fffffff;
            return hash;
        }
    }
}