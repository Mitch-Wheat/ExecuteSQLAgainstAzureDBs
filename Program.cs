using Microsoft.Data.SqlClient;
using System.Data;

// NOTE:  * Use at own risk!!

namespace ExecuteSQLAgainstAzureDBs
{
    internal class Program
    {
        internal static readonly string[] separator = [","];

        static int Main(string[] args)
        {
            SqlConnectionStringBuilder builder;

            if (args.Length < 3)
            {
                Console.WriteLine("Usage: ConnectionString ScriptFile DbNamesFile");
                Console.WriteLine("\"Server=<your server>.database.windows.net; Authentication=Active Directory Interactive\" \"C:\\Temp\\test.sql\" \"C:\\Temp\\dbnames.csv\"");
                return (-1);
            }

            string connectionStringBase = args[0];
            string sqlScriptFilePath    = args[1];
            string dbNamesFilePath      = args[2];

            try
            {
                builder = new(connectionStringBase)
                {
                    Encrypt = true,
                    TrustServerCertificate = true
                };
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                throw;
            }

            string sqlScript = File.ReadAllText(@sqlScriptFilePath);
            List<string> sqlBatch = [.. sqlScript.Split("GO" + Environment.NewLine, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)];

            var csvNames = string.Join("", File.ReadAllLines(dbNamesFilePath));
            List<string> dbNames = [.. csvNames.Replace("\"", "").Split(separator, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)];

            if (dbNames.Count == 0)
            {
                Console.WriteLine("Must specify at least one database in db names .csv file");
                return -1;
            }

            foreach (var db in dbNames)
            {
                builder.InitialCatalog = db;

                Console.WriteLine($"Running script against database: {db}");

                using SqlConnection cnn = new(builder.ToString());
                cnn.Open();

                foreach (var sql in sqlBatch)
                {
                    RunQuery(sql, cnn);
                }
            }

            Console.WriteLine("Press any Key to continue.");
            Console.ReadLine();

            return 0;
        }

        private static void RunQuery(string sql, SqlConnection cn)
        {
            if (sql.StartsWith("SELECT"))
            {
                RunSelectQuery(sql, cn);
            }
            else
            {
                RunNonSelectQuery(sql, cn);
            }
        }

        private static void RunSelectQuery(string sql, SqlConnection cn)
        {
            DataTable dt = new();
            int numrows;

            if (cn.State != ConnectionState.Open)
                cn.Open();

            using (var cmd = new SqlCommand(sql, cn))
            using (var sda = new SqlDataAdapter(cmd))
            {
                numrows = sda.Fill(dt);
            }

            if (dt.Rows.Count != 0)
            {
                DataColumn[] columns = [.. dt.Columns.Cast<DataColumn>()];

                string[] columnNames = [.. columns.Select(c => c.ColumnName)];

                string header = string.Join(", ", columnNames);
                Console.WriteLine(header);

                foreach (DataRow row in dt.Rows)
                {
                    string[] values = new string[dt.Columns.Count];
                    int i = 0;
                    foreach (DataColumn col in dt.Columns)
                    {
                        ConverttoType(row, values, i, col);

                        i++;
                    }

                    string data = string.Join("; ", values.Select(s => "\"" + s.Replace("\"", "\"\"") + "\""));

                    Console.WriteLine(data);
                }   
            }
            
            Console.WriteLine();
        }

        private static void ConverttoType(DataRow row, string[] values, int i, DataColumn col)
        {
            if (col.DataType == typeof(int))
            {
                values[i] = row[col] != DBNull.Value ? ((int)row[col]).ToString() : "";
            }
            else if (col.DataType == typeof(decimal))
            {
                values[i] = row[col] != DBNull.Value ? ((decimal)row[col]).ToString() : "";
            }
            else if (col.DataType == typeof(DateTime))
            {
                values[i] = row[col] != DBNull.Value ? ((DateTime)row[col]).ToString() : "";
            }
            else if (col.DataType == typeof(short))
            {
                values[i] = row[col] != DBNull.Value ? ((short)row[col]).ToString() : "";
            }
            else if (col.DataType == typeof(Guid))
            {
                values[i] = row[col] != DBNull.Value ? ((Guid)row[col]).ToString() : "";
            }
            else if (col.DataType == typeof(byte[]))
            {
                values[i] = row[col] != DBNull.Value ? "0x" + Convert.ToHexString((byte[])row[col]) : "";
            }
            else
            {
                values[i] = row[col] != DBNull.Value ? ((string)row[col]) : "";
            }
        }

        private static void RunNonSelectQuery(string sql, SqlConnection cn)
        {
            using var cmd = new SqlCommand(sql, cn);
            cmd.ExecuteNonQuery();
        }
    }
}
