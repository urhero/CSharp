using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Data.SqlClient;
using System.Data;
using System.Diagnostics;


namespace BBG2DB
{
    class NicerBloombergApiDemo
    {
        public static void Main()
        {
            string dir = System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetEntryAssembly().Location);

            //입력대상 폴더와 업로드 대상 DB정보 읽기
            string[] folder = null;
            string[] server_info = null;
            folder = Read_input(dir + "\\input_foler.txt");
            server_info = Read_input(dir + "\\server_info.txt");

            //대상 종목 리스트 읽기
            string[] stock_list = null;
            stock_list = Read_input(dir + "\\" + folder[0] + "\\stock_list.txt");

            var lines = File.ReadAllLines(dir + "\\" + folder[0] + "\\field_option_list.txt");

            string[] field_list = new string[100];
            string[,] option_list = new string[100, 2];
            int i = 0, j;
            int field_n = 0;
            int option_n = 0;
            int count_line_b4 = 0;
            string line = lines[i];
            string[] values = line.Split('\t');

            field_list = new string[100];
            option_list = new string[100, 2];

            //필드 옵션 단위 별로 읽어서 BloombergAPI 호출 (per_option 통해서)
            while (true)
            {
                if(i == 0 || (count_line_b4 == 1 && values.Length < 2))
                {
                    field_list[field_n++] = values[0];
                }else if(count_line_b4 > 0 && values.Length > 1)
                {
                    option_list[option_n, 0] = values[0];
                    option_list[option_n++, 1] = values[1];
                }
                count_line_b4 = values.Length;

                i++;

                if (i == lines.Length) break;

                line = lines[i];
                values = line.Split('\t');

                if ((count_line_b4 == 0 && values.Length != 1) || i == lines.Length || (count_line_b4 > 1 && values.Length < 2))
                {
                    for (j = 0; j < field_n; j++)
                        per_option(server_info[0], server_info[1], stock_list, new string[1] { field_list[j] }, option_list); //Loop 끝나고 최종 실행 한번더 있음 주의.
                    field_list = new string[100];
                    option_list = new string[100, 2];
                    field_n = 0;
                    option_n = 0;
                }

                if (count_line_b4 == 1 && values.Length == 0)
                    Console.WriteLine("\r\nNo opitons for the fields\r\n");
            }
            for (j = 0; j < field_n; j++)
                per_option(server_info[0], server_info[1], stock_list, new string[1] { field_list[j] }, option_list); //주의.
            Console.ReadLine();
        }

        // and also per field
        public static void per_option(string ConnectionString, string DB_Table, string[] stock_list, string[] field_list, string[,] option_list)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            SqlConnection con = new SqlConnection(ConnectionString);
            con.Open();

            SqlDataAdapter daBLPAPI_RAW
                = new SqlDataAdapter("Select * From " + DB_Table, con);

            con.Close();

            //컬럼과 컬럼 타입 정보 수집
            DataSet dsAPI = new DataSet("API");
            daBLPAPI_RAW.FillSchema(dsAPI, SchemaType.Source, DB_Table);
            ////daBLPAPI_RAW.Fill(dsAPI, DB_Table);

            DataTable tblBLPAPI_RAW;
            tblBLPAPI_RAW = dsAPI.Tables[DB_Table];

            DataRow drCurrent;

            System.Console.WriteLine("Bloomberg console running...");

            var startDateTime = DateTime.Today.AddYears(-16).AddMonths(1);
            var endDateTime = DateTime.Today;
            DateTime dt;

            var nicerApi = new BloombergApi();
            nicerApi.InitialiseSessionAndService();

            try
            {
                //Get fields for given dates 
                 var securitiesFieldsByDate = nicerApi.GetSecuritiesFieldsByDate( 
                     stock_list, 
                     field_list,
                     option_list); 
                 Console.WriteLine("\r\nExtrating Fields of \r\n" + field_list[0] + " \r\nByDate from Bloomberg API"); 

                //Loop by security 
                foreach (var security in securitiesFieldsByDate)
                {
                    //Console.WriteLine("Security: {0}", security.Key);

                    //Loop by date 
                    foreach (var dateAndFields in security.Value.OrderBy(d => d.Key))
                    {
                        //Console.WriteLine(dateAndFields.Key.ToString("yyyy-MM-dd"));

                        dt = ((System.DateTime)dateAndFields.Key).Date;

                        //Loop by field 
                        foreach (var keyValue in dateAndFields.Value)
                        {
                            //Console.WriteLine("{0}: {1} ({2})", keyValue.Key, keyValue.Value, keyValue.Value.GetType());

                            if (dateAndFields.Value.Count == 1)
                            {
                                drCurrent = tblBLPAPI_RAW.NewRow();   //행추가
                                drCurrent["data_dt"] = ((System.DateTime)keyValue.Value).Date.ToString().Substring(0, 10);
                                if (security.Key.ToString().Substring(security.Key.ToString().Length - 6, 6) == "EQUITY")
                                    drCurrent["co_cd"] = security.Key.ToString().Substring(0, security.Key.ToString().Length - 7);
                                else if (security.Key.ToString().Substring(security.Key.ToString().Length - 5, 5) == "INDEX")
                                    drCurrent["co_cd"] = security.Key.ToString();
                                //drCurrent["sec"] = DBNull.Value;
                                drCurrent["acct"] = field_list[0];
                                //drCurrent["val"] = null;
                                //drCurrent["release_dt"] = DBNull.Value;
                                //row["release_dt"] = null;
                                //row.ItemArray = new object[]{행의 값들 차례로 입력};
                                tblBLPAPI_RAW.Rows.Add(drCurrent);
                            }
                            else if(keyValue.Key=="date")
                            {
                                dt = ((System.DateTime)keyValue.Value).Date;
                            }
                            else{
                                //lst.Add(new bbg_info { date = dt, bbic = security.Key.ToString().Substring(0, security.Key.ToString().Length - 7), field = keyValue.Key, value = (double)keyValue.Value });
                                drCurrent = tblBLPAPI_RAW.NewRow();  //행추가
                                drCurrent["data_dt"] = dt.ToString().Substring(0, 10);
                                if (security.Key.ToString().Substring(security.Key.ToString().Length - 6, 6) == "EQUITY")
                                    drCurrent["co_cd"] = security.Key.ToString().Substring(0, security.Key.ToString().Length - 7);
                                else if (security.Key.ToString().Substring(security.Key.ToString().Length - 5, 5) == "INDEX")
                                    drCurrent["co_cd"] = security.Key.ToString();
                                //drCurrent["sec"] = DBNull.Value;
                                drCurrent["acct"] = keyValue.Key;
                                drCurrent["val"] = (double)keyValue.Value;
                                //drCurrent["release_dt"] = DBNull.Value;
                                //row["release_dt"] = null;
                                //row.ItemArray = new object[]{행의 값들 차례로 입력};
                                tblBLPAPI_RAW.Rows.Add(drCurrent);
                            }
                        }
                        
                    }
                }
                Console.WriteLine();

                /*
                //Get current rates  
                var flashRates = nicerApi.GetSecuritiesFields(
                    new string[] { LIBOR_1_MONTH, LIBOR_3_MONTH, LIBOR_6_MONTH, EURIBOR_1_MONTH, EURIBOR_3_MONTH, EURIBOR_6_MONTH
                    },
                    new string[] { PREV_CLOSE_VALUE_REALTIME, PX_LAST, CHG_NET_1D, CHG_PCT_1D, CHG_PCT_YTD
                    });
                foreach (var securityAndFields in flashRates.OrderBy(d => d.Key))
                {
                    Console.WriteLine(securityAndFields.Key);
                    foreach (var keyValue in securityAndFields.Value)
                        Console.WriteLine("{0}: {1} ({2})", keyValue.Key, keyValue.Value, keyValue.Value.GetType());
                }
                Console.WriteLine();
                */
            }
            catch (Exception e)
            {
                System.Console.WriteLine(e.ToString());
            }

            //DataTable data 저장 끝

            // txt_path = dir + "\\ouput_test.txt";

            /*
            SqlCommandBuilder objCommandBuilder = new SqlCommandBuilder(daBLPAPI_RAW);
            daBLPAPI_RAW.Update(dsAPI, "BLPAPI_RAW");
            */

            Console.WriteLine(sw.Elapsed);
            Console.WriteLine(": Bloomberg API download completed \r\n\r\nStarting copy to database");

            SqlBulkCopy bulkCopy = new SqlBulkCopy(ConnectionString, SqlBulkCopyOptions.TableLock);


            bulkCopy.DestinationTableName = "dbo." + DB_Table;
            bulkCopy.WriteToServer(tblBLPAPI_RAW);

            Console.WriteLine(sw.Elapsed);
            sw.Stop();

            Console.WriteLine(":SQL Server updated successfully\r\n");

            //if (File.Exists(txt_path)) File.Delete(txt_path);
            //System.IO.StreamWriter writer = new System.IO.StreamWriter(txt_path, true);


            //bbg_info.WriteXml("ouput.xml");



            //foreach (var ln in lst)
            //{
            //writer.WriteLine(string.Format("{0:yyyy-MM-dd}",ln.date) + "\t" + ln.bbic + "\t" + ln.field + "\t" + ln.value);
            //writer.WriteLine(sw.ElapsedMilliseconds.ToString() + "ms");
                //ln.ToDatabase();
            //}
            //writer.Close();
            /*
            System.Console.WriteLine("Press ENTER to quit");
            try
            {
                System.Console.Read();
            }
            catch (System.IO.IOException)
            {
            
            }*/
        }


        public static string[] Read_input(string csv_path)
        {
            var lines = File.ReadAllLines(csv_path);
            string[] list = new string[lines.Length];
            int dataRowStart = 0;
            int n = 0;
            for (int i = dataRowStart; i < lines.Length; i++)
            {
                string line = lines[i];
                string[] values = line.Split('\t');
                for (int j = 0; j < values.Length; j++)
                {
                    list[n] = values[j];
                    n++;
                }
            }
            return list;
        }

        public static string[,] Read_input2(string csv_path)
        {
            var lines = File.ReadAllLines(csv_path);
            string[,] list = new string[lines.Length, 2];
            int dataRowStart = 0;
            for (int i = dataRowStart; i < lines.Length; i++)
            {
                string line = lines[i];
                string[] values = line.Split('\t');
                for (int j = 0; j < 2; j++)
                {
                    list[i, j] = values[j];
                    
                }
            }
            return list;
        }
        /*
        public partial class DataTable1:DataTable
        {
            public DataTable1()
            {
                InitializeComponent();

            }

            public static void ToTXT(this DataTable dtDataTable, string strFilePath)
            {
                StreamWriter sw = new StreamWriter(strFilePath, false);
                //headers   
                for (int i = 0; i < dtDataTable.Columns.Count; i++)
                {
                    sw.Write(dtDataTable.Columns[i]);
                    if (i < dtDataTable.Columns.Count - 1)
                    {
                        sw.Write(",");
                    }
                }
                sw.Write(sw.NewLine);
                foreach (DataRow dr in dtDataTable.Rows)
                {
                    for (int i = 0; i < dtDataTable.Columns.Count; i++)
                    {
                        if (!Convert.IsDBNull(dr[i]))
                        {
                            string value = dr[i].ToString();
                            if (value.Contains(','))
                            {
                                value = String.Format("\"{0}\"", value);
                                sw.Write(value);
                            }
                            else
                            {
                                sw.Write(dr[i].ToString());
                            }
                        }
                        if (i < dtDataTable.Columns.Count - 1)
                        {
                            sw.Write("\t");
                        }
                    }
                    sw.Write(sw.NewLine);
                }
                sw.Close();
            }
        }*/
        /*
        public class bbg_info
        {
            public DateTime date { get; set; }
            public string bbic { get; set; }
            public string field { get; set; }
            public double value { get; set; }
            */
        /*

        internal void ToDatabase()
        {
            using (SqlConnection con = new SqlConnection("Server = 10.206.1.11; " + 
                "Database = API; " + 
                "User Id = sa; " +
                "Password = sa;"))
            {
                con.Open();
                try
                {
                    using (SqlCommand command = new SqlCommand("INSERT INTO BLPAPI_RAW(data_dt, co_cd, acct, val) VALUES(@data_dt, @co_cd, @acct, @val)", con))
                    {

                        command.Parameters.Add(new SqlParameter("data_dt", date));
                        command.Parameters.Add(new SqlParameter("co_cd", bbic));
                        command.Parameters.Add(new SqlParameter("acct", field));
                        command.Parameters.Add(new SqlParameter("val", value));

                        command.ExecuteNonQuery();
                    }

                }
                catch
                {
                    Console.WriteLine("Count not insert.");
                }
                con.Close();
            }
        }


    }*/




    }
}