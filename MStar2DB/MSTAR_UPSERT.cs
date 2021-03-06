﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SqlClient;
using System.Data;
using System.IO;
using System.Text.RegularExpressions;
using System.Diagnostics;
using System.Data.OleDb;
using Excel;


namespace MStar2DB
{
    class MSTAR_UPSERT
    {
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
        /* private static void showMatch(string text, string expr)                                                       //regex 정규식 string 만들때 디버깅용
        {
            Console.WriteLine("The Expression: " + expr);
            MatchCollection mc = Regex.Matches(text, expr);
            foreach (Match m in mc)
            {
                Console.WriteLine(m);
            }
        }
        */

        public static void SaveAsTxt(string path)
        {
            // OLEDB can only read 255 column!! therefore using ExcelDataReader open source
            FileStream stream = File.Open(path, FileMode.Open, FileAccess.Read);

            //string extension = Path.GetExtension(path);
            IExcelDataReader excelReader;
            if (path.EndsWith(".xls"))
                excelReader = ExcelReaderFactory.CreateBinaryReader(stream);
            else if (path.EndsWith(".xlsx"))
                excelReader = ExcelReaderFactory.CreateOpenXmlReader(stream);
            else
                throw new NotSupportedException("Wrong file extension");

            //if (excelContent.HasHeader)
            //    excelReader.IsFirstRowAsColumnNames = true;
            
            DataSet result = excelReader.AsDataSet();

            //5. Data Reader methods
            while (excelReader.Read())
            {
                //excelReader.GetInt32(0);
            }

            DataTable dt = result.Tables[0];
            DataRow dr;
            StreamWriter wrtr = null;
            wrtr = new StreamWriter(path.Replace(".xlsx", ".txt"));

            for (int i = 0; i < dt.Columns.Count; i++)
            {
                string rowString = null;
                dr = dt.Rows[i];
                for (int j = 0; j < dr.Table.Columns.Count; j++)
                {
                    rowString += "\"" + dr[j] + "\"\t";
                }
                wrtr.WriteLine(rowString);
            }
            
            excelReader.Close();
            wrtr.Close();
            wrtr.Dispose();
        }

        static List<string[]> TextDivider(string raw)
        {
            List<string[]> lines = new List<string[]>();
            int header_row = -1;

            //showMatch(raw, @"(\t\""[^""]*?)\n(\(Cumulative\)\"")");                                                       //regex 정규식 string 만들때 디버깅용
            raw = Regex.Replace(raw, @"(\t\""[^""]*?)\n(\(Cumulative\)\"")", "$1$2");  //한 셀 안의 줄바꿈\n 제거
            raw = Regex.Replace(raw, @"(\t\""[^""]*?)\n(\(Annualized\)\"")", "$1$2");  //한 셀 안의 줄바꿈\n 제거

            //showMatch(raw, @",|\""|\r");                                                                                  //regex 정규식 string 만들때 디버깅용
            raw = Regex.Replace(raw, @",|\""|\r", "");                             //, " 캐리지 리턴 제거

            var raw_lines = raw.Split('\n');
            for (int i = 0; i < raw_lines.Length; i++)
            {
                string[] line = raw_lines[i].Split('\t');
                lines.Add(line);

                if (header_row == -1 & lines[i][0] == "FundId")
                    header_row = i;
            }


            //날짜 정리                                                                    ///////////////////////////////////////////////////////////////
            for (int j = 1; j < lines[header_row].Length; j++)
            {
                Match match = Regex.Match(lines[header_row][j], @"([0-9]{4})\-([0-9]{2})");
                if (match.Success && lines[header_row - 1][j] == "")
                {
                    lines[header_row - 1][j] = string.Format("{0}-{1}-{2}", match.Groups[1].Value, match.Groups[2].Value, DateTime.DaysInMonth(Int32.Parse(match.Groups[1].Value), Int32.Parse(match.Groups[2].Value)).ToString());
                    lines[header_row][j] = Regex.Replace(lines[header_row][j], @"([0-9]{4})\-([0-9]{2})", "");
                }
                else if (lines[header_row][j] != "" && lines[header_row - 1][j - 1] != "" && lines[header_row - 1][j] == "")
                    lines[header_row - 1][j] = lines[header_row - 1][j - 1];
            }

            return lines;
        }

        static void Main(string[] args)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            string exedir = System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetEntryAssembly().Location);
            string dir = "M:\\MOON_Inkyu\\FUND_DATABASE\\";

            string[] input_info = null;
            input_info = Read_input(exedir + "\\input_info.txt");
            //0: Connection String
            //1: historical(timeseries) data Table
            //2: Static data Table
            //3: input file path

            //db 정보, input 파일 정보 수집
            foreach (string file in Directory.EnumerateFiles(dir + "\\files2Execute", "*.xlsx"))
            {
                int ROW_H = -1;                                                                                              //Header 행
                int COL_H = -1;                                                                                          //historical data가 시작되는 컬럼

                if (!File.Exists(file.Replace(".xlsx", ".txt")))
                    SaveAsTxt(file);

                List<string[]> lines = TextDivider(File.ReadAllText(file.Replace(".xlsx", ".txt")));

                SqlConnection con = new SqlConnection(input_info[0]);
                con.Open();
                SqlDataAdapter da_ts
                    = new SqlDataAdapter("Select * From " + input_info[1], con);
                SqlDataAdapter da_st
                    = new SqlDataAdapter("Select * From " + input_info[2], con);
                con.Close();

                //컬럼과 컬럼 타입 정보 수집
                DataSet db = new DataSet("GLOBAL");                                                                     //"GLOBAL" 향후 input파일 에서 연동 요망
                da_ts.FillSchema(db, SchemaType.Source, input_info[1]);
                da_st.FillSchema(db, SchemaType.Source, input_info[2]);                                                      //#CP# 가능?
                ////da.Fill(dsAPI, DB_Table);

                DataTable dt_ts; //data table_time series
                dt_ts = db.Tables[input_info[1]];


                DataTable dt_st; //data table_static
                dt_st = db.Tables[input_info[2]];


                //파싱을 통한 db행 생성
                DataRow dr;


                for (int i = 0; i < lines.Count; i++)
                {
                    if (ROW_H != -1 && lines[i][0] != "" && lines[i][0] != "Unclassified" && lines[i][0] != "Number of investments ranked" && !Regex.Match(lines[i][0], @"(Benchmark [0-9]{1}\:)|(Peer Group)").Success)
                    {
                        for (int j = 1; j < lines[i].Length; j++)
                        {

                            if (j < COL_H && lines[i][j] != "" && lines[ROW_H - 1][j] == "")                                     //static data
                            {
                                dr = dt_st.NewRow();
                                dr["id"] = lines[i][0];
                                //dr["ddt"] = lines[ROW_H-1][j];
                                dr["acct"] = lines[ROW_H][j];
                                dr["val"] = lines[i][j];
                                dt_st.Rows.Add(dr);

                            }
                            else if (j >= COL_H && lines[i][j] != "")                               //historical data
                            {
                                dr = dt_ts.NewRow();
                                dr["id"] = lines[i][0];
                                dr["ddt"] = lines[ROW_H - 1][j];
                                dr["acct"] = lines[ROW_H][j];
                                dr["val"] = lines[i][j];
                                dt_ts.Rows.Add(dr);
                            }
                        }
                    }

                    if (ROW_H == -1 && lines[i][0] == "FundId")
                    {
                        ROW_H = i;
                        for (int j = 1; j < lines[ROW_H - 1].Length; j++)
                        {
                            if (COL_H == -1 && lines[ROW_H - 1][j] != "")
                                COL_H = j;
                        }
                    }
                }

                Console.WriteLine(sw.Elapsed + file + ": 파싱 완료");

                //이하 UPSERT
                //임시 테이블 CREATE
                SqlCommand oSqlCommand = new SqlCommand("SELECT * INTO " + input_info[1] + "_temp FROM " + input_info[1] + " WHERE 0 = 1", con);
                oSqlCommand.CommandType = CommandType.Text;
                oSqlCommand.CommandTimeout = 0;
                con.Open();
                oSqlCommand.ExecuteNonQuery();
                con.Close();

                //임시 테이블에 db 업데이트
                SqlBulkCopy bulkCopy = new SqlBulkCopy(input_info[0], SqlBulkCopyOptions.TableLock);

                bulkCopy.DestinationTableName = "dbo." + input_info[1] + "_temp";
                bulkCopy.WriteToServer(dt_ts);

                //임시 테이블에서 오리지널로 UPSERT (기존 것들 val UPDATE & 기존에 없는 행 INSERT)
                oSqlCommand.CommandText = "MERGE INTO [GLOBAL].[dbo].[" + input_info[1] + "] AS Target " +
                                      "USING [GLOBAL].[dbo].[" + input_info[1] + "_temp] AS Source " +
                                      "ON Target.ddt = Source.ddt " +
                                      "AND Target.id = Source.id " +
                                      "AND Target.acct = Source.acct " +
                                      "WHEN MATCHED THEN " +
                                      "UPDATE SET Target.val=Source.val " +
                                      "WHEN NOT MATCHED THEN " +
                                      "INSERT (";
                foreach (DataColumn column in dt_ts.Columns)
                    oSqlCommand.CommandText += column.ColumnName + ",";
                oSqlCommand.CommandText = oSqlCommand.CommandText.TrimEnd(new char[] { ',', '\n' });
                oSqlCommand.CommandText += ") VALUES (";
                foreach (DataColumn column in dt_ts.Columns)
                    oSqlCommand.CommandText += "Source." + column.ColumnName + ",";
                oSqlCommand.CommandText = oSqlCommand.CommandText.TrimEnd(new char[] { ',', '\n' });
                oSqlCommand.CommandText += ");";

                con.Open();
                oSqlCommand.ExecuteNonQuery();

                //임시 테이블 제거
                oSqlCommand.CommandText = "DROP TABLE " + input_info[1] + "_temp";
                oSqlCommand.ExecuteNonQuery();
                con.Close();

                Console.WriteLine(sw.Elapsed + ": 임시 테이블 생성 완료");


                //#CP#

                //이하 UPSERT2
                //임시 테이블 CREATE
                oSqlCommand = new SqlCommand("SELECT * INTO " + input_info[2] + "_temp FROM " + input_info[2] + " WHERE 0 = 1", con);
                oSqlCommand.CommandType = CommandType.Text;
                oSqlCommand.CommandTimeout = 0;
                con.Open();
                oSqlCommand.ExecuteNonQuery();
                con.Close();

                //임시 테이블에 db 업데이트
                bulkCopy = new SqlBulkCopy(input_info[0], SqlBulkCopyOptions.TableLock);

                bulkCopy.DestinationTableName = "dbo." + input_info[2] + "_temp";
                bulkCopy.WriteToServer(dt_st);

                //임시 테이블에서 오리지널로 UPSERT (기존 것들 val UPDATE & 기존에 없는 행 INSERT)
                oSqlCommand.CommandText = "MERGE INTO " + input_info[2] + " AS Target " +
                                      "USING " + input_info[2] + "_temp AS Source " +
                                      //"ON Target.ddt = Source.ddt " +
                                      "ON Target.id = Source.id " +
                                      "AND Target.acct = Source.acct " +
                                      "WHEN MATCHED THEN " +
                                      "UPDATE SET Target.val=Source.val " +
                                      "WHEN NOT MATCHED THEN " +
                                      "INSERT (";
                foreach (DataColumn column in dt_st.Columns)
                    oSqlCommand.CommandText += column.ColumnName + ",";
                oSqlCommand.CommandText = oSqlCommand.CommandText.TrimEnd(new char[] { ',', '\n' });
                oSqlCommand.CommandText += ") VALUES (";
                foreach (DataColumn column in dt_st.Columns)
                    oSqlCommand.CommandText += "Source." + column.ColumnName + ",";
                oSqlCommand.CommandText = oSqlCommand.CommandText.TrimEnd(new char[] { ',', '\n' });
                oSqlCommand.CommandText += ");";

                con.Open();
                oSqlCommand.ExecuteNonQuery();

                //임시 테이블 제거
                oSqlCommand.CommandText = "DROP TABLE " + input_info[2] + "_temp";
                oSqlCommand.ExecuteNonQuery();
                con.Close();

                Console.WriteLine(sw.Elapsed + ": UPSERT 완료");


            }
            sw.Stop();
        }
    }
}
