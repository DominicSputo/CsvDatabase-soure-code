using System;
using System.IO;
using System.Data;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Xml;

namespace CsvDatabase
{
    public class CsvDataTable
    {
        private static bool _bFileNotFound = false;

        /// <summary>
        /// CSV Property Name and DataType of class.
        /// </summary>
        private class Csv
        {
            /// <summary>
            /// Property Name
            /// </summary>
            public string Column { get; set; }
            /// <summary>
            /// DataType
            /// </summary>
            public Type DataType { get; set; }
        }

        /// <summary>
        /// Checks value and Type to return a safe value.
        /// </summary>
        /// <param name="sValue">Value of property</param>
        /// <param name="tType">Value Data Type</param>
        /// <returns>Returns safe value</returns>
        private static object ConvertTo(string sValue, Type tType)
        {
            if (tType.FullName != "System.String")
            {
                if (sValue != "")
                {
                    return Convert.ChangeType(sValue, tType);
                }
                else if (tType.FullName == "System.Decimal")
                {
                    return Decimal.Zero;
                }
                else
                {
                    return 0;
                }
            }
            return sValue;
        }

        /// <summary>
        /// Get DataTable from CSV. Must have CSV Header.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="CSV_File">The CSV FileName and Path that exists</param>
        /// <param name="CSV_Delimiter">The CSV Delimiter used. Default | , but the if Delimiter NOT Found it will search for '\t', '|', ',', '^', ';', ':', '~', '\\', '/', '*', '-' as new Delimiter</param>
        /// <param name="CSV_TimeOut">Number of Seconds before searching for CSV File will end with File Not Found. Default 5 seconds.</param>
        /// <returns>DataTable of CSV based upon Class</returns>
        public static DataTable GetDataTable<T>(string CSV_File, char CSV_Delimiter = '|', int CSV_TimeOut = 5)
        {
            string CSV_TempFile = System.IO.Path.GetDirectoryName(CSV_File) + "\\" + System.IO.Path.GetFileNameWithoutExtension(CSV_File).ToUpper().Trim() + "_#_TEMPFILE" + System.IO.Path.GetExtension(CSV_File);
            _bFileNotFound = Helper.CheckAbandonedTempFiles(CSV_File, CSV_TempFile);
            if (_bFileNotFound)
            {
                return new DataTable();
            }
            DataTable dtbl = new DataTable();
            FileStream fs = null;
            StreamReader sr = null;
            DataRow dr = null;

            List<Csv> lstCsv = new List<Csv>();
            var item = Activator.CreateInstance<T>();
            foreach (var property in typeof(T).GetProperties())
            {
                Type tDataType = Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType;
                lstCsv.Add(new Csv() { Column = property.Name.ToUpper(), DataType = tDataType });
                dtbl.Columns.Add(property.Name.ToUpper(), tDataType);
            }

            int iCsvLength = lstCsv.Count();
            bool bFileLocked = true;
            bool bHeader = true;
            string[] sarHeader = null;
            int iHeaderLength = 0;
            bool bError = false;
            DateTime dtStart = DateTime.Now;
            bool bCSVFound = false;
            while (bFileLocked)
            {
                try
                {
                    if (File.Exists(CSV_File) == true || File.Exists(CSV_TempFile) == true)
                    {
                        bCSVFound = true;
                    }
                    fs = new FileStream(CSV_File, FileMode.Open, FileAccess.Read, FileShare.Read);
                    sr = new StreamReader(fs);
                    while (!sr.EndOfStream)
                    {
                        if (!bHeader)
                        {
                            string[] sarRow = sr.ReadLine().Split(CSV_Delimiter);

                            for (int i = 0; i < iCsvLength; i++)
                            {
                                if (i == 0)
                                {
                                    dr = dtbl.NewRow();
                                    for (int x = 0; x < sarRow.Length; x++)
                                    {
                                        if (lstCsv[i].Column == sarHeader[x])
                                        {
                                            dr[i] = ConvertTo(sarRow[x], lstCsv[i].DataType);
                                        }
                                    }
                                }
                                else if (i == (iCsvLength - 1))
                                {
                                    for (int x = 0; x < sarRow.Length; x++)
                                    {
                                        if (lstCsv[i].Column == sarHeader[x])
                                        {
                                            dr[i] = ConvertTo(sarRow[x], lstCsv[i].DataType);
                                            dtbl.Rows.Add(dr);
                                        }
                                    }
                                }
                                else
                                {
                                    for (int x = 0; x < sarRow.Length; x++)
                                    {
                                        if (lstCsv[i].Column == sarHeader[x])
                                        {
                                            dr[i] = ConvertTo(sarRow[x], lstCsv[i].DataType);
                                        }
                                    }
                                }
                            }
                        }
                        else //Header
                        {
                            string sHeader = sr.ReadLine().ToUpper();
                            FindDelimiter(sHeader, ref CSV_Delimiter);
                            sarHeader = sHeader.Split(CSV_Delimiter);
                            bHeader = false;
                            iHeaderLength = sarHeader.Length;
                            bool bFound = false;
                            for (int i = 0; i < iCsvLength; i++)
                            {
                                bFound = false;
                                for (int x = 0; x < iHeaderLength; x++)
                                {
                                    if (lstCsv[i].Column == sarHeader[x])
                                    {
                                        bFound = true;
                                    }
                                }
                                if (!bFound)
                                {
                                    bError = true;

                                }
                            }
                        }
                    }
                    bFileLocked = false;
                }
                catch (Exception ex)
                {
                    if (bCSVFound == false) //Checks if CSV Not Found for 10 seconds
                    {
                        TimeSpan tsInterval = DateTime.Now - dtStart;
                        if (tsInterval.Seconds >= CSV_TimeOut)
                        {
                            throw new Exception("CSV File Not Found");
                        }
                    }
                    bHeader = true;
                    bFileLocked = true;
                    if (bError)
                    {
                        bFileLocked = true;
                    }
                }
                finally
                {
                    if (sr != null)
                    {
                        sr.Dispose();
                    }
                    if (fs != null)
                    {
                        fs.Dispose();
                    }
                }
            }
            if (bError)
            {
                throw new Exception("CSV Header does not match IEnumerable.");
            }
            return dtbl;
        }

        private static void FindDelimiter(string sLine, ref char cDelimiter)
        {
            if (sLine.IndexOf(cDelimiter) == -1)
            {
                char[] carDelimiters = new char[] { '\t', '|', ',', '^', ';', '<', '>', ':', '~', '\\', '/', '*', '-' };
                int iDelimitersLength = carDelimiters.Length;
                for (int i = 0; i < iDelimitersLength; i++)
                {
                    if (sLine.IndexOf(carDelimiters[i]) != -1)
                    {
                        cDelimiter = carDelimiters[i];
                        return;
                    }
                }
            }
        }

        /// <summary>
        /// Get DataTable from CSV. Must have CSV Header.
        /// </summary>
        /// <param name="CSV_File">The CSV FileName and Path that exists</param>       
        /// <param name="CSV_Delimiter">The CSV Delimiter used. Default | , but the if Delimiter NOT Found it will search for '\t', '|', ',', '^', ';', ':', '~', '\\', '/', '*', '-' as new Delimiter.</param>
        /// <param name="CSV_TimeOut">Number of Seconds before searching for CSV File will end with File Not Found. Default 5 seconds.</param>
        /// <returns>Returns DataTable of entire CSV. All DataTable Columns are Type String.</returns>
        public static DataTable GetDataTable(string CSV_File, char CSV_Delimiter = '|', int CSV_TimeOut = 5)
        {
            string CSV_TempFile = System.IO.Path.GetDirectoryName(CSV_File) + "\\" + System.IO.Path.GetFileNameWithoutExtension(CSV_File).ToUpper().Trim() + "_#_TEMPFILE" + System.IO.Path.GetExtension(CSV_File);
            _bFileNotFound = Helper.CheckAbandonedTempFiles(CSV_File, CSV_TempFile);
            if (_bFileNotFound)
            {
                return new DataTable();
            }
            DataTable dtbl = new DataTable();
            FileStream fs = null;
            StreamReader sr = null;
            bool bFileLocked = true;
            string[] sarHeaderRow = null;
            bool bHeader = true;
            DataRow dr = null;
            int iHeaderRowLength = 0;
            DateTime dtStart = DateTime.Now;
            bool bCSVFound = false;
            while (bFileLocked)
            {
                try
                {
                    if (File.Exists(CSV_File) == true || File.Exists(CSV_TempFile) == true)
                    {
                        bCSVFound = true;
                    }
                    string[] sarLine = null;
                    fs = new FileStream(CSV_File, FileMode.Open, FileAccess.Read, FileShare.Read);
                    sr = new StreamReader(fs);
                    while (!sr.EndOfStream)
                    {
                        if (!bHeader)
                        {
                            sarLine = sr.ReadLine().Split(CSV_Delimiter);
                            for (int i = 0; i < iHeaderRowLength; i++)
                            {
                                if (i == 0)
                                {
                                    dr = dtbl.NewRow();
                                    dr[i] = sarLine[i] + "";
                                }
                                else if (i == (iHeaderRowLength - 1))
                                {
                                    dr[i] = sarLine[i] + "";
                                    dtbl.Rows.Add(dr);
                                }
                                else
                                {
                                    dr[i] = sarLine[i] + "";
                                }
                            }
                        }
                        else //Header
                        {
                            string sHeader = sr.ReadLine().ToUpper();
                            FindDelimiter(sHeader, ref CSV_Delimiter);
                            sarHeaderRow = sHeader.Split(CSV_Delimiter);
                            iHeaderRowLength = sarHeaderRow.Length;
                            for (int i = 0; i < iHeaderRowLength; i++)
                            {
                                dtbl.Columns.Add(sarHeaderRow[i], typeof(string));
                            }
                            bHeader = false;
                        }
                    }
                    bFileLocked = false;
                }
                catch
                {
                    if (bCSVFound == false)
                    {
                        TimeSpan tsInterval = DateTime.Now - dtStart;
                        if (tsInterval.Seconds >= CSV_TimeOut)
                        {
                            throw new Exception("CSV File Not Found");
                        }
                    }
                    bHeader = true;
                    bFileLocked = true;
                }
                finally
                {
                    if (sr != null)
                    {
                        sr.Dispose();
                    }
                    if (fs != null)
                    {
                        fs.Dispose();
                    }
                }
            }
            return dtbl;
        }

        /// <summary>
        /// Get DataTable from CSV. Must have CSV Header.
        /// </summary>
        /// <param name="CSV_File">The CSV FileName and Path that exists</param> 
        /// <param name="CSV_Filter">The CSV Filter to use. Example FirstName LIKE Domini | LastName LIKE Sput. FirstName = Dominic. FirstName IN Dominic | Rob | John</param>       
        /// <param name="CSV_Delimiter">The CSV Delimiter used. Default | , but the if Delimiter NOT Found it will search for '\t', '|', ',', '^', ';', ':', '~', '\\', '/', '*', '-' as new Delimiter.</param>
        /// <param name="CSV_TimeOut">Number of Seconds before searching for CSV File will end with File Not Found. Default 5 seconds.</param>
        /// <returns>Returns DataTable of entire CSV. All DataTable Columns are Type String.</returns>
        public static DataTable GetDataTable(string CSV_File, string CSV_Filter, char CSV_Delimiter = '|', int CSV_TimeOut = 5)
        {
            string CSV_TempFile = System.IO.Path.GetDirectoryName(CSV_File) + "\\" + System.IO.Path.GetFileNameWithoutExtension(CSV_File).ToUpper().Trim() + "_#_TEMPFILE" + System.IO.Path.GetExtension(CSV_File);
            _bFileNotFound = Helper.CheckAbandonedTempFiles(CSV_File, CSV_TempFile);
            if (_bFileNotFound)
            {
                return new DataTable();
            };
            DataTable dtbl = new DataTable();
            FileStream fs = null;
            StreamReader sr = null;
            bool bFileLocked = true;
            string[] sarHeaderRow = null;
            //bool bHeader = true;
            DataRow dr = null;
            int iHeaderRowLength = 0;
            DateTime dtStart = DateTime.Now;
            bool bCSVFound = false;
            while (bFileLocked)
            {
                try
                {
                    if (File.Exists(CSV_File) == true || File.Exists(CSV_TempFile) == true)
                    {
                        bCSVFound = true;
                    }
                    fs = new FileStream(CSV_File, FileMode.Open, FileAccess.Read, FileShare.Read);
                    sr = new StreamReader(fs);
                    while (!sr.EndOfStream)
                    {       
                        string sHeader = sr.ReadLine().ToUpper();
                        FindDelimiter(sHeader, ref CSV_Delimiter);
                        sarHeaderRow = sHeader.Split(CSV_Delimiter);
                        iHeaderRowLength = sarHeaderRow.Length;
                        for (int i = 0; i < iHeaderRowLength; i++)
                        {
                            dtbl.Columns.Add(sarHeaderRow[i], typeof(string));
                        }
                        break;
                    }
                    bFileLocked = false;
                }
                catch
                {
                    if (bCSVFound == false)
                    {
                        TimeSpan tsInterval = DateTime.Now - dtStart;
                        if (tsInterval.Seconds >= CSV_TimeOut)
                        {
                            throw new Exception("CSV File Not Found");
                        }
                    }
                    //bHeader = true;
                    bFileLocked = true;
                }
                finally
                {
                    if (sr != null)
                    {
                        sr.Dispose();
                    }
                    if (fs != null)
                    {
                        fs.Dispose();
                    }
                }

            }
            CsvDataReader dtr = new CsvDataReader(CSV_File, CSV_Filter, CSV_Delimiter, true, CSV_TimeOut);
            while (dtr.Read())
            {                
                for (int i = 0; i < iHeaderRowLength; i++)
                {
                    if (iHeaderRowLength == 1)
                    {
                        dr = dtbl.NewRow();
                        dr[i] = dtr[i] + "";
                        dtbl.Rows.Add(dr);
                    }
                    else if (i == 0)
                    {
                        dr = dtbl.NewRow();
                        dr[i] = dtr[i] + "";
                    }
                    else if ((iHeaderRowLength - 1) == i)
                    {
                        dr[i] = dtr[i] + "";
                        dtbl.Rows.Add(dr);
                    }
                    else
                    {
                        dr[i] = dtr[i] + "";
                    }
                }
            }
            return dtbl;
        }

        /// <summary>
        /// Get DataTable from CSV. Must have CSV Header.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="CSV_File">The CSV FileName and Path that exists</param>
        /// <param name="CSV_Filter">The CSV Filter to use. Example FirstName LIKE Domini | LastName LIKE Sput. FirstName = Dominic. FirstName IN Dominic | Rob | John</param>     
        /// <param name="CSV_Delimiter">The CSV Delimiter used. Default | , but the if Delimiter NOT Found it will search for '\t', '|', ',', '^', ';', ':', '~', '\\', '/', '*', '-' as new Delimiter</param>
        /// <param name="CSV_TimeOut">Number of Seconds before searching for CSV File will end with File Not Found. Default 5 seconds.</param>
        /// <returns>DataTable of CSV based upon Class</returns>
        public static DataTable GetDataTable<T>(string CSV_File, string CSV_Filter, char CSV_Delimiter = '|', int CSV_TimeOut = 5)
        {
            string CSV_TempFile = System.IO.Path.GetDirectoryName(CSV_File) + "\\" + System.IO.Path.GetFileNameWithoutExtension(CSV_File).ToUpper().Trim() + "_#_TEMPFILE" + System.IO.Path.GetExtension(CSV_File);
            _bFileNotFound = Helper.CheckAbandonedTempFiles(CSV_File, CSV_TempFile);
            if (_bFileNotFound)
            {
                DataTable dtblEmpty = new DataTable();
                foreach (var property in typeof(T).GetProperties())
                {
                    Type tDataType = Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType;                    
                    dtblEmpty.Columns.Add(property.Name.ToUpper(), tDataType);
                }
                return dtblEmpty;
            }
            DataTable dtbl = new DataTable();           
            DataRow dr = null;
            List<Csv> lstCsv = new List<Csv>();
            var item = Activator.CreateInstance<T>();
            foreach (var property in typeof(T).GetProperties())
            {
                Type tDataType = Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType;
                lstCsv.Add(new Csv() { Column = property.Name.ToUpper(), DataType = tDataType });
                dtbl.Columns.Add(property.Name.ToUpper(), tDataType);
            }
            int iCsvLength = lstCsv.Count();            
            CsvDataReader dtr = new CsvDataReader(CSV_File, CSV_Filter, CSV_Delimiter, true, CSV_TimeOut);
            while (dtr.Read())
            {  
                for (int i = 0; i < iCsvLength; i++)
                {
                    if (i == 0)
                    {
                        dr = dtbl.NewRow();
                        dr[lstCsv[i].Column] = ConvertTo(dtr[lstCsv[i].Column] + "", lstCsv[i].DataType);
                    }
                    else if (i == (iCsvLength - 1))
                    {
                        dr[lstCsv[i].Column] = ConvertTo(dtr[lstCsv[i].Column] + "", lstCsv[i].DataType);
                        dtbl.Rows.Add(dr);
                    }
                    else
                    {
                        dr[lstCsv[i].Column] = ConvertTo(dtr[lstCsv[i].Column] + "", lstCsv[i].DataType);
                    }
                }
            }
            return dtbl;
        }

    }
}