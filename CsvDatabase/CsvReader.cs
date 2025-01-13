using System;
using System.IO;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace CsvDatabase
{
    public class CsvReader
    {
        private static bool _bFileNotFound = false;

        /// <summary>
        /// Get an IEnumerable Class from CSV File. Must have CSV Header.
        /// </summary>
        /// <typeparam name="T">The Class with Properties to represent the CSV File Headers</typeparam>
        /// <param name="CSV_File">The CSV FileName and Path that exists</param>
        /// <param name="CSV_Delimiter">The CSV Delimiter used. Default | , but the if Delimiter NOT Found it will search for '\t', '|', ',', '^', ';', ':', '~', '\\', '/', '*', '-' as new Delimiter</param>
        /// <param name="CSV_TimeOut">Number of seconds before searching for CSV File will end with File Not Found. Default 5 seconds.</param>
        /// <returns>Returns IEnumerable of Class Type from CSV File</returns>
        public static IEnumerable<T> GetRecords<T>(string CSV_File, char CSV_Delimiter = '|', int CSV_TimeOut = 5)
        {
            string CSV_TempFile = System.IO.Path.GetDirectoryName(CSV_File) + "\\" + System.IO.Path.GetFileNameWithoutExtension(CSV_File).ToUpper().Trim() + "_#_TEMPFILE" + System.IO.Path.GetExtension(CSV_File);
            _bFileNotFound = Helper.CheckAbandonedTempFiles(CSV_File, CSV_TempFile);
            if (_bFileNotFound)
            {
                return new List<T>();
            }
            var lstCSV = new List<T>();
            FileStream fs = null;
            StreamWriter sw = null;
            StreamReader sr = null;
            bool bFileLocked = true;
            string[] sarHeaderRow = null;
            bool bHeader = true;
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
                        if (!bHeader)
                        { //load rows
                            string[] sarRow = sr.ReadLine().Split(CSV_Delimiter);
                            var item = Activator.CreateInstance<T>();
                            foreach (var property in typeof(T).GetProperties())
                            {
                                for (int i = 0; i < iHeaderRowLength; i++)
                                {
                                    if (property.Name.ToUpper() == sarHeaderRow[i].ToUpper())
                                    {
                                        Type convertTo = Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType;
                                        property.SetValue(item, ConvertTo(sarRow[i], convertTo), null);
                                        break;
                                    }
                                }
                            }
                            lstCSV.Add(item);
                        }
                        else
                        {   //load header
                            string sHeader = sr.ReadLine();
                            FindDelimiter(sHeader, ref CSV_Delimiter);
                            sarHeaderRow = sHeader.Split(CSV_Delimiter);
                            iHeaderRowLength = sarHeaderRow.Length;
                            bHeader = false;
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
                    bFileLocked = true;
                    bHeader = true;
                }
                finally
                {
                    if (sw != null)
                    {
                        sw.Dispose();
                    }
                    if (fs != null)
                    {
                        fs.Dispose();
                    }
                }
            }
            return lstCSV;
        }


        /// <summary>
        /// Get an IEnumerable Class from CSV File.
        /// </summary>
        /// <typeparam name="T">The Class with Properties to represent the CSV File Headers</typeparam>
        /// <param name="CSV_File">The CSV FileName and Path that exists</param>
        /// <param name="CSV_Filter">Filter the CSV File using HEADER = VALUE, HEADER IN VALUE1 | VALUE2 | Etc, or HEADER LIKE VALUE</param>
        /// <param name="CSV_Delimiter">The CSV Delimiter used. Default | , but the if Delimiter NOT Found it will search for '\t', '|', ',', '^', ';', ':', '~', '\\', '/', '*', '-' as new Delimiter</param>
        /// <param name="bFirstRowHeader">Specify the csv got a header in first row or not. Default is true and if argument is false then auto header 'ROW_xx will be used as per the order of columns.</param>
        /// <param name="CSV_TimeOut">Number of seconds before searching for CSV File will end with File Not Found. Default 5 seconds.</param>
        /// <returns>Returns IEnumerable of Class Type from CSV File</returns>
        public static IEnumerable<T> GetRecords<T>(string CSV_File, string CSV_Filter, char CSV_Delimiter = '|', bool bFirstRowHeader = true, int CSV_TimeOut = 5)
        {
            string CSV_TempFile = System.IO.Path.GetDirectoryName(CSV_File) + "\\" + System.IO.Path.GetFileNameWithoutExtension(CSV_File).ToUpper().Trim() + "_#_TEMPFILE" + System.IO.Path.GetExtension(CSV_File);
            _bFileNotFound = Helper.CheckAbandonedTempFiles(CSV_File, CSV_TempFile);
            if (_bFileNotFound)
            {
                return new List<T>();
            }
            var lstCSV = new List<T>();
            int iHeaderRowLength = 0;

            CsvDataReader dtr = new CsvDatabase.CsvDataReader(CSV_File, CSV_Filter, CSV_Delimiter, bFirstRowHeader, CSV_TimeOut);
            iHeaderRowLength = dtr.FieldCount;
            while (dtr.Read())
            {
                var item = Activator.CreateInstance<T>();
                foreach (var property in typeof(T).GetProperties())
                {
                    for (int i = 0; i < iHeaderRowLength; i++)
                    {
                        if (property.Name.ToUpper() == dtr.GetName(i).ToUpper())
                        {
                            Type convertTo = Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType;
                            property.SetValue(item, ConvertTo(dtr[i] + "", convertTo), null);
                            break;
                        }
                    }
                }
                lstCSV.Add(item);
            }
            dtr.Close();
            return lstCSV;
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
    }
}