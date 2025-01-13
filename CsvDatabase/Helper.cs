using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace CsvDatabase
{
    public class Helper
    {
        /// <summary>
        /// Used to Check for AbandonedTempFiles to recover from like a Redo Log to rollback uncompleted transactions.
        /// </summary>
        /// <param name="CSV_File">File and Path of CSV File</param>
        /// <param name="CSV_TempFile">Temp File and Path of CSV File</param>
        /// <param name="CSV_TempFile2">Temp File 2 and Path of CSV File</param>
        /// <returns>Returns True if CSV File Not Found</returns>
        public static bool CheckAbandonedTempFiles(string CSV_File, string CSV_TempFile, string CSV_TempFile2)
        {            
            DateTime dtExitNoFileFound = DateTime.Now;
            while (true)
            {
                try
                {
                    if (File.Exists(CSV_File) == true && File.Exists(CSV_TempFile) == true)
                    {
                        try
                        {
                            if (File.Exists(CSV_File) == true && File.Exists(CSV_TempFile) == true)
                            {
                                TimeSpan tsInterval = DateTime.Now - dtExitNoFileFound;
                                if (tsInterval.Seconds >= 10)
                                {
                                    File.Delete(CSV_File);
                                    File.Move(CSV_TempFile, CSV_File);
                                }
                            }
                        }
                        catch
                        {

                        }
                        return false;
                    }
                    else if (File.Exists(CSV_File) == true)
                    {
                        return false;
                    }
                    if (File.Exists(CSV_TempFile) == true && File.Exists(CSV_File) == false)
                    {
                        try
                        {
                            File.Move(CSV_TempFile, CSV_File);
                        }
                        catch
                        {
                        }
                        return false;
                    }
                    if (File.Exists(CSV_File) == false && File.Exists(CSV_TempFile) == false && File.Exists(CSV_TempFile2) == false)
                    {
                        if (File.Exists(CSV_File) == false && File.Exists(CSV_TempFile) == false && File.Exists(CSV_TempFile2) == false)
                        {
                            return true;
                        }
                        return false;
                    }
                }
                catch (Exception ex)
                {

                }
            }
        }

        /// <summary>
        /// Used to Check for AbandonedTempFiles to recover from like a Redo Log to rollback uncompleted transactions.
        /// </summary>
        /// <param name="CSV_File">File and Path of CSV File</param>
        /// <param name="CSV_TempFile">Temp File and Path of CSV File</param>
        /// <returns>Returns True if CSV File Not Found</returns>
        public static bool CheckAbandonedTempFiles(string CSV_File, string CSV_TempFile)
        {
            string CSV_TempFile2 = System.IO.Path.GetDirectoryName(CSV_File) + "\\" + System.IO.Path.GetFileNameWithoutExtension(CSV_File).ToUpper().Trim() + "_#_TEMPFILE2" + System.IO.Path.GetExtension(CSV_File);
            DateTime dtExitNoFileFound = DateTime.Now;
            while (true)
            {
                try
                {
                    if (File.Exists(CSV_File) == true && File.Exists(CSV_TempFile) == true)
                    {
                        try
                        {
                            if (File.Exists(CSV_File) == true && File.Exists(CSV_TempFile) == true)
                            {
                                TimeSpan tsInterval = DateTime.Now - dtExitNoFileFound;
                                if (tsInterval.Seconds >= 10)
                                {
                                    File.Delete(CSV_File);
                                    File.Move(CSV_TempFile, CSV_File);
                                }
                            }
                        }
                        catch
                        {

                        }
                        return false;
                    }
                    else if (File.Exists(CSV_File) == true)
                    {
                        return false;
                    }
                    if (File.Exists(CSV_TempFile) == true && File.Exists(CSV_File) == false)
                    {
                        try
                        {
                            File.Move(CSV_TempFile, CSV_File);
                        }
                        catch
                        {
                        }
                        return false;
                    }
                    if (File.Exists(CSV_File) == false && File.Exists(CSV_TempFile) == false && File.Exists(CSV_TempFile2) == false)
                    {
                        if (File.Exists(CSV_File) == false && File.Exists(CSV_TempFile) == false && File.Exists(CSV_TempFile2) == false)
                        {
                            return true;
                        }
                        return false;
                    }
                }
                catch (Exception ex)
                {

                }
            }
        }

        /// <summary>
        /// Used to Check for AbandonedTempFiles to recover from like a Redo Log to rollback uncompleted transactions.
        /// </summary>
        /// <param name="CSV_File">File and Path of CSV File</param>
        /// <returns>Returns True if CSV File Not Found</returns>
        public static bool CheckAbandonedTempFiles(string CSV_File)
        {
            string CSV_TempFile = System.IO.Path.GetDirectoryName(CSV_File) + "\\" + System.IO.Path.GetFileNameWithoutExtension(CSV_File).ToUpper().Trim() + "_#_TEMPFILE" + System.IO.Path.GetExtension(CSV_File);
            string CSV_TempFile2 = System.IO.Path.GetDirectoryName(CSV_File) + "\\" + System.IO.Path.GetFileNameWithoutExtension(CSV_File).ToUpper().Trim() + "_#_TEMPFILE2" + System.IO.Path.GetExtension(CSV_File);
            DateTime dtExitNoFileFound = DateTime.Now;
            while (true)
            {
                try
                {
                    if (File.Exists(CSV_File) == true && File.Exists(CSV_TempFile) == true)
                    {
                        try
                        {
                            if (File.Exists(CSV_File) == true && File.Exists(CSV_TempFile) == true)
                            {
                                TimeSpan tsInterval = DateTime.Now - dtExitNoFileFound;
                                if (tsInterval.Seconds >= 10)
                                {
                                    File.Delete(CSV_File);
                                    File.Move(CSV_TempFile, CSV_File);
                                }
                            }
                        }
                        catch
                        {

                        }
                        return false;
                    }
                    else if (File.Exists(CSV_File) == true)
                    {
                        return false;
                    }
                    if (File.Exists(CSV_TempFile) == true && File.Exists(CSV_File) == false)
                    {
                        try
                        {
                            File.Move(CSV_TempFile, CSV_File);
                        }
                        catch
                        {
                        }
                        return false;
                    }
                    if (File.Exists(CSV_File) == false && File.Exists(CSV_TempFile) == false && File.Exists(CSV_TempFile2) == false)
                    {
                        if (File.Exists(CSV_File) == false && File.Exists(CSV_TempFile) == false && File.Exists(CSV_TempFile2) == false)
                        {
                            return true;
                        }
                        return false;
                    }
                }
                catch (Exception ex)
                {

                }
            }
        }

        /// <summary>
        /// Checks 3000 times to see if CSV File exists
        /// </summary>
        /// <param name="CSV_File">CSV File to check if exist</param>
        /// <returns>Returns False if file does not exist and True if File Exists</returns>
        public static bool CheckCsvFileExist(string CSV_File)
        {
            string CSV_TempFile2 = System.IO.Path.GetDirectoryName(CSV_File) + "\\" + System.IO.Path.GetFileNameWithoutExtension(CSV_File).ToUpper().Trim() + "_#_TEMPFILE2" + System.IO.Path.GetExtension(CSV_File);
            Int16 MaxChecks = 3000;
            for (Int16 iCheckFile = 0; iCheckFile < MaxChecks; iCheckFile++)
            {
                if (System.IO.File.Exists(CSV_File) == false && System.IO.File.Exists(CSV_TempFile2) == false)
                {

                }
                else
                {
                    return true;
                }
            }
            return false;           
        }
    }
}
