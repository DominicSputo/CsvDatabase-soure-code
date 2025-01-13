using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using CsvDatabase;
using System.Data.SqlClient;
//using Oracle.ManagedDataAccess.Client;
using System.Diagnostics;

namespace TestWindowFormCsvDatabase
{
    public partial class frmMain : Form
    {
        public frmMain()
        {
            InitializeComponent();
        }

        private void frmMain_Load(object sender, EventArgs e)
        {            
            CsvCommand.Truncate(System.IO.Path.GetFullPath("CsvDatabase_Status_Time.txt"), 0);
            CsvCommand.Truncate(System.IO.Path.GetFullPath("CsvDatabase_1000000_ABC_1.txt"), 0);            
            object obj = "TestInsert100CsvDatabase()";
            lsbCsv.Items.Add(obj);
            obj = "TestUpdate100CsvDatabase()";
            lsbCsv.Items.Add(obj);
            obj = "TestBulkInsert1MillionCsvDatabase()";
            lsbCsv.Items.Add(obj);
            obj = "CsvDataReader1Million()";
            lsbCsv.Items.Add(obj);
            obj = "CsvDataReaderToList1Million()";
            lsbCsv.Items.Add(obj);
            obj = "CsvReaderGetRecordsList1Million()";
            lsbCsv.Items.Add(obj);
            obj = "CsvDataTableGetDataTableCSV1Million()";
            lsbCsv.Items.Add(obj);
            obj = "CsvDataTableGetDataTable1Million()";
            lsbCsv.Items.Add(obj);
        }


        public static void TestUpdate100CsvDatabase(string sID)
        {
            for (int i = 1; i <= 100; i++)
            {   
                CsvDatabase.CsvCommand.Update(new string[] { sID }, "A", new string[] { Guid.NewGuid().ToString(), Guid.NewGuid().ToString() },
                    new string[] { "B", "C" }, System.IO.Path.GetFullPath("CsvDatabase_1000000_ABC_1.txt"));
            }
        }

        public static void TestInsert100CsvDatabase()
        {
            for (int i = 1; i <= 100; i++)
            {
                CsvDatabase.CsvCommand.Insert(new string[] { Guid.NewGuid().ToString(), Guid.NewGuid().ToString(), Guid.NewGuid().ToString() },
                    new string[] { "A", "B", "C" }, System.IO.Path.GetFullPath("CsvDatabase_1000000_ABC_1.txt"));
            }
        }


        public static void TestBulkInsert1MillionCsvDatabase()
        {
            List<CSV> lstCsv = new List<CSV>();
            for (int i = 1; i <= 1000000; i++)
            {
                lstCsv.Add(new CSV() { A = Guid.NewGuid().ToString(), B = Guid.NewGuid().ToString(), C = Guid.NewGuid().ToString() });
            }
            CsvDatabase.CsvBulk.Insert(lstCsv, System.IO.Path.GetFullPath("CsvDatabase_1000000_ABC_1.txt"));
        }

        static void CsvDataReader1Million()
        {
            CsvDatabase.CsvDataReader dtr = new CsvDataReader(System.IO.Path.GetFullPath("CsvDatabase_1000000_ABC_1.txt"));
            while (dtr.Read())
            {
                string s = dtr[0] + "";
            }
            dtr.Close();
        }

        static void CsvDataReaderToList1Million()
        {
            List<CSV> lstCsv = new List<CSV>();
            CsvDatabase.CsvDataReader dtr = new CsvDataReader(System.IO.Path.GetFullPath("CsvDatabase_1000000_ABC_1.txt"));
            while (dtr.Read())
            {
                lstCsv.Add(new CSV() { A = dtr["A"] + "", B = dtr["B"] + "", C = dtr["C"] + "" });
            }
            dtr.Close();
        }

        static void CsvReaderGetRecordsList1Million()
        {
            List<CSV> lstCsv = CsvReader.GetRecords<CSV>(System.IO.Path.GetFullPath("CsvDatabase_1000000_ABC_1.txt")).ToList();
        }

        static void CsvDataTableGetDataTableCSV1Million()
        {
            DataTable dtblCsv = CsvDataTable.GetDataTable<CSV>(System.IO.Path.GetFullPath("CsvDatabase_1000000_ABC_1.txt"));
        }

        static void CsvDataTableGetDataTable1Million()
        {
            DataTable dtblCsv = CsvDataTable.GetDataTable(System.IO.Path.GetFullPath("CsvDatabase_1000000_ABC_1.txt"));
        }

        public class CSV
        {
            public string A { get; set; }
            public string B { get; set; }
            public string C { get; set; }
        }

        private void lsbCsv_SelectedIndexChanged(object sender, EventArgs e)
        {
            string sCsvId = "";
            if ("TestInsert100CsvDatabase()TestBulkInsert1MillionCsvDatabase()".Contains(lsbCsv.SelectedItem.ToString()))
            {
                //CsvCommand.Truncate(System.IO.Path.GetFullPath("CsvDatabase_1000000_ABC_1.txt"));
            }
            else if ("TestUpdate100CsvDatabase()".Contains(lsbCsv.SelectedItem.ToString()))
            {
                CsvCommand.Truncate(System.IO.Path.GetFullPath("CsvDatabase_1000000_ABC_1.txt"));
                TestInsert100CsvDatabase();
                CsvDataReader dtr = new CsvDataReader(System.IO.Path.GetFullPath("CsvDatabase_1000000_ABC_1.txt"));
                while (dtr.Read())
                {
                    sCsvId = dtr["A"] + "";
                    break;
                }
                dtr.Close();
            }
            else if ("CsvDataReader1Million()CsvDataReaderToList1Million()CsvReaderGetRecordsList1Million()CsvDataTableGetDataTableCSV1Million()CsvDataTableGetDataTable1Million()".Contains(lsbCsv.SelectedItem.ToString()))
            {
                CsvDataReader dtr = new CsvDataReader(System.IO.Path.GetFullPath("CsvDatabase_1000000_ABC_1.txt"));
                int iCount = 0;
                while (dtr.Read())
                {
                    iCount = iCount + 1;
                }
                dtr.Close();
                if (iCount != 1000000)
                {
                    CsvCommand.Truncate(System.IO.Path.GetFullPath("CsvDatabase_1000000_ABC_1.txt"));
                    TestBulkInsert1MillionCsvDatabase();
                }
            }
            Stopwatch sw = new Stopwatch();
            sw.Start();
            if ("TestInsert100CsvDatabase()" == lsbCsv.SelectedItem.ToString())
            {
                TestInsert100CsvDatabase();
            }
            else if ("TestUpdate100CsvDatabase()" == lsbCsv.SelectedItem.ToString())
            {
                TestUpdate100CsvDatabase(sCsvId);
            }            
            else if ("TestBulkInsert1MillionCsvDatabase()" == lsbCsv.SelectedItem.ToString())
            {
                TestBulkInsert1MillionCsvDatabase();
            }
            else if ("CsvDataReader1Million()" == lsbCsv.SelectedItem.ToString())
            {
                CsvDataReader1Million();
            }
            else if ("CsvDataReaderToList1Million()" == lsbCsv.SelectedItem.ToString())
            {
                CsvDataReaderToList1Million();
            }
            else if ("CsvReaderGetRecordsList1Million()" == lsbCsv.SelectedItem.ToString())
            {
                CsvReaderGetRecordsList1Million();
            }
            else if ("CsvDataTableGetDataTableCSV1Million()" == lsbCsv.SelectedItem.ToString())
            {
                CsvDataTableGetDataTableCSV1Million();
            }
            else if ("CsvDataTableGetDataTable1Million()" == lsbCsv.SelectedItem.ToString())
            {
                CsvDataTableGetDataTable1Million();
            }
            sw.Stop();
            string sTotalSeconds = sw.Elapsed.TotalSeconds.ToString();
            int iRecordCount = 0;
            try
            {

                CsvDataReader dtrRecord = new CsvDataReader(System.IO.Path.GetFullPath("CsvDatabase_1000000_ABC_1.txt"));
                while (dtrRecord.Read())
                {
                    iRecordCount = iRecordCount + 1;
                }
                dtrRecord.Close();
            }
            catch
            {
                
            }
 
            CsvCommand.Insert(new string[] { lsbCsv.SelectedItem.ToString(), sTotalSeconds, iRecordCount.ToString() }, new string[] { "Selected_Item", "Total_Seconds", "Record_Count" }, System.IO.Path.GetFullPath("CsvDatabase_Status_Time.txt"));
            lblStatus.Text = lblStatus.Text + "\n" + lsbCsv.SelectedItem + "=" + sTotalSeconds;
            dgvCsv.DataSource = CsvDatabase.CsvReader.GetRecords<Status>(System.IO.Path.GetFullPath("CsvDatabase_Status_Time.txt"));  //new List<Status>() { new Status() { Selected_Item = lsbCsv.SelectedItem.ToString(), Total_Seconds = sTotalSeconds } };
        }

        private void btnRecordCount_Click(object sender, EventArgs e)
        {
            int iRecordCount = 0;
            try
            {
                CsvDataReader dtrRecord = new CsvDataReader(System.IO.Path.GetFullPath("CsvDatabase_1000000_ABC_1.txt"));
                while (dtrRecord.Read())
                {
                    iRecordCount = iRecordCount + 1;
                }
                dtrRecord.Close();
            }
            catch
            {

            }

            CsvCommand.Insert(new string[] { "", "", iRecordCount.ToString() }, new string[] { "Selected_Item", "Total_Seconds", "Record_Count" }, System.IO.Path.GetFullPath("CsvDatabase_Status_Time.txt"));
            dgvCsv.DataSource = CsvDatabase.CsvReader.GetRecords<Status>(System.IO.Path.GetFullPath("CsvDatabase_Status_Time.txt"));  //new List<Status>() { new Status() { Selected_Item = lsbCsv.SelectedItem.ToString(), Total_Seconds = sTotalSeconds } };

        }
    }


    public class Status
    {
        public string Selected_Item { get; set; }
        public string Total_Seconds { get; set; }
        public string Record_Count { get; set; }
    }
}
