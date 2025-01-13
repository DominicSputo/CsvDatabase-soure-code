using System.Diagnostics;
using System.Data;
using CsvDatabase;
using System.Collections.Generic;
using System.Linq;
using System;
using System.IO;
using System.Text;
using System.Reflection;
using System.Threading.Tasks;
using System.Data.SqlClient;
//using Oracle.ManagedDataAccess.Client;
//Creator Dominic Sputo
namespace TestCsvDatabase
{
    class Program
    {
        public class Customer
        {
            public string ID { get; set; }
            public string FIRSTNAME { get; set; }
            public string LASTNAME { get; set; }
            public string MIDDLENAME { get; set; }
            public string PHONENUMBER { get; set; }
        }
        public class OrderLines
        {
            public string OrderNum { get; set; }
            public string CustomerID { get; set; }
            public int Quantity { get; set; }
            public string ProductID { get; set; }
        }

        public static List<Customer> Test()
        {
            List<Customer> lstCustomer22 = new List<Customer>();
            try
            {
                for (long i = 0; i < 1000000000; i++)
                {
                    lstCustomer22.Add(new Customer() { ID = "100.99", FIRSTNAME = Guid.NewGuid().ToString(), LASTNAME = Guid.NewGuid().ToString(), MIDDLENAME = Guid.NewGuid().ToString(), PHONENUMBER = Guid.NewGuid().ToString() });

                } // 528mb 3.7251577 //out of memory i = 7,705,400 //ID = "100.99" 417mb  3.0529718
            }
            catch
            {
                lstCustomer22.Clear();
                //lstCustomer22 = null;
                //GC.Collect();
            }


            for (long i = 0; i < 1000000; i++)
            {
                lstCustomer22.Add(new Customer() { ID = "100.99", FIRSTNAME = Guid.NewGuid().ToString(), LASTNAME = Guid.NewGuid().ToString(), MIDDLENAME = Guid.NewGuid().ToString(), PHONENUMBER = Guid.NewGuid().ToString() });

            } // 528mb 3.7251577 //out of memory i = 7,705,400 //ID = "100.99" 417mb  3.0529718
            return lstCustomer22.OrderBy(e => e.FIRSTNAME).ToList(); 
        }

        static void Main(string[] args)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();


            //bool bbb = CsvDatabase.Helper.CheckCsvFileExist("C:\\TEST.txt");
            //sw.Stop();
            //sw.Elapsed.TotalSeconds.ToString();

            //CsvCommand.Truncate("C:\\Users\\Dominic Sputo\\Documents\\Visual Studio 2017\\Projects\\CSVDatabase\\TestCsvDatabase\\bin\\Debug\\TEST_223416.txt");
            //CsvCommand.Insert(new string[] { "TEST" }, new string[] { "Header" }, "C:\\Users\\Dominic Sputo\\Documents\\Visual Studio 2017\\Projects\\CSVDatabase\\TestCsvDatabase\\bin\\Debug\\TEST_223416.txt");
            //for (long i = 0; i < 20000000; i++)
            //{
            //    if (CsvDatabase.Helper.CheckCsvFileExist("C:\\Users\\Dominic Sputo\\Documents\\Visual Studio 2017\\Projects\\CSVDatabase\\TestCsvDatabase\\bin\\Debug\\TEST_223416.txt") == false)
            //    {
            //        CsvCommand.Insert(new string[] { "TEST" }, new string[] { "Header" }, "C:\\Users\\Dominic Sputo\\Documents\\Visual Studio 2017\\Projects\\CSVDatabase\\TestCsvDatabase\\bin\\Debug\\TEST_223416.txt");
            //    }
            //} // 528mb

            //var lstCustomer55 = Test(); //450mb 9.3426432   445mb 9.3474976
            //CsvCommand.Truncate("C:\\Users\\Dominic Sputo\\Documents\\Visual Studio 2017\\Projects\\CSVDatabase\\TestCsvDatabase\\bin\\Debug\\TEST_223416.txt");
            //List<Customer> lstCustomer22 = new List<Customer>();
            //for (long i = 0; i < 2000000; i++)
            //{
            //    lstCustomer22.Add(new Customer() { ID = "100.99", FIRSTNAME = Guid.NewGuid().ToString(), LASTNAME = Guid.NewGuid().ToString(), MIDDLENAME = Guid.NewGuid().ToString(), PHONENUMBER = Guid.NewGuid().ToString() });
            //    //CsvCommand.CreateTable(new string[] { "Header" }, "C:\\Users\\Dominic Sputo\\Documents\\Visual Studio 2017\\Projects\\CSVDatabase\\TestCsvDatabase\\bin\\Debug\\TEST_223416.txt");
            //    //CsvCommand.Insert(new string[] { "TEST" }, new string[] { "Header" }, "C:\\Users\\Dominic Sputo\\Documents\\Visual Studio 2017\\Projects\\CSVDatabase\\TestCsvDatabase\\bin\\Debug\\TEST_223416.txt");

            //} // 528mb 3.7251577 //out of memory i = 7,705,400 //ID = "100.99" 417mb  3.0529718

            //foreach (Customer c in lstCustomer22)
            //{
            //    string s = c.FIRSTNAME;
            //} //522MB 3.7969063

            //lstCustomer22 = lstCustomer22.OrderBy(e => e.FIRSTNAME).ToList(); //539mb 9.9842285

            //var lstCustomer33 = lstCustomer22.OrderBy(e => e.FIRSTNAME).ToList(); //532mb  10.0699596

            //var lstCustomer44 = lstCustomer22.OrderBy(e => decimal.Parse(e.ID)).ToList(); //445mb 6.5204104 // decimal.Parse(e.ID) = 449mb 3.6917858

            //CsvBulk.Insert(lstCustomer22, "C:\\Users\\Dominic Sputo\\Documents\\Visual Studio 2017\\Projects\\CSVDatabase\\TestCsvDatabase\\bin\\Debug\\TEST_223416.txt");
            //lstCustomer22.Clear();

            //CsvDataReader dtr22 = new CsvDataReader("C:\\Users\\Dominic Sputo\\Documents\\Visual Studio 2017\\Projects\\CSVDatabase\\TestCsvDatabase\\bin\\Debug\\TEST_223416.txt");
            //while (dtr22.Read())
            //{

            //}
            //dtr22.Close();

            //DataTable dtbl22 = new DataTable();
            //try
            //{

            //    dtbl22.Columns.Add("ID", typeof(string));
            //    dtbl22.Columns.Add("FIRSTNAME", typeof(string));
            //    dtbl22.Columns.Add("LASTNAME", typeof(string));
            //    dtbl22.Columns.Add("MIDDLENAME", typeof(string));
            //    dtbl22.Columns.Add("PHONENUMBER", typeof(string));
            //    DataRow dr22;
            //    for (long i = 0; i < 5000000; i++)
            //    {
            //        dr22 = dtbl22.NewRow();
            //        dr22["ID"] = Guid.NewGuid().ToString();
            //        dr22["FIRSTNAME"] = Guid.NewGuid().ToString();
            //        dr22["LASTNAME"] = Guid.NewGuid().ToString();
            //        dr22["MIDDLENAME"] = Guid.NewGuid().ToString();
            //        dr22["PHONENUMBER"] = Guid.NewGuid().ToString();
            //        dtbl22.Rows.Add(dr22);
            //    } //607mb 5.7296356  //out of memory i = 6,231,873
            //    DataView dv = dtbl22.DefaultView;
            //    //dtbl22.Clear();
            //    dv.Dispose();
            //    foreach (DataRowView drData in dv)
            //    {
            //        string sss = drData[0] + "";
            //    }
            //}
            //catch
            //{
            //    dtbl22.Clear();
            //}
            List<Customer> lstCustomer22 = new List<Customer>();
            for (long i = 0; i < 1000; i++)
            {
                lstCustomer22.Add(new Customer() { ID = "100.99", FIRSTNAME = "Dominic", LASTNAME = "Sputo", MIDDLENAME = Guid.NewGuid().ToString(), PHONENUMBER = Guid.NewGuid().ToString() });
                //CsvCommand.CreateTable(new string[] { "Header" }, "C:\\Users\\Dominic Sputo\\Documents\\Visual Studio 2017\\Projects\\CSVDatabase\\TestCsvDatabase\\bin\\Debug\\TEST_223416.txt");
                //CsvCommand.Insert(new string[] { "TEST" }, new string[] { "Header" }, "C:\\Users\\Dominic Sputo\\Documents\\Visual Studio 2017\\Projects\\CSVDatabase\\TestCsvDatabase\\bin\\Debug\\TEST_223416.txt");

            } // 528mb 3.7251577 //out of memory i = 7,705,400 //ID = "100.99" 417mb  3.0529718
             CsvBulk.Insert(lstCustomer22, "C:\\Users\\Dominic Sputo\\Documents\\Visual Studio 2017\\Projects\\CSVDatabase\\TestCsvDatabase\\bin\\Debug\\Customer.txt");

            CsvDataReader dtr45 = new CsvDatabase.CsvDataReader(System.IO.Path.GetFullPath("Customer.txt"), "firstName liKE DominiC");
            while (dtr45.Read())
            {
                string a = dtr45["FIRSTNAME"] + "";
                string b = dtr45["LASTNAME"] + "";                
            }
            dtr45.Close();

            //foreach (DataRow dr11 in dtbl22.Rows)
            //{
            //    string ss = dr11["FirstName"] + "";
            //} //610mb  6.6013321

            //dtbl22.DefaultView.Sort = "FIRSTNAME ASC"; //752MB 14.0145382

            //sw.Stop();
            //sw.Elapsed.TotalSeconds.ToString();

            //CsvCommand.Insert(new string[] { "TEST" }, new string[] { "Header" }, System.IO.Path.GetFullPath("TEST_223416.txt"));

            CsvCommand.Truncate(System.IO.Path.GetFullPath("OrderLines.txt"));
            //Create DataTable to Bulk Insert
            DataTable dtbl = new DataTable("OrderLines");
            dtbl.Columns.Add("ORDERNUM");
            dtbl.Columns.Add("CustomerId");
            dtbl.Columns.Add("Quantity");
            dtbl.Columns.Add("ProductId");
            DataRow dr;
            //Create OrderLines Records
            dr = dtbl.NewRow();
            dr["OrderNum"] = "900001";
            dr["CustomerID"] = "1000001";
            dr["Quantity"] = "1";
            dr["ProductID"] = "3001";
            dtbl.Rows.Add(dr);

            dr = dtbl.NewRow();
            dr["OrderNum"] = "900002";
            dr["CustomerID"] = "1000001";
            dr["Quantity"] = "2";
            dr["ProductID"] = "3001";
            dtbl.Rows.Add(dr);

            dr = dtbl.NewRow();
            dr["OrderNum"] = "900003";
            dr["CustomerID"] = "1000002";
            dr["Quantity"] = "3";
            dr["ProductID"] = "3002";
            dtbl.Rows.Add(dr);

            dr = dtbl.NewRow();
            dr["OrderNum"] = "900003";
            dr["CustomerID"] = "1000002";
            dr["Quantity"] = "1";
            dr["ProductID"] = "3002";
            dtbl.Rows.Add(dr);

            dr = dtbl.NewRow();
            dr["OrderNum"] = "900004";
            dr["CustomerID"] = "1000003";
            dr["Quantity"] = "1";
            dr["ProductID"] = "3401";
            dtbl.Rows.Add(dr);
            //Bulk Insert DataTable
            CsvBulk.Insert(dtbl, System.IO.Path.GetFullPath("OrderLines.txt"));
            //Page Through CSV OrderLines.txt
            Int64 iTotalPages;
            Int64 iResultCount;
            Int32 iPage_Size = 3;
            Int64 iPage_Number = 0;
            List<OrderLines> lst_OrderLines = CsvPager.GetPage<OrderLines>(iPage_Size, iPage_Number, out iTotalPages, out iResultCount, System.IO.Path.GetFullPath("OrderLines.txt"), "OrderNum LIKE 900003 | CustomerID LIKE 1000001").ToList();
            //Sorted then paged data
            iPage_Size = 5;
            List<OrderLines> lst_OrderLinesSorted = CsvPager.GetPage<OrderLines>(iPage_Size, iPage_Number, out iTotalPages, out iResultCount, System.IO.Path.GetFullPath("OrderLines.txt"), "OrderNum LIKE 900001 | OrderNum LIKE 900002 | OrderNum LIKE 900003 | CustomerID LIKE 1000001", "OrderNum desc, CustomerID desc").ToList();
            


            //Removes all the data from the file but leaves the Header.
            CsvCommand.Truncate(System.IO.Path.GetFullPath("OrderLines.txt"));

            //CsvDataReader dtr8 = new CsvDatabase.CsvDataReader(System.IO.Path.GetFullPath("Customer.txt"), "firstName liKE DominiC", bFirstRowHeader: false);            
            //while (dtr8.Read())
            //{
            //    string a = dtr8["COL_1"] + "";
            //    string b = dtr8["COL_2"] + "";
            //    string c = dtr8["COL_3"] + "";
            //}
            //dtr8.Close();
            
            //Create Class List Where CustomerID IN 1000002 OR 1000001. Not case sensitive. Linq GroupBy, Order By and Having
            List<OrderLines> lstOrderLines = new List<OrderLines>();
            lstOrderLines.Add(new OrderLines() { OrderNum = "900001", CustomerID = "1000001", Quantity = 1, ProductID = "3001" });
            lstOrderLines.Add(new OrderLines() { OrderNum = "900002", CustomerID = "1000001", Quantity = 2, ProductID = "3001" });
            lstOrderLines.Add(new OrderLines() { OrderNum = "900003", CustomerID = "1000002", Quantity = 3, ProductID = "3002" });
            lstOrderLines.Add(new OrderLines() { OrderNum = "900003", CustomerID = "1000002", Quantity = 1, ProductID = "3002" });
            lstOrderLines.Add(new OrderLines() { OrderNum = "900003", CustomerID = "1000002", Quantity = 1, ProductID = "3001" });
            lstOrderLines.Add(new OrderLines() { OrderNum = "900004", CustomerID = "1000003", Quantity = 1, ProductID = "3401" });
            CsvBulk.Insert(lstOrderLines, System.IO.Path.GetFullPath("OrderLines.txt"));
            List<OrderLines> lstOrderLines2 = (CsvReader.GetRecords<OrderLines>(System.IO.Path.GetFullPath("OrderLines.txt"), "CustomerID  IN 1000002|1000001"))                
                .GroupBy(l => new { l.OrderNum, l.ProductID })                   
                .Select(ol => new OrderLines
                {
                    OrderNum = ol.First().OrderNum,
                    CustomerID = ol.First().CustomerID,
                    Quantity = ol.Sum(l => l.Quantity),
                    ProductID = ol.First().ProductID
                })   
                .Where(ol => ol.CustomerID == "1000002" || ol.CustomerID == "1000001")
                .Where(ol => ol.Quantity >= 1) //having
                .OrderByDescending(l => l.CustomerID)
                .ThenBy(l => l.ProductID)
                .ToList();
            
            //Writes a XML File using DataReader WHERE CustomerID = 1000002. Note Overwrites old file.
            CsvDataReader dtr9 = new CsvDataReader(System.IO.Path.GetFullPath("OrderLines.txt"), "CustomerID = 1000002");
            dtr9.WriteXml(System.IO.Path.GetFullPath("OrderLines2.xml"));
            DataSet ds = new DataSet();
            ds.ReadXml(System.IO.Path.GetFullPath("OrderLines2.xml"));

            //Writes a XML File using CSV File WHERE CustomerID IN 1000002 OR 1000001. Note Overwrites old file.
            CsvToXml.WriteXml(System.IO.Path.GetFullPath("OrderLines.txt"), System.IO.Path.GetFullPath("OrderLines.xml"), "CustomerID  IN 1000002|1000001");
            DataSet ds2 = new DataSet();
            ds2.ReadXml(System.IO.Path.GetFullPath("OrderLines.xml"));

            CsvDataReader dtr11 = new CsvDataReader(System.IO.Path.GetFullPath("OrderLines.txt"), "CustomerID = 1000002");
            CsvToXml.WriteXml(dtr11, System.IO.Path.GetFullPath("OrderLines11.xml"));
            
            CsvDataReader dtr12 = new CsvDataReader(System.IO.Path.GetFullPath("OrderLines.txt"), "CustomerID Like 1000002");
            dtr12.WriteXml(System.IO.Path.GetFullPath("OrderLines12.xml"));

            ////SQL Server Data to XML File using Extension Method WriteXml(New_XML_File). Note Overwrites old file.
            //SqlConnection conSQLServer = new SqlConnection(@"Data Source=LAPTOP-ASH6NMMV\SQLEXPRESS;Initial Catalog=MILLIONRECORDS;Integrated Security=True");
            //SqlCommand cmdSQLServer = new SqlCommand("SELECT Top 100 * FROM million", conSQLServer);
            //conSQLServer.Open();
            //SqlDataReader dtrSQLServer = cmdSQLServer.ExecuteReader();
            //dtrSQLServer.WriteXml(System.IO.Path.GetFullPath("Top 100 SQL Server.xml"));
            //conSQLServer.Close();

            ////Import Oracle Data to XML File using Extension Method WriteXml(New_XML_File). Note Overwrites old file.
            //OracleConnection conOracle = new OracleConnection(@"User Id=C##dominic;Password=123456;Data Source=localhost:1521/orcl;Pooling=false;");
            //OracleCommand cmdOracle = new OracleCommand("SELECT * FROM million WHERE rownum <= 100", conOracle);
            //conOracle.Open();
            //OracleDataReader dtrOracle = cmdOracle.ExecuteReader();
            //dtrOracle.WriteXml(System.IO.Path.GetFullPath("Top 100 Oracle.txt"));
            //conOracle.Close();

            //Writes CSV File using a XML File. Use less than 4GB XML Files. Appends data to CSV File if CSV File exist with data
            XmlToCsv.WriteCsv(System.IO.Path.GetFullPath("OrderLines.xml"), System.IO.Path.GetFullPath("OrderLines10.txt"));

            //Writes CSV File using XML File. Use more than 4GB XML Files. You have to list XML elements in string[]. Appends data to CSV File if CSV File exist with data
            XmlToCsv.WriteCsv(System.IO.Path.GetFullPath("OrderLines.xml"), new string[] { "ORDERNUM", "CUSTOMERID", "QUANTITY", "PRODUCTID" }, System.IO.Path.GetFullPath("OrderLines99.txt"));

            //Writes CSV File from CSV File Where OrderNum = 900003. Note Appends to CSV File if CSV File already exists.
            CsvDatabase.CsvDataReader dtr55 = new CsvDataReader(System.IO.Path.GetFullPath("OrderLines.txt"), "OrderNum = 900003");
            dtr55.ToCSV(System.IO.Path.GetFullPath("OrderLines55.txt"));

            //SQL Server Data to CSV File using DataReader Extension Method ToCSV(CSV File). Note Appends to CSV File if CSV File already exists.
            //SqlConnection conSQLServer2 = new SqlConnection(@"Data Source=LAPTOP-ASH6NMMV\SQLEXPRESS;Initial Catalog=MILLIONRECORDS;Integrated Security=True");
            //SqlCommand cmdSQLServer2 = new SqlCommand("SELECT Top 100 * FROM million", conSQLServer2);
            //conSQLServer2.Open();
            //SqlDataReader dtrSQLServer2 = cmdSQLServer2.ExecuteReader();
            //dtrSQLServer2.ToCSV(System.IO.Path.GetFullPath("Top 100 SQL Server.xml"));
            //conSQLServer2.Close();

            //Import Oracle Data to CSV File using Extension Method ToCSV(CSV File). Note Appends to CSV File if CSV File already exists.
            //OracleConnection conOracle2 = new OracleConnection(@"User Id=C##dominic;Password=123456;Data Source=localhost:1521/orcl;Pooling=false;");
            //OracleCommand cmdOracle2 = new OracleCommand("SELECT * FROM million WHERE rownum <= 100", conOracle2);
            //conOracle2.Open();
            //OracleDataReader dtrOracle2 = cmdOracle2.ExecuteReader();
            //dtrOracle2.ToCSV(System.IO.Path.GetFullPath("Top 100 Oracle.txt"));
            //conOracle2.Close();

            //Create Class List Where CustomerID equals 1000003
            List<OrderLines> lstOrderLines3 = (CsvReader.GetRecords<OrderLines>(System.IO.Path.GetFullPath("OrderLines.txt"), "CustomerID = 1000003")).ToList();

            //Create Class List Where CustomerID like 1000002
            List<OrderLines> lstOrderLines4 = (CsvReader.GetRecords<OrderLines>(System.IO.Path.GetFullPath("OrderLines.txt"), "CustomerID like 1000002")).ToList();

            //Create Class List Where CustomerID like 1000002 Or ProductId Like 3401
            List<OrderLines> lstOrderLines5 = (CsvReader.GetRecords<OrderLines>(System.IO.Path.GetFullPath("OrderLines.txt"), "CustomerID like 1000002|ProductId like 3401")).ToList();

            List<Customer> lstCustomer3 = new List<Customer>();
            lstCustomer3.Add(new Customer() { ID = Guid.NewGuid().ToString(), FIRSTNAME = "Dominic", LASTNAME = "Sputo", MIDDLENAME = "", PHONENUMBER = "8137505459" });
            lstCustomer3.Add(new Customer() { ID = Guid.NewGuid().ToString(), FIRSTNAME = "Tod", LASTNAME = "Rock", MIDDLENAME = "", PHONENUMBER = "8137505459" });
            lstCustomer3.Add(new Customer() { ID = Guid.NewGuid().ToString(), FIRSTNAME = "Cod", LASTNAME = "Sock", MIDDLENAME = "", PHONENUMBER = "8137505459" });
            CsvBulk.Insert(lstCustomer3, System.IO.Path.GetFullPath("Customer2.txt"));

            //Delete or Truncate entire Customer CSV File from any Data.
            CsvCommand.Truncate(System.IO.Path.GetFullPath("Customer2.txt"));

            //Delete or Drop the file
            CsvCommand.Drop(System.IO.Path.GetFullPath("Customer3.txt"));
            //Delete or Drop the file
            CsvCommand.Drop(System.IO.Path.GetFullPath("Customer4.txt"));

            //CSV File to Rename From To New File with Path.
            CsvCommand.Rename(System.IO.Path.GetFullPath("Customer2.txt"), System.IO.Path.GetFullPath("Customer3.txt"));
            //CSV File to Rename From To New File without Path (uses FROM directory).
            CsvCommand.Rename(System.IO.Path.GetFullPath("Customer3.txt"), "Customer2.txt");

            //Create DataTable using DataTypes of Customer Class properties and search for anything FIRSTNAME LIKE DoMinic not case sensitive.
            DataTable dtbl9 = CsvDataTable.GetDataTable<Customer>(System.IO.Path.GetFullPath("Customer2.txt"), "firstname  like  DoMinic  | lastName  LIKE  Ro  ");

            //Create DataTable with data of FirstName LIKE DoMinic. Value to search is not case sensitive. 
            DataTable dtbl7 = CsvDataTable.GetDataTable(System.IO.Path.GetFullPath("Customer2.txt"), "firstname  like  DoMinic  ");
            //Create DataTable with as many LIKE statements where pipe | means OR operator. Below Reads firstname like DoMinic OR lastname LIKE RO OR lastname LIKE So. Not Case Sensitive
            DataTable dtbl8 = CsvDataTable.GetDataTable(System.IO.Path.GetFullPath("Customer2.txt"), "firstname like DoMinic | lastname LIKE RO | lastname LIKE So");
            //Create DataTable with FirstName = DoMinic. Not Case Sensitive
            DataTable dtbl5 = CsvDataTable.GetDataTable(System.IO.Path.GetFullPath("Customer2.txt"), "firstname = DoMinic");
            //Create DataTable Where FirstName Equal to DoMinic OR Tod no case sensitive
            DataTable dtbl6 = CsvDataTable.GetDataTable(System.IO.Path.GetFullPath("Customer2.txt"), "firstname IN DoMinic | Tod");
            //Create DataTable Where FirstName LIKE DoMinic OR LastName LIKE RO. Not case sensitive.
            CsvDataReader dtr3 = new CsvDatabase.CsvDataReader(System.IO.Path.GetFullPath("Customer2.txt"), "firstname like DoMinic | lastname LIKE RO");
            while (dtr3.Read())
            {
                string a = dtr3[0] + "";
                string b = dtr3[1] + "";
                string c = dtr3[2] + "";
            }
            dtr3.Close();

            //Filter and create CSV File using DataReader Extension Method ToCSV(CSV File).
            CsvDatabase.CsvDataReader dtr7 = new CsvDataReader(System.IO.Path.GetFullPath("Customer2.txt"), "Firstname = DoMinic");
            dtr7.ToCSV(System.IO.Path.GetFullPath("Customer7.txt"));

            //Import SQL Server Data to CSV File using Extension Method ToCSV(CSV File).
            //SqlConnection conSQLServer = new SqlConnection(@"Data Source=LAPTOP-ASH6NMMV\SQLEXPRESS;Initial Catalog=MILLIONRECORDS;Integrated Security=True");
            //SqlCommand cmdSQLServer = new SqlCommand("SELECT Top 100 * FROM million", conSQLServer);
            //conSQLServer.Open();
            //SqlDataReader dtrSQLServer = cmdSQLServer.ExecuteReader();
            //dtrSQLServer.ToCSV(System.IO.Path.GetFullPath("Top 100 SQL Server.txt"));
            //conSQLServer.Close();

            //Import Oracle Data to CSV File using Extension Method ToCSV(CSV File).
            //OracleConnection conOracle = new OracleConnection(@"User Id=C##dominic;Password=123456;Data Source=localhost:1521/orcl;Pooling=false;");
            //OracleCommand cmdOracle = new OracleCommand("SELECT * FROM million WHERE rownum <= 100", conOracle);
            //conOracle.Open();
            //OracleDataReader dtrOracle = cmdOracle.ExecuteReader();
            //dtrOracle.ToCSV(System.IO.Path.GetFullPath("Top 100 Oracle.txt"));
            //conOracle.Close();


            //Create Customer Class, Insert and Create CSV File Customer2.txt if needed.
            Customer cCustomer2 = new Customer() { ID = Guid.NewGuid().ToString(), FIRSTNAME = "Dominic", LASTNAME = "Sputo", MIDDLENAME = "", PHONENUMBER = "8137505459" };
            CsvCommand.Insert(cCustomer2, System.IO.Path.GetFullPath("Customer2.txt"));

            //Delete from CSV File where FIRSTNAME = DOminic. Not case sensitive.
            CsvCommand.Delete(new string[] { "DOminiC" }, "FIRSTNAME", System.IO.Path.GetFullPath("Customer2.txt"));

            //INSERT or Create CSV File if does Not Exist with Header id, firstname, lastname, Middlename, PhoneNumber and Values Guid ID, Dominic, Sputo, , 8137505459
            CsvCommand.Insert(new string[] { Guid.NewGuid().ToString(), "Dominic", "Sputo", "", "8137505459" }, new string[] { "id", "firstname", "lastname", "Middlename", "PhoneNumber" }, System.IO.Path.GetFullPath("Customer2.txt"));

            //UPDATE FIRSTNAME equal Dominic, Joe, OR Ralph TO DominIC, SputO, "", 8137505459. Not case sensitive
            CsvCommand.Update(new string[] { "DominIC", "JoE", "RaLph" }, "FIrStnAME", new string[] { "DominIC", "SputO", "", "8137505459" }, new string[] { "fIRSTname", "lastnAME", "miDDLENAME", "phONENUMBER" }, System.IO.Path.GetFullPath("Customer2.txt"));
            
            //Create Customer CSV File if Does Not Exist with Headers ID, FIRSTNAME, LASTNAME, MIDDLENAME, and PHONENUMBER
            CsvCommand.CreateTable(new string[] { "ID", "FIRSTNAME", "LASTNAME", "MIDDLENAME", "PHONENUMBER" }, System.IO.Path.GetFullPath("Customer.txt"));

            //Create Customer CSV File if Does Not Exist and INSERT Customer List
            List<Customer> lstCustomer = new List<Customer>();
            for (int i = 0; i < 3; i++)
            {                
                lstCustomer.Add(new Customer() { ID = Guid.NewGuid().ToString(), FIRSTNAME = "Dominic", LASTNAME= "Sputo", MIDDLENAME = "", PHONENUMBER = "8137505459" });
            }
            CsvBulk.Insert(lstCustomer, System.IO.Path.GetFullPath("Customer.txt"));
            
            for (int i = 0; i < 100; i++)
            {
                //Create Customer CSV File if Does Not Exist and INSERT Customer Class one at a Time.
                Customer cCustomer = new Customer() { ID = Guid.NewGuid().ToString(), FIRSTNAME = "Dominic", LASTNAME = "Sputo", MIDDLENAME = "", PHONENUMBER = "8137505459" };
                CsvCommand.Insert(cCustomer, System.IO.Path.GetFullPath("Customer.txt"));
            }

            //Delete or Truncate entire Customer CSV File from any Data.
            CsvCommand.Truncate(System.IO.Path.GetFullPath("Customer.txt"));

            //CSV File to Rename From To New File.
            CsvCommand.Rename(System.IO.Path.GetFullPath("Customer.txt"), System.IO.Path.GetFullPath("Customer2.txt"));
            //CSV File to Rename From To New File.
            CsvCommand.Rename(System.IO.Path.GetFullPath("Customer2.txt"), "Customer.txt");

            //DROP or DELETE Customer CSV from Drive.
            CsvCommand.Drop(System.IO.Path.GetFullPath("Customer2.txt"));

            //CREATE Customer CSV If Does NOT Exist INSERT ID equal to Guid AND FIRSTNAME equal to Dominic
            CsvCommand.Insert(new string[] { Guid.NewGuid().ToString(), "Dominic" }, new string[] { "ID", "FIRSTNAME" }, System.IO.Path.GetFullPath("Customer.txt"));

            //DELETE WHERE FIRSTNAME equal to Peter, Joe, OR Ralph
            CsvCommand.Delete(new string[] { "Peter", "Joe", "Ralph" }, "FIRSTNAME", System.IO.Path.GetFullPath("Customer.txt"));

            //UPDATE FIRSTNAME equal Dominic, Joe, OR Ralph TO Dominic, Sputo, "", 8137505459
            CsvCommand.Update(new string[] { "Dominic", "Joe", "Ralph" }, "FIRSTNAME", new string[] { "Dominic", "Sputo", "", "8137505459" }, new string[] { "FIRSTNAME", "LASTNAME", "MIDDLENAME", "PHONENUMBER" }, System.IO.Path.GetFullPath("Customer.txt"));

            //DataTable of Customers using Customer class DataTypes
            DataTable dtbl1 = CsvDataTable.GetDataTable<Customer>(System.IO.Path.GetFullPath("Customer.txt"));

            //DataTable of all Customers as String DataType
            DataTable dtbl2 = CsvDataTable.GetDataTable(System.IO.Path.GetFullPath("Customer.txt"));
            
            //List of all Customers from CSV File
            List<Customer> lstCustomer2 = CsvReader.GetRecords<Customer>(System.IO.Path.GetFullPath("Customer.txt")).ToList();

            //DataReader searching for FIRSTNAME equal to DOMINIC OR PETER OR JOE. Not Case Sensitive
            CsvDataReader dtr = new CsvDatabase.CsvDataReader(System.IO.Path.GetFullPath("Customer.txt"), "FIRSTNAME IN Dominic | Peter | Joe");   
            while (dtr.Read())
            {
                string a = dtr[0] + "";
                string b = dtr[1] + "";
                string c = dtr[2] + "";
            }
            dtr.Close();

            //DataReader searching for FIRSTNAME equal to Dominic. Not Case Sensitive
            CsvDataReader dtr2 = new CsvDatabase.CsvDataReader(System.IO.Path.GetFullPath("Customer.txt"), "firstName = DominiC");   
            while (dtr2.Read())
            {
                string a = dtr2[0] + "";
                string b = dtr2[1] + "";
                string c = dtr2[2] + "";
            }
            dtr2.Close();

            //DataReader searching for FIRSTNAME Like Dominic Or LastName LikE Sputo. Not Case Sensitive
            CsvDataReader dtr4 = new CsvDatabase.CsvDataReader(System.IO.Path.GetFullPath("Customer.txt"), "firstName liKE DominiC | LastName LiKe SpuTo");
            while (dtr4.Read())
            {
                string a = dtr4[0] + "";
                string b = dtr4[1] + "";
                string c = dtr4[2] + "";
            }
            dtr4.Close();

            //DataReader searching for FIRSTNAME Like Dominic. Not Case Sensitive
            CsvDataReader dtr5 = new CsvDatabase.CsvDataReader(System.IO.Path.GetFullPath("Customer.txt"), "firstName liKE DominiC");
            while (dtr5.Read())
            {
                string a = dtr5[0] + "";
                string b = dtr5[1] + "";
                string c = dtr5[2] + "";
            }
            dtr5.Close();

            sw.Stop();
            sw.Elapsed.TotalSeconds.ToString();
        }
    }
}

































public class Customer
{
    public int CustomerID { get; set; }
    public string FirstName { get; set; }
    public string LastName { get; set; }
    //public int Phone { get; set; }
    //public string Street { get; set; }
    //public string State { get; set; }
    //public int ZipCode { get; set; }
}












//System.IO.File.Move(System.IO.Path.GetFullPath("A.txt"), System.IO.Path.GetFullPath("B.txt"));






















//string s = "A IN (21,3 | ABC | 123 | ABC | 'A')";
//string _CSV_Filter = s;
//string[] _sarIN_Filter = null;
/////////////////////////////////////

//_CSV_Filter = _CSV_Filter.Substring(_CSV_Filter.IndexOf(" IN") + 3, _CSV_Filter.Length - (_CSV_Filter.IndexOf(" IN") + 3)).Trim();
//if (_CSV_Filter.Substring(0, 1) == "(")
//{
//    _CSV_Filter = _CSV_Filter.Substring(1, _CSV_Filter.Length - 1);
//}
//if (_CSV_Filter.Substring(_CSV_Filter.Length - 1, 1) == ")")
//{
//    _CSV_Filter = _CSV_Filter.Substring(0, _CSV_Filter.Length - 1);
//}
//_sarIN_Filter = _CSV_Filter.Split('|').Select(p => p.Trim()).ToArray();
//_sarIN_Filter = _CSV_Filter.Split('|');

//string s = "A IN (21,3 | ABC | 123 | ABC | 'A')";
//string _CSV_Filter = s;
//string[] _sarIN_Filter = null;
/////////////////////////////////////

//_CSV_Filter = _CSV_Filter.Substring(_CSV_Filter.IndexOf(" IN") + 3, _CSV_Filter.Length - (_CSV_Filter.IndexOf(" IN") + 3)).Trim();
//if (_CSV_Filter.Substring(0, 1) == "(")
//{
//    _CSV_Filter = _CSV_Filter.Substring(1, _CSV_Filter.Length - 1);
//}
//if (_CSV_Filter.Substring(_CSV_Filter.Length - 1, 1) == ")")
//{
//    _CSV_Filter = _CSV_Filter.Substring(0, _CSV_Filter.Length - 1);
//}            
//_sarIN_Filter = _CSV_Filter.Split('|').Select(p => p.Trim()).ToArray();
//_sarIN_Filter = _CSV_Filter.Split('|');

//System.Text.RegularExpressions.Regex csvSplit = new System.Text.RegularExpressions.Regex("(?:^|,)(\"(?:[^\"]+|\"\")*\"|[^,]*)", System.Text.RegularExpressions.RegexOptions.Compiled);
//List<string> lstIN = new List<string>();
//string curr = null;
//foreach (System.Text.RegularExpressions.Match match in csvSplit.Matches(_CSV_Filter))
//{
//    curr = match.Value;
//    if (0 == curr.Length)
//    {
//        lstIN.Add("");
//    }
//    lstIN.Add(curr.TrimStart(','));
//}
//int iINCount = lstIN.Count();
//_sarIN_Filter = new string[iINCount];
//for (int i = 0; i < iINCount; i++)
//{
//    _sarIN_Filter[i] = lstIN[i].Trim(); //.Replace("\"", "").Replace("'", "").Replace(",", "").Trim();
//}

//System.Text.RegularExpressions.Regex csvSplit = new System.Text.RegularExpressions.Regex("(?:^|,)(\"(?:[^\"]+|\"\")*\"|[^,]*)", System.Text.RegularExpressions.RegexOptions.Compiled);
//List<string> lstIN = new List<string>();
//string curr = null;
//foreach (System.Text.RegularExpressions.Match match in csvSplit.Matches(s))
//{
//    curr = match.Value;
//    if (0 == curr.Length)
//    {
//        lstIN.Add("");
//    }

//    lstIN.Add(curr.TrimStart(','));
//}
//int iINCount = lstIN.Count();
//string[] sar = new string[iINCount];
//for (int i = 0; i < iINCount; i++)
//{
//    sar[i] = lstIN[i].Replace("\"", "").Replace("'", "").Replace(",", "").Trim();
//}
////string[] sar = list.ToArray();
//string s = "\"\", \"FF\", 'AA', BB, 111,222,\"33,44,55\",666,\"77,88\",\"99\"";
//System.Text.RegularExpressions.Regex csvSplit = new System.Text.RegularExpressions.Regex("(?:^|,)(\"(?:[^\"]+|\"\")*\"|[^,]*)", System.Text.RegularExpressions.RegexOptions.Compiled);
//List<string> lstIN = new List<string>();
//string curr = null;
//foreach (System.Text.RegularExpressions.Match match in csvSplit.Matches(s))
//{
//    curr = match.Value;
//    if (0 == curr.Length)
//    {
//        lstIN.Add("");
//    }

//    lstIN.Add(curr.TrimStart(','));
//}
//int iINCount = lstIN.Count();
//string[] sar = new string[iINCount];
//for (int i = 0; i < iINCount; i++)
//{
//    sar[i] = lstIN[i].Replace("\"", "").Replace("'", "").Replace(",", "").Trim();
//}
////string[] sar = list.ToArray();
///


//public class Company
//{
//    public string A { get; set; }
//    public string B { get; set; }
//}
//System.Data.DataTable dtbl = CsvDatabase.CsvDataTable.GetDataTable<ABC>(System.IO.Path.GetFullPath("TEXT_DATA_DLL.txt")); //0.1984706  //0.2189173
//System.Data.DataTable dtbl1 = CsvDatabase.CsvDataTable.GetDataTable(System.IO.Path.GetFullPath("TEXT_DATA_DLL.txt"), new string[] { "A", "B", "C", "E" }); //0.1834905
//System.Data.DataTable dtbl2 = CsvDatabase.CsvDataTable.GetDataTable(System.IO.Path.GetFullPath("TEXT_DATA_DLL.txt"), new string[] { "A", "B", "C", "E" }, new string[] { "string", "string", "string", "string" }); //0.1954434
//var v = CsvDatabase.CsvReader.GetRecords<ABC>(System.IO.Path.GetFullPath("TEXT_DATA_DLL.txt")); //0.3485031

//System.Data.DataTable dtbl1 = CsvDatabase.CsvCommand.GetDataTable(System.IO.Path.GetFullPath("TEXT_DATA_DLL.txt"), new string[] { "A", "B", "C", "E" }, new string[] { "string", "string", "string", "string" }); // 0.1954434
//System.Data.DataTable dtbl2 = CsvDatabase.CsvCommand.GetDataTable(System.IO.Path.GetFullPath("TEXT_DATA_DLL.txt"), new string[] { "A", "B", "C", "E" }); // 0.1893447

//List<Company> lstCompany = new List<Company>();
//for (int i = 0; i < 10; i++)
//{
//    lstCompany.Add(new Company() {  A = i.ToString(), B = i.ToString() });
//}
//CsvBulk csv = new CsvBulk();
//csv.Insert(lstCompany);


//CsvDatabase.CsvReader csv = new CsvDatabase.CsvReader();
//var v = csv.GetRecords<ABC>(System.IO.Path.GetFullPath("TEXT_DATA_DLL.txt")); //0.25968 //0.4096108 0.3548716



//System.Data.DataTable dtbl = new System.Data.DataTable();
//dtbl.Columns.Add("A", typeof(Decimal));

//System.Data.DataRow dr;
//dr = dtbl.NewRow();
//dr[0] = "";
//dtbl.Rows.Add(dr);
//string s = dtbl.Rows[0][0] + "";

//for (int v = 0; v < 100000; v++)
//{
//    for (int i = 0; i < 10; i++)
//    {
//        Distinct(new string[] { i.ToString(), i.ToString() }); //0.1733753
//    }
//}



//for (int v = 0; v < 100000; v++)
//{
//    List<Customer> lstCustomer = new List<Customer>(); //"0.0001653" 0.0001332
//    for (int i = 0; i < 10; i++)
//    {
//        lstCustomer.Add(new Customer() { ID = i.ToString(), CN = i.ToString() });
//    }

//    int ilen = lstCustomer.Count();
//    bool bDuplicate = false;
//    string sValue = "1".ToUpper();
//    for (int i = 0; i < ilen; i++)
//    {
//        if (lstCustomer[i].ID.ToUpper() == sValue)
//        {
//            bDuplicate = true;
//            break;
//        }
//    } //0.0004712
//}


//for (int v = 0; v < 100000; v++)
//{
//    string sValue = "1".ToUpper();
//    System.Collections.Specialized.OrderedDictionary headercollection = new System.Collections.Specialized.OrderedDictionary(); //0.0001094 ms 0.0573
//    for (int i = 0; i < 10; i++)
//    {
//        headercollection.Add(i, i);
//    }
//    headercollection.Contains(sValue);
//} //0.1188786

//    string sValue = "1".ToUpper();
//System.Collections.Specialized.OrderedDictionary headercollection = new System.Collections.Specialized.OrderedDictionary(); //0.0001094 ms 0.0573
//for (int i = 0; i < 10; i++)
//{
//    headercollection.Add(i, i);
//}
//headercollection.Contains(sValue); //0.0002294
//0.000221

//string sValue = "1".ToUpper();
//bool bDuplicate = false;
//foreach (System.Collections.DictionaryEntry de in headercollection)
//{
//    if (de.Key.ToString().ToUpper() == sValue)
//    {
//        bDuplicate = true;
//        break;
//    }
//}//0.000289


//for (int i = 0; i < 100000; i++)
//{
//    //char c = CsvDatabase.CsvCommand.FindDelimiter("dfgdfgdfgdfg*fgdfgdfg*DFGDFG*dfgdfgfdgdfg*fdgdfgdfg*dsfdsfds*fgdfgdfgfghgfhgfh*fdfgdfgfdg", '*');
//    //string[] s = new string[] { "CN", "R", "X" };
//    //CsvDatabase.CsvCommand.Distinct(s);
//    //CsvDatabase.CsvCommand.Distinct2(s);
//    //CsvDatabase.CsvCommand.Insert(new string[] { Guid.NewGuid().ToString(), Guid.NewGuid().ToString(), Guid.NewGuid().ToString() }, new string[] { "A", "B", "C" }, System.IO.Path.GetFullPath("TEXT_DATA_DLL.txt"));

//System.Collections.Specialized.OrderedDictionary headercollection = new System.Collections.Specialized.OrderedDictionary();
//headercollection.Add("A", null);
//headercollection.Add("B", null);
//headercollection.Add("C", null);
//headercollection.Contains("A");
////string s = headercollection[0].ToString();
//foreach (System.Collections.DictionaryEntry de in headercollection)
//{
//    string s = de.Key + " --> " + de.Value;
//} 
//string sFilter = "A = ";
//int iIN = sFilter.IndexOf(" IN");
//int iEqual = sFilter.IndexOf("=");
//if (iEqual != -1)
//{
//    sFilter = sFilter.Substring(0, iEqual).Trim();
//}
//else if (iIN != -1)
//{
//    sFilter = sFilter.Substring(0, iIN).Trim();
//}


//    //List<string> lstHeader = new List<string>() { "A", "B", "C" };
//    //lstHeader.Contains("A");

//    //string s = " A  =  3 ";
//    //System.Text.RegularExpressions.Regex.Replace(s, @"\s+", " ").Split(' ');
//    //string s = " A  =  3 ".Trim();
//    //string[] ss = s.Split(' ');
//    //int iFilterLength = ss.Length;
//    //for (int t = 0; t < iFilterLength; t++)
//    //{
//    //    if (ss[t] == "=")
//    //    {

//    //    }
//    //}


////}
//string s = "A = \"'ABC'\"";
//string x = s.Substring(s.IndexOf('=') + 1, s.Length - (s.IndexOf('=') + 1)).Replace("'", "").Replace("\"", "").Trim();

//_CSV_Filter.Substring(_CSV_Filter.IndexOf(" IN ") + 3, _CSV_Filter.Length - (_CSV_Filter.IndexOf(" IN ") + 3)).Replace("'", "").Replace("\"", "").Trim().Split(',');
//string _CSV_Filter = "A IN \"'ABC', 213,2,\"AB\"\"";
//string[] x = _CSV_Filter.Substring(_CSV_Filter.IndexOf(" IN ") + 3, _CSV_Filter.Length - (_CSV_Filter.IndexOf(" IN ") + 3)).Replace("'", "").Replace("\"", "").Trim().Split(',');
//string _CSV_Filter = "A IN('1','2','3')";
//string ss = _CSV_Filter.Substring(_CSV_Filter.IndexOf(" IN") + 3, _CSV_Filter.Length - (_CSV_Filter.IndexOf(" IN") + 3)).Replace("'", "").Replace("\"", "").Replace("(", "").Replace(")", "").Trim();

//if (System.IO.File.Exists(System.IO.Path.GetFullPath("TEXT_DATA_DLL.txt")))
//{
//    System.IO.File.Delete(System.IO.Path.GetFullPath("TEXT_DATA_DLL.txt"));
//}
//CsvDatabase.CsvCommand.Insert(new string[] { "1", "1", "3" }, new string[] { "A", "B", "C" }, System.IO.Path.GetFullPath("TEXT_DATA_DLL.txt"));
//CsvDatabase.CsvCommand.Insert(new string[] { "2", "2", "5" }, new string[] { "A", "B", "E" }, System.IO.Path.GetFullPath("TEXT_DATA_DLL.txt"));
//CsvDatabase.CsvCommand.Insert(new string[] { "1", "3", "5" }, new string[] { "A", "B", "E" }, System.IO.Path.GetFullPath("TEXT_DATA_DLL.txt"));
//CsvDatabase.CsvCommand.Insert(new string[] { "4", "2", "5" }, new string[] { "A", "B", "E" }, System.IO.Path.GetFullPath("TEXT_DATA_DLL.txt"));

//for (int i = 0; i < 100000; i++)
//{
//    CsvDatabase.CsvCommand.Insert(new string[] { "1", "2", "3" }, new string[] { "A", "B", "C" }, System.IO.Path.GetFullPath("TEXT_DATA_DLL.txt"));
//}

//CsvDatabase.CsvDataReader dtr = new CsvDataReader(System.IO.Path.GetFullPath("TEXT_DATA_DLL.txt"), '|', "", true); //0.0547522 B IN (1, 2)
//dtr.AddVirtualColumn("D", "9");
//dtr.AddVirtualColumn("G", "8");
//dtr.RenameCSVHeader("A", "Z");
//dtr.UpdateVirtualColumnValue("G", "-");
//while (dtr.Read())
//{
//    string s1 = dtr[0] + "";
//    //string s2 = dtr[1] + "";
//}
//dtr.Close();
//CsvDatabase.CsvDataReader dtr = new CsvDataReader(System.IO.Path.GetFullPath("TEXT_DATA_DLL.txt"), '|', "A = 1", true);
//List<Company> customers = dtr.AutoMap<Company>().ToList();''
//string _csvHeaderstring = "dfdsf^FDSFSD^^";
//string s = _csvHeaderstring.TrimEnd('^');
//string sLine = System.IO.Path.GetFullPath("TEXT_DATA_DLL.txt");
//string CSV_TempFile = System.IO.Path.GetDirectoryName(sLine) + "\\" + Guid.NewGuid() + System.IO.Path.GetExtension(sLine);
//sLine = sLine.Substring(0, (sLine.Length - 1));
//public class Customer
//{
//    public string ID;
//    public string CN;
//}

//private static bool Distinct(string[] sarHeader)
//{
//    int iHeaderLength = sarHeader.Length;
//    int iIndex = 0;
//    for (int i = 0; i < iHeaderLength; i++)
//    {
//        iIndex = 0;
//        for (int a = 0; a < iHeaderLength; a++)
//        {
//            if (sarHeader[i] == sarHeader[a])
//            {
//                iIndex = iIndex + 1;
//            }
//            if (iIndex == 2)
//            {
//                return false;
//            }
//        }
//    }
//    return true;
//}
