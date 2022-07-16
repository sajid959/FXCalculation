using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Globalization;
namespace Nunit
{
    public class FXCalculation

    {
       
        public class Trade
        {
            public string TradeID { get; set; }
            public string ISIN { get; set; }
            public string TradeDate { get; set; }
            public string MaturityDate { get; set; }
            public string SchemeName { get; set; }
            public string TradeType { get; set; }
            public string Currency { get; set; }
            public string Amount { get; set; }
        }
        public class FXRate
        {
            public string TradeID { get; set; }
            public string Currency { set; get; }

            public string Amount { set; get; }
            public float AppliedFXRate { get; set; }
            public float CalculatedFXRate { get; set; }
        }
        public class FXCalculationException : ApplicationException
        {
            public override string Message
            {
                get { return "File Doesn't Exists"; }
            }

        }
        public void ProcessData(string sourceFolder, string fileName, string errorLogFilePath, string errorLogFileName,
            SqlConnection connectionObject, string archiveFilePath, string archiveFileName)
        {

            List<Trade> trades = ReadAllDataFromInputFile(sourceFolder, fileName);


            List<Trade> validateTrade = PickValidTradeDetails(trades, errorLogFilePath, errorLogFileName);

       
            SaveValidRecordsToDB(validateTrade, connectionObject);

     
            List<FXRate> fxRates = CalculateFXRate(connectionObject);


            SaveFXRate(fxRates, connectionObject);
 

            CopyToArchive(archiveFilePath, archiveFileName);



        }

        public List<Trade> ReadAllDataFromInputFile(string sourceFolder, string fileName)
        {

            List<Trade> trades = new List<Trade>();
            using (StreamReader sr = File.OpenText(sourceFolder + fileName))
            {
                string s = "";
                while ((s = sr.ReadLine()) != null)
                {

                    if (s.Contains(','))

                    {
                        string[] splited = s.Split(',');
                        var tradeitem = new Trade()
                        {
                            TradeID = splited[0],
                            ISIN = splited[1],
                            TradeDate = splited[2],
                            MaturityDate = splited[3],
                            SchemeName = splited[4],
                            TradeType = splited[5],
                            Currency = splited[6],
                            Amount = splited[7]
                        };

                        trades.Add(tradeitem);

                    }
                }
            }


            return trades;
        }

        public List<Trade> PickValidTradeDetails(List<Trade> trades, string errorLogFilePath, string errorLogFileName)
        {
          
            List<Trade> validTrades = new List<Trade>(); 

            List<Trade> invalidTrade = new List<Trade>();
            bool isValid;
            foreach (Trade x in trades)
            {
                isValid = (!string.IsNullOrEmpty(x.TradeID));
                Regex rgxTradeID = new Regex(@"\bTR\d{3}\b");
                isValid = isValid && rgxTradeID.IsMatch(x.TradeID);
                isValid = (isValid && (!string.IsNullOrEmpty(x.ISIN)));
                Regex rgxisin = new Regex(@"\bISIN\d{3}\b");
                isValid = (rgxisin.IsMatch(x.ISIN));
                Regex rgxdate = new Regex("((0[1-9]|1[0-2])\\/((0|1)[0-9]|2[0-9]|3[0-1])\\/((19|20)\\d\\d))$");
                isValid = (isValid && (!string.IsNullOrEmpty(x.TradeDate)) && (rgxdate.IsMatch(x.TradeDate)));
                isValid = (isValid && (!string.IsNullOrEmpty(x.MaturityDate)) && (rgxdate.IsMatch(x.MaturityDate)));
                if (isValid)
                {
                    DateTime MD = DateTime.Parse(x.MaturityDate, CultureInfo.InvariantCulture.DateTimeFormat);
                    DateTime TD = DateTime.Parse(x.TradeDate, CultureInfo.InvariantCulture.DateTimeFormat);
                    int year = MD.Year - TD.Year;
                    isValid = isValid && (year > 5);
                    isValid = isValid && (!string.IsNullOrEmpty(x.TradeType));
                    isValid = isValid && (x.Currency.Equals("GBP") || x.Currency.Equals("EUR") || x.Currency.Equals("USD") || x.Currency.Equals("INR"));
                    isValid = isValid && (!string.IsNullOrEmpty(x.Amount));
                    bool isnumeric = int.TryParse(x.Amount, out int i);
                    isValid = isValid && (isnumeric);
                }
                if (isValid)
                {
                    var val = new Trade()
                    {
                        TradeID = x.TradeID,
                        ISIN = x.ISIN,
                        TradeDate = x.TradeDate,
                        MaturityDate = x.MaturityDate,
                        SchemeName = x.SchemeName,
                        TradeType = x.TradeType,
                        Currency = x.Currency,
                        Amount = x.Amount
                    };
                   
                    validTrades.Add(val);
                }
                else
                {
                    var valerror = new Trade()
                    {
                        TradeID = x.TradeID,
                        ISIN = x.ISIN,
                        TradeDate = x.TradeDate,
                        MaturityDate = x.MaturityDate,
                        SchemeName = x.SchemeName,
                        TradeType = x.TradeType,
                        Currency = x.Currency,
                        Amount = x.Amount
                    };

                    invalidTrade.Add(valerror);
                }

            }
            SaveInvalidRecordsToLogFile(invalidTrade, errorLogFilePath, errorLogFileName);

            return validTrades;

        }

        public bool SaveValidRecordsToDB(List<Trade> validTrades, SqlConnection sqlConnectionObject)
        {
           
            if (validTrades.Count > 0 && validTrades != null)
            {
                SqlConnection conn = sqlConnectionObject;

                conn.Open();
                foreach (Trade valid in validTrades)
                {
                    SqlCommand cmd = new SqlCommand(("Insert into SBA.Trade_Details (TradeID,ISIN,TradeDate,MaturityDate,SchemeName,TradeType,Currency,Amount) values (@TradeID,@ISIN,@TradeDate,@MaturityDate,@SchemeName,@TradeType,@Currency,@Amount)"), conn);
                    cmd.Parameters.AddWithValue("@TradeID", valid.TradeID);
                    cmd.Parameters.AddWithValue("@ISIN", valid.ISIN);
                    cmd.Parameters.AddWithValue("@TradeDate", valid.TradeDate);
                    cmd.Parameters.AddWithValue("@MaturityDate", valid.MaturityDate);
                    cmd.Parameters.AddWithValue("@SchemeName", valid.SchemeName);
                    cmd.Parameters.AddWithValue("@TradeType", valid.TradeType);
                    cmd.Parameters.AddWithValue("@Currency", valid.Currency);
                    cmd.Parameters.AddWithValue("@Amount", valid.Amount);
                    cmd.ExecuteNonQuery();

                }
                conn.Close();
            }



            return true;

        }

        public bool SaveInvalidRecordsToLogFile(List<Trade> invalidTrades, string errorLogFilePath, string errorLogFileName)
        {

            if (invalidTrades != null && invalidTrades.Count > 0)
            {
                string errorLogfile = errorLogFilePath + errorLogFileName;
                try
                {
                    if (!File.Exists(errorLogfile))
                    {
                        var invalidfile = File.Create(errorLogfile);
                        invalidfile.Close();
                    }

                    using (StreamWriter swinvalid = File.AppendText(errorLogfile))
                    {
                        swinvalid.WriteLine("TradeID|ISIN|TradeDate|MaturityDate|Tradetype|Currency|Amount");
                        foreach (Trade ivt in invalidTrades)
                        {
                            swinvalid.WriteLine(ivt.TradeID + "," + ivt.ISIN + "," + ivt.TradeDate + "," + ivt.MaturityDate + "'" + ivt.TradeType + "," + ivt.Currency + "," + ivt.Amount);
                        }
                    }

                }
                catch (Exception)
                {
                    throw new FXCalculationException();
                }
            }
            return true;

        }

        public List<FXRate> CalculateFXRate(SqlConnection sqlConnectionObject)
        {
            
            List<FXRate> FxRates = new List<FXRate>();

            List<Trade> trades = new List<Trade>();

            try
            {
                SqlConnection conne = sqlConnectionObject;
                string queryString = "Select * from SBA.Trade_Details";
                SqlCommand cmd = new SqlCommand(queryString, conne);
                conne.Open();


                SqlDataReader datareader = cmd.ExecuteReader();

                while (datareader.Read())
                {
                    Trade validfx = new Trade
                    {
                        TradeID = datareader["TradeID"].ToString(),
                        ISIN = datareader["ISIN"].ToString(),
                        TradeDate = datareader["TradeDate"].ToString(),
                        MaturityDate = datareader["MaturityDate"].ToString(),
                        SchemeName = datareader["SchemeName"].ToString(),
                        TradeType = datareader["TradeType"].ToString(),
                        Currency = datareader["Currency"].ToString(),
                        Amount = datareader["Amount"].ToString()
                    };
                    trades.Add(validfx);
                }

                conne.Close();

                foreach (Trade trad_para_to_calc_fx in trades)
                {
                    var fx = new FXRate()
                    {
                        TradeID = trad_para_to_calc_fx.TradeID,
                        Currency= trad_para_to_calc_fx.Currency,
                        Amount = trad_para_to_calc_fx.Amount
                    };
                    


                    float amount = float.Parse(fx.Amount, CultureInfo.InvariantCulture.NumberFormat);
                    if (trad_para_to_calc_fx.Currency == "USD")
                    {
                        fx.AppliedFXRate = float.Parse("0.5", CultureInfo.InvariantCulture.NumberFormat);
                        float app_fx_rate = float.Parse("0.5", CultureInfo.InvariantCulture.NumberFormat);
                        fx.CalculatedFXRate = ((app_fx_rate) * (amount));
                    }
                    if (trad_para_to_calc_fx.Currency == "GBP")
                    {
                        fx.AppliedFXRate = float.Parse("0.6", CultureInfo.InvariantCulture.NumberFormat);
                        float app_fx_rate = float.Parse("0.7", CultureInfo.InvariantCulture.NumberFormat);
                        fx.CalculatedFXRate = ((app_fx_rate) * (amount));
                    }
                    if (trad_para_to_calc_fx.Currency == "EUR")
                    {
                        fx.AppliedFXRate = float.Parse("0.7", CultureInfo.InvariantCulture.NumberFormat);
                        float app_fx_rate = float.Parse("0.7", CultureInfo.InvariantCulture.NumberFormat);
                        fx.CalculatedFXRate = ((app_fx_rate) * (amount));
                    }
                    if (trad_para_to_calc_fx.Currency == "INR")
                    {
                        fx.AppliedFXRate = float.Parse("1", CultureInfo.InvariantCulture.NumberFormat);
                        float app_fx_rate = float.Parse("1", CultureInfo.InvariantCulture.NumberFormat);
                        fx.CalculatedFXRate = ((app_fx_rate) * (amount));
                    }
                    FxRates.Add(fx);
                }

            }

            catch (Exception)
            {
                throw new FXCalculationException();
            }




            return FxRates;

        }
        public bool SaveFXRate(List<FXRate> fxRates, SqlConnection sqlConnectionObject)
        {
            
            try
            {
                if (fxRates.Count > 0 && fxRates != null)
                {
                    SqlConnection conne = sqlConnectionObject;
                    conne.Open();
                    foreach (FXRate calculated in fxRates)
                    {
                        SqlCommand cmd = new SqlCommand("Insert into SBA.FX_Rate (TradeID,Currency,Amount,AppliedFXRate,CalculatedFXRate) values (@TradeID,@Currency,@Amount,@AppliedFXRate,@CalculatedFXRate)", conne);
                        cmd.Parameters.AddWithValue("@TradeID", calculated.TradeID);
                        cmd.Parameters.AddWithValue("@Currency", calculated.Currency);
                        cmd.Parameters.AddWithValue("@Amount", calculated.Amount);
                        cmd.Parameters.AddWithValue("@AppliedFXRate", calculated.AppliedFXRate);
                        cmd.Parameters.AddWithValue("@CalculatedFXRate", calculated.CalculatedFXRate);
                        cmd.ExecuteNonQuery();
                    }
                    conne.Close();
                }
            }

            catch (Exception)
            {
                throw new FXCalculationException();
            }

            return true;

        }

        public bool CopyToArchive(string sourcePathWithFileName, string targetPathWithFileName)
        {
            try
            {
                string inputpath = "";
                string input = "";
                FileInfo[] files;
                DirectoryInfo Di;
                string targetFile = sourcePathWithFileName + targetPathWithFileName;
                Di = new DirectoryInfo(@"E:\TestProj\");
                files = Di.GetFiles("*.txt", SearchOption.AllDirectories);
                foreach (FileInfo di1 in files)
                {

                    if (di1.Name == "TradeOrders_032013.txt")
                    {
                        inputpath = di1.DirectoryName.ToString();
                        input = inputpath + "\\" + di1.Name.ToString();
                    }
                }

                if (!Directory.Exists(sourcePathWithFileName))
                {
                    Directory.CreateDirectory(sourcePathWithFileName);
                    var targetfilecreation = File.Create(targetFile);
                    targetfilecreation.Close();
                }
                else
                {
                    File.Delete(targetFile);
                    Directory.Delete(sourcePathWithFileName, true);
                    Directory.CreateDirectory(sourcePathWithFileName);
                    var targetfilecreation = File.Create(targetFile);
                    targetfilecreation.Close();
                }

                System.IO.File.Copy(input, targetFile, true);
            }
            catch (Exception)
            {
                throw new FXCalculationException();
            }
            return true;

        }

        static void Main()
        {
            SqlConnection connectionObject = new SqlConnection(@"Data Source=DESKTOP-P6KBEME\SQLEXPRESS;Initial Catalog= DBFXCalculation;Integrated Security=True");
            FXCalculation fxcalculatorobj = new FXCalculation();
            fxcalculatorobj.ProcessData(@"E:\TestProj\", "TradeOrders_032013.txt",
                @"E:\TestProj\ErrorLog\", "InvalidRecords_032014.txt", connectionObject,
                @"E:\TestProj\Archive\", "TradeOrders_032013_Processed.txt");
            Console.WriteLine("Operation Done Successfully");
            Console.ReadLine();


        }

    }
}
