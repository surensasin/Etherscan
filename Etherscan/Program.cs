// See https://aka.ms/new-console-template for more information
//Console.WriteLine("Hello, World!");

using Newtonsoft.Json;
using System.Data.SqlClient;
using NLog;
using System.Transactions;
using System;

namespace Etherscan
{
    class Program
    {
        static convertHex fromHex = new convertHex();

        static CallEndpoint call = new CallEndpoint();

        static HttpResponseMessage response = new HttpResponseMessage();

        static Database DB = new Database();

        private static readonly ILogger Logger = LogManager.GetCurrentClassLogger();

        static async Task Main(string[]args)
        {
            int blockNumMin = 12100001;
            int blockNumMax = 12100500;

            DateTime startTime = DateTime.Now;

            Logger.Info("Application started at " + startTime);
            Console.WriteLine("Application started at " + startTime);

            do
            {
                try
                {
                    Logger.Info("Block (Start): " + blockNumMin);
                    Console.WriteLine("Block (Start): " + blockNumMin);

                    await eth_getBlockByNumber(blockNumMin);
                }
                catch (Exception ex)
                {
                    Logger.Error("Block: " + blockNumMin + "|ERROR: " + ex.Message);
                    Console.WriteLine("Block: " + blockNumMin + "|ERROR: " + ex.Message);
                }
                Logger.Info("Block (End): " + blockNumMin);
                Console.WriteLine("Block (End): " + blockNumMin);

                Logger.Info("========================================================");
                Console.WriteLine("========================================================");

                blockNumMin += 1;

            } while (blockNumMin <= blockNumMax);

            DateTime endTime = DateTime.Now;
            Logger.Info("Application ending at " + endTime);
            Console.WriteLine("Application ending at " + endTime);

            TimeSpan totalTime = endTime - startTime;
            double totalMinutes = totalTime.TotalMinutes;

            Logger.Info("Total Processing time " + totalMinutes);
            Console.WriteLine("Application ending at " + totalMinutes);

        }
        
        static async Task eth_getBlockByNumber(int blockNum)
        {
            try
            {
                returnObj responseMessage = await call.toAPI("eth_getBlockByNumber", blockNum);
                response = responseMessage.response;

                if (response.IsSuccessStatusCode)
                {
                    string responseBody = await response.Content.ReadAsStringAsync();

                    if (responseBody != null && responseBody != "")
                    {
                        try
                        {
                            dynamic resultJson = JsonConvert.DeserializeObject<dynamic>(responseBody);
                            int blockNumber = blockNum;

                            string hash = resultJson.result.hash.ToString();
                            string parentHash = resultJson.result.parentHash.ToString();
                            string miner = resultJson.result.miner.ToString();

                            decimal blockReward = 0;

                            decimal gasLimit = fromHex.toDecimal(resultJson.result.gasLimit.ToString());
                            decimal gasUsed = fromHex.toDecimal(resultJson.result.gasUsed.ToString());

                            string insertString = "INSERT INTO blocks (blockNumber, hash, parentHash, miner, blockReward, gasLimit, gasUsed) OUTPUT INSERTED.blockID VALUES (@param0, @param1, @param2, @param3, @param4, @param5, @param6)";
                            object[] values = { blockNumber, hash, parentHash, miner, blockReward, gasLimit, gasUsed };

                            int blockID = DB.insertData(insertString, values);
                            
                            if (blockID != 0)
                            {
                                Logger.Info("Block: " + blockNum + "|Block Found");
                                Console.WriteLine("Block: " + blockNum + "|Block Found");
                                await eth_getBlockTransactionCountByNumber(blockNum, blockID);
                            }
                            else
                            {
                                Logger.Error("Block: " + blockNum + "|Block Found but cannot proceed due to data could not insert in DB");
                                Console.WriteLine("Block: " + blockNum + "|Block Found but cannot proceed due to data could not insert in DB");
                            }
                        }
                        catch (Exception ex)
                        {
                            Logger.Error("Block: " + blockNum + "|Error extracting data|ErrorMsg:" + ex.Message);
                            Console.WriteLine("Block: " + blockNum + "|Error extracting data|ErrorMsg:" + ex.Message);
                        }

                    }
                    else
                    {
                        Logger.Error("Block: " + blockNum + "|No responseBody");
                        Console.WriteLine("Block: " + blockNum + "|No responseBody");
                    }
                }
                else
                {
                    Logger.Error("Block: " + blockNum + "|Failed to get block|ResponseCode: " + response.StatusCode);
                    Console.WriteLine("Block: " + blockNum + "|Failed to get block|ResponseCode: " + response.StatusCode);
                }

            }
            catch (Exception ex)
            {
                Logger.Error("Block: " + blockNum + "|Error getting block|ErrorMsg: " + ex.Message);
                Console.WriteLine("Block: " + blockNum + "|Error getting block|ErrorMsg: " + ex.Message);
            }
            
        }

        static async Task eth_getBlockTransactionCountByNumber(int blockNum, int blockID)
        {
            try
            {
                returnObj responseMessage = await call.toAPI("eth_getBlockTransactionCountByNumber", blockNum);
                response = responseMessage.response;

                if (response.IsSuccessStatusCode)
                {
                    string responseBody = await response.Content.ReadAsStringAsync();

                    if (responseBody != null && responseBody != "")
                    {
                        try
                        {
                            dynamic resultJson = JsonConvert.DeserializeObject<dynamic>(responseBody);
                            int transCount = fromHex.toInt(resultJson.result.ToString());

                            if (transCount != 0)
                            {
                                Logger.Info("Block: " + blockNum + "|Number of transactions: " + transCount);
                                Console.WriteLine("Block: " + blockNum + "|Number of transactions: " + transCount);
                                await eth_getTransactionByBlockNumberAndIndex(blockNum, transCount, blockID);
                            }
                            else
                            {
                                Logger.Error("Block: " + blockNum + "|No transaction in block");
                                Console.WriteLine("Block: " + blockNum + "|No transaction in block");
                            }

                        }
                        catch(Exception ex)
                        {
                            Logger.Error("Block: " + blockNum + "|Error extracting transaction count data|ErrorMsg:" + ex.Message);
                            Console.WriteLine("Block: " + blockNum + "|Error extracting transaction count data|ErrorMsg:" + ex.Message);
                        }
                        
                    }
                    else
                    {
                        Logger.Error("Block: " + blockNum + "|No responseBody");
                        Console.WriteLine("Block: " + blockNum + "|No responseBody");
                    }
                }
                else
                {
                    Logger.Error("Block: " + blockNum + "|Failed to get transaction count|ResponseCode: " + response.StatusCode);
                    Console.WriteLine("Block: " + blockNum + "|Failed to get transaction count|ResponseCode: " + response.StatusCode);
                }
            }
            catch(Exception ex)
            {
                Logger.Error("Block: " + blockNum + "|Error getting transaction count|ErrorMsg: " + ex.Message);
                Console.WriteLine("Block: " + blockNum + "|Error getting transaction count|ErrorMsg: " + ex.Message);
            }

        }

        static async Task eth_getTransactionByBlockNumberAndIndex(int blockNum, int transCount, int blockID)
        {
            for (int i = 0; i <= transCount-1; i++)
            {
                try
                {
                    returnObj responseMessage = await call.toAPI("eth_getTransactionByBlockNumberAndIndex", blockNum, true, i);
                    response = responseMessage.response;

                    if (response.IsSuccessStatusCode)
                    {
                        string responseBody = await response.Content.ReadAsStringAsync();

                        if (responseBody != null && responseBody != "")
                        {
                            try
                            {
                                dynamic resultJson = JsonConvert.DeserializeObject<dynamic>(responseBody);

                                string hash = resultJson.result.hash.ToString();
                                string from = resultJson.result.from.ToString();
                                string to = resultJson.result.to.ToString();

                                decimal value = fromHex.toDecimal(resultJson.result.value.ToString());
                                decimal gas = fromHex.toDecimal(resultJson.result.gas.ToString());
                                decimal gasPrice = fromHex.toDecimal(resultJson.result.gasPrice.ToString());

                                int transactionIndex = i;

                                string insertString = "INSERT INTO transactions (blockID, hash, [from], [to], value, gas, gasPrice, transactionIndex) OUTPUT INSERTED.transactionID VALUES (@param0, @param1, @param2, @param3, @param4, @param5, @param6, @param7)";
                                object[] values = { blockID, hash, from, to, value, gas, gasPrice, transactionIndex };

                                int transID = DB.insertData(insertString, values);

                                if (transID != 0)
                                {
                                    Logger.Info("Block: " + blockNum + "|Transaction index: " + i + "|Transaction Found and inserted in DB");
                                    Console.WriteLine("Block: " + blockNum + "|Transaction index: " + i + "|Transaction Found and inserted in DB");
                                }
                                else
                                {
                                    Logger.Error("Block: " + blockNum + " | Transaction index: " + i + " | Transaction Found but data could not insert in DB");
                                    Console.WriteLine("Block: " + blockNum + " | Transaction index: " + i + " | Transaction Found but data could not insert in DB");
                                }
                            }
                            catch (Exception ex)
                            {
                                Logger.Error("Block: " + blockNum + "|Transaction index: " + i + "|Error extracting transaction data|ErrorMsg:" + ex.Message);
                                Console.WriteLine("Block: " + blockNum + "|Transaction index: " + i + "|Error extracting transaction data|ErrorMsg:" + ex.Message);
                            }
                            
                        }
                        else
                        {
                            Logger.Error("Block: " + blockNum + "|Transaction index: " + i + "|No responseBody");
                            Console.WriteLine("Block: " + blockNum + "|Transaction index: " + i + "|No responseBody");
                        }
                    }
                    else
                    {
                        Logger.Error("Block: " + blockNum + "|Transaction index: "+ i +"|Failed to get transaction details|ResponseCode: " + response.StatusCode);
                        Console.WriteLine("Block: " + blockNum + "|Transaction index: " + i + "|Failed to get transaction details|ResponseCode: " + response.StatusCode);
                    }
                }
                catch (Exception ex)
                {
                    Logger.Error("Block: " + blockNum + "|Transaction index: "+ i +"|Error getting transaction details|ErrorMsg: " + ex.Message);
                    Console.WriteLine("Block: " + blockNum + "|Transaction index: " + i + "|Error getting transaction details|ErrorMsg: " + ex.Message);
                }
                
            }
        }

        private class Database
        {
            public int insertData(string insertString, params object[] values)
            {
                int ID = 0;
                try
                {
                    string connectionString = "Data Source=localhost;Initial Catalog=Etherscan;User ID=sa;Password=H@llow0rld";
                    using (SqlConnection connection = new SqlConnection(connectionString))
                    {
                        connection.Open();
                        using (SqlCommand command = new SqlCommand(insertString, connection))
                        {
                            // Add parameters to the command object based on the values array
                            for (int i = 0; i < values.Length; i++)
                            {
                                command.Parameters.AddWithValue("@param" + i, values[i]);
                            }

                            // Open the connection, execute the query, and close the connection

                            ID = (int)command.ExecuteScalar();
                            connection.Close();
                        }
                    }
                    return ID;
                }
                catch (Exception ex)
                {
                    Logger.Error("Error inserting data: " + ex.Message);
                    Console.WriteLine("Error inserting data: " + ex.Message);
                    return ID;
                }
            }
        }        
    }
    public class convertHex
    {
        public decimal toDecimal(string hexString)
        {
            string[] parts = hexString.Substring(2).Split(',');
            hexString = parts[0];

            Int64 output = Int64.Parse(hexString, System.Globalization.NumberStyles.HexNumber);
            return (decimal)output;
        }

        public int toInt(string hexString)
        {
            string[] parts = hexString.Substring(2).Split(',');
            hexString = parts[0];

            Int32 output = Int32.Parse(hexString, System.Globalization.NumberStyles.HexNumber);
            return output;
        }
    }

    public class returnObj
    {
        public HttpResponseMessage response { get; set; }
    }

    public class CallEndpoint
    {
        private static readonly ILogger Logger = LogManager.GetCurrentClassLogger();
        public async Task<returnObj> toAPI(string actionInput, int blockNum, bool indexOn = false, int indexVal = 0)
        {
            returnObj obj = new returnObj();
            try
            {
                string endpoint = "https://api.etherscan.io/api";
                string module = "proxy";
                string action = actionInput; //eth_getBlockByNumber
                string tag = "0x" + blockNum.ToString("X");
                string boolean = "true";
                string apikey = "9JQWMHVCKIF5WRRHP1I55KSBGBQC7DIF1J";
                int index = indexVal;

                string url = endpoint + "?module=" + module + "&action=" + action + "&tag=" + tag + "&boolean=" + boolean;
                url = indexOn ? url + "&index=" + "0x" + indexVal.ToString("X") + "&apikey=" + apikey : url + "&apikey=" + apikey;

            
                HttpClient client = new HttpClient();
                obj.response = await client.GetAsync(url);

                return obj;
            }
            catch (Exception ex)
            {
                Logger.Error("Error calling endpoint: " + ex.Message);
                Console.WriteLine("Error calling endpoint: " + ex.Message);

                return obj;
            }
        }
    }
}


