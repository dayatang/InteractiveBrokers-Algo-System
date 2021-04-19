import com.ib.client.*
import simplePriceFeed.RequestPriceData
import simplePriceFeed.RequestPriceWrapper
import java.io.BufferedReader
import java.io.FileReader
import java.io.IOException
import java.sql.*

// TickerList.csv fields: Active (T/F) | symbol | secType | exchange | currency
// | expiry | strike | right | multiplier
// Output prices to IBAlgoSystem.price
class SimplePriceFeed {
    var sqlConnection: Connection? = null
    var preparedStatement: PreparedStatement? = null

    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            try {
                val runProcess = SimplePriceFeed()
            } catch (e: Exception) {
                e.printStackTrace()
            }
        }
    }

    init {
        // read in TickerList.csv
        val csvFile = "TickerList.csv"
        val delimiter = ","

        // find number of rows in TickerList
        var rows = 0
        try {
            BufferedReader(FileReader(csvFile)).use { br1 ->
                var input1: String
                while (br1.readLine().also { input1 = it } != null) {
                    if (input1.split(delimiter).toTypedArray()[0] == "T") {
                        rows++
                    }
                }
                br1.close()
            }
        } catch (e: IOException) {
            e.printStackTrace()
        }


        // connect to IBAlgoSystem.price, clear previous data
        Class.forName("com.mysql.jdbc.Driver")
        sqlConnection = DriverManager.getConnection(
            "jdbc:mysql://localhost/IBAlgoSystem?user=user&password=pw"
        )
        // this line assumes TickerList.csv only contains stocks (secType = STK)
        preparedStatement = sqlConnection.prepareStatement(
            "DELETE FROM IBAlgoSystem.price WHERE secType = 'STK'"
        )
        preparedStatement.executeUpdate()


        // write dummy values of TickerList to IBAlgoSystem.price
        val TickerLines = arrayOfNulls<String>(rows)
        try {
            BufferedReader(FileReader(csvFile)).use { br2 ->
                var input2: String
                var row_iter = 0
                while (br2.readLine().also { input2 = it } != null) {
                    if (input2.split(delimiter).toTypedArray()[0] == "T") {
                        if (input2.split(delimiter).toTypedArray()[2] == "STK") {
                            preparedStatement = sqlConnection.prepareStatement(
                                "INSERT INTO IBAlgoSystem.price (entry, symbol, "
                                        + "secType, currency, bid, ask, last, close, "
                                        + "bugCounter, updateTime) VALUES (default,'"
                                        + input2.split(delimiter).toTypedArray()[1] + "','"
                                        + input2.split(delimiter).toTypedArray()[2] + "','"
                                        + input2.split(delimiter).toTypedArray()[4]
                                        + "', -1.0, -1.0, -1.0, -1.0, 0, 0)"
                            )
                        } else if (input2.split(delimiter).toTypedArray()[2] == "OPT") {
                            preparedStatement = sqlConnection.prepareStatement(
                                "DELETE FROM IBAlgoSystem.price WHERE symbol = '"
                                        + input2.split(delimiter).toTypedArray()[1] +
                                        "' and secType = 'OPT' and currency = '"
                                        + input2.split(delimiter).toTypedArray()[4] + "' and expiry = '"
                                        + input2.split(delimiter).toTypedArray()[5] + "' and strike = "
                                        + input2.split(delimiter).toTypedArray()[6] + " and callorput = '"
                                        + input2.split(delimiter).toTypedArray()[7]
                                        + "' and multiplier = '"
                                        + input2.split(delimiter).toTypedArray()[8] + "';"
                            )
                            preparedStatement.executeUpdate()
                            preparedStatement = sqlConnection.prepareStatement(
                                "INSERT INTO IBAlgoSystem.price (entry, symbol, "
                                        + "secType, currency, expiry, strike, callorput, "
                                        + "multiplier, bid, ask, last, close, bugCounter, "
                                        + "updateTime) VALUES (default,'" +
                                        input2.split(delimiter).toTypedArray()[1] + "','"
                                        + input2.split(delimiter).toTypedArray()[2] + "','"
                                        + input2.split(delimiter).toTypedArray()[4] + "','"
                                        + input2.split(delimiter).toTypedArray()[5] + "',"
                                        + input2.split(delimiter).toTypedArray()[6] + ",'"
                                        + input2.split(delimiter).toTypedArray()[7] + "','"
                                        + input2.split(delimiter).toTypedArray()[8]
                                        + "', -1.0, -1.0, -1.0, -1.0, 0, 0)"
                            )
                        }
                        preparedStatement.executeUpdate()
                        TickerLines[row_iter] = input2
                        row_iter++
                    }
                }
                br2.close()
            }
        } catch (e: IOException) {
            e.printStackTrace()
        }

        // connect to IB socket
        val requestPriceWrapper: EWrapper = RequestPriceWrapper()
        val socket = EClientSocket(requestPriceWrapper)
        socket.eConnect("", 4002, 0)
        try {
            while (!socket.isConnected);
        } catch (e: Exception) {
        }

        // request live data setting
        socket.reqMarketDataType(1)

        // submit a new contract for every request
        for (i in 0 until rows) {
            val line = TickerLines[i]
            val cont = Contract()
            cont.m_symbol = line!!.split(delimiter).toTypedArray()[1]
            cont.m_secType = line.split(delimiter).toTypedArray()[2]
            cont.m_exchange = line.split(delimiter).toTypedArray()[3]
            cont.m_currency = line.split(delimiter).toTypedArray()[4]
            if (cont.m_secType == "OPT") {
                cont.m_expiry = line.split(delimiter).toTypedArray()[5]
                cont.m_strike = line.split(delimiter).toTypedArray()[6].toDouble()
                cont.m_right = line.split(delimiter).toTypedArray()[7]
                cont.m_multiplier = line.split(delimiter).toTypedArray()[8]
            }
            val data = RequestPriceData(
                cont, socket,
                sqlConnection
            )
        }
    }
}