import com.ib.client.*
import largePriceFeed.RequestPriceData
import largePriceFeed.RequestPriceWrapper
import java.io.BufferedReader
import java.io.FileReader
import java.io.IOException
import java.sql.*
import java.util.Date

// receive live price streams from
// The number of entries may exceed ib's feed limit (FEED_LIMIT)
// This script divides the entries into smaller batches to submit to ib
// Output prices to IBAlgoSystem.price
class LargePriceFeed {
    private val socket: EClientSocket? = null
    var delimiter = ","
    var delimiter_under = "_"
    var TickerListCSVFile = "TickerList.csv"
    var TickerLines: Array<String?>
    var FileUpdated: MutableMap<Int, Boolean> = HashMap()
    var updateTime: Long = 0
    var dt_now: Long = 0
    var AllUpdated = false
    var running_updated = false
    var FEED_LIMIT = 100
    var sqlConnection: Connection? = null
    var preparedStatement: PreparedStatement? = null
    var resultSet: ResultSet? = null

    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            try {
                val runProcess = LargePriceFeed()
            } catch (e: Exception) {
                e.printStackTrace()
            }
        }
    }

    init {
        var NUM_FEEDS = 0

        // find NUM_FEEDS from 100 - rows of TickerList
        try {
            BufferedReader(FileReader(TickerListCSVFile)).use { br3 ->
                var input3: String
                while (br3.readLine().also { input3 = it } != null) {
                    if (input3.split(delimiter).toTypedArray()[0] != "active") {
                        NUM_FEEDS++
                    }
                }
                br3.close()
            }
        } catch (e: IOException) {
            e.printStackTrace()
        }

        // make sure NUM_FEEDS is sufficient
        if (NUM_FEEDS >= FEED_LIMIT - 5) {
            println("Not enough sockets for feed!")
        } else {
            NUM_FEEDS = FEED_LIMIT - 5 - NUM_FEEDS

            // connect to SQL
            try {
                Class.forName("com.mysql.jdbc.Driver")
                sqlConnection = DriverManager.getConnection(
                    "jdbc:mysql://localhost/IBAlgoSystem?user=user&password=pw"
                )

                // find number of rows in MasterChainList
                preparedStatement = sqlConnection.prepareStatement(
                    "SELECT COUNT(*) FROM IBAlgoSystem.MasterChainList;"
                )
                resultSet = preparedStatement.executeQuery()
                val rows = resultSet.getInt("COUNT(*)")


                // write values of MasterChainList to TickerLines, dummy price
                // TickerLines format: active, symbol, secType, exchange, 
                // currency, expiry, strike, right, multiplier, pennyPilot, 
                // moneyness
                TickerLines = arrayOfNulls(rows)
                preparedStatement = sqlConnection.prepareStatement(
                    "SELECT * FROM IBAlgoSystem.MasterChainList"
                )
                resultSet = preparedStatement.executeQuery()
                val row_iter = 0
                while (resultSet.next()) {
                    val symbol = resultSet.getString("symbol")
                    val exchange = resultSet.getString("exchange")
                    val currency = resultSet.getString("currency")
                    val expiry = resultSet.getString("expiry")
                    val strike = resultSet.getDouble("strike")
                    val right = resultSet.getString("callorput")
                    val multiplier = resultSet.getString("multiplier")
                    val pennyPilot = resultSet.getString("pennyPilot")
                    val moneyness = resultSet.getString("moneyness")
                    val bid = -1.0
                    val ask = -1.0

                    // delete previous entry
                    preparedStatement = sqlConnection.prepareStatement(
                        "DELETE FROM IBAlgoSystem.price WHERE symbol = '"
                                + symbol + "' and secType = 'OPT' and currency = '"
                                + currency + "' and expiry = '" + expiry +
                                "' and strike = " + java.lang.Double.toString(strike) +
                                " and callorput = '" + right + "' and multiplier = '"
                                + multiplier + "';"
                    )
                    preparedStatement.executeUpdate()

                    // write new entry
                    preparedStatement = sqlConnection.prepareStatement(
                        "INSERT INTO IBAlgoSystem.price (entry, symbol, "
                                + "secType, currency, expiry, strike, callorput, "
                                + "multiplier, bid, ask, last, close, bugCounter, "
                                + "updateTime) VALUES (default,'" + symbol +
                                "', 'OPT', '" + currency + "', '" + expiry + "', "
                                + java.lang.Double.toString(strike) + ", '" + right + "', '"
                                + multiplier + "', 0.0, 0.01, -1.0, -1.0, 0, 0);"
                    )
                    preparedStatement.executeUpdate()
                }


                // divide the list of names into batches of NUM_FEEDS
                val num_batches = rows / NUM_FEEDS + 1


                // connect to socket
                val requestPriceWrapper: EWrapper = RequestPriceWrapper()
                val socket = EClientSocket(requestPriceWrapper)


                // update prices by batch        

                // connect to socket
                socket.eConnect(null, 4002, 101)
                try {
                    while (!socket.isConnected);
                } catch (e: Exception) {
                }

                // add while loop to make perpeptual
                while (true) {
                    for (i in 0 until num_batches) {

                        // send price feed requests
                        for (j in 0 until NUM_FEEDS) {
                            if (i * NUM_FEEDS + j < rows) {
                                // submit a new contract for every request
                                val line = TickerLines[i * NUM_FEEDS + j]
                                val cont = Contract()
                                cont.m_symbol = line!!.split(delimiter_under).toTypedArray()[1]
                                cont.m_secType = line.split(delimiter_under).toTypedArray()[2]
                                // cont.m_exchange = line.split(delimiter)[3];
                                cont.m_exchange = "SMART"
                                cont.m_currency = line.split(delimiter_under).toTypedArray()[4]
                                cont.m_expiry = line.split(delimiter_under).toTypedArray()[5]
                                cont.m_strike = line.split(delimiter_under).toTypedArray()[6].toDouble()
                                cont.m_right = line.split(delimiter_under).toTypedArray()[7]
                                cont.m_multiplier = line.split(delimiter_under).toTypedArray()[8]
                                FileUpdated[i * NUM_FEEDS + j] = false
                                val data = RequestPriceData(
                                    cont,
                                    true, socket, sqlConnection
                                )
                            } else {
                                FileUpdated[i * NUM_FEEDS + j] = true
                            }
                        }


                        // check price entry is updated to continue
                        AllUpdated = false
                        while (!AllUpdated) {
                            for (j in 0 until NUM_FEEDS) {
                                if (!FileUpdated[i * NUM_FEEDS + j]!!) {
                                    val line = TickerLines[i * NUM_FEEDS + j]
                                    val symbol = line!!.split(delimiter_under).toTypedArray()[1]
                                    val exchange = line.split(delimiter_under).toTypedArray()[3]
                                    val currency = line.split(delimiter_under).toTypedArray()[4]
                                    val expiry = line.split(delimiter_under).toTypedArray()[5]
                                    val strike = line.split(delimiter_under).toTypedArray()[6].toDouble()
                                    val right = line.split(delimiter_under).toTypedArray()[7]
                                    val multiplier = line.split(delimiter_under).toTypedArray()[8]
                                    preparedStatement = sqlConnection.prepareStatement(
                                        "SELECT updateTime FROM IBAlgoSystem.price WHERE symbol = '"
                                                + symbol + "' and secType = 'OPT' and currency = '"
                                                + currency + "' and expiry = '" + expiry
                                                + "' and strike = " + java.lang.Double.toString(strike)
                                                + " and callorput = '" + right
                                                + "' and multiplier = '" + multiplier + "';"
                                    )
                                    resultSet = preparedStatement.executeQuery()
                                    while (resultSet.next()) {
                                        updateTime = resultSet.getLong("updateTime")
                                    }

                                    // check the last (last updated field) is actually updated, and within 5*NUM_FEEDS secs
                                    if (fetchPrice.FetchSTKPrice(symbol, "USD", sqlConnection).get(2) > -0.01 &&
                                        Date().time - updateTime < 5 * NUM_FEEDS * 1000
                                    ) {
                                        FileUpdated[i * NUM_FEEDS + j] = true
                                    }
                                }
                            }
                            for (j in 0 until NUM_FEEDS) {
                                running_updated = true
                                if (!FileUpdated[i * NUM_FEEDS + j]!!) {
                                    running_updated = false
                                    break
                                }
                            }
                            if (running_updated) {
                                AllUpdated = true
                            }
                        }

                        // pause for 1 sec between each batch
                        dt_now = Date().time
                        while (Date().time - dt_now < 1 * 1000);
                    }

                    // pause for 1 min between each loop
                    if (num_batches < 60) {
                        dt_now = Date().time
                        while (Date().time - dt_now < 60 * 1000);
                    }
                }
            } catch (e: Exception) {
                e.printStackTrace()
            }
        }
    }
}