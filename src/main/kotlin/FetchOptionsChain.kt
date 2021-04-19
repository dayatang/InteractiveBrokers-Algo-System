import com.ib.client.Contract
import com.ib.client.EClientSocket
import com.ib.client.EWrapper
import fetchOptionsChain.RequestOptionChain
import fetchOptionsChain.RequestOptionChainWrapper
import java.io.BufferedReader
import java.io.FileReader
import java.io.IOException
import java.sql.*
import java.util.*

// TickerList.csv fields: Active (T/F) | symbol | secType | exchange | currency
// | expiry | strike | right | multiplier
// Read in PennyPilot.csv, a list of Penny Pilot tickers
// Output option chain to IBAlgoSystem.MasterChainList
class FetchOptionsChain {
    private val socket: EClientSocket? = null
    var delimiter = ","
    var delimiter_under = "_"
    var csvFile = "TickerList.csv"
    var PennyPilotFile = "PennyPilot.csv"
    var TickerList: Array<String?>
    var counter_iter = 0
    var price = -1.0
    var sqlConnection: Connection? = null
    var preparedStatement: PreparedStatement? = null
    var resultSet: ResultSet? = null
    fun sqlClose() {
        try {
            if (resultSet != null) {
                resultSet!!.close()
            }
            if (sqlConnection != null) {
                sqlConnection!!.close()
            }
        } catch (e: Exception) {
        }
    }

    @Throws(SQLException::class)
    private fun WriteInt(resultSet: ResultSet?, column_name: String): Int {
        var output_counter = 0
        while (resultSet!!.next()) {
            output_counter = resultSet.getInt(column_name)
        }
        return output_counter
    }

    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            try {
                val runProcess = FetchOptionsChain()
            } catch (e: Exception) {
                e.printStackTrace()
            }
        }
    }

    init {
        val PennyPilotMap: MutableMap<String, String> = HashMap()
        var foundPennyTick: Boolean

        // load Penny Pilot Tickers
        val PennyPilotTickers: MutableList<String> = ArrayList()
        var temp_ticker = StringBuilder("")
        try {
            BufferedReader(FileReader(PennyPilotFile)).use { br1 ->
                var input1: String
                while (br1.readLine().also { input1 = it } != null) {
                    if (input1.split(delimiter).toTypedArray()[0] != temp_ticker.toString()) {
                        temp_ticker = StringBuilder(input1.split(delimiter).toTypedArray()[0])
                        PennyPilotTickers.add(temp_ticker.toString())
                    }
                }
                br1.close()
            }
        } catch (e: IOException) {
            e.printStackTrace()
        }
        Collections.sort(PennyPilotTickers)


        // find number of rows in TickerList
        var rows = 0
        try {
            BufferedReader(FileReader(csvFile)).use { br2 ->
                var input2: String?
                while (br2.readLine().also { input2 = it } != null) {
                    rows++
                }
                br2.close()
            }
        } catch (e: IOException) {
            e.printStackTrace()
        }


        // write values of TickerList
        TickerList = arrayOfNulls(rows)
        try {
            BufferedReader(FileReader(csvFile)).use { br3 ->
                var input3: String?
                var row_iter = 0
                while (br3.readLine().also { input3 = it } != null) {
                    TickerList[row_iter] = input3
                    row_iter++
                }
                br3.close()
            }
        } catch (e: IOException) {
            e.printStackTrace()
        }


        // connect to sql, update counter
        try {
            Class.forName("com.mysql.jdbc.Driver")
            sqlConnection = DriverManager.getConnection(
                "jdbc:mysql://localhost/IBAlgoSystem?user=user&password=pw"
            )
            preparedStatement = sqlConnection.prepareStatement(
                "UPDATE IBAlgoSystem.counter SET counter = 0"
            )
            preparedStatement.executeUpdate()
            preparedStatement = sqlConnection.prepareStatement(
                "TRUNCATE TABLE IBAlgoSystem.MasterChainList"
            )
            preparedStatement.executeUpdate()


            // write PennyPilotMap
            for (i in TickerList.indices) {
                foundPennyTick = false
                for (j in PennyPilotTickers.indices) {
                    if (TickerList[i]!!.split(delimiter).toTypedArray()[1] == PennyPilotTickers[j]) {
                        foundPennyTick = true
                        break
                    }
                }
                if (foundPennyTick) {
                    PennyPilotMap[TickerList[i]!!.split(delimiter).toTypedArray()[1]] = "T"
                } else {
                    PennyPilotMap[TickerList[i]!!.split(delimiter).toTypedArray()[1]] = "F"
                }
            }


            // connect to socket
            val requestOptionChainWrapper: EWrapper = RequestOptionChainWrapper(sqlConnection)
            val socket = EClientSocket(requestOptionChainWrapper)
            socket.eConnect(null, 4002, 100)
            try {
                while (!socket.isConnected);
            } catch (e: Exception) {
            }

            // submit a new contract for every request
            for (i in 0 until rows) {
                val line = TickerList[i]
                val cont = Contract()
                cont.m_symbol = line!!.split(delimiter).toTypedArray()[1]
                cont.m_secType = "OPT"
                cont.m_exchange = "SMART"
                cont.m_currency = line.split(delimiter).toTypedArray()[4]
                cont.m_multiplier = "100"
                val data = RequestOptionChain(cont, socket, sqlConnection)
            }


            // check counter to disconnect socket
            preparedStatement = sqlConnection.prepareStatement(
                "SELECT counter FROM IBAlgoSystem.counter;"
            )
            resultSet = preparedStatement.executeQuery()
            counter_iter = WriteInt(resultSet, "counter")
            while (counter_iter < rows) {
                resultSet = preparedStatement.executeQuery()
                counter_iter = WriteInt(resultSet, "counter")
            }
            socket.eDisconnect()
        } catch (e: Exception) {
            e.printStackTrace()
        } finally {
            sqlClose()
        }
    }
}