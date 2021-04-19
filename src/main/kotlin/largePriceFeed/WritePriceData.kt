package largePriceFeed

import com.ib.client.EClientSocket
import java.sql.*
import java.util.Date

class WritePriceData internal constructor(
    private val data: RequestPriceData, private val socket: EClientSocket,
    private val sqlConnection: Connection?
) {
    var sysState = NULL
    private var statement: Statement? = null
    private var preparedStatement: PreparedStatement? = null
    private var resultSet: ResultSet? = null
    private var prevLastPrice = 0.0
    private var bugCounter = 0
    fun check() {
        try {
            if (data.cont.m_secType == "STK") {
                // check that lastPrice isn't a bug
                statement = sqlConnection!!.createStatement()
                resultSet = statement.executeQuery(
                    "SELECT last FROM IBAlgoSystem.price WHERE symbol='"
                            + data.cont.m_symbol + "' AND secType='STK';"
                )
                prevLastPrice = outputLastPrice(resultSet)
                resultSet = statement.executeQuery(
                    "SELECT bugCounter FROM IBAlgoSystem.price WHERE symbol='"
                            + data.cont.m_symbol + "' AND secType='STK';"
                )
                bugCounter = outputBugCounter(resultSet)
                preparedStatement = if (prevLastPrice > 0.0 &&
                    Math.abs(data.lastPrice / prevLastPrice - 1) > 0.1 &&
                    bugCounter < 3
                ) {
                    bugCounter++
                    sqlConnection.prepareStatement(
                        "UPDATE IBAlgoSystem.price SET bugCounter="
                                + Integer.toString(bugCounter) + ";"
                    )
                } else {
                    sqlConnection.prepareStatement(
                        "UPDATE IBAlgoSystem.price SET bid ="
                                + java.lang.Double.toString(data.bidPrice) + ", ask ="
                                + java.lang.Double.toString(data.askPrice) + ", last ="
                                + java.lang.Double.toString(data.lastPrice) + ", close = "
                                + java.lang.Double.toString(data.closePrice) +
                                ", bugCounter = 0, updateTime = "
                                + java.lang.Long.toString(Date().time)
                                + " WHERE symbol = '" + data.cont.m_symbol +
                                "' AND secType = 'STK' AND currency = '"
                                + data.cont.m_currency + "';"
                    )
                }
            } else if (data.cont.m_secType == "OPT") {
                preparedStatement = sqlConnection!!.prepareStatement(
                    "UPDATE IBAlgoSystem.price SET bid ="
                            + java.lang.Double.toString(data.bidPrice) + ", ask =" +
                            java.lang.Double.toString(data.askPrice) + ", last ="
                            + java.lang.Double.toString(data.lastPrice) + ", close = "
                            + java.lang.Double.toString(data.closePrice) +
                            ", updateTime = " + java.lang.Long.toString(Date().time)
                            + " WHERE symbol = '" + data.cont.m_symbol +
                            "' AND secType = 'OPT' AND currency = '"
                            + data.cont.m_currency + "' AND expiry = '"
                            + data.cont.m_expiry + "' AND strike = " +
                            java.lang.Double.toString(data.cont.m_strike) + " AND callorput = '"
                            + data.cont.m_right + "' AND multiplier = '"
                            + data.cont.m_multiplier + "';"
                )
            } else if (data.cont.m_secType == "CASH") {
                preparedStatement = sqlConnection!!.prepareStatement(
                    "UPDATE IBAlgoSystem.price SET bid ="
                            + java.lang.Double.toString(data.bidPrice) + ", ask =" +
                            java.lang.Double.toString(data.askPrice) + ", last ="
                            + java.lang.Double.toString(data.lastPrice) + ", close ="
                            + java.lang.Double.toString(data.closePrice) + ", updateTime = "
                            + java.lang.Long.toString(Date().time)
                            + " WHERE symbol = '" + data.cont.m_symbol
                            + "' AND secType = 'CASH' AND currency = '"
                            + data.cont.m_currency + "';"
                )
            }
            preparedStatement!!.executeUpdate()
        } catch (e: Exception) {
            e.printStackTrace()
        }
    }

    @Throws(SQLException::class)
    fun outputLastPrice(resultSet: ResultSet?): Double {
        var lastPrice = -1.0
        while (resultSet!!.next()) {
            lastPrice = resultSet.getDouble("last")
        }
        return lastPrice
    }

    @Throws(SQLException::class)
    fun outputBugCounter(resultSet: ResultSet?): Int {
        var bugCounter = 0
        while (resultSet!!.next()) {
            bugCounter = resultSet.getInt("bugCounter")
        }
        return bugCounter
    }

    companion object {
        const val NULL = 0
        const val LOOK = 1 shl 0
        const val LONG = 1 shl 1
        const val SHORT = 1 shl 2
        const val WAIT_FILL = 1 shl 3
        const val WAIT_CANCEL = 1 shl 4
        private const val nextOrderId = 1
    }

    init {
        sysState = LOOK
    }
}