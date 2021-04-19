import com.ib.client.EClientSocket
import com.ib.client.EWrapper
import tradeStrategy.TradeStrategyWrapper
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.ResultSet

class KillSwitch {
    var sqlConnection: Connection? = null
    var preparedStatement: PreparedStatement? = null
    var resultSet: ResultSet? = null

    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            val runProcess = KillSwitch()
        }
    }

    init {

        // connect to SQL
        try {
            Class.forName("com.mysql.jdbc.Driver")
            sqlConnection = DriverManager.getConnection(
                "jdbc:mysql://localhost/IBAlgoSystem?user=user&password=pw"
            )


            // connect to socket
            val tradeStrategyWrapper: EWrapper = TradeStrategyWrapper(sqlConnection)
            val socket = EClientSocket(tradeStrategyWrapper)
            socket.eConnect("", 4002, 1000)
            try {
                while (!socket.isConnected);
            } catch (e: Exception) {
            }


            // find outstanding active orders
            preparedStatement = sqlConnection.prepareStatement(
                "SELECT orderID FROM IBAlgoSystem.orderTracking WHERE "
                        + "status <> 'Filled' AND status <> 'Cancelled' AND "
                        + "status <> 'ApiCancelled' AND status <> 'Inactive';"
            )
            resultSet = preparedStatement.executeQuery()

            // submit cancel request
            while (resultSet.next()) {
                val orderId = resultSet.getInt("orderID")
                socket.cancelOrder(orderId)
            }

            // wait for all orders to cancel
            var ContinueProcess = false
            preparedStatement = sqlConnection.prepareStatement(
                "SELECT COUNT(*) FROM IBAlgoSystem.orderTracking WHERE "
                        + "status <> 'Filled' AND status <> 'Cancelled' AND "
                        + "status <> 'ApiCancelled' AND status <> 'Inactive';"
            )
            while (!ContinueProcess) {
                resultSet = preparedStatement.executeQuery()
                if (resultSet.getInt("COUNT(*)") == 0) {
                    ContinueProcess = true
                }
            }
            socket.eDisconnect()
        } catch (e: Exception) {
            e.printStackTrace()
        }
    }
}