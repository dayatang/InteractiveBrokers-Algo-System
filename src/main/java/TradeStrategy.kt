import java.sql.Connection

// template to implement trade strategies
class TradeStrategy {
    var sqlConnection: Connection? = null
    var preparedStatement: PreparedStatement? = null
    var resultSet: ResultSet? = null

    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            val runProcess = TradeStrategy()
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
            val tradeStrategyWrapper: EWrapper = TradeStrategyWrapper()
            val socket = EClientSocket(tradeStrategyWrapper)
            socket.eConnect("", 7496, 1000)
            try {
                while (!socket.isConnected());
            } catch (e: Exception) {
            }


            // load account summary
            socket.reqAccountSummary(
                7496, "All", "NetLiquidation,"
                        + "TotalCashValue, SettledCash, AccruedCash, BuyingPower,"
                        + "EquityWithLoanValue, PreviousEquityWithLoanValue,"
                        + "GrossPositionValue, ReqTEquity, ReqTMargin, SMA, "
                        + "InitMarginReq, MaintMarginReq, AvailableFunds,"
                        + "ExcessLiquidity, Cushion, FullInitMarginReq,"
                        + "FullMaintMarginReq, FullAvailableFunds, FullExcessLiquidity,"
                        + "LookAheadNextChange, LookAheadInitMarginReq,"
                        + "LookAheadMaintMarginReq, LookAheadAvailableFunds,"
                        + "LookAheadExcessLiquidity, HighestSeverity, Leverage"
            )


            // Here, call core functions to fetch prices, submit orders, manage 
            // margin limit, etc.
        } catch (e: Exception) {
            e.printStackTrace()
        }
    }
}