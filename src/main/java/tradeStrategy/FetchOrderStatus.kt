package tradeStrategy

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException

class FetchOrderStatus {
    var num_fields = 4
    private val sqlConnection: Connection? = null
    private var preparedStatement: PreparedStatement? = null
    private var resultSet: ResultSet? = null
    fun ReadOrderStatus(orderId: Int, sqlConnection: Connection): Array<String?> {
        val order_stats = arrayOfNulls<String>(num_fields)
        var check_order_stats: StringBuilder? = null
        var checks_out = false
        while (!checks_out) {
            try {
                preparedStatement = sqlConnection.prepareStatement(
                    "SELECT status FROM IBAlgoSystem.orderTracking WHERE orderID = "
                            + Integer.toString(orderId) + ";"
                )
                resultSet = preparedStatement.executeQuery()
                while (resultSet.next()) {
                    check_order_stats = StringBuilder(
                        resultSet.getString(
                            "status"
                        )
                    )
                }
                if (check_order_stats.toString() == "PendingSubmit" ||
                    check_order_stats.toString() == "PendingCancel" ||
                    check_order_stats.toString() == "PreSubmitted" ||
                    check_order_stats.toString() == "ApiCancelled" ||
                    check_order_stats.toString() == "Cancelled" ||
                    check_order_stats.toString() == "Filled" ||
                    check_order_stats.toString() == "Inactive"
                ) {
                    order_stats[0] = check_order_stats.toString()
                    preparedStatement = sqlConnection.prepareStatement(
                        "SELECT filled FROM IBAlgoSystem.orderTracking WHERE "
                                + "orderID = " + Integer.toString(orderId) + ";"
                    )
                    resultSet = preparedStatement.executeQuery()
                    order_stats[1] = Integer.toString(
                        outputSQLInt(
                            resultSet,
                            "filled"
                        )
                    )
                    preparedStatement = sqlConnection.prepareStatement(
                        "SELECT remaining FROM IBAlgoSystem.orderTracking WHERE "
                                + "orderID = " + Integer.toString(orderId) + ";"
                    )
                    resultSet = preparedStatement.executeQuery()
                    order_stats[2] = Integer.toString(
                        outputSQLInt(
                            resultSet,
                            "remaining"
                        )
                    )
                    preparedStatement = sqlConnection.prepareStatement(
                        "SELECT avgFillPrice FROM IBAlgoSystem.orderTracking "
                                + "WHERE orderID = " + Integer.toString(orderId) + ";"
                    )
                    resultSet = preparedStatement.executeQuery()
                    order_stats[3] = java.lang.Double.toString(
                        outputSQLDouble(
                            resultSet,
                            "avgFillPrice"
                        )
                    )
                    checks_out = true
                }
            } catch (e: Exception) {
            }
        }
        return order_stats
    }

    fun WriteOrderStatus(
        orderID: Int, status: String, filled: Int,
        remaining: Int, avgFillPrice: Double, sqlConnection: Connection?
    ) {
        try {
            preparedStatement = sqlConnection!!.prepareStatement(
                "UPDATE IBAlgoSystem.orderTracking SET orderID = "
                        + Integer.toString(orderID) + ", status = '" + status
                        + "', filled = " + Integer.toString(filled) + ", remaining = "
                        + Integer.toString(remaining) + ", avgFillPrice = "
                        + java.lang.Double.toString(avgFillPrice) + " WHERE orderID = "
                        + Integer.toString(orderID) + ";"
            )
            preparedStatement.executeUpdate()
        } catch (e: Exception) {
            e.printStackTrace()
        }
    }

    fun OrderRecorded(orderID: Int, sqlConnection: Connection): Boolean {
        var order_recorded = false
        try {
            preparedStatement = sqlConnection.prepareStatement(
                "SELECT COUNT(*) FROM IBAlgoSystem.orderTracking WHERE orderID = "
                        + Integer.toString(orderID) + ";"
            )
            resultSet = preparedStatement.executeQuery()
            while (resultSet.next()) {
                if (resultSet.getInt("COUNT(*)") > 0) {
                    order_recorded = true
                }
            }
        } catch (e: Exception) {
            e.printStackTrace()
        }
        return order_recorded
    }

    @Throws(SQLException::class)
    fun outputSQLInt(resultSet: ResultSet?, field_name: String?): Int {
        var output_value = -1
        while (resultSet!!.next()) {
            output_value = resultSet.getInt(field_name)
        }
        return output_value
    }

    @Throws(SQLException::class)
    fun outputSQLDouble(resultSet: ResultSet?, field_name: String?): Double {
        var output_value = -1.0
        while (resultSet!!.next()) {
            output_value = resultSet.getInt(field_name).toDouble()
        }
        return output_value
    }
}