package tradeStrategy

import com.ib.client.Contract
import com.ib.client.Execution
import com.ib.client.Order
import com.ib.client.UnderComp
import java.sql.Connection

class TradeStrategyWrapper(private val sqlConnection: Connection?) : EWrapper {
    private var preparedStatement: PreparedStatement? = null
    var fetchOrderStatus = FetchOrderStatus()
    override fun bondContractDetails(reqId: Int, contractDetails: ContractDetails) {}
    override fun contractDetails(reqId: Int, contractDetails: ContractDetails) {}
    override fun contractDetailsEnd(reqId: Int) {}
    override fun fundamentalData(reqId: Int, data: String) {}
    fun bondContractDetails(contractDetails: ContractDetails?) {}
    fun contractDetails(contractDetails: ContractDetails?) {}
    override fun currentTime(time: Long) {}
    override fun displayGroupList(requestId: Int, contraftInfo: String) {}
    override fun displayGroupUpdated(requestId: Int, contractInfo: String) {}
    override fun verifyCompleted(completed: Boolean, contractInfo: String) {}
    override fun verifyMessageAPI(message: String) {}
    override fun execDetails(orderId: Int, contract: Contract, execution: Execution) {}
    override fun execDetailsEnd(reqId: Int) {}
    fun historicalData(
        reqId: Int, date: String?, open: Double, high: Double,
        low: Double, close: Double, volume: Int, count: Int, WAP: Double,
        hasGaps: Boolean
    ) {
    }

    override fun managedAccounts(accountsList: String) {}
    override fun commissionReport(cr: CommissionReport) {}
    fun position(
        account: String?, contract: Contract?, pos: Int,
        avgCost: Double
    ) {
    }

    override fun positionEnd() {}
    override fun accountSummary(
        reqId: Int, account: String, tag: String,
        value: String, currency: String
    ) {
        try {
            preparedStatement = sqlConnection!!.prepareStatement(
                "UPDATE IBAlgoSystem.margin SET $tag = $value;"
            )
            preparedStatement.executeUpdate()
        } catch (e: Exception) {
            e.printStackTrace()
        }
    }

    override fun accountSummaryEnd(reqId: Int) {}
    override fun accountDownloadEnd(accountName: String) {}
    override fun openOrder(
        orderId: Int, contract: Contract, order: Order,
        orderState: OrderState
    ) {
    }

    override fun openOrderEnd() {}
    fun orderStatus(
        orderId: Int, status: String, filled: Int,
        remaining: Int, avgFillPrice: Double, permId: Int, parentId: Int,
        lastFillPrice: Double, clientId: Int, whyHeld: String?
    ) {
        fetchOrderStatus.WriteOrderStatus(
            orderId, status, filled, remaining,
            avgFillPrice, sqlConnection
        )
    }

    override fun receiveFA(faDataType: Int, xml: String) {}
    override fun scannerData(
        reqId: Int, rank: Int, contractDetails: ContractDetails,
        distance: String, benchmark: String, projection: String, legsStr: String
    ) {
    }

    override fun scannerDataEnd(reqId: Int) {}
    override fun scannerParameters(xml: String) {}
    override fun tickEFP(
        symbolId: Int, tickType: Int, basisPoints: Double,
        formattedBasisPoints: String, impliedFuture: Double, holdDays: Int,
        futureExpiry: String, dividendImpact: Double, dividendsToExpiry: Double
    ) {
    }

    override fun tickGeneric(symbolId: Int, tickType: Int, value: Double) {}
    override fun tickOptionComputation(
        tickerId: Int, field: Int,
        impliedVol: Double, delta: Double, optPrice: Double, pvDividend: Double,
        gamma: Double, vega: Double, theta: Double, undPrice: Double
    ) {
    }

    fun deltaNeutralValidation(reqId: Int, underComp: UnderComp?) {}
    override fun updateAccountTime(timeStamp: String) {}
    override fun updateAccountValue(
        key: String, value: String, currency: String,
        accountName: String
    ) {
    }

    override fun updateMktDepth(
        symbolId: Int, position: Int, operation: Int,
        side: Int, price: Double, size: Int
    ) {
    }

    fun updateMktDepthL2(
        symbolId: Int, position: Int, marketMaker: String?,
        operation: Int, side: Int, price: Double, size: Int
    ) {
    }

    override fun updateNewsBulletin(
        msgId: Int, msgType: Int, message: String,
        origExchange: String
    ) {
    }

    fun updatePortfolio(
        contract: Contract?, position: Int,
        marketPrice: Double, marketValue: Double, averageCost: Double,
        unrealizedPNL: Double, realizedPNL: Double, accountName: String?
    ) {
    }

    override fun marketDataType(reqId: Int, marketDataType: Int) {}
    override fun tickSnapshotEnd(tickerId: Int) {}
    override fun connectionClosed() {}
    override fun realtimeBar(
        reqId: Int, time: Long, open: Double, high: Double,
        low: Double, close: Double, volume: Long, wap: Double, count: Int
    ) {
    }

    override fun tickSize(orderId: Int, field: Int, size: Int) {}
    override fun tickString(orderId: Int, tickType: Int, value: String) {}
    override fun error(e: Exception) {
        e.printStackTrace()
    }

    override fun error(str: String) {
        System.err.println(str)
    }

    override fun error(id: Int, errorCode: Int, errorMsg: String) {
        System.err.println("error: $id,$errorCode,$errorMsg")
    }

    override fun nextValidId(orderId: Int) {}
    fun tickPrice(
        orderId: Int, field: Int, price: Double,
        canAutoExecute: Int
    ) {
    }
}