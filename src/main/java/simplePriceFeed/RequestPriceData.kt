package simplePriceFeed

import com.ib.client.*
import java.sql.*

class RequestPriceData(
    val cont: Contract, private val socket: EClientSocket,
    private val sqlConnection: Connection?
) {
    private val writePriceData: WritePriceData
    private val myId: Int
    var bidPrices: MutableList<Double> = ArrayList()
    var askPrices: MutableList<Double> = ArrayList()
    var lastPrices: MutableList<Double> = ArrayList()
    var closePrices: MutableList<Double> = ArrayList()
    var bidPrice = -1.0
    var askPrice = -1.0
    var lastPrice = -1.0
    var closePrice = -1.0
    private fun reqData() {
        socket.reqMktData(myId, cont, "", false, null)
    }

    // record bid price
    fun dataRecdBid(inputPrice: Double) {
        bidPrice = inputPrice
        bidPrices.add(inputPrice)
        writePriceData.check()
    }

    // record ask price
    fun dataRecdAsk(inputPrice: Double) {
        askPrice = inputPrice
        askPrices.add(inputPrice)
        writePriceData.check()
    }

    // record last price
    fun dataRecdLast(inputPrice: Double) {
        lastPrice = inputPrice
        lastPrices.add(inputPrice)
        writePriceData.check()
    }

    // record close price
    fun dataRecdClose(inputPrice: Double) {
        closePrice = inputPrice
        closePrices.add(inputPrice)
        writePriceData.check()
    }

    companion object {
        private var nextId = 1
    }

    init {
        writePriceData = WritePriceData(this, socket, sqlConnection)
        myId = nextId++
        (socket.requestPriceWrapper() as RequestPriceWrapper).dataMap[myId] = this
        reqData()
    }
}