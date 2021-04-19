package fetchOptionsChain

import com.ib.client.*
import java.sql.*

class RequestOptionChain(
    val cont: Contract, private val socket: EClientSocket,
    private val sqlConnection: Connection?
) {
    private val myId: Int
    private fun reqOptionData() {
        socket.reqContractDetails(myId, cont)
    }

    companion object {
        private var nextId = 1
    }

    init {
        myId = nextId++
        socket.requestOptionChainWrapper()
        reqOptionData()
    }
}