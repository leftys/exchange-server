import asyncio
import asyncio.streams
import json
import datetime
import exchange
import abc


class GenericServer(metaclass=abc.ABCMeta):
    """
    TCP server base class.
    """

    def __init__(self, host: str, port: int, exchange_obj: exchange.Exchange):
        self.server = None
        self.host = host
        self.port = port
        self.next_clientid = 0
        self.exchange = exchange_obj
        self.clients = {}  # task -> (reader, writer)

    def _client_done(self, task):
        try:
            print("Client %d disconnected" % task.result())
            del self.clients[task.result()]
        except:
            print("Client forced to disconnect.")

    def _accept_client(self, client_reader, client_writer):
        clientid = self.exchange.get_clientid()
        task = asyncio.Task(self._handle_client(clientid, client_reader, client_writer))
        self.clients[clientid] = (client_reader, client_writer)
        task.add_done_callback(self._client_done)

    @abc.abstractmethod
    async def _handle_client(self, clientid, client_reader, client_writer):
        raise NotImplementedError("This is a method of abstract class")

    async def _send_json(self, writer, json_str):
        writer.write((json.dumps(json_str)).encode("utf-8"))
        writer.write("\n".encode("utf-8"))
        await writer.drain()

    def start(self, loop: asyncio.AbstractEventLoop):
        """Start listening on specified address and port."""
        self.server = loop.run_until_complete(
            asyncio.streams.start_server(self._accept_client,
                                         self.host, self.port,
                                         loop=loop))

    def stop(self, loop: asyncio.AbstractEventLoop):
        """Abort all client connections and stop listening."""
        if self.server is not None:
            self.server.close()
            for task in asyncio.Task.all_tasks():
                task.cancel()
            loop.run_until_complete(self.server.wait_closed())
            self.server = None


class OrderServer(GenericServer):
    """
    Server handling order requests of clients.
    """
    async def _handle_client(self, clientid, client_reader, client_writer):
        print("Client %d connected." % clientid)
        while True:
            try:
                string = (await client_reader.readline()).decode("utf-8")
                if not string:  # an empty string means the client disconnected
                    break
                data = json.loads(string.rstrip())
                # print("Received: ",data)
                if data["message"] == "createOrder":
                    await self._send_json(client_writer, {
                        "message": "executionReport",
                        "orderId": data["orderId"],
                        "report": "NEW"
                    })
                    await self.exchange.open_order(data["orderId"], clientid, data["side"], data["price"],
                                                   data["quantity"])
                elif data["message"] == "cancelOrder":
                    await self._send_json(client_writer, {
                        "message": "cancelOrder",
                        "orderId": data["orderId"]
                    })
                    await self.exchange.cancel_order(clientid, data["orderId"])
            except ConnectionResetError:
                break
        return clientid  # return clientid as task result, so we can recognize the disconnected client in _client_done()

    async def fill_order_report(self, clientid: str, orderid: int, price: int, qty: int) -> None:
        if clientid not in self.clients:
            print("Client %s already disconnected. Not sending fill report." % clientid)
            return
        (reader, writer) = self.clients[clientid]
        await self._send_json(writer, {
            "message": "executionReport",
            "report": "FILL",
            "orderId": orderid,
            "price": price,
            "quantity": qty  # Report how many were traded
        })


class DatastreamServer(GenericServer):
    """
    Server providing anonymous data, which we call "datastream".
    """
    def _client_done(self, task):
        try:
            del self.clients[task.result()]
        except:
            print("Datastream client forced to disconnect.")

    async def _handle_client(self, clientid, client_reader, client_writer):
        while True:
            string = await client_reader.readline()
            if not string:  # an empty string means the client disconnected
                break
        return clientid  # return clientid as task result, so we can recognize the disconnected client in _client_done()

    async def send_datastream_report(self, type: str, side: str, time: datetime.time, price: int, qty: int) -> None:
        translate = {"BUY": "bid", "SELL": "ask"}
        assert type != "trade" or qty != 0
        for (reader, writer) in self.clients.values():
            message = {
                "type": type,
                "price": price,
                "quantity": qty,
                "time": time.timestamp(), # todo: overit ze se to posila spravne
            }
            if side:  # only for some types of reports, not for "trade"
                message["side"] = translate[side]
            await self._send_json(writer, message)
