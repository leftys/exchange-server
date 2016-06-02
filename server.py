import asyncio
import asyncio.streams
import json
import datetime


class Server:
    """
    TCP server processing client's messages and calling Exchange methods
    """

    def __init__(self, host, port, exchange):
        self.order_server = None # encapsulates the server sockets
        self.datastream_server = None
        self.host = host
        self.port = port
        self.next_clientid = 0
        self.exchange = exchange
        self.exchange.set_callbacks(self.fill_order_report, self.send_datastream_report)

        # this keeps track of all the clients that connected to our
        # server.  It can be useful in some cases, for instance to
        # kill client connections or to broadcast some data to all
        # clients...
        self.clients = {}  # task -> (reader, writer)
        self.datastream_clients = {}  # task -> (reader, writer)

    def _accept_client(self, client_reader, client_writer):
        """
        This method accepts a new client connection and creates a Task
        to handle this client.  self.clients is updated to keep track
        of the new client.
        """

        # start a new Task to handle this specific client connection
        clientid = self.exchange.get_clientid()
        task = asyncio.Task(self._handle_client(clientid, client_reader, client_writer))
        self.clients[clientid] = (client_reader, client_writer)

        def client_done(task):
            print("Client {0} disconnected".format(task.result()))  # , file=sys.stderr)
            del self.clients[task.result()]

        task.add_done_callback(client_done)

    def _accept_datastream(self, client_reader, client_writer):
        """
        This method accepts a new client connection and creates a Task
        to handle this client.  self.clients is updated to keep track
        of the new client.
        """

        # start a new Task to handle this specific client connection
        task = asyncio.Task(self._handle_datastream(client_reader, client_writer))
        self.datastream_clients[task] = (client_reader, client_writer)

        def client_done(task):
            print("Datastream client disconnected.")  # , file=sys.stderr)
            del self.datastream_clients[task]

        task.add_done_callback(client_done)

    async def _send_json(self, writer, json_str):
        writer.write((json.dumps(json_str)+"\n").encode("utf-8"))
        await writer.drain()

    async def _handle_client(self, clientid, client_reader, client_writer):
        """
        This method actually does the work to handle the requests for
        a specific client.  The protocol is line oriented, so there is
        a main loop that reads a line with a request and then sends
        out one or more lines back to the client with the result.
        """
        while True:
            string = (await client_reader.readline()).decode("utf-8")
            if not string:  # an empty string means the client disconnected
                break
            data = json.loads(string.rstrip())
            print("Received: ",data)
            if data["message"] == "createOrder":
                await self._send_json(client_writer,{
                    "message": "executionReport",
                    "orderId": data["orderId"],
                    "report": "NEW"
                })
                await self.exchange.open_order(data["orderId"], clientid, data["side"], data["price"], data["quantity"])
            elif data["message"] == "cancelOrder":
                await self._send_json(client_writer,{
                    "message": "cancelOrder",
                    "orderId": data["orderId"]
                })
                await self.exchange.cancel_order(data["orderId"], clientid)
        return clientid

    async def _handle_datastream(self, client_reader, client_writer):
        """
        This method actually does the work to handle the requests for
        a specific client.  The protocol is line oriented, so there is
        a main loop that reads a line with a request and then sends
        out one or more lines back to the client with the result.
        """
        while True:
            string = await client_reader.readline()
            if not string:  # an empty string means the client disconnected
                break

    async def fill_order_report(self, clientid: int, orderid: int, price: int, qty: int) -> None:
        if clientid not in self.clients:
            print("Client {0} already disconnected. Not sending fill report.".format(clientid))
            return
        (reader, writer) = self.clients[clientid]
        await self._send_json(writer,{
            "message": "executionReport",
            "report": "FILL",
            "orderId": orderid,
            "price": price, # todo: price_close?
            "quantity": qty
        })

    async def send_datastream_report(self, type: str, side: str, time: datetime.time, price: int, qty: int) -> None:
        translate = {"BUY":"bid", "SELL":"ask"}
        for (reader,writer) in self.datastream_clients.values():
            message = {
                "type": type,
                "price": price, # todo: price_close?
                "quantity": qty,
                "time": time.timestamp(),
            }
            if side:  # only for some types of reports, not for "trade"
                message["side"] = translate[side]
            await self._send_json(writer,message)

    def start(self, loop):
        """
        Starts the TCP server, so that it listens on port 7001.
        For each client that connects, the accept_client method gets
        called.  This method runs the loop until the server sockets
        are ready to accept connections.
        """
        self.order_server = loop.run_until_complete(
            asyncio.streams.start_server(self._accept_client,
                                         self.host, self.port,
                                         loop=loop))
        self.datastream_server = loop.run_until_complete(
            asyncio.streams.start_server(self._accept_datastream,
                                         self.host, self.port+1,
                                         loop=loop))

    def stop(self, loop):
        """
        Stops the TCP server, i.e. closes the listening socket(s).
        This method runs the loop until the server sockets are closed.
        """
        if self.order_server is not None:
            self.order_server.close()
            loop.run_until_complete(self.order_server.wait_closed())
            self.order_server = None
        if self.datastream_server is not None:
            self.datastream_server.close()
            loop.run_until_complete(self.datastream_server.wait_closed())
            self.datastream_server = None

