import json, hmac, hashlib, time, requests, base64, dateutil.parser
from requests.auth import AuthBase
from datetime import date, timedelta
import calendar
from datetime import datetime
from autobahn.twisted.websocket import WebSocketClientFactory, WebSocketClientProtocol, connectWS
from twisted.internet import reactor

class CoinbaseExchangeAuth(AuthBase):
    def __init__(self, api_key, secret_key, passphrase):
        self.api_key = api_key
        self.secret_key = secret_key
        self.passphrase = passphrase

    def __call__(self, request):
        timestamp = str(time.time())
        message = timestamp + request.method + request.path_url + (request.body or '')
        hmac_key = base64.b64decode(self.secret_key)
        signature = hmac.new(hmac_key, message, hashlib.sha256)
        signature_b64 = signature.digest().encode('base64').rstrip('\n')

        request.headers.update({
            'CB-ACCESS-SIGN': signature_b64,
            'CB-ACCESS-TIMESTAMP': timestamp,
            'CB-ACCESS-KEY': self.api_key,
            'CB-ACCESS-PASSPHRASE': self.passphrase,
            'Content-Type': 'application/json'
        })
        return request


class ClientProtocol(WebSocketClientProtocol):
    handler = None

    def onConnect(self, response):
        print("Server connected: {0}".format(response.peer))

    def initMessage(self):
        message_data = {"type": "subscribe", "product_id": "BTC-USD"}
        message_json = json.dumps(message_data)
        print "sendMessage: " + message_json
        self.sendMessage(message_json)

    def onOpen(self):
        print "onOpen calls initMessage()"
        self.initMessage()

    def onMessage(self, msg, binary):
        msg = json.loads(msg)
        self.handler.accept_feed(msg)

    def onClose(self, wasClean, code, reason):
        print("WebSocket connection closed: {0}".format(reason))
        self.handler.listen()


class Coinbase:

    def __init__(self, api_key, secret_key, passphrase, handler):
        self.api_url = 'https://api.exchange.coinbase.com/'
        self.auth = CoinbaseExchangeAuth(api_key, secret_key, passphrase)
        self.ws = None
        self.sub = None
        self.handler = handler
        self.usd_account, self.btc_account = '', ''
        self.store_account_numbers()

    def store_account_numbers(self):
        r = requests.get(self.api_url + 'accounts/', auth=self.auth).json()
        for account in r:
            if account['currency'] == 'USD':
                self.usd_account = account['id']
            elif account['currency'] == 'BTC':
                self.btc_account = account['id']

    def get_usd_balance(self):
        r = requests.get(self.api_url + 'accounts/' + self.usd_account, auth=self.auth).text
        r = json.loads(r)
        try:
            return float(r['available'])
        except KeyError:
            print r
            return 0

    def get_bitcoin_balance(self):
        r = requests.get(self.api_url + 'accounts/' + self.btc_account, auth=self.auth).text
        r = json.loads(r)
        try:
            return float(r['available'])
        except KeyError:
            return 0

    def get_accounts(self):
        r = requests.get(self.api_url + 'accounts', auth=self.auth).text
        return json.loads(r)

    def get_fills(self):
        orders = []
        r = requests.get(self.api_url + 'fills', auth=self.auth)
        orders.append(json.loads(r.text))
        h = r.headers
        if 'CB-AFTER' in h:
            return self.paginate_fills(orders, h['CB-AFTER'])
        else:
            return orders

    def paginate_fills(self, orders, after):
        r = requests.get(self.api_url + 'fills?after=' + str(after), auth=self.auth)
        orders.append(json.loads(r.text))
        h = r.headers
        if 'CB-AFTER' in h:
            return self.paginate_fills(orders, h['CB-AFTER'])
        else:
            return orders

    def get_orders(self):
        orders = []
        r = requests.get(self.api_url + 'orders?status=open', auth=self.auth)
        orders.append(json.loads(r.text))
        h = r.headers

        if 'CB-AFTER' in h:
            return self.paginate_orders(orders, h['CB-AFTER'])
        else:
            return orders

    def paginate_orders(self, orders, after):
        r = requests.get(self.api_url + 'orders?status=open&after=' + str(after), auth=self.auth)
        orders.append(json.loads(r.text))
        h = r.headers
        if 'CB-AFTER' in h:
            return self.paginate_fills(orders, h['CB-AFTER'])
        else:
            return orders

    def delete_order(self, order_id):
        delete = requests.delete(self.api_url + 'orders/'+order_id, auth=self.auth).text
        return delete

    def get_past_prices(self):
        time = requests.get(self.api_url + '/time').text
        time = json.loads(time)['iso']
        yesterday = date.today() - timedelta(days=1)
        yesterday = dateutil.parser.parse(str(yesterday))
        args = {
            'start' : yesterday.isoformat(),
            'end' : time,
            'granularity' : 30
        }

        r = requests.get(self.api_url + 'products/BTC-USD/candles', json=args, auth=self.auth).text
        return json.loads(r)

    def get_time(self):
        try:
            current_time = requests.get(self.api_url + '/time').text
            current_time = json.loads(current_time)['iso']
            current_time = calendar.timegm(datetime.strptime(current_time, "%Y-%m-%dT%H:%M:%S.%fZ").timetuple())
        except:
            print "CURRENT TIME"
        return float(current_time)

    def setup_websocket(self):
        try:
            factory = WebSocketClientFactory("wss://ws-feed.exchange.coinbase.com")
            ClientProtocol.handler = self.handler
            factory.protocol = ClientProtocol
            connectWS(factory)
            reactor.run()
        except KeyboardInterrupt:
            factory.close()
            self.handler.close_client()

    def place_btc_order(self, size, price):
        order = {
            'type' : 'limit',
            'size': "%.4f" % size,
            'price': "%.2f" % price,
            'side': 'buy',
            'product_id': 'BTC-USD',
            'post_only' : True
        }

        r = requests.post(self.api_url + 'orders', json=order, auth=self.auth).text
        return json.loads(r)

    def sell_btc(self, size, price):
        order = {
            'type' : 'limit',
            'size' : "%.8f" % size,
            'price': "%.2f" % price,
            'side' : 'sell',
            'product_id' : 'BTC-USD',
            'post_only' : True
        }

        r = requests.post(self.api_url + 'orders', json=order, auth=self.auth).text
        return json.loads(r)

