import json
import websocket
from analytical_pipeline.config.settings import Config

class TwelveDataWebSocket:
    def __init__(self):
        self.ws = None
        self.twelvedata_url = Config.TWELVE_DATA_WS_URL
        self.twelvedata_api_key = Config.TWELVE_DATA_API_KEY
        self.symbols = Config.STOCK_SYMBOLS

    def on_message(self, ws, message):
        print("Received:", message)
    
    def on_error(self, ws, error):
        print("Error:", error)

    def on_close(self, ws, close_status_code, close_msg):
        print("Connection is closed.")

    def on_open(self, ws):
        print("Connection is opened.")
        payload = {
            "action": "subscribe",
            "params": {
                "symbols": self.symbols,
                "apikey": self.twelvedata_api_key
            }
        }
        ws.send(json.dumps(payload))

    def run(self):
        websocket.enableTrace(False)
        self.ws = websocket.WebSocketApp(
            url = f"{self.twelvedata_url}?apikey={self.twelvedata_api_key}",
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
        )
        self.ws.run_forever()


client = TwelveDataWebSocket()
client.run()