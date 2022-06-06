import time
import hmac
from requests import Request, Session
import config_FTX
import json


class ftx_rest_client:
    def __init__(self, key=None, secret=None, account=None):
        self.key = key
        self.secret = secret
        self.account = account
        self.session = Session()

    def send_request(self, method, path, **kwargs):
        url_base = "https://ftx.com/api"
        url_cible = url_base + path
        ts = int(time.time() * 1000)
        request = Request(method, url_cible, **kwargs)
        prepared = request.prepare()
        signature_payload = f"{ts}{prepared.method}{prepared.path_url}".encode()
        signature = hmac.new(
            self.secret.encode(),
            signature_payload,
            "sha256",
        ).hexdigest()

        prepared.headers["FTX-KEY"] = self.key
        prepared.headers["FTX-SIGN"] = signature
        prepared.headers["FTX-TS"] = str(ts)
        if self.account:
            prepared.header["FTX-SUBACCOUNT"] = self.account

        x = self.session.send(prepared)

        return json.loads(x.text)

    def funding_rates(self, start_time=None, end_time=None, future=None):
        params = {"start_time": start_time, "end_time": end_time, "future": future}
        return self.send_request("GET", "/funding_rates", params=params)

    def liste_future(self):
        return self.send_request("GET", "/futures")

    def details_future(self, token=None):
        return self.send_request("GET", "/futures/" + token)

    def funding_futur_details(self, token=None):
        path1 = "/futures/"
        path2 = "/stats"
        return self.send_request("GET", path1 + token + path2)

    def historical_prices(
        self, token=None, resolution=None, start_time=None, end_time=None
    ):
        path1 = "/markets/"
        path2 = "/candles?resolution="
        path3 = "&start_time="
        path4 = "&end_time="

        return self.send_request(
            "GET",
            path1 + token + path2 + resolution + path3 + start_time + path4 + end_time,
        )


requete = ftx_rest_client(config_FTX.api_key, config_FTX.api_secret)


# Récupérer l'ensemble des funding rate et les trier par tri croissant
extraction_resultat_rate = requete.funding_rates()["result"]
print(sorted(extraction_resultat_rate, key=lambda item: item["rate"]))
"""
# Récupérer l'historique des ordres
requete.send_request("GET", "/orders/history", params={"market": "MOB/USDT"})

# Récupérer l'ensemble des futures existants
liste_futures = requete.liste_future()
for i in liste_futures["result"]:
    print(i)

# Récupérer l'ensemble des information d'un futur spécifique
print(requete.details_future("BTC-PERP"))

# Récupérer les funding rate informations d'un futur
print(requete.funding_futur_details("BTC-PERP"))
"""

# Récupérer les prix historiques d'un token (la resolution est égale à la bougie mais en secondes =, dispo : 15, 60, 300, ... tout multiple de 86400)
# configuré depuis le 01/01/2022 à maintenant en 1 min
# import dans SQL
time_stamp_actuel = int(time.time())
ts_str = str(time_stamp_actuel)
liste = requete.historical_prices("BTC-PERP", "60", "1640991600", ts_str)

for i in liste["result"]:
    start_time = i["startTime"]
    time_stamp = i["time"]
    ouverture = i["open"]
    max_bougie = i["high"]
    min_bougie = i["low"]
    cloture = i["close"]
    volume = i["volume"]

    print(i)
