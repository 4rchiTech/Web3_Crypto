import ccxt
import time
import logging
import sqlite3
import statistics
from threading import Thread
import time

# module logging #############################################################

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter(
    "%(asctime)s:%(funcName)s:%(levelname)s:%(lineno)d:%(message)s"
)

file_handler = logging.FileHandler("journal_log_grid_bot.log")
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)

###############################################################################
#                                 CONSTANTES                                  #
###############################################################################

CEX = ccxt.binance()

# LEXIQUE

ACHAT = "achat"
VENTE = "vente"

# TOKEN
TOKEN_CIBLE = "ETH"
QUOTE_CIBLE = "BUSD"
PAIRE_CIBLE = "ETH/BUSD"

# BOLLINGER
TIMEFRAME_BB = "30m"  # 30 min = 1800 s
NB_SEANCES_BB = 100

# MONEY MANAGEMENT
TAILLE_ORDRE_QUOTE = 30  # en dollars

# TRADING
DELAI_VEILLEUR_OPERATION_BDD = 900 * 1000  # en secondes * 1000
DELAI_AVANT_ANNULATION_ORDRE = None  # en secondes

TIME_SLEEP_MODULE_INDICATEURS = 1805  # en secondes // module autonome

TIME_SLEEP_MODULE_ACHAT = 2700  # en secondes // a additionner avec délai veilleur (15 min + 45) soit 900 + 2700
TIME_SLEEP_MODULE_VENTE = 900  # en secondes // a additionner avec délai veilleur

TIME_SLEEP_MAJ_STATUT_ORDRES = 900  # en secondes

# CDS
BINANCE_AUTH = ccxt.binance(
    {
        "apiKey": "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
        "secret": "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
    }
)

################################## REQUETES CEX ##############################


# PRICE ACTION

########################## MATRICE ##########################
def extraction_bdd():

    try:

        connexionBDD = sqlite3.connect("bdd_grid_bot.db")
        curseur = connexionBDD.cursor()
        curseur.execute("SELECT * FROM klines")
        selection_BDD = curseur.fetchall()
        print("la selection BDD est :", selection_BDD)
        resultat = selection_BDD[0][2]
        print("la variable resultat est :", resultat)
        connexionBDD.close()

        return True

    except Exception as e:
        logger.exception(e)


############################################################# MODULE 1 ##################################################
# récupérer, enregistrer, calculer indicateurs
##########################################################################################################################
def extraction_liste_timestamp():

    try:
        logger.info("------------------------")
        logger.info("entrée fonction extraction_liste_timestamp")

        connexionBDD = sqlite3.connect("bdd_grid_bot.db")
        curseur = connexionBDD.cursor()
        curseur.execute("SELECT * FROM klines")
        selection_BDD = curseur.fetchall()
        connexionBDD.close()

        logger.info("liste timestamp extraite")
        logger.info("------------------------")

        return [str(kline[1]) for kline in selection_BDD]

    except Exception as e:
        logger.exception(e)


def extraction_liste_prix():

    try:
        logger.info("------------------------")
        logger.info("entrée fonction extraction liste des prix de la bdd")

        connexionBDD = sqlite3.connect("bdd_grid_bot.db")
        curseur = connexionBDD.cursor()
        curseur.execute("SELECT * FROM klines ORDER BY close LIMIT 100")
        selection_BDD = curseur.fetchall()
        connexionBDD.close()

        logger.info("liste des prix extraite")
        logger.info("------------------------")

        return [float(kline[6]) for kline in selection_BDD]

    except Exception as e:
        logger.exception(e)


def comparaison_liste_timestamp(timestamp_test=None):

    if timestamp_test not in extraction_liste_timestamp():
        return True
    else:
        return False


def mm100_close():
    try:
        logger.info("------------------------")
        logger.info("entrée fonction calcul mm")

        connexionBDD = sqlite3.connect("bdd_grid_bot.db")
        curseur = connexionBDD.cursor()
        curseur.execute("SELECT close FROM klines ORDER BY close LIMIT 100")
        selection_BDD = curseur.fetchall()

        somme = 0
        for prix_cloture in selection_BDD:
            somme += float(prix_cloture[0])

        connexionBDD.close()

        mm_20 = round(somme / NB_SEANCES_BB, 2)

        logger.info("sortie fonction calcul mm")
        logger.info("------------------------")

        return mm_20

    except Exception as e:
        logger.exception(e)


def calcul_ecart_type(liste_entree=None):
    return float(statistics.stdev(liste_entree))


def insertion_klines(timestamp, open, high, low, close, volume):

    try:
        logger.info("------------------------")
        logger.info("entrée fonction insertion klines")

        connexionBDD = sqlite3.connect("bdd_grid_bot.db")
        curseur = connexionBDD.cursor()
        insertion = "INSERT INTO klines(timestamp_kline, time_frame, open, high, low, close, volume) VALUES(?, ?, ?, ?, ?, ?, ?)"
        creation = timestamp, TIMEFRAME_BB, open, high, low, close, volume
        curseur.execute(insertion, creation)
        connexionBDD.commit()
        connexionBDD.close()

        logger.info("sortie fonction insertion klines")
        logger.info("------------------------")

    except Exception as e:
        logger.exception(e)


def extraction_klines_cex():  # Enregistrer dans la BDD tous les prix non enregistrés (clé Timestamp)

    try:
        logger.info("------------------------")
        logger.info("entrée fonction extraction kline CEX")
        klines_MM18 = BINANCE_AUTH.fetch_ohlcv(
            PAIRE_CIBLE, timeframe=TIMEFRAME_BB, limit=NB_SEANCES_BB
        )

        for kline in klines_MM18:
            timestamp, open, high, low, close, volume = kline
            if comparaison_liste_timestamp(timestamp_test=str(timestamp)):
                # condition if comparaison du time stamp à la liste TS dans BDD
                insertion_klines(timestamp, open, high, low, close, volume)
                logger.info("kline nouvelle, enregistrée dans DB")

        logger.info("sortie fonction extraction kline CEX")
        logger.info("------------------------")

        return True

    except Exception as e:
        logger.exception(e)
        return False


def extraction_last_kline_null():  # renvoie STR

    try:

        connexionBDD = sqlite3.connect("bdd_grid_bot.db")
        curseur = connexionBDD.cursor()
        curseur.execute(
            "SELECT * FROM klines WHERE mm_20 IS NULL ORDER BY numero_kline DESC LIMIT 1"
        )
        selection_BDD = curseur.fetchall()[0]

        connexionBDD.close()

        return str(selection_BDD[1])

    except Exception as e:
        logger.exception(e)


def maj_bdd_calcul_last_kline(
    mm_100,
    ecart_type,
    bb_basse,
    bb_haute,
    timestamp_kline_cible,
):
    try:
        logger.info("------------------------")
        logger.info("entrée fonction enregistrement calcul indicateurs")

        connexionBDD = sqlite3.connect("bdd_grid_bot.db")
        curseur_selection = connexionBDD.cursor()
        curseur_selection.execute(
            "SELECT * FROM klines WHERE mm_20 IS NULL ORDER BY numero_kline DESC LIMIT 1"
        )

        maj_trade = "UPDATE klines SET mm_20 = ?, ecart_type = ?, bb_basse = ?, bb_haute = ? WHERE timestamp_kline = ?"
        curseur_maj = connexionBDD.cursor()
        colonnes_maj = (
            mm_100,
            ecart_type,
            bb_basse,
            bb_haute,
            timestamp_kline_cible,
        )
        curseur_maj.execute(maj_trade, colonnes_maj)
        connexionBDD.commit()
        connexionBDD.close()

        logger.info("sortie fonction enregistrement calcul indicateurs")
        logger.info("------------------------")

    except Exception as e:
        logger.exception(e)


def calcul_indicateurs():
    try:
        logger.info("------------------------")
        logger.info("entrée fonction calcul BB")

        mm_100 = mm100_close()
        ecart_type = calcul_ecart_type(liste_entree=extraction_liste_prix())
        bb_basse = round(mm_100 - 2 * ecart_type, 2)
        bb_haute = round(mm_100 + 2 * ecart_type, 2)

        logger.info("sortie fonction calcul BB")
        logger.info("------------------------")

        return mm_100, ecart_type, bb_basse, bb_haute

    except Exception as e:
        logger.exception(e)


def module_indicateurs():

    try:
        logger.info(
            "++++++++++++++++++++++ ------------------------ ++++++++++++++++++++++"
        )
        logger.info("entrée module indicateurs")

        extraction_klines_cex()  # 1°) extraction klines & enregistrement bdd
        (
            mm_100,
            ecart_type,
            bb_basse,
            bb_haute,
        ) = calcul_indicateurs()  # 2°) calcul indicateurs
        timestamp_kline_cible = extraction_last_kline_null()  # TS du last klines
        maj_bdd_calcul_last_kline(
            mm_100=mm_100,
            ecart_type=ecart_type,
            bb_basse=bb_basse,
            bb_haute=bb_haute,
            timestamp_kline_cible=timestamp_kline_cible,
        )  # 3°) maj des indicateurs

        logger.info("sortie module indicateurs")
        logger.info(
            "++++++++++++++++++++++ ------------------------ ++++++++++++++++++++++"
        )

        return True

    except Exception as e:
        logger.exception(e)
        return False


############################################################# FIN DU MODULE 1 ##################################################
# récupérer, enregistrer, calculer indicateurs
################################################################################################################################


############################################################# MODULE 2 ##############################################################
# VEILLE : comparer le prix actuel aux indicateurs, envoyer signal d'achat ou vente ou continuer veille suivant liquidités et timeout
#####################################################################################################################################


def extraction_bb_basse():
    try:

        connexionBDD = sqlite3.connect("bdd_grid_bot.db")
        curseur = connexionBDD.cursor()
        curseur.execute("SELECT * FROM klines ORDER BY numero_kline DESC LIMIT 1")
        selection_BDD = curseur.fetchall()
        bb_basse = selection_BDD[0][10]
        connexionBDD.close()

        return float(bb_basse)

    except Exception as e:
        logger.exception(e)


def extraction_bb_haute():
    try:

        connexionBDD = sqlite3.connect("bdd_grid_bot.db")
        curseur = connexionBDD.cursor()
        curseur.execute("SELECT * FROM klines ORDER BY numero_kline DESC LIMIT 1")
        selection_BDD = curseur.fetchall()
        bb_haute = selection_BDD[0][11]
        connexionBDD.close()

        return float(bb_haute)

    except Exception as e:
        logger.exception(e)


def last_price():
    try:

        requete = BINANCE_AUTH.fetch_ticker(PAIRE_CIBLE)["info"]["lastPrice"]

        return round(float(requete), 2)

    except Exception as e:
        logger.exception(e)
        return False


# LIQUIDITY


def balance_free_liquidity():  # ajouter TS?
    try:
        logger.info("------------------------")

        balance = BINANCE_AUTH.fetch_balance()
        liquidity = balance[QUOTE_CIBLE]["free"]  # free ou used ou total possibles

        logger.info(f"Le solde restant de liquidités est de {liquidity} {QUOTE_CIBLE}")
        logger.info("------------------------")

        return liquidity
    except Exception as e:
        logger.exception(e)


def balance_free_token():  # ajouter TS?

    logger.info("------------------------")

    balance = BINANCE_AUTH.fetch_balance()
    liquidity = balance[TOKEN_CIBLE]["free"]  # free ou used ou total possibles

    logger.info(f"Le solde restant est de {liquidity} {TOKEN_CIBLE}")
    logger.info("------------------------")

    return liquidity


# LAST TIME STAMP


def extraction_last_timestamp_orderbook():

    try:

        connexionBDD = sqlite3.connect("bdd_grid_bot.db")
        curseur = connexionBDD.cursor()
        curseur.execute(
            "SELECT timestamp_order FROM orders_book ORDER BY numero_ordre DESC LIMIT 1"
        )
        selection_BDD = curseur.fetchall()
        connexionBDD.close()

        if selection_BDD == []:
            return int(0)
        else:
            return int(selection_BDD[0][0])

    except Exception as e:
        logger.exception(e)


def expiration_delai_timestamp():

    try:
        logger.info("------------------------")
        logger.info("entrée fonction calcul délai timestamp / timeout")

        timestamp = int(time.time()) * 1000
        last_ts = extraction_last_timestamp_orderbook()
        if int(timestamp) - int(last_ts) > int(DELAI_VEILLEUR_OPERATION_BDD):
            logger.info(
                f"achat autorisé car délai expiré, timestamp actuel : {timestamp}, dernier timestamp : {last_ts}"
            )
            logger.info("------------------------")
            return True  # si le délai est expiré, retourne Vrai
        else:
            logger.info(
                f"achat interdit car délai non expiré, timestamp actuel : {timestamp}, dernier timestamp : {last_ts}"
            )
            logger.info("------------------------")
            return False

    except Exception as e:
        logger.exception(e)


def veilleur_achat():
    logger.info(
        "++++++++++++++++++++++ ------------------------ ++++++++++++++++++++++"
    )
    logger.info("entrée module veilleur")
    if expiration_delai_timestamp():
        logger.info("Pas d'ordre passé récemment, étape 1 validée")
        if float(balance_free_liquidity()) > float(TAILLE_ORDRE_QUOTE):
            logger.info("Il y a assez de liquidités, étape 2 validée")
            if last_price() < extraction_bb_basse():

                logger.info(
                    "Le prix actuel a franchi à la baisse la bb_basse, achat accepté, étape 3 validée"
                )
                logger.info(
                    "++++++++++++++++++++++ ------------------------ ++++++++++++++++++++++"
                )
                return ACHAT

            else:
                logger.info(
                    "le prix n'est pas en dehors des bandes bb, étape 3 invalidée"
                )
                logger.info(
                    "++++++++++++++++++++++ ------------------------ ++++++++++++++++++++++"
                )
                return False

        else:
            logger.info("Pas assez de liquidités, étape 2 invalidée")
            logger.info(
                "++++++++++++++++++++++ ------------------------ ++++++++++++++++++++++"
            )
            return False

    else:
        logger.info(
            f"Il y a eu un ordre passé il y a moins de {DELAI_VEILLEUR_OPERATION_BDD} secondes, étape 1 invalidée"
        )
        logger.info(
            "++++++++++++++++++++++ ------------------------ ++++++++++++++++++++++"
        )
        return False


def veilleur_vente():
    logger.info(
        "++++++++++++++++++++++ ------------------------ ++++++++++++++++++++++"
    )
    logger.info("entrée module veilleur")
    if expiration_delai_timestamp():
        logger.info("Pas d'ordre passé récemment, étape 1 validée")

        if last_price() > extraction_bb_haute():

            logger.info(
                "Le prix actuel a franchi à la hausse la bb_haute, vente acceptée, étape 3 validée"
            )
            logger.info(
                "++++++++++++++++++++++ ------------------------ ++++++++++++++++++++++"
            )
            return VENTE

        else:
            logger.info("le prix n'est pas en dehors des bandes bb, étape 3 invalidée")
            logger.info(
                "++++++++++++++++++++++ ------------------------ ++++++++++++++++++++++"
            )
            return False


############################################################# FIN DU MODULE 2 #######################################################
# VEILLE : comparer le prix actuel aux indicateurs, envoyer signal d'achat ou vente ou continuer veille suivant liquidités et timeout
#####################################################################################################################################

############################################################# MODULE 3 ########################################################
# acheter / vendre, enregistrer les ordres, MAJ bdd suivi ordre, annuler ceux qui ne sont pas réalisés suivant délai
################################################################################################################################


def limit_buy_order(price=None):

    try:
        logger.info("------------------------")
        logger.info("entrée fonction achat cours limité")

        budget = TAILLE_ORDRE_QUOTE
        BINANCE_AUTH.load_markets()
        symbol = PAIRE_CIBLE

        # déterminer prix achat
        formatted_price = BINANCE_AUTH.price_to_precision(symbol, price)

        # déterminer la quantité de token (amount) par rapport à la constante budget ordre ($)
        amount_brut = float(budget) / float(formatted_price)
        # formater le montant pour arrondir
        formatted_amount = BINANCE_AUTH.amount_to_precision(symbol, amount_brut)

        prix_revient = round(float(formatted_amount) * float(formatted_price), 5)

        response = BINANCE_AUTH.create_limit_buy_order(
            symbol,
            amount=formatted_amount,
            price=formatted_price,
        )

        logger.info("sortie fonction achat cours limité")
        logger.info("------------------------")

        return (
            response["info"]["orderId"],
            formatted_price,
            formatted_amount,
            prix_revient,
        )

    except Exception as e:
        logger.exception(e)


def annuler_ordre(order_id):
    try:
        logger.info("------------------------")

        BINANCE_AUTH.cancel_order(order_id, PAIRE_CIBLE)

        logger.info("------------------------")
    except Exception as e:
        logger.exception(e)


def enregistrer_ordre_achat(
    order_id=None, prix_achat=None, montant_token=None, prix_revient=None
):
    try:
        logger.info("------------------------")
        logger.info("entrée fonction enregistrement ordre achat")

        timestamp = int(time.time()) * 1000
        type_ordre = "achat"
        statut_ordre = "running"
        en_stock = "oui"
        montant_ordre_net = prix_revient
        montant_token_net = montant_token

        connexionBDD = sqlite3.connect("bdd_grid_bot.db")
        curseur = connexionBDD.cursor()

        insertion = "INSERT INTO orders_book(timestamp_order, order_id_achat, type_ordre, statut_ordre, en_stock, prix_achat, montant_ordre_net, montant_token_net) VALUES(?, ?, ?, ?, ?, ?, ?, ?)"
        creation = (
            timestamp,
            order_id,
            type_ordre,
            statut_ordre,
            en_stock,
            prix_achat,
            montant_ordre_net,
            montant_token_net,
        )
        curseur.execute(insertion, creation)
        connexionBDD.commit()
        connexionBDD.close()

        logger.info("ordre enregistré, sortie de fonction")
        logger.info("------------------------")

    except Exception as e:
        logger.exception(e)


def module_achat(price=None):

    logger.info(
        "++++++++++++++++++++++ ------------------------ ++++++++++++++++++++++"
    )
    logger.info("entrée module achat")

    order_id, prix_achat, montant_token, prix_revient = limit_buy_order(price=price)

    enregistrer_ordre_achat(
        order_id=order_id,
        prix_achat=prix_achat,
        montant_token=montant_token,
        prix_revient=prix_revient,
    )

    logger.info(f"Achat N° {order_id} en cours, et enregistré en bdd")

    logger.info("sortie module achat")
    logger.info(
        "++++++++++++++++++++++ ------------------------ ++++++++++++++++++++++"
    )


def liste_closed_orders():

    closed_orders = BINANCE_AUTH.fetch_closed_orders(symbol=PAIRE_CIBLE)
    liste_closed_orders = [
        str(orders["info"]["orderId"])
        for orders in closed_orders
        if orders["status"] == "closed"
    ]

    return liste_closed_orders


def check_order_closed(order_id_cible=None):

    logger.info("------------------------")

    if str(order_id_cible) in liste_closed_orders():

        logger.info("l'ordre visé est bien dans la liste des ordres closed")
        logger.info("------------------------")

        return True
    else:

        logger.info("l'ordre visé n'est pas encore closed")
        logger.info("------------------------")
        return False


############################# MODULE VENTE ##############################


def selection_liste_ordres_vendables():
    try:

        logger.info("------------------------")
        logger.info(
            "entrée fonction extraction liste ordres achat complétés et en stock"
        )

        connexionBDD = sqlite3.connect("bdd_grid_bot.db")
        curseur = connexionBDD.cursor()
        curseur.execute(
            "SELECT * FROM orders_book WHERE en_stock = 'oui' AND statut_ordre = 'completed'"
        )
        selection_BDD = curseur.fetchall()
        connexionBDD.close()

        liste_extraction = [
            (orders[1], orders[6], orders[11]) for orders in selection_BDD
        ]
        # liste de tuples = N° order id achat, prix achat, quantité token

        logger.info("sortie fonction")
        logger.info("------------------------")

        return liste_extraction

    except Exception as e:
        logger.exception(e)


def limit_sell_order(price=None, quantite_token=None):  # last price

    try:

        logger.info("------------------------")
        logger.info("entrée fonction vente cours limité")

        BINANCE_AUTH.load_markets()
        symbol = PAIRE_CIBLE

        # déterminer prix achat
        formatted_price = BINANCE_AUTH.price_to_precision(symbol, price)

        response = BINANCE_AUTH.create_limit_sell_order(
            symbol,
            amount=quantite_token,
            price=formatted_price,
        )

        logger.info("------------------------")

        return (
            response["info"]["orderId"],
            formatted_price,
        )

    except Exception as e:
        logger.exception(e)


def maj_BDD_statut_sell_order_running(
    order_id_vente=None, prix_vente=None, montant_vente=None, order_id_achat=None
):

    try:

        requete_selection = "SELECT * FROM orders_book"
        connexionBDD = sqlite3.connect("bdd_grid_bot.db")
        curseur_selection = connexionBDD.cursor()
        curseur_selection.execute(requete_selection)

        statut_ordre_vente = "running"
        en_stock = "non"

        maj_trade = "UPDATE orders_book SET en_stock = ?, order_id_vente = ?, prix_vente = ?, montant_vente = ?, statut_ordre_vente = ? WHERE order_id_achat = ?"
        curseur_maj = connexionBDD.cursor()

        colonnes_maj = (
            en_stock,
            order_id_vente,
            prix_vente,
            montant_vente,
            statut_ordre_vente,
            order_id_achat,
        )

        curseur_maj.execute(maj_trade, colonnes_maj)
        connexionBDD.commit()
        connexionBDD.close()

        return True

    except Exception as e:
        logger.exception(e)


def module_vente():

    logger.info(
        "++++++++++++++++++++++ ------------------------ ++++++++++++++++++++++"
    )
    logger.info("entrée module vente")
    try:

        logger.info("------------------------")

        dernier_prix = last_price()

        for ordre in selection_liste_ordres_vendables():
            order_id_achat = ordre[0]
            prix_achat = ordre[1]
            quantite_token = ordre[2]

            if float(dernier_prix) > float(prix_achat):
                order_id_vente, prix_vente = limit_sell_order(
                    price=dernier_prix, quantite_token=quantite_token
                )
                maj_BDD_statut_sell_order_running(
                    order_id_vente=order_id_vente,
                    prix_vente=prix_vente,
                    montant_vente=quantite_token,
                    order_id_achat=order_id_achat,
                )

                logger.info("sortie module vente")
                logger.info(
                    "++++++++++++++++++++++ ------------------------ ++++++++++++++++++++++"
                )

                continue
            else:
                logger.info(
                    f"Le prix d'achat ({prix_achat}) est inférieur au prix marché actuel ({dernier_prix}), pas de vente"
                )

                logger.info("sortie module vente")
                logger.info(
                    "++++++++++++++++++++++ ------------------------ ++++++++++++++++++++++"
                )

            continue

    except Exception as e:
        logger.exception(e)


################################################################ VEILLEUR


######## BUY


def selection_bdd_liste_buy_ordre_running():
    try:

        connexionBDD = sqlite3.connect("bdd_grid_bot.db")
        curseur = connexionBDD.cursor()
        curseur.execute(
            "SELECT order_id_achat FROM orders_book WHERE statut_ordre = 'running'"
        )
        selection_BDD = curseur.fetchall()

        return [orders_id[0] for orders_id in selection_BDD]

    except Exception as e:
        logger.exception(e)


def maj_BDD_statut_buy_order(order_id_cible=None):

    try:

        requete_selection = "SELECT * FROM orders_book"
        connexionBDD = sqlite3.connect("bdd_grid_bot.db")
        curseur_selection = connexionBDD.cursor()
        curseur_selection.execute(requete_selection)

        statut = "completed"

        maj_trade = "UPDATE orders_book SET statut_ordre = ? WHERE order_id_achat = ?"
        curseur_maj = connexionBDD.cursor()
        colonnes_maj = (statut, order_id_cible)
        curseur_maj.execute(maj_trade, colonnes_maj)
        connexionBDD.commit()
        connexionBDD.close()

        return True

    except Exception as e:
        logger.exception(e)


def module_maj_statut_buy_orders():

    try:
        logger.info(
            "++++++++++++++++++++++ ------------------------ ++++++++++++++++++++++"
        )
        logger.info("entrée module maj buy orders")

        for order_id in selection_bdd_liste_buy_ordre_running():
            if check_order_closed(order_id_cible=order_id):
                maj_BDD_statut_buy_order(order_id_cible=order_id)
                logger.info(
                    f"le statut de l'ordre N°{order_id} est désormais 'completé'"
                )

            else:
                logger.info(f"le statut de l'ordre N°{order_id} est toujours 'running'")

        logger.info("sortie module maj buy orders")
        logger.info(
            "++++++++++++++++++++++ ------------------------ ++++++++++++++++++++++"
        )

    except Exception as e:
        logger.exception(e)


####### SELL


def selection_bdd_liste_sell_ordre_running():

    try:

        connexionBDD = sqlite3.connect("bdd_grid_bot.db")
        curseur = connexionBDD.cursor()
        curseur.execute(
            "SELECT order_id_vente FROM orders_book WHERE statut_ordre_vente = 'running'"
        )
        selection_BDD = curseur.fetchall()

        return [orders_id[0] for orders_id in selection_BDD]

    except Exception as e:
        logger.exception(e)


def maj_BDD_statut_sell_order(order_id_vente_cible=None):

    try:

        requete_selection = "SELECT * FROM orders_book"
        connexionBDD = sqlite3.connect("bdd_grid_bot.db")
        curseur_selection = connexionBDD.cursor()
        curseur_selection.execute(requete_selection)

        statut = "completed"

        maj_trade = (
            "UPDATE orders_book SET statut_ordre_vente = ? WHERE order_id_vente = ?"
        )
        curseur_maj = connexionBDD.cursor()
        colonnes_maj = (statut, order_id_vente_cible)
        curseur_maj.execute(maj_trade, colonnes_maj)
        connexionBDD.commit()
        connexionBDD.close()

        return True

    except Exception as e:
        logger.exception(e)


def module_maj_statut_sell_orders():
    try:

        logger.info(
            "++++++++++++++++++++++ ------------------------ ++++++++++++++++++++++"
        )
        logger.info("entrée module maj sell orders")

        for order_id in selection_bdd_liste_sell_ordre_running():
            if check_order_closed(order_id_cible=order_id):
                maj_BDD_statut_sell_order(order_id_vente_cible=order_id)  # a changer
                logger.info(
                    f"le statut de l'ordre N°{order_id} est désormais 'completé'"
                )

            else:
                logger.info(f"le statut de l'ordre N°{order_id} est toujours 'running'")

        logger.info("sortie module maj sell orders")
        logger.info(
            "++++++++++++++++++++++ ------------------------ ++++++++++++++++++++++"
        )

    except Exception as e:
        logger.exception(e)


############################################################# MAIN ###########################################################
#
################################################################################################################################


class robot_module_indicateurs(Thread):
    def __init__(self):
        Thread.__init__(self)
        self.start()

    def run(self):

        while True:
            module_indicateurs()
            time.sleep(TIME_SLEEP_MODULE_INDICATEURS)


class robot_module_trading(Thread):
    def __init__(self):
        Thread.__init__(self)
        self.start()

    def run(self):

        while True:

            try:

                if veilleur_achat() == ACHAT:
                    logger.info("activation du module achat")
                    module_achat(price=last_price())
                    time.sleep(TIME_SLEEP_MODULE_ACHAT)

                else:
                    logger.info("activation du module maj des ordres")
                    module_maj_statut_buy_orders()
                    time.sleep(5)
                    module_maj_statut_sell_orders()
                    time.sleep(TIME_SLEEP_MAJ_STATUT_ORDRES)

                if veilleur_vente() == VENTE:
                    logger.info("activation du module vente")
                    module_vente()
                    time.sleep(TIME_SLEEP_MODULE_VENTE)

                else:
                    logger.info("activation du module maj des ordres")
                    module_maj_statut_buy_orders()
                    time.sleep(5)
                    module_maj_statut_sell_orders()
                    time.sleep(TIME_SLEEP_MAJ_STATUT_ORDRES)

            except Exception as e:
                logger.exception(e)


if __name__ == "__main__":
    try:
        robot_module_indicateurs()
        robot_module_trading()
    except Exception as e:
        logger.exception(e)
