import ccxt
import time
import logging
import smtplib
import psycopg2
from email.message import EmailMessage

# constantes #########################################################################

ACHAT = "achat"
BUYING = "buying"
CANCELED = "canceled"
CLOSED = "closed"
COMPLETED = "completed"
LIQUIDATED = "liquidated"

CHEMIN_LOG = "C:\\Users\crypt\Desktop\DustEater\script_v10\journal_log_DustEater_MATICUSDT_OKX.log"
NAME_LOG = "journal_log_DustEater_MATICUSDT_OKX.log"

CEX = ccxt.okex()
SYMBOL = "MATIC/USDT"
BANKROLL = 1000
BUDGET_TRADE = 500

DELAI_AVANT_EXPIRATION = 60  # secondes
DELAI_MAJ_BDD_INDICATEURS = 25  # secondes

TIME_FRAME = "3m"

STEP_ACHAT = 0.00  # AVANT : 0.06
STEP_ACTIVATION_STOP_SUIVEUR = 1 + (0.09 / 100)
STEP_LIQUIDATION = 1 - (0.09 / 100)

time_stamp_actuel = int(time.time())

# infos connexion ########################################################################

okex_id = ccxt.okex(
    {
        "apiKey": "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
        "secret": "XXXXXXXXXXXXXXXXXXXXXXXXXXX",
        "password": "XXXXXXXXXXXXX",
    }
)

# module logging ########################################################################

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter(
    "%(asctime)s:%(funcName)s:%(levelname)s:%(lineno)d:%(message)s"
)

file_handler = logging.FileHandler(NAME_LOG)
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)

# Fonctions script ####################################################################

############################################################### FONCTIONS - UTILITAIRES


def requeteur(requete):

    connexion = psycopg2.connect(
        host="localhost",
        database="dusteater_bdd_trading",
        user="postgres",
        password="archi",
    )

    try:
        curseur = connexion.cursor()
        curseur.execute(requete)
        selection_BDD = curseur.fetchall()
        resultat = selection_BDD[0][0]
        connexion.close()

        return resultat

    except Exception as e:
        return e


def BDD_en_dictionnaire():

    connexion = psycopg2.connect(
        host="localhost",
        database="dusteater_bdd_trading",
        user="postgres",
        password="archi",
    )

    requete_fetch_all = "SELECT * FROM bdd_trading"
    curseur = connexion.cursor()
    curseur.execute(requete_fetch_all)
    selection_BDD = curseur.fetchall()

    liste_dictionnaire = []

    for i in selection_BDD:
        num = i[0]
        timestamp = i[1]
        order_id_achat = i[2]
        statut = i[3]
        prix_ordre = i[4]
        amount = i[5]
        order_id_vente = i[6]
        prix_liquidation = i[7]

        dico = {
            "numero": num,
            "timestamp": timestamp,
            "order_id_achat": order_id_achat,
            "statut": statut,
            "prix_ordre": prix_ordre,
            "amount": amount,
            "order_id_vente": order_id_vente,
            "prix_liquidation": prix_liquidation,
        }

        liste_dictionnaire.append(dico)
    connexion.close()

    logger.debug("Etape 0 : Le dictionnaire a bien été créé")

    return liste_dictionnaire


def cleaner_log():

    logger.debug("--------------------------------------------------------")

    try:
        with open(
            CHEMIN_LOG,
            "w",
        ) as fichier:
            fichier.write("")

        logger.debug("Fichier log nettoyé")

    except Exception as e:
        logger.exception(e)

    finally:

        logger.debug("--------------------------------------------------------")
        return True


############################################################### ORDRES ACHAT buying : MAJ EN COMPLETED SINON CANCEL AU BOUT DE XX SECONDES


def maj_bdd_statut_ordre_achat(
    order_id_cible=None, statut=None, prix_ordre=None, amount=None
):
    logger.debug("--------------------------------------------------------")

    logger.debug("Etape 0 : entrée fonction MAJ BDD statut ordre")

    connexion = psycopg2.connect(
        host="localhost",
        database="dusteater_bdd_trading",
        user="postgres",
        password="archi",
    )

    curseur_selection = connexion.cursor()
    curseur_selection.execute(
        f"SELECT * FROM bdd_trading WHERE order_id_achat ='{order_id_cible}'"
    )
    maj_trade = "UPDATE bdd_trading SET statut = %s, prix_ordre = %s, amount = %s WHERE order_id_achat = %s"
    curseur_maj = connexion.cursor()
    colonnes_maj = (
        statut,
        prix_ordre,
        amount,
        order_id_cible,
    )
    curseur_maj.execute(maj_trade, colonnes_maj)
    connexion.commit()
    connexion.close()

    logger.debug("Etape 1 : MAJ BDD réalisée : ordre completé, sortie de fonction")

    logger.debug("--------------------------------------------------------")

    return True


def maj_ordres_buying(
    dictionnaire_cible=None,
):

    try:
        if (
            requeteur("SELECT COUNT (statut) FROM bdd_trading  WHERE statut = 'buying'")
            != 0
        ):
            logger.debug("Existence d'ordres buying")

            closed_orders = okex_id.fetch_closed_orders(symbol=SYMBOL)
            closed_orders.reverse()

            for ligne in dictionnaire_cible:
                if ligne["statut"] == BUYING:
                    for orders in closed_orders:
                        if int(orders["info"]["ordId"]) == int(ligne["order_id_achat"]):
                            amount_brut = orders["amount"]
                            price = orders["price"]
                            fees_cex = orders["fee"]["cost"]
                            # fees = 0.0006005
                            # amount_net = round(amount_brut * (1 - fees), 9)
                            amount_net = float(amount_brut) - float(fees_cex)
                            print("le montant net est", amount_net)
                            maj_bdd_statut_ordre_achat(
                                order_id_cible=ligne["order_id_achat"],
                                statut=COMPLETED,
                                prix_ordre=price,
                                amount=amount_net,
                            )
                        else:
                            logger.debug(
                                "ordre comparé à la liste des ordres clôturé : ordre non complété"
                            )
                else:
                    logger.debug("ligne traitée non buying")
        else:
            logger.debug("Aucun ordre buying dans BDD, fin de fonction")

    except Exception as e:
        logger.exception(e)

    finally:
        return True


def maj_bdd_statut_ordre_canceled(order_id_cible=None, statut=None):

    logger.debug("--------------------------------------------------------")

    logger.debug("Etape 0 : entrée fonction MAJ BDD statut ordre")

    connexion = psycopg2.connect(
        host="localhost",
        database="dusteater_bdd_trading",
        user="postgres",
        password="archi",
    )

    curseur_selection = connexion.cursor()
    curseur_selection.execute(
        f"SELECT * FROM bdd_trading WHERE order_id_achat ='{order_id_cible}'"
    )
    maj_trade = "UPDATE bdd_trading SET statut = %s WHERE order_id_achat = %s"
    curseur_maj = connexion.cursor()
    colonnes_maj = (
        statut,
        order_id_cible,
    )
    curseur_maj.execute(maj_trade, colonnes_maj)
    connexion.commit()
    connexion.close()

    logger.debug("Etape 1 : MAJ BDD réalisée : ordre completé, sortie de fonction")

    logger.debug("--------------------------------------------------------")

    return True


def canceler_ordres_buying(dictionnaire_cible=None):

    logger.warning("--------------------------------------------------------")

    try:

        logger.debug("Etape 0 : entrée dans la fonction canceler")
        delai_expiration = DELAI_AVANT_EXPIRATION

        for ligne in dictionnaire_cible:

            seuil_horaire = ligne["timestamp"] + delai_expiration

            if ligne["statut"] == BUYING and int(time.time()) > int(seuil_horaire):

                logger.debug(
                    "Etape 1 : traitement d'une ligne qui a dépassé le délai d'expiration"
                )

                okex_id.cancel_order(ligne["order_id_achat"], SYMBOL)

                logger.debug(
                    "Etape 2 : annulation réalisée du trade d'achat en cours, délai expiré"
                )

                maj_bdd_statut_ordre_canceled(
                    order_id_cible=ligne["order_id_achat"], statut=CANCELED
                )

                logger.debug("Etape 3 : MAJ enregistrée de l'annulation dans BDD")

            else:
                logger.info("la ligne visée n'est pas un achat buying")

                continue

        logger.debug("fin de fonction de la fonction canceler, ordres analysés")

        logger.debug("--------------------------------------------------------")

        return True

    except Exception as e:
        logger.exception(e)

        logger.debug("--------------------------------------------------------")

        return False


############################################################### ORDRES ACHAT COMPLETED : LIQUIDATION OU UPDATE PRIX LIQUIDATION


def maj_ordres_liquidation_closed(
    dictionnaire_cible=None,
):

    try:
        if (
            requeteur(
                "SELECT COUNT (statut) FROM bdd_trading  WHERE statut = 'liquidated'"
            )
            != 0
        ):
            logger.debug("Existence d'ordres liquidés")

            closed_orders = okex_id.fetch_closed_orders(symbol=SYMBOL)
            closed_orders.reverse()

            for ligne in dictionnaire_cible:
                if ligne["statut"] == LIQUIDATED:
                    for orders in closed_orders:
                        if int(orders["info"]["ordId"]) == int(ligne["order_id_vente"]):
                            amount_brut = orders["amount"]
                            price = orders["price"]
                            maj_bdd_prix_realise_ordre_liquidation(
                                order_id_liquidation=ligne["order_id_vente"],
                                statut=CLOSED,
                                prix_liquidation=price,
                            )
                        else:
                            logger.debug(
                                "ordre comparé à la liste des ordres clôturé : ordre non complété"
                            )
                else:
                    logger.debug("ligne traitée non liquidée")
        else:
            logger.debug("Aucun ordre liquidé dans BDD, fin de fonction")

    except Exception as e:
        logger.exception(e)

    finally:
        return True


def maj_bdd_prix_realise_ordre_liquidation(
    order_id_liquidation=None, statut=None, prix_liquidation=None
):

    logger.debug("--------------------------------------------------------")

    logger.debug("Etape 0 : entrée fonction MAJ BDD statut ordre liquidé")

    connexion = psycopg2.connect(
        host="localhost",
        database="dusteater_bdd_trading",
        user="postgres",
        password="archi",
    )

    curseur_selection = connexion.cursor()
    curseur_selection.execute(
        f"SELECT * FROM bdd_trading WHERE order_id_vente ='{order_id_liquidation}'"
    )
    maj_trade = "UPDATE bdd_trading SET statut = %s, prix_liquidation = %s WHERE order_id_vente = %s"
    curseur_maj = connexion.cursor()
    colonnes_maj = (
        statut,
        prix_liquidation,
        order_id_liquidation,
    )
    curseur_maj.execute(maj_trade, colonnes_maj)
    connexion.commit()
    connexion.close()

    logger.debug("Etape 1 : MAJ BDD réalisée : ordre closed, sortie de fonction")

    logger.debug("--------------------------------------------------------")

    return True


def maj_prix_liquidation(
    dictionnaire_cible=None,
):

    logger.debug("--------------------------------------------------------")

    connexion = psycopg2.connect(
        host="localhost",
        database="dusteater_bdd_trading",
        user="postgres",
        password="archi",
    )

    try:

        logger.debug("Etape 0 : entrée dans fonction trailing stop")
        curseur = connexion.cursor()
        logger.debug(
            "Etape 1 : sélection du dernier prix pour UPDATE du prix de liquidation"
        )
        curseur.execute(
            "SELECT dernier_prix FROM bdd_indicateurs ORDER BY cle_primaire DESC LIMIT 1"
        )
        selection_last_price = curseur.fetchall()
        last_price = selection_last_price[0][0]
        connexion.close()

        for ligne in dictionnaire_cible:
            cast_prix_liquidation = float(ligne["prix_liquidation"])

            if (
                cast_prix_liquidation * float(STEP_ACTIVATION_STOP_SUIVEUR)
                < float(last_price)
                and ligne["statut"] == COMPLETED
            ):
                logger.debug(
                    "Etape 2 : la ligne visée est concernée par un UPDATE du prix de liquidation"
                )

                new_liquidation_price = ligne["prix_liquidation"] + (
                    (last_price - ligne["prix_liquidation"]) / 2
                )

                connexion = psycopg2.connect(
                    host="localhost",
                    database="dusteater_bdd_trading",
                    user="postgres",
                    password="archi",
                )

                curseur_selection = connexion.cursor()
                curseur_selection.execute(
                    f"SELECT * FROM bdd_trading WHERE order_id_achat ='{ligne['order_id_achat']}'"
                )
                maj_trade = "UPDATE bdd_trading SET prix_liquidation = %s WHERE order_id_achat = %s"
                curseur_maj = connexion.cursor()
                colonnes_maj = (
                    new_liquidation_price,
                    ligne["order_id_achat"],
                )

                curseur_maj.execute(maj_trade, colonnes_maj)
                connexion.commit()
                connexion.close()

                logger.debug(
                    "Etape 2.2 : le prix de liquidation de la ligne visée a été updaté"
                )

            else:
                logger.debug("la ligne visée n'est pas concernée par un UPDATE")
    except Exception as e:
        logger.exception(e)


def market_sell_order(qte_vente=None):

    logger.debug("--------------------------------------------------------")
    logger.debug("Etape 0 : entrée fonction MARKET order sell")
    okex_id.load_markets()
    formatted_amount = okex_id.amount_to_precision(SYMBOL, qte_vente)
    response = okex_id.create_market_sell_order(SYMBOL, amount=formatted_amount)
    order_id = response["info"]["ordId"]
    logger.debug("Etape 1/1 : MARKET order sell créé, sortie de fonction")
    logger.debug("--------------------------------------------------------")

    return order_id


def liquidation_ordres(
    dictionnaire_cible=None,
):

    logger.debug("--------------------------------------------------------")

    connexion = psycopg2.connect(
        host="localhost",
        database="dusteater_bdd_trading",
        user="postgres",
        password="archi",
    )

    try:

        logger.debug("Etape 0 : entrée dans fonction trailing stop")
        curseur = connexion.cursor()
        logger.debug("Etape 1 : sélection du dernier prix pour UPDATE ou LIQUIDATION")
        curseur.execute(
            "SELECT dernier_prix FROM bdd_indicateurs ORDER BY cle_primaire DESC LIMIT 1"
        )
        selection_last_price = curseur.fetchall()
        last_price = selection_last_price[0][0]

        for ligne in dictionnaire_cible:

            if (
                float(ligne["prix_liquidation"]) > float(last_price)
                and ligne["statut"] == COMPLETED
            ):

                logger.debug(
                    "Etape 2 : la ligne visée est concernée par une liquidation, le prix marché est inférieur au prix de liquidation"
                )

                order_id_liquidation = market_sell_order(ligne["amount"])

                logger.debug("Etape 2.1 : ordre en cours annulé, liquidation au marché")

                curseur_selection = connexion.cursor()
                curseur_selection.execute(
                    f"SELECT * FROM bdd_trading WHERE order_id_achat ='{ligne['order_id_achat']}'"
                )
                statut = LIQUIDATED

                maj_trade = "UPDATE bdd_trading SET statut = %s, order_id_vente = %s WHERE order_id_achat = %s"
                curseur_maj = connexion.cursor()
                colonnes_maj = (
                    statut,
                    order_id_liquidation,
                    ligne["order_id_achat"],
                )
                curseur_maj.execute(maj_trade, colonnes_maj)
                connexion.commit()

                logger.debug("Etape 2.2 : enregistrement de la liquidation dans BDD")

                continue

        logger.debug("Etape 3 : traitement fini, sortie de boucle for")

    except Exception as e:
        logger.exception(e)

    finally:
        logger.debug("Fin de fonction trailing stop, sortie")

        logger.debug("--------------------------------------------------------")


############################################################### CREATION ORDRES ACHAT : PRE VALIDATION & INSERTION EN BDD


def create_order_BDD(
    timestamp=None,
    order_id_achat=None,
    statut=None,
    prix_ordre=None,
    amount=None,
    prix_liquidation=None,
):

    logger.debug("--------------------------------------------------------")

    logger.debug("Etape 0 : entrée fonction création ordre BDD")
    connexion = psycopg2.connect(
        host="localhost",
        database="dusteater_bdd_trading",
        user="postgres",
        password="archi",
    )

    curseur = connexion.cursor()
    insertion = "INSERT INTO bdd_trading(timestamp, order_id_achat, statut, prix_ordre, amount, prix_liquidation) VALUES(%s, %s, %s, %s, %s, %s)"
    creation = (
        timestamp,
        order_id_achat,
        statut,
        prix_ordre,
        amount,
        prix_liquidation,
    )
    curseur.execute(insertion, creation)
    connexion.commit()
    connexion.close()

    logger.debug("Etape 1/1 : création ordre effectuée")
    logger.debug("--------------------------------------------------------")

    return True


def buy_order(qte_achat=None, prix_achat=None):

    logger.debug("--------------------------------------------------------")
    logger.debug("Etape 0 : entrée fonction order buy")

    formatted_amount = okex_id.amount_to_precision(SYMBOL, qte_achat)
    formatted_price = okex_id.price_to_precision(SYMBOL, prix_achat)
    response = okex_id.create_limit_buy_order(
        SYMBOL, amount=formatted_amount, price=formatted_price
    )
    order_id = response["info"]["ordId"]

    logger.debug("Etape 1/1 : order buy créé, sortie de fonction")
    logger.debug("--------------------------------------------------------")

    return order_id


def prevalidate_buying_order(dictionnaire_cible=None):

    try:
        logger.info("Etape 0 : entrée dans la pré-validation de l'achat")

        completed = requeteur(
            "SELECT COUNT (statut) FROM bdd_trading  WHERE statut = 'completed'"
        )
        running = requeteur(
            "SELECT COUNT (statut) FROM bdd_trading  WHERE statut = 'buying'"
        )

        return completed == 0 and running == 0

    except Exception as e:
        logger.exception(e)
        return False


def generateur_buy_order(dictionnaire_cible=None):

    logger.debug("--------------------------------------------------------")

    logger.debug("Etape 0 : entrée fonction création achat")

    if prevalidate_buying_order(dictionnaire_cible):

        logger.debug("Etape 1/4 : entrée dans le if True")

        okex_id.load_markets()
        requete_OB = CEX.fetch_order_book(SYMBOL)
        first_ask = requete_OB["bids"][0][0]  # PREMIERE OFFRE A l'ACHAT
        step = first_ask * (STEP_ACHAT / 100)
        prix_achat = first_ask - step
        qte_achat = BUDGET_TRADE / prix_achat
        statut = BUYING
        prix_liquidation = prix_achat * STEP_LIQUIDATION
        fees = 0.0006005
        amount_net = round(qte_achat * (1 - fees), 9)

        logger.debug("Etape 2 : load market réalisé")

        extraction_order_id = buy_order(qte_achat=qte_achat, prix_achat=prix_achat)

        logger.debug("Etape 3 : ordre d'achat envoyé")

        create_order_BDD(
            timestamp=int(time.time()),
            order_id_achat=extraction_order_id,
            statut=statut,
            prix_ordre=prix_achat,
            amount=amount_net,
            prix_liquidation=prix_liquidation,
        )

        logger.debug("Etape 4 : ordre d'achat créé dans la BDD, fin de fonction")
        logger.debug("--------------------------------------------------------")

        return True

    else:

        logger.debug(
            "Etape 1/1 : entrée dans le if False, fin de fonction : la BDD n'est pas vide"
        )
        logger.debug("--------------------------------------------------------")

        return False


############################################################### BDD INDICATEURS & VEILLEUR


def veilleur():

    logger.debug("--------------------------------------------------------")

    logger.debug("Etape 0 : entrée fonction veilleur")

    try:

        connexion = psycopg2.connect(
            host="localhost",
            database="dusteater_bdd_trading",
            user="postgres",
            password="archi",
        )

        curseur = connexion.cursor()
        curseur.execute(
            "SELECT ratio_MM6_MM18 FROM bdd_indicateurs ORDER BY cle_primaire DESC LIMIT 2"
        )
        selection_BDD_MM6 = curseur.fetchall()
        dernier_ratio_MM6 = selection_BDD_MM6[0][0]
        avant_dernier_ratio_MM6 = selection_BDD_MM6[1][0]

        curseur.execute(
            "SELECT ratio_MM3_MM18 FROM bdd_indicateurs ORDER BY cle_primaire DESC LIMIT 1"
        )
        selection_BDD_MM3 = curseur.fetchall()
        dernier_ratio_MM3 = selection_BDD_MM3[0][0]

        connexion.close()

        logger.debug("Etape 1 : requetes sur BDD réalisées")

        if (
            dernier_ratio_MM6 > avant_dernier_ratio_MM6
            and dernier_ratio_MM6 > 1.001
            and dernier_ratio_MM3 > 1.001
        ):
            logger.debug(
                "Etape 2 : Ratio supérieurs aux seuils, requête balance en cours"
            )
            balance = okex_id.fetch_balance()
            liquidity = round(balance["USDT"]["free"], 2)

            if liquidity > BUDGET_TRADE:
                logger.debug(
                    "Etape 3 : Liquidités disponibles, le veilleur autorise le trading"
                )

                logger.debug("--------------------------------------------------------")

                return True
            else:
                logger.debug(
                    "Etape 3 : Liquidités insuffisantes, le veilleur interdit le trading"
                )

                logger.debug("--------------------------------------------------------")

                return False
        else:
            logger.debug(
                "Etape 2 : Ratios inférieurs aux seuils, le veilleur interdit le trading"
            )

            logger.debug("--------------------------------------------------------")

            return False

    except Exception as e:
        logger.exception(e)

        logger.debug("--------------------------------------------------------")

        return False


def builder_BDD_indicator():

    logger.debug("--------------------------------------------------------")

    try:
        klines_MM18 = CEX.fetch_ohlcv(SYMBOL, timeframe=TIME_FRAME, limit=18)
        klines_cloture_MM18 = [i[4] for i in klines_MM18]
        klines_MM6 = klines_cloture_MM18[12:]
        klines_MM3 = klines_cloture_MM18[15:]
        last_kline_price = klines_cloture_MM18[17:][0]

        volumes_bougies_MM18 = [i[5] for i in klines_MM18]
        moyenne_volumes_MM18 = round(sum(volumes_bougies_MM18) / 18, 2)

        ratio_MM6_MM18 = round(
            (sum(klines_MM6) / 6) / (sum(klines_cloture_MM18) / 18), 5
        )
        ratio_MM3_MM18 = round(
            (sum(klines_MM3) / 3) / (sum(klines_cloture_MM18) / 18), 5
        )

        timestamp_requete = max(i[0] for i in klines_MM18)

        connexion = psycopg2.connect(
            host="localhost",
            database="dusteater_bdd_trading",
            user="postgres",
            password="archi",
        )

        curseur = connexion.cursor()
        insertion = "INSERT INTO bdd_indicateurs(timestamp, volume_MM18, ratio_MM3_MM18, ratio_MM6_MM18, dernier_prix) VALUES(%s, %s, %s, %s, %s)"
        creation = (
            timestamp_requete,
            moyenne_volumes_MM18,
            ratio_MM3_MM18,
            ratio_MM6_MM18,
            last_kline_price,
        )
        curseur.execute(insertion, creation)
        connexion.commit()
        connexion.close()

    except Exception as e:
        logger.exception(e)

    finally:
        logger.debug("Etape n : alimentation de la BDD indicateurs")

        logger.debug("--------------------------------------------------------")

        time.sleep(DELAI_MAJ_BDD_INDICATEURS)

    return True


############################################################### ANALYSE BDD & REPORTING


def analyse_BDD():

    buying = requeteur(
        "SELECT COUNT (statut) FROM bdd_trading  WHERE statut = 'buying'"
    )

    completed = requeteur(
        "SELECT COUNT (statut) FROM bdd_trading  WHERE statut = 'completed'"
    )

    canceled = requeteur(
        "SELECT COUNT (statut) FROM bdd_trading  WHERE statut = 'canceled'"
    )

    liquidated = requeteur(
        "SELECT COUNT (statut) FROM bdd_trading  WHERE statut = 'liquidated'"
    )

    closed = requeteur(
        "SELECT COUNT (statut) FROM bdd_trading  WHERE statut = 'closed'"
    )

    total = requeteur("SELECT COUNT (num) FROM bdd_trading")

    compilation_avant_str = f"Nb d'ordres complétés : {closed}\nNb d'achats réalisés : {completed}\nNb d'achats en cours : {buying}\nNb d'ordres annulés : {canceled}\nNb d'ordres liquidés : {liquidated}\nNb total d'ordres en BDD : {total}"

    return str(compilation_avant_str)


def pnl(dictionnaire_cible=None):
    sum_PRU_achat = 0
    sum_PRU_vente = 0

    for ligne in dictionnaire_cible:
        if ligne["statut"] == "closed":
            pru_achat = round(ligne["amount"] * ligne["prix_ordre"], 3)
            montant_vente = round(ligne["amount"] * ligne["prix_liquidation"], 3)
            sum_PRU_achat += pru_achat
            sum_PRU_vente += montant_vente
            continue

    resultat = round(sum_PRU_vente - sum_PRU_achat, 3)

    return str(
        f"Résultat actuel : {resultat}\nMontant total des achats : {sum_PRU_achat}\nMontant total des ventes : {sum_PRU_vente}"
    )


def messenger():

    logger.debug("--------------------------------------------------------")

    try:

        msg = EmailMessage()
        msg["Subject"] = f"OKX - Journal LOG  BOT LIQUIDATION - {SYMBOL}"
        msg["From"] = "webhookmsg@gmail.com"
        msg["To"] = "webhookmsg@gmail.com"
        msg.set_content(analyse_BDD() + "\n" + "\n" + pnl(BDD_en_dictionnaire()))

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login("webhookmsg@gmail.com", "mdmm okra qqnx vapr")
            smtp.send_message(msg)

        logger.debug("Mail web hook envoyé par robot messenger")
        logger.debug("--------------------------------------------------------")

        return True

    except Exception as e:
        logger.exception(e)
        logger.debug("--------------------------------------------------------")


############################################################### MAIN LOCAL

"""
if __name__ == "__main__":

    maj_ordres_buying(BDD_en_dictionnaire())
    canceler_ordres_buying(BDD_en_dictionnaire())
    maj_ordres_liquidation_closed(BDD_en_dictionnaire())
    maj_prix_liquidation(BDD_en_dictionnaire())
    liquidation_ordres(BDD_en_dictionnaire())
    generateur_buy_order(BDD_en_dictionnaire())
"""
