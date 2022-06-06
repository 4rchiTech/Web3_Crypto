from web3 import Web3
import psycopg2
import logging
import re
import json

# Commandes ##################################################

# lancer noeud light dans console : geth --syncmode light --http --http.api "eth,debug"

########## pour rapidité +++
# lancer noeud light dans console : geth --syncmode snap --http --http.api "eth,debug"

# Constantes ##################################################

CHEMIN_LOG = "C:\\Users\crypt\Desktop\projet_seanode\kraken_extraction\logs\journal_logging_extraction_bloc.log"
NAME_LOG = "journal_logging_extraction_bloc.log"


url_infura = "https://mainnet.infura.io/v3/b5ae0e5cfbb141818b853505cdb74170"
w3 = Web3(Web3.HTTPProvider(url_infura))

"""w3 = Web3(Web3.HTTPProvider("http://127.0.0.1:8545"))"""
###############################################################


contrat_test = "0x514910771AF9Ca656af840dff83E8264EcF986CA"


###############################################################

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

################################ REQUETES BDD ################################


def liste_contrats_brute():
    conn = None
    try:
        conn = psycopg2.connect(
            host="localhost", database="bdd_seanode", user="postgres", password="archi"
        )

        curseur_selection = conn.cursor()
        curseur_selection.execute(
            "SELECT adresse_contrat FROM seanode_core_transactions"
        )
        liste_tx = curseur_selection.fetchall()
        curseur_selection.close()

        return [i[0] for i in liste_tx if i[0] != None]

    except Exception as e:
        logger.exception(e)
    finally:
        if conn is not None:
            conn.close()
            print("Database connection closed.")


def insertion_genese_contrat(
    valeur1=None,
    valeur2=None,
    valeur3=None,
    valeur4=None,
    valeur5=None,
):
    conn = psycopg2.connect(
        host="localhost", database="bdd_seanode", user="postgres", password="archi"
    )
    try:
        insertion = f"INSERT INTO seanode_core_tokens(addresse_contrat, nom_token, acronyme_token, total_supply, all_functions) VALUES('{valeur1}', '{valeur2}', '{valeur3}', '{valeur4}', '{valeur5}');"

        cursor = conn.cursor()
        cursor.execute(insertion)
        conn.commit()
        cursor.close()
        conn.close()

    except Exception as e:
        logger.exception(e)

    finally:
        if conn is not None:
            conn.close()

    return True


def existence_token_bdd(addresse_test=None):
    connexion = psycopg2.connect(
        host="localhost", database="bdd_seanode", user="postgres", password="archi"
    )

    try:
        curseur_selection = connexion.cursor()
        curseur_selection.execute("SELECT addresse_contrat FROM seanode_core_tokens")

        addresses = curseur_selection.fetchall()

        connexion.close()

        """liste_addresses = []

        for addresse in addresses:
            addresse_extraite = addresse[0]
            liste_addresses.append(addresse_extraite)"""

        liste_addresses = [addresse[0] for addresse in addresses]

        if str(addresse_test) in liste_addresses:
            return True

    except Exception as e:
        logger.exception(e)
        return False


##############################################################################

################################ REQUETES NODE ###############################


def get_contrat(contrat_cible=None):

    abi = json.loads(
        '[{"constant":true,"inputs":[],"name":"name","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"_spender","type":"address"},{"name":"_value","type":"uint256"}],"name":"approve","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"totalSupply","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"_from","type":"address"},{"name":"_to","type":"address"},{"name":"_value","type":"uint256"}],"name":"transferFrom","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint8"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"_to","type":"address"},{"name":"_value","type":"uint256"},{"name":"_data","type":"bytes"}],"name":"transferAndCall","outputs":[{"name":"success","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"_spender","type":"address"},{"name":"_subtractedValue","type":"uint256"}],"name":"decreaseApproval","outputs":[{"name":"success","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"name":"_owner","type":"address"}],"name":"balanceOf","outputs":[{"name":"balance","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"symbol","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"_to","type":"address"},{"name":"_value","type":"uint256"}],"name":"transfer","outputs":[{"name":"success","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"_spender","type":"address"},{"name":"_addedValue","type":"uint256"}],"name":"increaseApproval","outputs":[{"name":"success","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"name":"_owner","type":"address"},{"name":"_spender","type":"address"}],"name":"allowance","outputs":[{"name":"remaining","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"inputs":[],"payable":false,"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":true,"name":"from","type":"address"},{"indexed":true,"name":"to","type":"address"},{"indexed":false,"name":"value","type":"uint256"},{"indexed":false,"name":"data","type":"bytes"}],"name":"Transfer","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"owner","type":"address"},{"indexed":true,"name":"spender","type":"address"},{"indexed":false,"name":"value","type":"uint256"}],"name":"Approval","type":"event"}]'
    )

    try:

        contrat = w3.eth.contract(address=contrat_cible, abi=abi)

        addresse_contrat = contrat_cible
        nom_token = contrat.functions.name().call()
        acronyme_token = contrat.functions.symbol().call()
        total_supply = contrat.functions.totalSupply().call()
        all_functions = contrat.all_functions()
        cast_name = nom_token.replace(" Token", "")

        tuple_contrat = (
            addresse_contrat,
            cast_name,
            acronyme_token,
            total_supply,
            all_functions,
        )

        print(tuple_contrat[0])

        insertion_genese_contrat(
            valeur1=tuple_contrat[0],
            valeur2=tuple_contrat[1],
            valeur3=tuple_contrat[2],
            valeur4=tuple_contrat[3],
            valeur5=tuple_contrat[4],
        )

        return True

    except Exception as e:
        logger.exception(e)


##############################################################################

################################ MAIN  #######################################


get_contrat(contrat_cible="0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")

"""if __name__ == "__main__":

    try:
        for contrat in liste_contrats_brute():
            if existence_token_bdd() == False:
                get_contrat(contrat_cible=contrat)
                print("Contrat analysé N°: ", contrat)

    except Exception as e:
        logger.exception(e)
"""
