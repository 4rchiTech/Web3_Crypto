from web3 import Web3
import psycopg2
import logging
import json

# Commandes ##################################################

# lancer noeud light dans console : geth --syncmode light --http --http.api "eth,debug"

########## pour rapidité +++
# lancer noeud snap dans console : geth --syncmode snap --http --http.api "eth,debug"

# Constantes ##################################################

"""w3 = Web3(Web3.HTTPProvider("http://127.0.0.1:8545"))"""

url_infura = "https://mainnet.infura.io/v3/XXXXXXXXXXXXXXXXXXXXXXXXXXXX"
w3 = Web3(Web3.HTTPProvider(url_infura))

seuil_whale = 500000000000000000000  # en wei =  500 ETH

CHEMIN_LOG = "C:\\Users\crypt\Desktop\projet_seanode\kraken_extraction\logs\journal_logging_extraction_tx.log"
NAME_LOG = "journal_logging_extraction_tx.log"

#################################

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

# SELECTION #########################


def requete(requete=None):
    conn = None
    try:
        conn = psycopg2.connect(
            host="localhost", database="bdd_seanode", user="postgres", password="archi"
        )
        cur = conn.cursor()
        # execute a statement
        cur.execute(requete)
        requete = cur.fetchall()
        print(requete)
        cur.close()

    except Exception as e:
        logger.exception(e)
    finally:
        if conn is not None:
            conn.close()
            print("Database connection closed.")


def liste_tx_brute():
    conn = None
    try:
        conn = psycopg2.connect(
            host="localhost", database="bdd_seanode", user="postgres", password="archi"
        )

        curseur_selection = conn.cursor()
        curseur_selection.execute("SELECT hash_tx FROM seanode_core_transactions")
        liste_tx = curseur_selection.fetchall()
        curseur_selection.close()

        liste_clean = []
        for tx in liste_tx:
            hash_tx_extrait = tx[0]
            liste_clean.append(hash_tx_extrait)

        return liste_clean

    except Exception as e:
        logger.exception(e)
    finally:
        if conn is not None:
            conn.close()
            print("Database connection closed.")


def existence_whale_bdd(addresse_test=None):
    connexion = psycopg2.connect(
        host="localhost", database="bdd_seanode", user="postgres", password="archi"
    )

    try:
        curseur_selection = connexion.cursor()
        curseur_selection.execute("SELECT addresse FROM seanode_core_whales")

        addresses = curseur_selection.fetchall()

        connexion.close()

        liste_addresses = []

        for addresse in addresses:
            addresse_extraite = addresse[0]
            liste_addresses.append(addresse_extraite)

        if str(addresse_test) in liste_addresses:
            return True

    except Exception as e:
        logger.exception(e)
        return False


# CREATION #########################


def emetteur_ou_receiver_contrat(
    addresse_from=None, adresse_to=None
):  # le destinataire ou l'emetteur de la tx est-il un contrat? Si oui il faut le notifier
    # recuperer liste contrats connus et croiser avec les deux autres

    conn = None
    try:
        conn = psycopg2.connect(
            host="localhost", database="bdd_seanode", user="postgres", password="archi"
        )

        curseur_selection = conn.cursor()
        curseur_selection.execute("SELECT addresse_contrat FROM seanode_core_tokens")
        liste_tx = curseur_selection.fetchall()
        print("from", addresse_from)
        print("to", adresse_to)
        print(liste_tx)
        curseur_selection.close()

        liste_clean = [i[0] for i in liste_tx if i[0] != None]
        print(liste_clean)
        # retour fonction suivant resultats croisement listes

        if str(addresse_from) in liste_clean:
            return "from"
        elif str(adresse_to) in liste_clean:
            return "to"
        else:
            return "aucun"

    except Exception as e:
        logger.exception(e)
    finally:
        if conn is not None:
            conn.close()
            print("Database connection closed.")


def create_whale(table, colonne1, colonne2, valeur1, valeur2):
    conn = psycopg2.connect(
        host="localhost", database="bdd_seanode", user="postgres", password="archi"
    )
    try:
        cursor = conn.cursor()
        requete = f"INSERT INTO {table}({colonne1},{colonne2}) VALUES(%s, %s)"
        valeur_insertion = (valeur1, valeur2)
        cursor.execute(requete, valeur_insertion)
        conn.commit()
        cursor.close()
        conn.close()

    except Exception as e:
        logger.exception(e)
    finally:
        if conn is not None:
            conn.close()

    return True


def update_whale(table, colonne1, colonne2, valeur1, valeur2):
    conn = psycopg2.connect(
        host="localhost", database="bdd_seanode", user="postgres", password="archi"
    )
    try:
        cursor = conn.cursor()
        requete = f"INSERT INTO {table}({colonne1},{colonne2}) VALUES(%s, %s)"
        valeur_insertion = (valeur1, valeur2)
        cursor.execute(requete, valeur_insertion)
        conn.commit()
        cursor.close()
        conn.close()

    except Exception as e:
        logger.exception(e)
    finally:
        if conn is not None:
            conn.close()

    return True


def insertion_bdd_tx(
    valeur1=None,
    valeur2=None,
    valeur3=None,
    valeur4=None,
    valeur5=None,
    valeur6=None,
    valeur7=None,
    valeur8=None,
    hash_tx=None,
):
    connexion = psycopg2.connect(
        host="localhost", database="bdd_seanode", user="postgres", password="archi"
    )
    try:

        curseur_selection = connexion.cursor()
        curseur_selection.execute(
            f"SELECT hash_tx FROM seanode_core_transactions WHERE hash_tx ='{hash_tx}'"
        )

        maj_tx = "UPDATE seanode_core_transactions SET emetteur_from = %s, destinataire_to = %s, value_tx = %s, adresse_contrat = %s, statut_tx = %s, gas_used = %s, cumulative_gas_used = %s, from_ou_to_estuncontrat = %s WHERE hash_tx = %s"
        curseur_maj = connexion.cursor()
        colonnes_maj = (
            valeur1,
            valeur2,
            valeur3,
            valeur4,
            valeur5,
            valeur6,
            valeur7,
            valeur8,
            hash_tx,
        )

        curseur_maj.execute(maj_tx, colonnes_maj)
        connexion.commit()
        connexion.close()

    except Exception as e:
        logger.exception(e)
    finally:
        if connexion is not None:
            connexion.close()

    return True


##############################################################################

################################ REQUETES NODE ###############################


def get_balance(addresse=None):
    requete = w3.eth.get_balance(addresse)
    return int(requete)


def get_tx_extraction(hash_tx=None):

    try:
        requete = w3.eth.get_transaction(transaction_hash=hash_tx)
        emetteur_from = requete["from"]
        destinataire_to = requete["to"]
        value_tx = requete["value"]

        requete_receipt = w3.eth.get_transaction_receipt(transaction_hash=hash_tx)
        adresse_contrat = requete_receipt["contractAddress"]
        statut_tx = bool(requete_receipt["status"])
        gas_used = requete_receipt["gasUsed"]
        cumulative_gas_used = requete_receipt["cumulativeGasUsed"]

        cast_emetteur_from = emetteur_from.lower()
        cast_destinataire_to = destinataire_to.lower()

        from_ou_to_estuncontrat = emetteur_ou_receiver_contrat(
            addresse_from=cast_emetteur_from, adresse_to=cast_destinataire_to
        )

        print(requete_receipt)

        print("adresse contrat :", adresse_contrat)
        print("fonction :", from_ou_to_estuncontrat)

        """insertion_bdd_tx(
            valeur1=emetteur_from,
            valeur2=destinataire_to,
            valeur3=value_tx,
            valeur4=adresse_contrat,
            valeur5=statut_tx,
            valeur6=gas_used,
            valeur7=cumulative_gas_used,
            valeur8=from_ou_to_estuncontrat,
            hash_tx=hash_tx,
        )"""
        try:
            if existence_whale_bdd(addresse_test=emetteur_from) == False:
                solde_emetteur = get_balance(emetteur_from)
                if solde_emetteur > seuil_whale:
                    create_whale(
                        table="seanode_core_whales",
                        colonne1="addresse",
                        colonne2="dernier_solde",
                        valeur1=emetteur_from,
                        valeur2=solde_emetteur,
                    )
            if existence_whale_bdd(addresse_test=destinataire_to) == False:
                solde_destinataire = get_balance(destinataire_to)
                if solde_destinataire > seuil_whale:
                    create_whale(
                        table="seanode_core_whales",
                        colonne1="addresse",
                        colonne2="dernier_solde",
                        valeur1=destinataire_to,
                        valeur2=solde_destinataire,
                    )

        except Exception as e:
            logger.exception(e)
            return True

    except Exception as e:
        logger.exception(e)

    return False


print(
    get_tx_extraction(
        hash_tx="0x5488510df045770efbff57f25d0c6d2c1404d58c1199b21eb8dc5072b22d91d7"
    )
)


"""test = w3.eth.get_transaction(
    transaction_hash="0x0624ad62e358879577109398a25898027f39728d8ace1c541b7d903a52b4cf18"
)"""


# A°) Créer script pour "finir" via UPDATE SQL de compléter les lignes TX

# B°) Créer script pour "finir" via UPDATE SQL d'ajouter la TX à la liste du portefeuille concerné
# CREER TABLE "WALLET" pour recenser tous les wallet scannés

# C°) Créer script pour créer les mineurs inexistants avec :
# statut whales ou non  / requete balance
# statut ENS ou non     / requete balance
# dernier solde         / requete balance
# total_nb_tx_validees = nb_tx de bloc
# total_volume_tx_validees =
# derniere_date_activite = timestamp

# ou updater les mineurs existants avec :
# statut whales ou non  / requete balance
# dernier solde         / requete balance
# total_nb_tx_validees = nb_tx de bloc
# total_volume_tx_validees =
# derniere_date_activite = timestamp

# créer autres scripts


##############################################################################

################################ MAIN  #######################################

"""if __name__ == "__main__":

    try:

        while True:  # analyser chaque transaction
            for tx in liste_tx_brute():
                get_tx_extraction(
                    hash_tx=tx
                )  # liste tx brute à affiner pour ne pas reprendre des lignes traitées
                print("FIN : transaction analysée N° :", tx)
                continue

    except Exception as e:
        logger.exception(e)"""


##############################################################################
