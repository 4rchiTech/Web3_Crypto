from web3 import Web3
import psycopg2
import logging

# Commandes ##################################################

# lancer noeud light dans console : geth --syncmode light --http --http.api "eth,debug"

########## pour rapidité +++
# lancer noeud snap dans console : geth --syncmode snap --http --http.api "eth,debug"

# Constantes ##################################################

"""w3 = Web3(Web3.HTTPProvider("http://127.0.0.1:8545"))"""

url_infura = "https://mainnet.infura.io/v3/XXXXXXXXXXXXXXXXXXXXXXXX"
w3 = Web3(Web3.HTTPProvider(url_infura))

seuil_whale = 500000000000000000000  # en wei =  500 ETH

CHEMIN_LOG = "C:\\Users\crypt\Desktop\projet_seanode\kraken_extraction\logs\journal_logging_extraction_mineurs.log"
NAME_LOG = "journal_logging_extraction_mineurs.log"

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


def liste_mineurs_brute():
    conn = None
    try:
        conn = psycopg2.connect(
            host="localhost", database="bdd_seanode", user="postgres", password="archi"
        )

        curseur_selection = conn.cursor()
        curseur_selection.execute("SELECT addresse_mineur FROM seanode_core_bloc")
        liste_mineurs = curseur_selection.fetchall()
        curseur_selection.close()

        liste_clean = []
        for mineur in liste_mineurs:
            addresse_extraite = mineur[0]
            if addresse_extraite != None:
                liste_clean.append(addresse_extraite)

        return liste_clean

    except Exception as e:
        logger.exception(e)
    finally:
        if conn is not None:
            conn.close()
            print("Database connection closed.")


def existence_mineur_bdd(addresse_test=None):
    connexion = psycopg2.connect(
        host="localhost", database="bdd_seanode", user="postgres", password="archi"
    )

    try:
        curseur_selection = connexion.cursor()
        curseur_selection.execute("SELECT addresse FROM seanode_core_mineurs")

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


def create_mineur(table, colonne1, colonne2, colonne3, valeur1, valeur2, valeur3):
    conn = psycopg2.connect(
        host="localhost", database="bdd_seanode", user="postgres", password="archi"
    )
    try:
        cursor = conn.cursor()
        requete = (
            f"INSERT INTO {table}({colonne1},{colonne2},{colonne3}) VALUES(%s, %s, %s)"
        )
        valeur_insertion = (valeur1, valeur2, valeur3)
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


def update_mineur(table, colonne1, colonne2, valeur1, valeur2):
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


##############################################################################

################################ REQUETES NODE ###############################


def get_balance(addresse=None):
    requete = w3.eth.get_balance(addresse)
    return int(requete)


##############################################################################

################################ MAIN  #######################################

if __name__ == "__main__":

    try:

        while True:  # analyser chaque transaction
            for mineur in liste_mineurs_brute():
                if existence_mineur_bdd(addresse_test=mineur):
                    print("mineur déjà existant en BDD :", mineur)
                else:
                    if get_balance(addresse=mineur) >= seuil_whale:
                        create_mineur(
                            table="seanode_core_mineurs",
                            colonne1="addresse",
                            colonne2="statut_whale",
                            colonne3="dernier_solde",
                            valeur1=mineur,
                            valeur2=True,
                            valeur3=get_balance(addresse=mineur),
                        )
                        print("mineur WHALE ajouté à la BDD :", mineur)
                    else:
                        create_mineur(
                            table="seanode_core_mineurs",
                            colonne1="addresse",
                            colonne2="statut_whale",
                            colonne3="dernier_solde",
                            valeur1=mineur,
                            valeur2=False,
                            valeur3=get_balance(addresse=mineur),
                        )
                        print("mineur NON WHALE ajouté à la BDD :", mineur)
                continue

    except Exception as e:
        logger.exception(e)

##############################################################################
