from web3 import Web3
import psycopg2
import logging

# Commandes ##################################################

# lancer noeud light dans console : geth --syncmode light --http --http.api "eth,debug"

########## pour rapidité +++
# lancer noeud light dans console : geth --syncmode snap --http --http.api "eth,debug"

# Constantes ##################################################

"""w3 = Web3(Web3.HTTPProvider("http://127.0.0.1:8545"))"""

url_infura = "https://mainnet.infura.io/v3/XXXXXXXXXXXXXXXXXXXXXXXXX"
w3 = Web3(Web3.HTTPProvider(url_infura))

seuil_whale = 500000000000000000000  # en wei =  500 ETH

CHEMIN = r"C:\\Users\crypt\Desktop\projet_seanode"
CHEMIN_LOG = "C:\\Users\crypt\Desktop\projet_seanode\kraken_extraction\logs\journal_logging_extraction_bloc.log"
NAME_LOG = "journal_logging_extraction_bloc.log"

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


def existence_row_tx(hash_tx=None):
    connexion = psycopg2.connect(
        host="localhost", database="bdd_seanode", user="postgres", password="archi"
    )
    if hash_tx != None:

        try:
            curseur_selection = connexion.cursor()
            curseur_selection.execute(
                f"SELECT hash_tx FROM seanode_core_transactions WHERE hash_tx = '{hash_tx}'"
            )

            selection_hash = curseur_selection.fetchall()

            if len(selection_hash) == 0:
                print("tx analysée N°:", hash_tx)
                return False
            else:
                extraction = selection_hash[0][0]
                connexion.close()

            if hash_tx == extraction:
                print("la transaction existe déja en BDD..")
                return True
            else:
                print("tx analysée N°:", hash_tx)
                return False

        except Exception as e:
            logger.exception(e)
            return print("dead")


# CREATION #########################


def insertion_bloc(
    colonne1=None,
    colonne2=None,
    colonne3=None,
    colonne4=None,
    colonne5=None,
    colonne6=None,
    colonne7=None,
    colonne8=None,
    colonne9=None,
    valeur1=None,
    valeur2=None,
    valeur3=None,
    valeur4=None,
    valeur5=None,
    valeur6=None,
    valeur7=None,
    valeur8=None,
    valeur9=None,
):
    conn = psycopg2.connect(
        host="localhost", database="bdd_seanode", user="postgres", password="archi"
    )
    try:
        cursor = conn.cursor()
        cursor.execute(
            f"INSERT INTO seanode_core_bloc({colonne1},{colonne2},{colonne3},{colonne4},{colonne5},{colonne6},{colonne7},{colonne8},{colonne9}) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (
                valeur1,
                valeur2,
                valeur3,
                valeur4,
                valeur5,
                valeur6,
                valeur7,
                valeur8,
                valeur9,
            ),
        )
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        logger.exception(e)
    finally:
        if conn is not None:
            conn.close()

    return True


def insertion_genese_tx_bloc(
    colonne1=None,
    colonne2=None,
    colonne3=None,
    colonne4=None,
    valeur1=None,
    valeur2=None,
    valeur3=None,
    valeur4=None,
):
    conn = psycopg2.connect(
        host="localhost", database="bdd_seanode", user="postgres", password="archi"
    )
    try:

        cursor = conn.cursor()
        cursor.execute(
            f"INSERT INTO seanode_core_transactions({colonne1},{colonne2},{colonne3},{colonne4}) VALUES(%s, %s, %s, %s)",
            (
                valeur1,
                valeur2,
                valeur3,
                valeur4,
            ),
        )
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

################################ NETTOYAGE BDD ###############################


def query_last_block():
    connexion = psycopg2.connect(
        host="localhost", database="bdd_seanode", user="postgres", password="archi"
    )

    try:
        curseur_selection = connexion.cursor()
        curseur_selection.execute(
            "SELECT numero_bloc FROM seanode_core_bloc ORDER BY numero_bloc DESC LIMIT 1"
        )
        selection_BDD = curseur_selection.fetchall()
        last_block = selection_BDD[0][0]
        connexion.close()
        return last_block

    except Exception as e:
        logger.exception(e)


def clean_lastblock_avant_lancement():

    connexion = psycopg2.connect(
        host="localhost", database="bdd_seanode", user="postgres", password="archi"
    )

    try:
        curseur_delete = connexion.cursor()
        requete_delete = (
            f"DELETE FROM seanode_core_bloc WHERE numero_bloc = '{query_last_block()}'"
        )
        curseur_delete.execute(requete_delete)

        connexion.commit()
        connexion.close()
        return True

    except Exception as e:
        logger.exception(e)


##############################################################################

################################ REQUETES NODE ###############################


def get_block_extraction(numero_bloc):

    try:
        requete = w3.eth.get_block(numero_bloc)
        timestamp = requete["timestamp"]  # model Bloc
        numero_bloc = numero_bloc  # model Bloc
        difficulty = requete["difficulty"]  # model Bloc
        total_difficulty = requete["totalDifficulty"]  # model Bloc
        size = requete["size"]  # model Bloc
        gas_used = requete["gasUsed"]  # model Bloc
        gas_limit = requete["gasLimit"]  # model Bloc
        addresse_mineur = requete["miner"]  # models Bloc / Mineur / Transactions

        nombre_tx = len(requete["transactions"])
        liste_tx = requete["transactions"]

        insertion_bloc(
            colonne1="timestamp",
            colonne2="numero_bloc",
            colonne3="difficulty",
            colonne4="total_difficulty",
            colonne5="size",
            colonne6="gas_used",
            colonne7="gas_limit",
            colonne8="addresse_mineur",
            colonne9="nb_tx_bloc",
            valeur1=timestamp,
            valeur2=numero_bloc,
            valeur3=difficulty,
            valeur4=total_difficulty,
            valeur5=size,
            valeur6=gas_used,
            valeur7=gas_limit,
            valeur8=addresse_mineur,
            valeur9=nombre_tx,
        )

        try:
            for tx in liste_tx:
                cast_tx = tx.hex()
                if existence_row_tx(hash_tx=cast_tx) == False:
                    insertion_genese_tx_bloc(
                        colonne1="timestamp",
                        colonne2="numero_bloc",
                        colonne3="hash_tx",
                        colonne4="addresse_mineur",
                        valeur1=timestamp,
                        valeur2=numero_bloc,
                        valeur3=cast_tx,
                        valeur4=addresse_mineur,
                    )
            return numero_bloc

        except Exception as e:
            logger.exception(e)
            return numero_bloc

    except Exception as e:
        logger.exception(e)
        return False


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

if __name__ == "__main__":

    try:
        clean_lastblock_avant_lancement()  # suppression du dernier bloc analysé pour completer la liste tx inachevée

        while True:  # analyser chaque bloc
            last_block = query_last_block()
            if last_block is None:
                print("Aucun bloc dans la bdd, initialisation des blocs")
                block_cible = 1
                get_block_extraction(numero_bloc=block_cible)
                print("bloc analysé N° :", block_cible)
                continue
            elif int(last_block) >= 1:
                block_cible = int(last_block) + 1
                get_block_extraction(numero_bloc=block_cible)
                print("bloc analysé N° :", block_cible)
                continue
    except Exception as e:
        logger.exception(e)

##############################################################################
