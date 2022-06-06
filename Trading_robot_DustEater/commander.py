import script_v10b as main_1
from threading import Thread
import time
import psycopg2

############################################################################################


class robot_trading(Thread):
    def __init__(self):
        Thread.__init__(self)
        self.start()

    def run(self):

        while True:
            signal_trading = main_1.veilleur()

            while signal_trading:
                decompte = 10

                while decompte != 0:
                    time.sleep(30)
                    decompte -= 1
                    main_1.maj_ordres_buying(main_1.BDD_en_dictionnaire())
                    main_1.canceler_ordres_buying(main_1.BDD_en_dictionnaire())
                    main_1.maj_prix_liquidation(main_1.BDD_en_dictionnaire())
                    main_1.liquidation_ordres(main_1.BDD_en_dictionnaire())
                    main_1.maj_ordres_liquidation_closed(main_1.BDD_en_dictionnaire())
                    main_1.generateur_buy_order(main_1.BDD_en_dictionnaire())

                break

            else:
                decompte = 10

                while decompte != 0:
                    time.sleep(30)
                    decompte -= 1
                    main_1.maj_ordres_buying(main_1.BDD_en_dictionnaire())
                    main_1.canceler_ordres_buying(main_1.BDD_en_dictionnaire())
                    main_1.maj_prix_liquidation(main_1.BDD_en_dictionnaire())
                    main_1.liquidation_ordres(main_1.BDD_en_dictionnaire())
                    main_1.maj_ordres_liquidation_closed(main_1.BDD_en_dictionnaire())

            continue


class robot_messenger(Thread):
    def __init__(self):
        Thread.__init__(self)
        self.start()

    def run(self):
        while True:
            time.sleep(3600)
            main_1.messenger()
            main_1.cleaner_log()


class robot_BDD_indicateurs(Thread):
    def __init__(self):
        Thread.__init__(self)
        self.start()

    def run(self):
        while True:
            main_1.builder_BDD_indicator()


###########################################
if __name__ == "__main__":

    try:
        robot_trading()
        robot_messenger()
        robot_BDD_indicateurs()

    except Exception as e:
        print(e)


###########################################

"""
class robot_B(Thread):
    def __init__(self):
        Thread.__init__(self)
        self.start()

    def run(self):
        while True:
            main_2.main()
            continue


class robot_B_messenger(Thread):
    def __init__(self):
        Thread.__init__(self)
        self.start()

    def run(self):
        while True:
            main_2.messenger_robot_LUNA()


###########################################


class robot_C(Thread):
    def __init__(self):
        Thread.__init__(self)
        self.start()

    def run(self):
        while True:
            main_3.main()
            continue


class robot_C_messenger(Thread):
    def __init__(self):
        Thread.__init__(self)
        self.start()

    def run(self):
        while True:
            main_3.messenger_step_LUNA()

###########################################

robot_B()
robot_B_messenger()

robot_C()
robot_C_messenger()

###########################################

"""
