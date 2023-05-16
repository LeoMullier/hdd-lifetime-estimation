import math
import multiprocessing
import os
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from tqdm import tqdm
from scipy import stats
from scipy.optimize import curve_fit
import numpy as np
from scipy.stats import weibull_min
import csv
import argparse


# --------------------- Utilitaire global  --------------------- 

'''
Parcours l'aborescence des fichiers
'''
def parcourir_repertoire(chemin):
    fichiers = []
    # Parcourir le répertoire et stocker tous les noms de fichiers dans le tableau
    for nom_fichier in os.listdir(chemin):
        chemin_fichier = os.path.join(chemin, nom_fichier)
        if os.path.isfile(chemin_fichier):
            fichiers.append(chemin_fichier)
        elif os.path.isdir(chemin_fichier):
            fichiers.extend(parcourir_repertoire(chemin_fichier))

    return fichiers

'''
Transforme un string en liste : [a,b,c]
'''
def create_list_from_string(s):
    # Supprimer les crochets de début et de fin
    s = s.strip("[]")
    
    # Diviser la chaîne en fonction des virgules
    values = s.split(",")
    
    # Convertir les valeurs en float et les ajouter à une liste
    result = [float(value) for value in values]
    
    return result

# --------------------- Utilitaire pour les données smart --------------------- 

'''
Fonction qui permet d'initialiser le dictionnaire des valeurs des donnée
'''
def remplir_dico_moyenne(fichier, liste):
    print('Entrée dans la fonction : remplir_dico_moyenne()')
    
    dico = {}
    nb_fichiers = len(fichier)
    liste_valeurs = {}

    print('Etape 1 : Somme des valeurs pour tous les disques')

    for f in tqdm(fichier):
        df = pd.read_csv(f, sep='\t')
        df = df.fillna(value=0)

        for l in liste:
            df[l] = df[l].apply(lambda x: float(x.replace(',', '.')) if isinstance(x, str) else x)
            if l not in dico:
                dico[l] = {}
                nb_valeurs_par_date[l] = {}

            if l not in liste_valeurs:
                liste_valeurs[l] = {}

            for i in range(len(df)):
                date = df.loc[i, 'trace']
                valeur = df.loc[i, l]

                if date not in liste_valeurs[l]:
                    liste_valeurs[l][date] = []

                if isinstance(valeur, float) and valeur != 0:
                    liste_valeurs[l][date].append(valeur)
                    if date not in dico[l]:
                        dico[l][date] = valeur
                        nb_valeurs_par_date[l][date] = 1
                    else:
                        dico[l][date] += valeur
                        nb_valeurs_par_date[l][date] += 1

    print("Etape 2 : Calcul de la moyenne et de l'erreur")

    for l in tqdm(dico):
        for date in dico[l]:
            moyenne = dico[l][date] / nb_valeurs_par_date[l][date]
            erreur = np.nanstd(liste_valeurs[l][date]) / np.sqrt(nb_valeurs_par_date[l][date])
            dico[l][date] = (moyenne, erreur)
    
    print('Sortie de la fonction : remplir_dico_moyenne()')
    return dico



'''
Fonction qui permet de paralléliser l'exécution du code ajouter_colonne_trace()
'''
def process_file(f):
    df = pd.read_csv(f, sep='\t')
    trace = pd.Series(range(121, -1, -1))
    df['trace'] = trace[: len(df)]
    df.to_csv(f, sep='\t', index=False, columns=list(df.columns) + ['trace'])


'''
Fonction qui permet d'ajouter dans le csv la colonne "trace" 
qui correspond à une date relative, 
afin que tous les disques et la même date artificielle de début et de fin
'''
def ajouter_colonne_trace(fichier):
    pool = multiprocessing.Pool(
        processes=None
    )  # crée un pool de processus avec tous les CPU disponibles
    for _ in tqdm(pool.imap_unordered(process_file, fichier), total=len(fichier)):
        pass




'''
Fonction qui permet de tracer le dictionnaire des données smart
'''
def tracer_dico(dico):
    for col, valeurs in dico.items():
        x = []
        y = []
        yerr = []
        test = []
        for date, (valeur, erreur) in valeurs.items():
            x.append(date)
            y.append(valeur)
            yerr.append(erreur)
        plt.figure()
        plt.errorbar(x, y, yerr=yerr, fmt='-', label=col, ecolor='r', capsize=3)
        plt.xlabel('Date')
        plt.ylabel('Valeur')
        plt.legend()
        if not os.path.exists(f'results/graphs/{col}'):
            os.makedirs(f'results/graphs/{col}')
        plt.savefig(f'results/graphs/{col}/graph.png')
        plt.show(block=False)



# --------------------- Utilitaire pour la courbe en baignoire  --------------------- 

'''
Fonction qui permet d'ajouter la colonne des durée de vie
'''
def ajouter_colonne_duree_vie(fichier, annee_voulu, duree):
    global nbdisque
    compteur = 0
    print('Ajouter duree de vie')
    for f in fichier:
        df = pd.read_csv(f, sep='\t')
        
        #if len(df) >= 120 and df.iloc[0]['model'] in ["ST12000NM0007","WDC WD30EFRX","ST12000NM0008","ST4000DM000","ST8000NM0055","HGST HMS5C4040ALE640","TOSHIBA MQ01ABF050M","ST12000NM0008","TOSHIBA MG07ACA14TA"] :

        # Sélection des années voulues
        if any(os.path.basename(f).startswith(str(annee)) for annee in annee_voulu) :
            nbdisque += 1

            nb_heures_tot = df.iloc[0]['smart_9_raw']
            if isinstance(nb_heures_tot, str):
                nb_heures_tot = nb_heures_tot.replace(',', '.')[
                    :-2
                ]  # remplace la virgule par un point
            else:
                if math.isnan(nb_heures_tot):
                    continue

            if(duree =="mois"):
                mois = round(int(nb_heures_tot) / (30 * 24), 0)
            elif(duree=="trimestre"):
                mois = math.ceil(int(nb_heures_tot) / (30 * 24) / 3) * 3
            
            id = df.iloc[0]['serial_number']
            model = df.iloc[0]['model']

            dico_duree_vie[id] = round(mois, 0)


        else:
            compteur = compteur + 1


'''
Fonction qui permet d'initialiser le dictionnaire correspondant à la courbe en baignoire
'''
def courbe_baignoire():
    print('courbe baignoire')
    for i, m in tqdm(dico_duree_vie.items()):
        if m not in dico_baignoire:
            dico_baignoire[m] = 1
        else:
            dico_baignoire[m] += 1


'''
Fonction qui permet de tracer le nombre de disque qui cesse de fonctionné en fonction du temps en mois
'''
def tracer_courbe_de_vie():
    x = sorted(dico_baignoire.keys())

    # On récupère les fréquences correspondantes
    y = [dico_baignoire[mois] for mois in x]

    # On trace la courbe
    plt.plot(x, y, '.')

    plt.yticks(range(min(y), max(y) + 1, 10))

    # On ajoute des titres et des étiquettes d'axes
    plt.title('Courbe de vie')
    plt.xlabel('Mois')
    plt.ylabel('Fréquence')

    # On affiche le graphique
    plt.show()

'''
Fonction lié à la courbe de Weibull ?
'''
def weib(x, k, scale):
    return (k / scale) * (x / scale)**(k-1) * np.exp(-(x/scale)**k)

'''
Fonction qui trace la courbe en baignoire
'''
def tracer_courbe_baignoire(annee_voulu,duree):
    global nbdisque

    # Calcul du nombre cumulatif de défaillances
    x = sorted(dico_baignoire.keys())
    y = []
    for mois in x:
        y.append(dico_baignoire[mois] / nbdisque)
        nbdisque -= dico_baignoire[mois]

    # Sauvegarde des valeurs
    fichier_csv = "C:\\Users\\utcpret\\Documents\\Benjamin\\P23\\SR09\\baignoire.csv"
    with open(fichier_csv, 'w', newline='') as fichier:
            writer = csv.writer(fichier)
            writer.writerow(['x', 'y'])  # Écriture de l'en-tête
            writer.writerows(zip(x, y))  # Écriture des données
    '''
        # Paramètres de la distribution de Weibull à ajuster
        shape, loc, scale = weibull_min.fit(y)
        
        # Génération de points pour la courbe de tendance
        x_tendance = np.linspace(min(x), max(x), 100)
        
        # Calcul du taux de défaillance (fonction de survie inversée)
        y_tendance = (shape / scale) * ((x_tendance - loc) / scale) ** (shape - 1)
        print("y : ",y_tendance)
        print("shape : ",shape)
        print("sclae : ",scale)
        print("loc : ",loc)
        # Tracé des points et de la courbe de tendance
        plt.plot(x_tendance, y_tendance, label='Courbe de tendance')
    '''

    # Tracé des points et de la courbe de tendance
    plt.plot(x, y,'o',label='Données')

    annee_string = ""
    for a in annee_voulu :
        annee_string += "-" + str(a)


    # Ajouter des titres et des étiquettes d'axes
    plt.title('Courbe en baignoire des disques en panne en '+annee_string)
    plt.xlabel('Temps (en '+duree+' )')
    plt.ylabel('Taux de disque en panne')

    # Afficher le graphique
    plt.show()


# ====================     Variables     ====================


nom_fichier = "C:\\Users\\utcpret\\Documents\\Benjamin\\P23\\SR09\\v4"
fichier_chemin = parcourir_repertoire(nom_fichier)
dico_baignoire = {}
dico_duree_vie = {}
nbdisque = 0
nb_valeurs_par_date = {}
nb_valeurs_par_date = {}



# ====================     Main     ====================

def main():

    print("-----------------  Traitement des graphs  -----------------")

    # Créer le parseur d'arguments
    parser = argparse.ArgumentParser(description='?')
    
    parser.add_argument('-d', type=int, help='Si d=1 on traite les données smart')

    parser.add_argument('-b', type=str, help='Permet de donnée les années voulues. Syntaxe [a,b,c]')

    parser.add_argument('-p', type=str, help='Permet de donnée la période. Valeur attendu : "mois" ou "trimestre')

    parser.add_argument('-i', type=int, help='Permet de ne plus ajouter dans les fichiers la valeurs de la "trace" si ça a déjà été lancé une fois')

    parser.add_argument('-c', type=str, help='Précise la racine du fichier là ou il a les données')

    # Analyser les arguments de la ligne de commande
    args = parser.parse_args()


    if args.c is not None :
        nom_fichier = args.c



    if args.d is not None:
        d = args.d
        # ====================     Données smart     ====================

        if(d==1):    

            print("-----------------  Traitement des donées smart  -----------------")

            liste_des_donnees_smart = ['smart_1_raw', 'smart_5_raw', 'smart_188_raw', 'smart_10_raw', 'smart_187_raw', 'smart_190_raw',
                    'smart_196_raw', 'smart_197_raw', 'smart_198_raw', 'smart_201_raw', 'smart_220_raw']

            if args.i is None:
                ajouter_colonne_trace(fichier_chemin)
            dictio = remplir_dico_moyenne(fichier_chemin, liste_des_donnees_smart)
            tracer_dico(dictio)

        else :
            print("Valeur de d non prise en compte ! Pour plus d'aide : -h")
   

        
    if args.b is not None:

        print("-----------------  Traitement de la courbe en baignoire  -----------------")

        if args.p is not None : 
            if args.p not in ["mois","trimestre"] :
                p = "mois"
            else: 
                p = args.p 
        else: 
            p = "mois"

        annee_voulu = create_list_from_string(args.b)

        # ====================     Courbe en baignoire     ====================  
        ajouter_colonne_duree_vie(fichier_chemin, annee_voulu, p)               
        courbe_baignoire()          
        tracer_courbe_baignoire(annee_voulu,p)




main()





