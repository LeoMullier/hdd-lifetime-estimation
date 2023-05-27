"""
Created on 25 March. 2023.

@author: benjamin.roussel, nicolas.parmentier
"""

import argparse
import csv
import math
import multiprocessing
import os
from collections import Counter

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from tqdm import tqdm

# ====================     Variables Globales    ====================
NOM_FICHIER = 'C:\\Users\\utcpret\\Documents\\Benjamin\\P23\\SR09\\v4\\2013-04-10'
DICO_DUREE_VIE = {}


def parcourir_repertoire(chemin):
    """Parcourt l'arborescence des fichiers et retourne une liste de chemins de fichiers."""
    fichiers = []
    for repertoire, _, fichiers_dans_repertoire in os.walk(chemin):
        for nom_fichier in fichiers_dans_repertoire:
            chemin_fichier = os.path.join(repertoire, nom_fichier)
            fichiers.append(chemin_fichier)

    return fichiers


def create_list_from_string(string_input):
    """Transform a string to liste : [a,b,c]."""
    # Supprimer les crochets de début et de fin
    string_input = string_input.strip('[]')

    # Diviser la chaîne en fonction des virgules
    values = string_input.split(',')

    # Convertir les valeurs en float et les ajouter à une liste
    result = [int(value) for value in values]

    return result


# --------------------- Utilitaire pour les données smart ---------------------


def remplir_dico_moyenne(fichiers, smart_list):
    """Fonction qui permet d'initialiser le dictionnaire des valeurs des données."""
    print('Entrée dans la fonction : remplir_dico_moyenne()')

    dico = {}
    liste_valeurs = {}
    nb_valeurs_par_date = {}

    print('Etape 1 : Somme des valeurs pour tous les disques')

    for fichier in tqdm(fichiers, total=len(fichiers)):
        dataframe = pd.read_csv(fichier, sep='\t')
        if dataframe.empty:
            continue  # Ignorer les fichiers vides
        dataframe = dataframe.fillna(0)

        for smart in smart_list:
            dataframe[smart] = dataframe[smart].apply(
                lambda x: float(x.replace(',', '.')) if isinstance(x, str) else x
            )
            dico.setdefault(smart, {})
            liste_valeurs.setdefault(smart, {})
            nb_valeurs_par_date.setdefault(smart, {})

            for i in range(len(dataframe)):
                date = dataframe.loc[i, 'trace']
                valeur = dataframe.loc[i, smart]

                liste_valeurs[smart].setdefault(date, [])

                if isinstance(valeur, float) and valeur != 0:
                    liste_valeurs[smart][date].append(valeur)
                    dico[smart].setdefault(date, 0)
                    nb_valeurs_par_date[smart].setdefault(date, 0)
                    dico[smart][date] += valeur
                    nb_valeurs_par_date[smart][date] += 1

    print("Etape 2 : Calcul de la moyenne et de l'erreur")

    for smart in tqdm(dico):
        for date in dico[smart]:
            moyenne = dico[smart][date] / nb_valeurs_par_date[smart][date]
            erreur = np.nanstd(liste_valeurs[smart][date]) / np.sqrt(
                nb_valeurs_par_date[smart][date]
            )
            dico[smart][date] = (moyenne, erreur)

    print('Sortie de la fonction : remplir_dico_moyenne()')
    return dico


def process_file(file):
    """Fonction qui permet de paralléliser l'exécution du code ajouter_colonne_trace()."""
    dataframe = pd.read_csv(file, sep='\t')
    if 'trace' not in dataframe.columns:
        trace = pd.Series(range(121, -1, -1))
        dataframe['trace'] = trace[: len(dataframe)]
        dataframe.to_csv(file, sep='\t', index=False, columns=list(dataframe.columns) + ['trace'])


def ajouter_colonne_trace(fichiers):
    """Ajoute la colonne "trace" - date relative, afin que les disques aient la même date de début et de fin."""
    with multiprocessing.Pool(processes=None) as pool:
        for _ in tqdm(pool.imap_unordered(process_file, fichiers), total=len(fichiers)):
            pass


def tracer_dico(dico):
    """Fonction qui permet de tracer le dictionnaire des données smart."""
    for col, valeurs in dico.items():
        x_axis = []
        y_axis = []
        yerr = []
        for date, (valeur, erreur) in valeurs.items():
            x_axis.append(date)
            y_axis.append(valeur)
            yerr.append(erreur)
        plt.figure()
        plt.errorbar(x_axis, y_axis, yerr=yerr, fmt='-', label=col, ecolor='r', capsize=3)
        plt.xlabel('Date')
        plt.ylabel('Valeur')
        plt.legend()
        if not os.path.exists(f'results/graphs/{col}'):
            os.makedirs(f'results/graphs/{col}')
        plt.savefig(f'results/graphs/{col}/graph.png')
        plt.show(block=False)


# --------------------- Utilitaire pour la courbe en baignoire  ---------------------

def calcul_duree_vie(fichiers, annee_voulu, duree):
    """Fonction qui permet d'ajouter la colonne des durée de vie."""
    compteur = 0
    mois = 0.0
    nb_disques = 0
    print(annee_voulu)
    print('Ajouter duree de vie')
    for fichier in fichiers:
        dataframe = pd.read_csv(fichier, sep='\t')

        # Sélection des années voulues
        if any(os.path.basename(fichier).startswith(str(annee)) for annee in annee_voulu):

            nb_disques += 1
            print(os.path.basename(fichier))
            nb_heures_tot = dataframe.iloc[0]['smart_9_raw']
            if isinstance(nb_heures_tot, str):
                nb_heures_tot = nb_heures_tot.replace(',', '.')[
                    :-2
                ]  # remplace la virgule par un point
            else:
                if math.isnan(nb_heures_tot):
                    continue

            if duree == 'mois':
                mois = round(int(nb_heures_tot) / (30 * 24), 0)
            elif duree == 'trimestre':
                mois = math.ceil(int(nb_heures_tot) / (30 * 24) / 3) * 3

            serial_number = dataframe.iloc[0]['serial_number']
            # model = dataframe.iloc[0]['model']

            DICO_DUREE_VIE[serial_number] = round(mois, 0)

        else:
            compteur = compteur + 1

    return nb_disques

def calcul_vie_donnee_smart_duree(fichiers, annee_voulu, donnee):
    """Fonction qui permet d'ajouter la colonne des durée de vie."""
    compteur = 0
    dico_duree_vie = {}
    nb_disques = 0
    print(annee_voulu)
    print('Ajouter duree de vie')
    for fichier in fichiers:
        dataframe = pd.read_csv(fichier, sep='\t')

        # Sélection des années voulues
        if any(os.path.basename(fichier).startswith(str(annee)) for annee in annee_voulu):

            nb_disques += 1
            print(os.path.basename(fichier))
            valeur_totale = dataframe.iloc[0][donnee]
            if isinstance(valeur_totale, str):
                valeur_totale = valeur_totale.replace(',', '.')[:-2]
                print(valeur_totale)
            else:
                if math.isnan(valeur_totale):
                    continue

            semaine = round(int(valeur_totale) / (24*7), 0)
            

            serial_number = dataframe.iloc[0]['serial_number']
            dico_duree_vie[serial_number] = round(semaine, 0)

        else:
            compteur = compteur + 1
    print(dico_duree_vie)
    return Counter(dico_duree_vie.values()), nb_disques



def calcul_vie_donnee_smart_valeur(fichiers, annee_voulu, donnee, m):
    """Fonction qui permet d'ajouter la colonne des durée de vie."""
    compteur = 0
    i = 0
    dico_duree_vie = {}
    nb_disques = 0
    print(annee_voulu)
    print('Ajouter duree de vie')
    for fichier in fichiers:
        dataframe = pd.read_csv(fichier, sep='\t')
        i+=1
        
        # Sélection des années voulues
        if any(os.path.basename(fichier).startswith(str(annee)) for annee in annee_voulu):
            print(i)
           
            print(os.path.basename(fichier))
            valeur_totale = dataframe.iloc[0][donnee]
            if isinstance(valeur_totale, str):
                valeur_totale = valeur_totale.replace(',', '.')[:-2]
            else:
                if math.isnan(valeur_totale):
                    continue

            if valeur_totale == "0,0":
                continue
            nb_disques += 1
            serial_number = dataframe.iloc[0]['serial_number']
            dico_duree_vie[serial_number] = valeur_totale

        else:
            compteur = compteur + 1

    #Phase de subdivition
    min_value = min(dico_duree_vie.values())
    max_value = max(dico_duree_vie.values())
    print(min_value,max_value)
    sub = round((int(max_value) - int(min_value)) / m, 0)

    dico_organise={}

    for cle, valeur in dico_duree_vie.items():
        val = int(int(valeur) / int(sub)) * sub
        dico_organise[cle] = (val)

    

    return Counter(dico_organise.values()), nb_disques




def init_courbe_baignoire():
    """Fonction qui permet d'initialiser le dictionnaire correspondant à la courbe en baignoire."""
    print('courbe baignoire')
    return Counter(DICO_DUREE_VIE.values())


def tracer_courbe_de_vie(dict_baignoire):
    """Fonction qui permet de tracer le nombre de disque qui cesse de fonctionné en fonction du temps en mois."""
    x_axis = sorted(dict_baignoire.keys())

    # On récupère les fréquences correspondantes
    y_axis = [dict_baignoire[mois] for mois in x_axis]

    # On trace la courbe
    plt.plot(x_axis, y_axis, '.')

    plt.yticks(range(min(y_axis), max(y_axis) + 1, 10))

    # On ajoute des titres et des étiquettes d'axes
    plt.title('Courbe de vie')
    plt.xlabel('Mois')
    plt.ylabel('Fréquence')

    # On affiche le graphique
    plt.show()


def weib(x_axis, k, scale):
    """Fonction lié à la courbe de Weibull ?."""
    return (k / scale) * (x_axis / scale) ** (k - 1) * np.exp(-((x_axis / scale) ** k))


def tracer_courbe_baignoire(annees_voulues, duree, nb_disques, dict_baignoire,donnee):
    """Fonction qui trace la courbe en baignoire."""
    # Calcul du nombre cumulatif de défaillances
    x_axis = sorted(dict_baignoire.keys())
    y_axis = []
    for mois in x_axis:
        y_axis.append(dict_baignoire[mois] / nb_disques)
        nb_disques -= dict_baignoire[mois]

    # Sauvegarde des valeurs
    fichier_csv = 'C:\\Users\\utcpret\\Documents\\Benjamin\\P23\\SR09\\baignoire'+ donnee +'.csv'
    with open(fichier_csv, 'w', newline='', encoding='utf-8') as fichier:
        writer = csv.writer(fichier)
        writer.writerow(['x', 'y'])  # Écriture de l'en-tête
        writer.writerows(zip(x_axis, y_axis))  # Écriture des données

    # Tracé des points et de la courbe de tendance
    plt.plot(x_axis, y_axis, '.', label='Données')

    annee_string = ''
    for annee_voulue in annees_voulues:
        annee_string += '-' + str(annee_voulue)

    # Ajouter des titres et des étiquettes d'axes
    plt.title('Courbe en baignoire des disques en panne en ' + annee_string)
    plt.xlabel('Temps (en ' + duree + ' )')
    plt.ylabel('Taux de disque en panne')

    # Afficher le graphique
    plt.show(block=False)


# ====================     Main     ====================


def main():
    """Entry point."""
    print('-----------------  Traitement des graphs  -----------------')
    fichiers = parcourir_repertoire(NOM_FICHIER)
    choix_mois = 'mois'

    # Créer le parseur d'arguments
    parser = argparse.ArgumentParser(description='?')
    parser.add_argument(
        '-d', action='store_true', help='Si -d est présent, on traite les données smart'
    )
    parser.add_argument(
        '-b',
        type=str,
        help='Active la génération de la courbe en baignoire. Spécifier les années '
        'voulues : Syntaxe [a,b,c]',
    )
    parser.add_argument(
        '-p', type=str, help='Permet de donner la période. Valeurs attendues : "mois" ou "trimestre'
    )
    parser.add_argument(
        '-i',
        action='store_true',
        help='Permet de ne plus ajouter dans les fichiers la valeurs de la "trace" si ça a déjà été lancé une fois',
    )
    parser.add_argument('-c', type=str, help='Précise la racine du fichier là ou il a les données')

    # Analyser les arguments de la ligne de commande
    args = parser.parse_args()

    if args.d:
        # ====================     Données smart     ====================
        print('-----------------  Traitement des donées smart  -----------------')

        liste_des_donnees_smart = [
            'smart_1_raw',
            'smart_5_raw',
            'smart_188_raw',
            'smart_10_raw',
            'smart_187_raw',
            'smart_190_raw',
            'smart_196_raw',
            'smart_197_raw',
            'smart_198_raw',
            'smart_201_raw',
            'smart_220_raw',
        ]

        if not args.i:
            ajouter_colonne_trace(fichiers)
        dictio = remplir_dico_moyenne(fichiers, liste_des_donnees_smart)
        tracer_dico(dictio)

    if args.b:
        print('-----------------  Traitement de la courbe en baignoire  -----------------')
        if args.p in ['mois', 'trimestre']:
            choix_mois = args.p

        annees_voulues = create_list_from_string(args.b)

        # ====================     Courbe en baignoire     ====================
        nb_disques = calcul_duree_vie(fichiers, annees_voulues, choix_mois)
        dict_baignoire = init_courbe_baignoire()
        tracer_courbe_baignoire(annees_voulues, choix_mois, nb_disques, dict_baignoire)



'''
***** Test *****
'''

liste_des_donnees_smart = [
    'smart_5_raw',
    'smart_11_raw',
    'smart_160_raw',
    'smart_161_raw',
    'smart_164_raw',
    'smart_197_raw',

]
f=parcourir_repertoire(NOM_FICHIER)
for smart in liste_des_donnees_smart :
    dico,nb_disques = calcul_vie_donnee_smart_valeur(f,[2013,2014,2015,2016,2017,2018,2019,2020,2021,2022],smart,50)
    tracer_courbe_baignoire([2013,2022], "mois", nb_disques, dico,smart)



'''
if __name__ == '__main__':
    main()
'''

