# SR09-BackBlaze

## Introduction

Dans le cadre de ce projet, nous avons créé un dépôt GitLab contenant le code Python associé au traitement des données de BackBlaze. Le code du projet est disponible à l'adresse suivante : https://gitlab.utc.fr/niparmen/sr09-backblaze

## Auteurs
MULLIER Léo\
PARMENTIER Nicolas\
ROUSSEL Benjamin

## License
License Creative Commons `CC-BY-NC-SA`
- Adaptation autorisées sous condition de partage dans les mêmes conditions
- Pas d'utilisation commerciale

## Project status
If you have run out of energy or time for your project, put a note at the top of the README saying that development has slowed down or stopped completely. Someone may choose to fork your project or volunteer to step in as a maintainer or owner, allowing your project to keep going. You can also make an explicit request for maintainers.


## Affichage des données S.M.A.R.T d'un disque

### Linux

Smartmontools est un paquet (à installer avec apt) regroupant un ensemble d'outils basés sur la "technologie" SMART. Cette technologie est un protocole qui permet de suivre et contrôler l'état des disques durs entiers. Les données SMART ignorent les partitions et le partitionnement.

#### Installation

`sudo apt-get install --no-install-recommends smartmontools`

Pour les disques NVME, il faut aussi installer le paquet nvme-cli :\
`sudo apt install nvme-cli`

#### Utilisation

Dans les exemples suivants, on va considérer le disque dur nommé /dev/sdX.

Pour activer SMART sur un disque, taper la commande suivante (nécessaire une seule fois seulement pour chaque disque, mais peut être répétée sans danger) :\
`sudo smartctl --smart=on --offlineauto=on --saveauto=on /dev/sdX`

Pour obtenir quelques infos disponibles par SMART sur le disque :\
`sudo smartctl -H -i /dev/sdX`

Pour obtenir toutes les infos disponibles par SMART sur le disque :\
`sudo smartctl -s on -a /dev/sdX`

Le paquet smartmontools permet aussi de :
- Recevoir des notifications automatiquement lorsque le daemon smartmontools détecte une erreur importante sur un disque
- Lancer des tests (entre 10 min et 90 min) pour tester les disques

Plus d'infos : https://doc.ubuntu-fr.org/smartmontools

### Windows

CrystalDiskInfo est un utilitaire complet de diagnostics, vérification et surveillance de disque qui permet de :

- Afficher les valeurs les S.M.A.R.T afin de prévenir les pannes de disque
- Mesurer la température du disque pour éviter les surchauffes
- Alerter et notifier par mail

#### Installation

Télécharger le .exe sur https://crystalmark.info/en/software/crystaldiskinfo/
Executer le .exe
Lancer le logiciel

#### Utilisation

La page de CrystalDiskInfo se présente ainsi :

En haut, les informations du disque (modèle, numéro de série, etc.)
En haut à gauche, l'état de santé et la température du disque dur. C'est un résumé de l'état général.
À sa droite, les informations du disque (numéro de série, interface, lettre de lecteur, etc.)
Puis dans la seconde partie, la liste des attributs S.M.A.R.T.


## Utilisation de bbdata_parser.py

Le programme nécessite la présence des fichiers sources de BackBlaze (au format CSV) dans le répertoire data/csv, situé à la racine du dépôt.

`-h, --help`\
Affiche le message d'aide

`--history_length_recent`\
Entier représentant la longueur de l'historique (début de vie) - si nul alors tout l'historique sera récupéré

`--history_length_old`\
Entier représentant la longueur de l'historique (fin de vie) - si nul alors tout l'historique sera récupéré

`--failure_start_date`\
A partir de quelle date commencer la recherche de défaillances ? (format YYYY-mm-dd)

### Exemple d'exécution :

Obtenir les données des disques tombés en panne après le 01/01/2015 (30 premiers et 90 derniers jours de vie du disque) :\
`--failure_start_date 2015-01-01 --history_length_recent 90 --history_length_old 30`

Obtenir toutes les données des disques tombés en panne après le 01/01/2015 :\
`--failure_start_date 2015-01-01 --history_length_old 0`

## Utilisation de graph.py

Le programme s'exécute simplement avec Python : python ./graph.py (ou py3 si la version de Python est la 3). Les fichiers CSV seront créés à la racine du projet, sauf demande contraire, et auront pour nom : "baignoire_+donnee+.csv", où donnée correspond au nom de la donnée S.M.A.R.T.

Concernant les paramètres, nous avons pour les données S.M.A.R.T. :

`-d, --donnee-smart`\
Si le paramètre d est présent, on trace les données S.M.A.R.T.

`-e, --liste-donnee-smart`\
Permet de donner si on le souhaite la liste des données smarts. Syntaxe : [smart_5_raw, smart_1_raw]. Une valeur par défaut est déjà présente.

Pour la courbe en baignoire :

`-b, --weibull-annee-voulu`\
On précise ici que l'on souhaite tracer la courbe en baignoire, nous devons aussi spécifier sur quelles années on souhaite la tracer, la syntaxe est "[a,b,...]", où a,b,... sont des années entre 2015 et 2022.

`-p, --weibull-periode-voulu`\
Permets de donner la période pour tracer la courbe en baignoire. Les valeurs sont "mois" et "trimestre". Par exemple, si vous choisissez "mois", le taux de mortalité sera donné en fonction du temps en mois. Par défaut, la valeur est "mois".

Pour la courbe en baignoire des données S.M.A.R.T. :

`-s, --weibull-donnee-smart`\
Permet de tracer la courbe de Weibull pour les données SMART.

`-w, --weibull-donnee-smart-voulu`\
Permet de donner si on le souhaite la liste des données smarts, pour les courbes de Weibull. Syntaxe : [smart_5_raw, smart_1_raw]. Une valeur par défaut est déjà présente.

Nous pouvons donner des exemples d'exécution :

Si nous souhaitons afficher le graphique des données S.M.A.R.T. pour le n°5 :\
`--donnee-smart --liste-donnee-smart [smart_5_raw]`

Si nous souhaitons tracer la courbe de Weibull entre les années 2021 - 2022 en mois :\
`--weibull-annee-voulu [2021,2022] --weibull-periode-voulu mois`

Si nous souhaitons tracer les courbes en baignoire des données S.M.A.R.T. pour le n°11 :\
`--weibull-donnee-smart --weibull-donnee-smart-voulu [smart_11_raw]`
