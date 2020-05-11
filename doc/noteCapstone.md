Projet Capstone d'ingénierie des données

[STEP I](#STEP-I)  
* find Topic
* find main dataset
* find optionnal dataset

[STEP II](#STEP-II)
* explore dataset


___

L'objectif du projet Data Engineering Capstone est de vous donner la possibilité de combiner ce que vous avez appris tout au long du programme. Ce projet constituera une partie importante de votre portfolio qui vous aidera à atteindre vos objectifs de carrière en matière d'ingénierie des données.

Dans le cadre de ce projet, vous pouvez choisir de mener à bien le projet qui vous est proposé, ou de définir la portée et les données d'un projet de votre propre conception. Dans les deux cas, vous devrez suivre les mêmes étapes que celles décrites ci-dessous.

Projet fourni par Udacity :
Dans le projet Udacity fourni, vous travaillerez avec quatre ensembles de données pour mener à bien le projet. Le principal ensemble de données comprendra des données sur l'immigration aux États-Unis, et les ensembles de données supplémentaires comprendront des données sur les codes d'aéroport, la démographie des villes américaines et des données sur la température. Vous pouvez également enrichir le projet avec des données supplémentaires si vous souhaitez vous démarquer.

Projet ouvert :
Si vous décidez de concevoir votre propre projet, vous pouvez trouver des informations utiles dans la section Ressources du projet. Plutôt que de suivre les étapes ci-dessous avec les données fournies par Udacity, vous rassemblerez vos propres données et suivrez le même processus.

Instructions
Pour vous aider à orienter votre projet, nous l'avons décomposé en une série d'étapes.
Étape 1 : Étendue du projet et collecte des données
Étant donné que l'ampleur du projet dépendra fortement des données, ces deux choses se produisent simultanément. Dans cette étape, vous :
   - Identifier et rassembler les données que vous utiliserez pour votre projet (au moins deux sources et plus d'un million de lignes). Consultez les ressources du projet pour avoir une idée des données que vous pouvez utiliser.
    - Expliquer pour quels cas d'utilisation finale vous souhaitez préparer les données (par exemple, tableau d'analyse, application back-end, base de données des sources de vérité, etc.)

Étape 2 : Explorer et évaluer les données
   - Explorer les données pour identifier les problèmes de qualité des données, comme les valeurs manquantes, les données en double, etc.
  - Documenter les étapes nécessaires pour nettoyer les données

Étape 3 : Définir le modèle de données
    - Exposez le modèle conceptuel de données et expliquez pourquoi vous avez choisi ce modèle
    - Énumérer les étapes nécessaires pour intégrer les données dans le modèle de données choisi

Étape 4 : Lancer l'ETL pour modéliser les données
    - Créer les pipelines de données et le modèle de données
   - Inclure un dictionnaire de données
   - Effectuer des contrôles de qualité des données pour s'assurer que le pipeline fonctionne comme prévu
        * Contraintes d'intégrité de la base de données relationnelle (par exemple, clé unique, type de données, etc.)
        * Tests unitaires pour les scripts afin de s'assurer qu'ils font ce qu'il faut
        * Contrôles à la source/au comptage pour s'assurer de l'exhaustivité

Étape 5 : Rédaction du projet
    - Quel est l'objectif ? Quelles questions voulez-vous poser ? Comment Spark ou Airflow seront-ils incorporés ? Pourquoi avez-vous choisi le modèle que vous avez choisi ?
   - Expliquez clairement la raison du choix des outils et des technologies pour le projet.
    - Documentez les étapes du processus.
    - Proposez la fréquence de mise à jour des données et la raison pour laquelle elle doit être effectuée.
    - Publiez votre modèle de données écrit et final dans un repo GitHub.
    - Incluez une description de la manière dont vous aborderiez le problème différemment dans les scénarios suivants :
        - Si les données étaient multipliées par 100.
        - Si les pipelines étaient exploités quotidiennement avant 7 heures du matin.
        - Si la base de données devait être consultée par plus de 100 personnes.

Rubrique
Dans la rubrique "Projet", vous trouverez plus de détails sur les exigences. Utilisez la rubrique pour évaluer votre propre projet avant de le soumettre à Udacity pour examen. Comme pour les autres projets, les examinateurs d'Udacity utiliseront cette rubrique pour évaluer votre projet et vous fournir un retour d'information. Si votre projet ne répond pas aux spécifications, vous pouvez y apporter des modifications et le soumettre à nouveau.

# STEP I
FIND THE DATASET

---
### main dataset

* I-94: immigration_data_sample.csv
    * telecharge: oui.
    * date de debut, date de fin? Quelle periode?
    * combien de ligne
    * describe?
* gedelt project:
    * telecharge: non, voir sur AWS
    * date de debut
    * quelles colonne? 
    * exploration?

### optionnal dataset provide by Udacity
* sas_data: snappy-parquet
* airport-codes_csv.csv
* us-cities-demographics.csv
* I94_SAS_LAbels_Descriptions



## STEP II
EXPLORE DATASETS

---
* With a Jupyter notebook, pandas and spark.
    * Explore with spark and notebook gdelt, but always the same error within Docker or not. I didn't found the why. Next step, 
        - airflow and spark? 
        - ec2 and notebook and s3? I have credits from AWS to do that. 



