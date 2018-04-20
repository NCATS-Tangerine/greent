#Example command lines 
#All assume that robokop-interfaces is installed in a sibling directory to protocop.  

#Type 1 query (Disease/Gene/GeneticCondition)
#Start at Ebola, support with chemotext, chemotext2 and cdw
#Label in neo4j will be "test1"
#PYTHONPATH=../../robokop-interfaces python builder.py -s chemotext -q 1 --start "Ebola Virus Disease" -l test1_program_x
#PYTHONPATH=../../robokop-interfaces python builder.py -s chemotext -q 1 --start "Ebola Virus Disease" -l test1_syn 

#Type 2 query (Substance/Gene/Process/Cell/Anatomy/Phenotype/Disease)
#Start at PRAMIPEXOLE, end at Restless Legs Syndrom
#support with chemotext and chemotext2
#Label in neo4j will be "test2"
PYTHONPATH=../../robokop-interfaces python builder.py -s chemotext -q 2 --start "ARTEMETHER" --end "Malaria" -l test_ARTEMETHER_program_c
#PYTHONPATH=../../robokop-interfaces python builder.py -s chemotext -p SGPC --start "ARTEMETHER" -l test_ARTEMETHER

#PYTHONPATH=../../robokop-interfaces python builder.py -s chemotext -q 2 --start "MECLIZINE" --end "Motion Sickness" -l test_MECLIZINE_program
#PYTHONPATH=../../robokop-interfaces python builder.py -s chemotext -q 2 --start "KETOCONAZOLE" --end "Candidiasis, Cutaneous" -l test_KETOCONAZOLE
#PYTHONPATH=../../robokop-interfaces python builder.py -s chemotext -q 2 --start "TACRINE" --end "Alzheimer Disease" -l test_TACRINE
#PYTHONPATH=../../robokop-interfaces python builder.py -s chemotext -q 2 --start "KETOROLAC" --end "Pain" -l test_KETOROLAC
#PYTHONPATH=../../robokop-interfaces python builder.py -s chemotext -q 2 --start "AZELASTINE" --end "Rhinitis, Allergic, Perennial" -l test_AZELASTINE
#PYTHONPATH=../../robokop-interfaces python builder.py -s chemotext -q 2 --start "SORAFENIB" --end "Carcinoma, Renal Cell" -l test_SORAFENIB
#PYTHONPATH=../../robokop-interfaces python builder.py -s chemotext -q 2 --start "CELECOXIB" --end "Arthritis, Rheumatoid" -l test_CELECOXIB

#PYTHONPATH=../../robokop-interfaces python builder.py -s chemotext2 -s chemotext -q 2 --start "KETOROLAC" --end "Pain" -l test_KETOLORAC
#PYTHONPATH=../../robokop-interfaces python builder.py -s chemotext2 -s chemotext -q 2 --start "AZELASTINE" --end "Rhinitis, Allergic, Perennial" -l test_AZELASTINE

#PYTHONPATH=../../robokop-interfaces python builder.py -s chemotext -p "SG" --start "Ozone" -l Ozone_gene
#PYTHONPATH=../../robokop-interfaces python builder.py -s chemotext -p "SGD" --start "Ozone" --end "Asthma" -l Ozone_Gene_Asthma

#PYTHONPATH=../../robokop-interfaces python builder.py -s chemotext -p "SGWGD" --start "Ozone" --end "Asthma" -l CQ2_PW

#PYTHONPATH=../../robokop-interfaces python builder.py -s chemotext -p "SGD" --start "Particulate Matter" --end "Asthma" -l CQ2_Particulates_Simplified
#PYTHONPATH=../../robokop-interfaces python builder.py -s chemotext -p "SGWGD" --start "Particulate Matter" --end "Asthma" -l CQ2_Particulates_PW

#PYTHONPATH=../../robokop-interfaces python builder.py -s chemotext -p "SGS" --start "Ozone"  -l CQ3_Ozone
#PYTHONPATH=../../robokop-interfaces python builder.py -s chemotext -p "SGS" --start "Particulate Matter"  -l CQ3_ParticulateMatter

#Same as the above query, but not using the -q 2 shortcut
#PYTHONPATH=../../robokop-interfaces python builder.py -s chemotext2 -s chemotext -p "SGPCATD" --start "PRAMIPEXOLE" --end "Restless Legs Syndrome" -l test2

#Query specifying: Start at Ebola, end at a genetic condition. Link should either be direct or via one other node of any type
#PYTHONPATH=../../robokop-interfaces python builder.py -s cdw -s chemotext2 -s chemotext -p "D(1-2)X" --start "Ebola Virus Disease" -l test3 

#Start at the Substance LISINOPRIL
#Find phenotypes that are connected to it either directly (1 edge), or via links including up to 2 other nodes (3 edges).
#Support with chemotext & chemotext2
#Label in neo4j will be test4
#PYTHONPATH=../../robokop-interfaces python builder.py -s chemotext2 -s chemotext -p "S(1-3)T" --start "LISINOPRIL" -l test4 

#What can I get to from LISINOPRIL? Just Genes, right?
#Currently,this does not do anything.  See issue #42.
#PYTHONPATH=../../robokop-interfaces python builder.py -s chemotext -p "S?" --start "LISINOPRIL" -l test_lisinopril 

#PYTHONPATH=../../robokop-interfaces python builder.py -s chemotext -p "S(1-3)D" --start "LISINOPRIL" --end "HYPERTENSION" -l test_lisinopril_hypertension
