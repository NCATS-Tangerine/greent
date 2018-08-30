import os
from greent.util import Resource

model_path = os.path.join (os.path.dirname (__file__), "../greent/conf", "biolink-model.yaml")
print(model_path)
model_obj = Resource.load_yaml (model_path)

names = []
for obj in model_obj["classes"]:
    name = obj["name"].replace (" ", "_")
    if 'ASSOCIATION' in name.upper():
        continue
    if 'RELATIONSHIP' in name.upper():
        continue
    names.append(name)

#hack. should maybe pull overlay too....
names.append('genetic_condition')
names.append('unspecified')

outfname = os.path.join(os.path.dirname(__file__),"node_types.py")
with open(outfname,'w') as outf:
    for name in names:
        outf.write(f"{name.upper()} = '{name}'\n")

    outf.write('\n')
    outf.write('#The root of all biolink_model entities, which every node in neo4j will also have as a label. used to specify constraints/indices\n')
    outf.write("ROOT_ENTITY = 'named_thing'\n")
    outf.write('\n')

    outf.write(f"node_types = set([")
    for name in names[:-1]:
        outf.write(name.upper())
        outf.write(',\n')
    outf.write(names[-1].upper())
    outf.write('])\n')

    #outf.write('\n')
    #outf.write("type_codes = { 'S': CHEMICAL_SUBSTANCE, 'G':GENE, 'P':PROCESS_OR_FUNCTION, 'C':CELL, 'A':ANATOMY, 'T':PHENOTYPE, 'D':DISEASE, 'X':GENETIC_CONDITION , 'W': PATHWAY, '?': UNSPECIFIED}')")
