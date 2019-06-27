import os
from greent.util import Resource

def get_descriptions(name):
    model_path = os.path.join (os.path.dirname (__file__), "..", "greent", "conf", f"{name}.yaml")
    model_obj = Resource.load_yaml (model_path)
    d = 'description'
    desc = { sname: (obj[d] if d in obj else None) for sname, obj in model_obj['slots'].items()  }
    return desc

desc1 = get_descriptions('biolink-model')
desc2 = get_descriptions('biolink-model_overlay')
print( len(desc1), len(desc2))
key1 = set(desc1.keys())
key2 = set(desc2.keys())
allkeys = key1.union(key2)
print( len(allkeys))

with open(os.path.join (os.path.dirname (__file__), "predicates.tsv"),'w') as outf:
    left = 0
    for k in allkeys:
        if k in desc1:
            if desc1[k] is not None:
                outf.write(f'{k}\t{desc1[k]}\n')
            elif k in desc2 and desc2[k] is not None:
                outf.write(f'{k}\t{desc2[k]}\n')
            else:
                left += 1
                print(k)
        else:
            if desc2[k] is not None:
                outf.write(f'{k}\t{desc2[k]}\n')
            else:
                left += 1
                print(k)

print(left)

