import csv
import os
import json

conceptsAndIds = [
  ("anatomical_entity", "A"),
  ("biological_process_or_activity", "P"),
  ("cell", "C"),
  ("chemical_substance", "S"),
  ("disease", "D"),
  ("gene", "G"),
  ("phenotypic_feature", "T")
]
map = {c[0]:c[1] for c in conceptsAndIds}

def build_spec(sequence_ids, start_name, start_id, end_name=None, end_id=None):
    spec_sequence = ''.join([map[c] for c in sequence_ids])
    
    nodes = [build_step(sequence_ids[0], start_name, start_id)]
    if end_name:
        nodes.extend([build_step(s) for s in sequence_ids[1:-1]])
        nodes.extend([build_step(sequence_ids[-1], end_name, end_id)])

        sequence_name = ' -> '.join(sequence_ids[1:-1])

        name = f'{start_name} -> {sequence_name} -> {end_name}'
        natural = f'{spec_sequence}({start_name}, {end_name})'
        
    else:
        nodes.extend([build_step(s) for s in sequence_ids[1:]])
        
        sequence_name = ' -> '.join(sequence_ids[1:])

        name = f'{start_name} -> {sequence_name}'
        natural = f'{spec_sequence}({start_name})'

    edges = []
    for idx, n in enumerate(nodes):
        n['id'] = idx
        if idx:
            edges.append({
                'source_id': idx-1,
                'target_id': idx
            })

    out = {"name": name,
           "natural_question": natural, 
           "notes": '',
           "machine_question": {
               'nodes': nodes,
               'edges': edges
           }
    }
    return out

def build_step(spec, name=None, id=None):
    if name and id:
        out = {
            "type": spec,
            "name": name,
            "curie": id
        }
    else:
        out = {
            "type": spec
        }
    return out

def writeJson(obj, name):
    write_path = os.path.join('.', 'json_fail')
    if not os.path.exists(write_path):
        os.makedirs(write_path)
    try:
        with open(os.path.join(write_path, name), "w") as json_file:
            json.dump(obj, json_file, indent=4)
        return True
    except:
        return False

def main():
    all_lines = []
    with open('queries.txt') as f:
        reader = csv.reader(f, delimiter='\t')
        next(reader, None)
        for line in reader:
            all_lines.append(line)
    
    [writeJson(build_spec(line[5].split(','), line[4], line[3], line[1], line[0]), f'{line[2]}_{idx}.json') for idx, line in enumerate(all_lines)]

if __name__ == '__main__':
    main()