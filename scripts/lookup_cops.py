import os
import requests
import json

def lookup(name,concept):
    url = f'https://bionames.renci.org/lookup/{name}/{concept}/'
    response = requests.get(url).json()
    return response

def lookup_and_filter(name,concept,prefix):
    results = lookup(name,concept)
    prefix_results = list(filter( lambda x: x['id'].startswith(prefix), results))
    prefix_matches = list(filter( lambda x: x['label'].upper() == name.upper(), prefix_results))
    if len(prefix_matches) > 0:
        return prefix_matches, True
    return prefix_results, False

def choose_drug(druglist):
    for prefix in ['CHEMBL','PUBCHEM','MESH']:
        goods = list(filter(lambda x: x.startswith(prefix), druglist))
        if len(goods) > 0:
            return goods[0]
    if len(druglist) == 0:
        return ''
    return druglist[0]

def run_cop_lookups():
    filename = os.path.join(os.path.dirname(__file__),"../builder", "q2-drugandcondition-list.txt")
    outfile = 'COPS.txt'
    badfile = 'COPS_need_work.txt'
    with open(filename,'r') as inf, open(outfile,'w') as outf, open(badfile,'w') as badf:
        inf.readline() #header
        outf.write('DRUGNAME\tENDPOINT\tDRUG_ID\tPHENOTYPE_ID\tDISEASE_ID\n')
        badf.write('DRUGNAME\tENDPOINT\tDRUG_ID\tPHENOTYPE_ID\tDISEASE_ID\n')
        for line in inf:
            x = line.strip().split('\t')
            drugname = x[0]
            endname = x[1]
            try:
                drugs = [x['id'] for x in lookup(drugname,'drug')]
                diseases,d_exact = lookup_and_filter(endname,'disease','MONDO')
                phenotypes,p_exact = lookup_and_filter(endname,'phenotypic_feature', 'HP')
            except:
                badf.write(f'{drugname}\t{endname}\t\t\t\n')
                continue
            print(f'\n{drugname}-{endname}')
            print('DRUG:',drugs)
            print('phenotypes',phenotypes)
            print('diseases',diseases)
            drug_choice=choose_drug(drugs)
            if p_exact or d_exact:
                outf.write(f'{drugname}\t{endname}\t{drug_choice}\t')
                if p_exact:
                    ps = ','.join(list(set([x['id'] for x in phenotypes])))
                else:
                    ps = ''
                if d_exact:
                    ds = ','.join(list(set([x['id'] for x in diseases])))
                else:
                    ds = ''
                outf.write(f"{ps}\t{ds}\n")
            else:
                badf.write(f'{drugname}\t{endname}\t{drug_choice}\t')
                pls = ','.join(list(set([f"{x['id']}({x['label']})" for x in phenotypes])))
                dls = ','.join(list(set([f"{x['id']}({x['label']})" for x in diseases])))
                badf.write(f"{pls}\t{dls}\n")



if __name__ == '__main__':
    run_cop_lookups()