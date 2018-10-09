def makeline(line):
    firstcolon = line.index(':')
    secondcolon = line.index(':',firstcolon+1)
    umls = line[firstcolon-4:secondcolon-1]
    ids = [x.strip()[1:] for x in line.strip()[secondcolon+3:-1].split(',')]

    mondos = list(filter(lambda x: x.startswith('MONDO'),ids))
    return f'{umls}\t{",".join(mondos)}\n'


with open('disease.txt','r') as inf, open('crap.txt','w') as outf:
    numls = 0
    numbad = 0
    for line in inf:
        if line.startswith('synonymize(MONDO'):
            numls += 1
            firstcolon = line.index(':')
            secondcolon = line.index(':',firstcolon+1)
            value = line[secondcolon+2:]
            nummondo = value.count('MONDO')
            if nummondo > 1:
                numbad += 1
                outf.write(makeline(line))
print( numbad, numls, numbad/numls )
