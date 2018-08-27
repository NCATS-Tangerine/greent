with open('crawler/disease.txt','r') as inf, open('crawler/crap.txt','w') as outf:
    numls = 0
    numbad = 0
    for line in inf:
        if line.startswith('synonymize(UMLS'):
            numls += 1
            firstcolon = line.index(':')
            secondcolon = line.index(':',firstcolon+1)
            value = line[secondcolon+2:]
            nummondo = value.count('MONDO')
            if nummondo > 1:
                numbad += 1
                outf.write(line)
print( numbad, numls, numbad/numls )
