s = set()
with open('darpa.csv','r') as infile:
    outfile = open('darpa_rm_dup.csv','w')
    for line in infile.xreadlines():
        line = ','.join(line.strip().split(',')[:-1])
        s.add(line)
    for line in s:
        #binarize
        outfile.write(line + ',1\n')
    outfile.close()

with open('')
