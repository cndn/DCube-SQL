d = {}
with open('darpa.csv','r') as infile:
    outfile = open('darpa_rm_dup.csv','w')
    for line in infile.xreadlines():
        line = ','.join(line.strip().split(',')[:-1])
        d[line] = d.get(line,0) + 1
    for line in d:
        outfile.write(line + ',' + str(d[line]) + '\n')
    outfile.close()
