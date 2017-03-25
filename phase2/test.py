f = open('example_data.txt','r')
s = 0
for line in f.readlines():
    att1,att2,att3,x = line.strip().split(',')
    if att1 == '1625':
        s += int(x)
print s
f.close()
