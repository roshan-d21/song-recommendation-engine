import h5py

f = h5py.File('a.h5', 'r')

print(f.keys())

metadata = f['metadata']['songs']

csv = open('metadata.csv', 'w')

columns = metadata[0].dtype.names

for column in columns[:-1]:
    csv.write(column + ', ')
csv.write(columns[-1] + '\n')

for row in metadata:
    csv.write(str(row).replace('(', '').replace(')', '') + '\n')

csv.close()
f.close()