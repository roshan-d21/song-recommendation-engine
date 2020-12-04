import h5py, numpy

f = h5py.File('./data subset/a.h5', 'r')

print(f.keys())

metadata = f['metadata']['songs']

csv = open('./data subset/metadata.csv', 'w')

columns = metadata[0].dtype.names

for column in columns[:-1]:
    csv.write(column + ', ')
csv.write(columns[-1] + '\n')

for row in metadata:
    row = list(row)
    for value in row[:-1]:
        if type(value) == numpy.bytes_:
            value = value.decode('UTF-8').replace(',', '').replace("'", '').replace('"', '')
        csv.write(str(value) + ', ')
    csv.write(str(row[-1]) + '\n')

csv.close()
f.close()