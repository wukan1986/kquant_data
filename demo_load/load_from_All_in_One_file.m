file = 'd:\all.h5'
index = h5read(file,'/index');
columns = h5read(file,'/columns');
values = h5read(file,'/values');
values = values';

data = array2table(values);

