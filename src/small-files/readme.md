### Lab  small-files-problem apache spark


#### Testes realizados para identificar os melhores parametros para resolver o problema de small files
```
[1 arquivo]
default parallelism: 1
max partition bytes: 10kb
number of partitions for [device]: 3
total time spend in [secs]: 5.43
number of partitions for [subscription]: 3
total time spend in [secs]: 0.4

[5 arquivos]
default parallelism: 1
max partition bytes: 10kb
number of partitions for [device]: 15
total time spend in [secs]: 5.94
number of partitions for [subscription]: 15
total time spend in [secs]: 0.68

[5 arquivos]
default parallelism: 2
max partition bytes: 10kb
number of partitions for [device]: 15
total time spend in [secs]: 5.63
number of partitions for [subscription]: 15
total time spend in [secs]: 0.51

[5 arquivos]
default parallelism: 4
max partition bytes: 10kb
number of partitions for [device]: 15
total time spend in [secs]: 5.69
number of partitions for [subscription]: 15
total time spend in [secs]: 0.48

[85 files]
default parallelism: 4
max partition bytes: 10kb
number of partitions for [device]: 255
total time spend in [secs]: 8.31
number of partitions for [subscription]: 255
total time spend in [secs]: 2.6

[85 files 128mb]
default parallelism: 4
max partition bytes: true
number of partitions for [device]: 4
total time spend in [secs]: 5.6
number of partitions for [subscription]: 4
total time spend in [secs]: 0.4


[all files]
default parallelism: 4
max partition bytes: 10kb
number of partitions for [device]: 8874
total time spend in [secs]: 5.6
number of partitions for [subscription]: 8874
total time spend in [secs]: 0.4


[all files 128mb]
default parallelism: 4
max partition bytes: true
number of partitions for [device]: 93
total time spend in [secs]: 18.66
number of partitions for [subscription]: 93
total time spend in [secs]: 15.71

```