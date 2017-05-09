# RDF-graphx

## Load n-triple files into GraphX and save to hdfs
```
./bin/spark−submit \
−−classorg.orion.input.GraphCreate \
−−master local [  ] \
/path/to/runner.jar \
/path/to/ntriplefile
/path/to/outputgraph
```

## Append to an existing graph (GraphX graph saved on hdfs)
```
/ bin / spark−submit \
−−classorg.orion.input.GraphAppend \
−−master local [  ] \
/path/to/runner.jar \
/path/to/graph
/path/to/ntriplefile
/path/to/outputgraph
```


## Compute closeness centrality analysis on GraphX graph
### Run example 
```
. / bin / spark−submit −−classorg.orion.algo.ClosenessCentralityMeasurement \
−−master local [  ] \
/path/to/runner.jar \
/path/to/graph
/path/to/output
```
