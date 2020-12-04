# Song Recommendation Engine

A song recommendation engine built using Spark and Hadoop.

The model is optimised using the Alternating Least Sqaures (ALS) algorithm.

## Dependencies

- `Hadoop - 3.2.x`
- `Spark - 3.0.x`

## Usage

Extract the dataset
```zsh
unzip dataset.zip
```
Copy the files to Hadoop
```zsh
hdfs dfs -copyFromLocal 10000.txt /user/<name>
hdfs dfs -copyFromLocal song_data.csv /user/<name>
```
Run the Spark job
```zsh
$SPARK_HOME/bin/spark-submit rec_engine.py
```

## Contributors:
- Roshan Daivajna - PES2201800372
- [Nikhil KR](https://github.com/Nikateen) - PES2201800044
- [Ruchira R Vadiraj](https://github.com/Ruchira-R) - PES2201800602