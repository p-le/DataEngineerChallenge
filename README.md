### Datastudio Dashboard

[![Datastudio Dashboard](https://datastudio.google.com/reporting/2c0d85af-f9f1-4660-86e5-dec74e47e7df/page/7xDO/thumbnail)](https://datastudio.google.com/reporting/2c0d85af-f9f1-4660-86e5-dec74e47e7df)


### Requirements
- Text Editor: VSCode
- Docker
- docker-compose
- make

### How to
1. Start Development Enviroment
- Local Spark (1 Master + 2 Worker)
- Local Jupyter Notebook

```
make start-stack
```

2. Submit spark app (Scala)
```
make submit
```

3. Test
```
make test
```

### Using Jupyter Notebook
1a. If running on localhost, open: http://localhost:8888
1b. If running inside Vagrant, open: http://<Vagrant-IP>:8888
2. Check docker-compose log and, copy jupyter token


### Analyze Outputs
I saved Spark Dataframe to CSV Output in
- data/avgSessionTime.csv
- data/mostEngagedUser.csv
- data/sessionTimeIncluded.csv
- data/uniqPageHit.csv


