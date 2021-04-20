### Datastudio Dashboard
I saved Spark Dataframe to CSV Output in
- `data/avgSessionTime.csv`
- `data/mostEngagedUser.csv`
- `data/sessionTimeIncluded.csv`
- `data/uniqPageHit.csv`

To visuallize with Data Studio
- Upload `data/*csv` to Google Cloud Storage `make upload-gcs`
- Create Google Data Studio Dashboard with Google Cloud Storage csv Data Sources

[![Datastudio Dashboard](https://datastudio.google.com/reporting/2c0d85af-f9f1-4660-86e5-dec74e47e7df/page/7xDO/thumbnail)](https://datastudio.google.com/reporting/2c0d85af-f9f1-4660-86e5-dec74e47e7df)


### Requirements
- Text Editor: VSCode
- Docker
- docker-compose
- make

### How to
1. Start Development Enviroment

- Local Spark (1 Master + 2 Worker): Published port 8080 and 7070
- Local Jupyter Notebook

```
make start-stack
```

2. Submit spark app (Scala)

Compile Scala Spark Application and run spark-submit
```
make submit
```

3. Test

Running sbt test
```
make test
```

### Using Jupyter Notebook

1a. If running on localhost, open: http://localhost:8888

1b. If running inside Vagrant, open: http://{{Vagrant-IP}}:8888

2. Check Jupyter access token in `docker-compose logs jupyter` output, then copy and paste to log in

