# spark-postgres-metastore

```bash
docker exec -it spark_playground bash

/opt/spark/bin/spark-submit \
  --jars /opt/spark/jars/postgresql-42.7.1.jar \
  --driver-class-path /opt/spark/jars/postgresql-42.7.1.jar \
  /opt/spark/app/process.py
```

```bash
docker exec -it spark_metastore_db psql -U spark_user -d metastore_db
```

