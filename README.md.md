

## Goal

Read metadata from datalake files (in ".parquet" format) in HDFS :
- File directory path (data stream)
- Structure of the fields in these files (name and type: integer, boolean, etc.)

→ To determine :
    - The corresponding data stream
    - The name and type of the fields in the stream
    - Whether or not this data stream should be anonymised

Anonymisation issues :
- Quickly identifying the data flows to be anonymised (containing personal and therefore sensitive data)
- RGPD compliance (final anonymisation or mandatory deletion)

---------------------

## Responding to technical / business constraints

Time-consuming and repetitive work :
- Large and numerous data streams on disk (several hundred GB) 
- Rules for partitioning this data across the different nodes in the cluster
- Limits of the available CPU resources on the cluster for distributed computing (Spark 2 and 6-node cluster, 95 GB RAM each)

Consequences (DevOps approach) :
- Time-consuming and energy-consuming for a developer (not rewarding in the long run)
- Redundancy: penalises the company in the long run; doesn't allow them to concentrate on value-added tasks, i.e. more rewarding tasks that save the company time and money. 

---------------------

## Technical stack

- Scala
- Spark
- Hadoop HDFS
- Apache Zeppelin
- Apache Airflow

---------------------

## Project division

- Mapping solution (***`/cartographie/src/main/scala/App.scala`*** file)
- Zeppelin to test this solution
- Packaging and compilation of the solution as a *".jar"* file using Maven
- Run the solution :

  - ***"spark-submit"*** command execution in a CLI :
    ```bash
    export SPARK_MAJOR_VERSION=2
    spark-submit \
    --name "carte_carto-1.0" \
    --master yarn \
    --class com.l*****.App \
    'hdfs:/user/allieart/cartographie/carte_carto-1.0.jar' \
    '/user/allieart/cartographie/conf' \
    '/user/allieart/cartographie/output' \
    'hdfs://endor.l*****-marketing.local:8020'
    ```  

  - Automation of this analysis process using ***Airflow***, a data pipeline orchestration tool :
  ```bash
    - Configuration of a *DAG file* using the Python programming language (***`/airflow/cartographie.py`*** file)
    - Connection to a server machine using *WinSCP* or *FileZilla* client (*SSH protocols*)
      → This machine hosts *AirFlow* (in dev/prod environments)
      → SSH connection to this machine using a login, to administer AirFlow's Python DAG files (in dev/prod environments)
    
    - Putting the Python script which name is *"cartographie.py"* in the *"srv/airflow/dags/"* directory on this machine.