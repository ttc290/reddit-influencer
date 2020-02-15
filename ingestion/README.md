## Data ingestion

![ingestion](~/assets/ingestion.png)

Data ingestion is controlled by the `ingest.sh` shell script

## How to execute the shell script

1. Copy the script from local to one of the workers using Pegasus:

`peg scp to-rem <cluster name> <node number> ingest.sh /home/ubuntu`

2. Log into the worker node:

`peg ssh <cluster name> <node number>`

3. Grant permission to the script:

`chmod +x ingest.sh`

4. Execute the script:

`./ingest.sh <YEAR> <MONTH> <FILE EXTENSION>`