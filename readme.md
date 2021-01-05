# Kafka Example Data Enrichment Pipeline

This project is an example data enrichment pipeline for street addresses. 

## Setup

### Dependencies

Packages are managed with pipenv. Install all dependencies:

```
pipenv install
```

### Configuration

This project requires a kafka and postgres instance.

All project modules will read from config.ini for kafka and postgres connection information. Copy over the template.

```
cp config-template.ini config.ini
```

Edit config.ini and replace the defaults with authentication info for your kafka and postgresql instance.

### Configuring Kafka

In your kafka instance, add two topics under the following names:

*raw\_address
*enriched\_address

If you are using Aiven, you can find out how to configure Kafka and add topics [here](https://help.aiven.io/en/articles/489572-getting-started-with-aiven-kafka).

This project assumes your kafka instance is using SSL. If so, you will need to download the certs and provide the paths to those certs in config.ini. The above link can also help you with that.

## Components

The data passed through this pipeline is a three key object representing a street address. A street address used by this project contains exactly 3 fields and looks like the JSON object below.

```
    {
        'street_number': '59',
        'street_name': 'Sunnybrook',
        'postal_code': 'M5P 7R2'
    }
```

This project includes 3 modules - a producer, an enricher and a consumer. Each module represents a node in an example data pipeline using kafka. Each module is run independently. All modules can be run at the same time, or run one after another.

### producer.py

Run using `pipenv run producer.py [JSON_FILEPATH]`.

Takes a path to a json file containing an array of JSON address objects (like the one above) and submits each one to the first of two kafka topics ('raw\_buildings', 'enriched\_buildings'). Runs once and ends once the last JSON object has been sent.

Any extra keys in the address object will be discarded by the producer. Any addresses missing one of the three fields will not be sent.

### enricher.py

Run using `pipenv run enricher.py`.

Reads from the first of the two kafka topics - 'raw\_buildings' and forwards to 'enriched\_buildings'. Continually polls until keyboard interrupt.

Actual data enrichment is not yet implemented - the idea is to make calls to the OpenStreetMaps API to get further information.

### consumer.py

Run using `pipenv run consumer.py`

Reads from the second kafka topic ('enriched\_buildings') and stores it in postgres database. Continually polls until keyboard interrupt.

## Testing

No tests have yet been written for this project. 

Almost all non-trivial code in this project relates to accessing external resources - querying postgres, kafka, reading JSON or the config file. A future set of tests for this project might include:

* An end-to-end test that sends an address to the raw\_address topic and queries the postgres endpoint for that address.
