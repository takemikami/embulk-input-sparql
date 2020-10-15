# SPARQL input plugin for Embulk

SPARQL input plugin for Embulk loads records by SPARQL from any endpoint.

## Overview

* **Plugin type**: input
* **Resume supported**: no
* **Cleanup supported**: no
* **Guess supported**: no

## Configuration

- **endpoint**: SPARQL endpoint url (string, required)
- **query**: SPARQL query (string, required)
- **columns**: Output columns list (list, required)

## Example

```yaml
in:
  type: sparql
  endpoint: https://data.e-stat.go.jp/lod/sparql/alldata/query
  query: |
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    PREFIX dcterms: <http://purl.org/dc/terms/>
    SELECT ?publisher ?label ?homepage
    WHERE {
      ?s rdf:type <http://data.e-stat.go.jp/lod/otherSurvey/Concept>;
         rdfs:label ?label;
         foaf:homepage ?homepage;
         dcterms:publisher ?publisher
    }
    order by ?publisher
  columns:
    - { name: publisher, type: string }
    - { name: label, type: string }
    - { name: homepage, type: string }
```

## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
