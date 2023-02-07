from neo4j import GraphDatabase
import os

URI = "bolt://localhost:7687"
AUTH = ("neo4j", "azerty92")

with GraphDatabase.driver(URI, auth=AUTH) as driver: 
    driver.verify_connectivity()

def exec_write(query, csv_filename, db):
    # Execute a write query in the provided DB from a csv
    with driver.session(database=db) as session:
        summary = session.run(query.format(fn=csv_filename)).consume()

        print("Created {nodes_created} nodes and {relationship_created} relationships and set {properties_set} properties in {time} ms.".format(
            nodes_created=summary.counters.nodes_created,
            properties_set=summary.counters.properties_set,
            relationship_created=summary.counters.relationships_created,
            time=summary.result_available_after
        ))

# Create themes nodes
create_themes = """ 
            LOAD CSV WITH HEADERS FROM 'file:///{fn}' AS row
            WITH row WHERE row.Themes IS NOT NULL
            CALL {{
                WITH row
                UNWIND split(row.Themes, ';') as theme
                MERGE (:Theme {{name:theme}})
            }} IN TRANSACTIONS OF 15000 ROWS;
        """

# Create source nodes
create_sources = """ 
            LOAD CSV WITH HEADERS FROM 'file:///{fn}' AS row
            WITH row WHERE row.SourceCommonName IS NOT NULL
            CALL {{
                WITH row
                MERGE (:Source {{name:row.SourceCommonName, type:row.SourceCollectionIdentifier}})
            }} IN TRANSACTIONS OF 15000 ROWS;
        """

# Update exsting ressource nodes with gkg info
update_resource = """ 
            LOAD CSV WITH HEADERS FROM 'file:///{fn}' AS row
            WITH row WHERE row.DocumentIdentifier IS NOT NULL
            CALL {{
                WITH row
                MATCH (r:Resource {{originalID:row.DocumentIdentifier}})
                SET r += {{date: Datetime(substring(row.DATE, 0, 8)), originalLanguage:coalesce(row.TranslationInfo, "eng"), tone:coalesce(toFloat(split(row.V2Tone, ',')[0]), 0.0)}}
            }} IN TRANSACTIONS OF 15000 ROWS;
        """

# Create relationship between ressources and themes
create_resource_themes = """ 
            LOAD CSV WITH HEADERS FROM 'file:///{fn}' AS row
            WITH row WHERE row.Themes IS NOT NULL AND row.DocumentIdentifier IS NOT NULL
            CALL {{
                WITH row
                UNWIND split(row.Themes, ';') as theme
                MATCH (t:Theme {{name:theme}})
                MATCH (resource:Resource {{originalID : row.DocumentIdentifier}})
                CREATE (resource)-[:HAS]->(t)
            }} IN TRANSACTIONS OF 15000 ROWS;
        """

# Create relationship between source and resource
create_source_resource = """ 
            LOAD CSV WITH HEADERS FROM 'file:///{fn}' AS row
            WITH row WHERE row.SourceCommonName IS NOT NULL
            CALL {{
                WITH row
                MATCH (source:Source {{name:row.SourceCommonName}})
                MATCH (resource:Resource {{originalID : row.DocumentIdentifier}})
                CREATE (source)-[:PUBLISH]->(resource)
            }} IN TRANSACTIONS OF 15000 ROWS;
        """

def main():
    files = os.listdir('/var/lib/neo4j/import/')
    export_files = [fn for fn in files if "gkg.csv" in fn]
    print(export_files)
    for fn in export_files:
        exec_write(create_sources, fn, "gdelt")
        exec_write(create_source_resource, fn, "gdelt")
        exec_write(update_resource, fn, "gdelt")
        exec_write(create_themes, fn, "gdelt")
        exec_write(create_resource_themes, fn, "gdelt")

if __name__=='__main__':
    main()
