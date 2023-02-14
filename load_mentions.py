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

create_resource = """ 
            LOAD CSV WITH HEADERS FROM 'file:///{fn}' AS row
            WITH row WHERE row.MentionIdentifier IS NOT NULL
            CALL {{
                WITH row
                MERGE (:Resource {{originalID:row.MentionIdentifier}})
            }} IN TRANSACTIONS OF 15000 ROWS;
        """

create_resource_event = """
            LOAD CSV WITH HEADERS FROM 'file:///{fn}' AS row
            WITH row WHERE row.MentionIdentifier IS NOT NULL AND row.GlobalEventID IS NOT NULL
            CALL {{
                WITH row
                MATCH (event:Event {{globalEventID : row.GlobalEventID}})
                MATCH (resource:Resource {{originalID : row.MentionIdentifier}})
                CREATE (resource)-[:MENTIONS {{date:Datetime(substring(row.EventTimeDate, 0, 8)), confidence:coalesce(toInteger(row.Confidence), 0)}}]->(event)
            }} IN TRANSACTIONS OF 15000 ROWS;
        """

def main():
    files = os.listdir('/var/lib/neo4j/import/')
    export_files = [fn for fn in files if "mentions.csv" in fn]
    print(export_files)
    for fn in export_files:
        exec_write(create_resource, fn, "gdelt")
        exec_write(create_resource_event, fn, "gdelt")

if __name__=='__main__':
    main()
