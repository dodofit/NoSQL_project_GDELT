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

# Create event nodes
create_event = """
LOAD CSV WITH HEADERS FROM 'file:///{fn}' AS row
WITH row WHERE row.GlobalEventID IS NOT NULL
CALL {{
    WITH row
    MERGE (event:Event {{globalEventID:row.GlobalEventID}})
    ON CREATE
    SET
        event.date = Datetime(row.Day),
        event.type = row.EventCode
}} IN TRANSACTIONS OF 15000 ROWS;
"""

# Create relationship between event and country
create_event_country = """
LOAD CSV WITH HEADERS FROM 'file:///{fn}' AS row
WITH row, COALESCE(row.ActionGeo_Lat, null) AS latitude, COALESCE(row.ActionGeo_Long, null) AS longitude
WHERE row.ActionGeo_CountryCode IS NOT NULL
CALL {{
    WITH row, latitude, longitude
    MATCH (event:Event {{globalEventID : row.GlobalEventID}})
    MATCH (country:Country {{FIPS : row.ActionGeo_CountryCode}})
    CREATE (event)-[:TAKES_PLACE {{lat: toFloat(latitude), lon: toFloat(longitude)}}]->(country)
}} IN TRANSACTIONS OF 15000 ROWS;
"""

# Create actor1 node
create_actor1 = """ 
            LOAD CSV WITH HEADERS FROM 'file:///{fn}' AS row
            WITH row WHERE row.Actor1Name IS NOT NULL
            CALL {{
                WITH row
                MERGE (:Actor {{name:row.Actor1Name}})
            }} IN TRANSACTIONS OF 15000 ROWS;
        """

# Create actor2 node
create_actor2 = """ 
            LOAD CSV WITH HEADERS FROM 'file:///{fn}' AS row
            WITH row WHERE row.Actor2Name IS NOT NULL
            CALL {{
                WITH row
                MERGE (:Actor {{name:row.Actor2Name}})
            }} IN TRANSACTIONS OF 15000 ROWS;
        """

# Create relationship between actor1 and event
create_actor1_event = """
            LOAD CSV WITH HEADERS FROM 'file:///{fn}' AS row
            WITH row WHERE row.Actor1Name IS NOT NULL
            CALL {{
                WITH row
                MATCH (event:Event {{globalEventID : row.GlobalEventID}})
                MATCH (actor:Actor {{name : row.Actor1Name}})
                MERGE (actor)-[:ACTS_IN {{actor_type:1}}]->(event)
            }} IN TRANSACTIONS OF 15000 ROWS;
        """

# Create relationship between event and actor2
create_actor2_event = """ 
            LOAD CSV WITH HEADERS FROM 'file:///{fn}' AS row
            WITH row WHERE row.Actor2Name IS NOT NULL
            CALL {{
                WITH row
                MATCH (event:Event {{globalEventID : row.GlobalEventID}})
                MATCH (actor:Actor {{name : row.Actor2Name}})
                MERGE (actor)-[:ACTS_IN {{actor_type:2}}]->(event)
            }} IN TRANSACTIONS OF 15000 ROWS;
        """

def main():
    files = os.listdir('data/raw/')
    export_files = [fn for fn in files if "export.csv" in fn]
    print(export_files)
    for fn in export_files:
        exec_write(create_event, fn, "gdelt")
        exec_write(create_event_country, fn, "gdelt")
        exec_write(create_actor1, fn, "gdelt")
        exec_write(create_actor2, fn, "gdelt")
        exec_write(create_actor1_event, fn, "gdelt")
        exec_write(create_actor2_event, fn, "gdelt")

if __name__=='__main__':
    main()
