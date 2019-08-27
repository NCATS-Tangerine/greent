"""Get Neo4j index setup cypher."""
import os
from neo4j.v1 import GraphDatabase, basic_auth
driver = GraphDatabase.driver(
    f"bolt://{os.environ['NEO4J_HOST']}:{os.environ['NEO4J_BOLT_PORT']}",
    auth=basic_auth("neo4j", os.environ['NEO4J_PASSWORD'])
)
with driver.session() as session:
    # edge id index
    result = session.run("MATCH ()-[x]-() RETURN DISTINCT type(x) as predicate")
    predicates = list(row['predicate'] for row in result)
    predicates.remove('is_a')
    predicates.remove('Unmapped_Relation')
    session.run(f"""CALL db.index.fulltext.createRelationshipIndex('edge_id_index', [{', '.join(f"'{predicate}'" for predicate in predicates)}], ['id'], {{analyzer: 'keyword'}})""")

    # node name index
    session.run("""CALL db.index.fulltext.createNodeIndex("node_name_index", ["named_thing"], ["name"], {analyzer: "whitespace"})""")

    # node id indexes
    result = session.run("MATCH (n) UNWIND labels(n) as l RETURN DISTINCT l as type")
    for row in result:
        if row['type'] not in ('Concept', 'named_thing'):
            session.run(f"""CREATE INDEX ON :{row['type']}(id)""")
