from neo4j import GraphDatabase
from app.core.config import settings
import logging

logger = logging.getLogger(__name__)

class GraphService:
    def __init__(self):
        self.driver = GraphDatabase.driver(
            settings.NEO4J_URI,
            auth=(settings.NEO4J_USER, settings.NEO4J_PASSWORD)
        )

    def close(self):
        self.driver.close()

    def check_connection(self):
        try:
            with self.driver.session() as session:
                result = session.run("RETURN 1")
                return result.single()[0] == 1
        except Exception as e:
            logger.error(f"Failed to connect to Neo4j: {e}")
            return False

    def run_gds_pipeline(self, projection_name="fraud_graph"):
        """
        Orchestrates the GDS pipeline:
        1. Create Projection
        2. Run Louvain (Community Detection)
        3. Run PageRank (Centrality)
        """
        with self.driver.session() as session:
            # 1. Drop projection if exists
            session.run(f"""
                CALL gds.graph.drop('{projection_name}', false)
            """)

            # 2. Create Projection (Native Projection)
            # Assuming 'Client' and 'Transaction' nodes, and 'PERFORMS', 'BENEFICIARY' relationships
            # Adjusting to PaySim schema: usually 'Client' nodes and 'TRANSACTION' relationships?
            # PaySim: Node types: Customer, Merchant. Rel: TRANSACTION.
            # Let's assume a simple monopartite graph for now: Account -> [TRANSACTION] -> Account
            # Or bipartite: Account -> Transaction -> Account.
            # The prompt mentions "Fan-In Mule Hubs" and "Circular Flow", implying Account-to-Account flow.
            # Let's project 'Account' nodes and 'TRANSACTION' relationships (aggregated).
            
            # For PaySim, usually it's (Customer)-[:TRANSACTION]->(Customer/Merchant)
            # We'll project all nodes with label 'Account' (or whatever the user has) and 'TRANSACTION' rels.
            # Safest generic projection:
            
            create_query = f"""
            CALL gds.graph.project(
                '{projection_name}',
                '*',
                'TRANSACTION',
                {{
                    relationshipProperties: 'amount'
                }}
            )
            """
            session.run(create_query)
            logger.info(f"Graph projection '{projection_name}' created.")

            # 3. Run Louvain (Write back communityId)
            louvain_query = f"""
            CALL gds.louvain.write(
                '{projection_name}',
                {{
                    writeProperty: 'communityId'
                }}
            )
            """
            session.run(louvain_query)
            logger.info("Louvain algorithm executed.")

            # 4. Run PageRank (Write back rankScore)
            pagerank_query = f"""
            CALL gds.pageRank.write(
                '{projection_name}',
                {{
                    maxIterations: 20,
                    dampingFactor: 0.85,
                    writeProperty: 'rankScore'
                }}
            )
            """
            session.run(pagerank_query)
            logger.info("PageRank algorithm executed.")

graph_service = GraphService()
