from app.services.graph_service import graph_service

class FraudRules:
    def __init__(self, graph_service):
        self.graph_service = graph_service

    def detect_fan_in(self, transaction_id, min_senders=5, time_window_minutes=60):
        """
        Rule 1: Fan-In Mule Hubs
        Find accounts receiving funds from > N distinct senders within T minutes.
        """
        query = """
        MATCH (t:TRANSACTION {id: $tx_id})
        MATCH (receiver:Account)<-[:TRANSACTION]-(t)
        WITH receiver
        MATCH (sender:Account)-[other_t:TRANSACTION]->(receiver)
        WHERE abs(duration.inSeconds(other_t.timestamp, datetime()).minutes) <= $time_window
        WITH receiver, count(distinct sender) as distinct_senders
        WHERE distinct_senders > $min_senders
        RETURN distinct_senders
        """
        # Note: The above query assumes a specific schema. 
        # Adapting to a more standard (Sender)-[TRANSACTION]->(Receiver) model where Transaction is a relationship.
        
        # Revised Query for (Account)-[t:TRANSACTION]->(Account)
        # We need to find the receiver of the current transaction.
        # Then check that receiver's incoming transactions.
        
        query = """
        MATCH (sender:Account)-[current_tx:TRANSACTION {id: $tx_id}]->(receiver:Account)
        WITH receiver, current_tx
        MATCH (other_sender:Account)-[t:TRANSACTION]->(receiver)
        WHERE t.timestamp >= current_tx.timestamp - duration({minutes: $time_window})
          AND t.timestamp <= current_tx.timestamp
        WITH receiver, count(distinct other_sender) as distinct_senders
        WHERE distinct_senders > $min_senders
        RETURN distinct_senders
        """
        
        with self.graph_service.driver.session() as session:
            result = session.run(query, tx_id=transaction_id, min_senders=min_senders, time_window=time_window_minutes)
            record = result.single()
            if record:
                return f"Fan-In Alert: Receiver got funds from {record['distinct_senders']} distinct senders in {time_window_minutes} mins."
            return None

    def detect_circular_flow(self, transaction_id):
        """
        Rule 2: Circular Flow
        Detect loops (A->B->C->A) involving the transaction.
        """
        query = """
        MATCH (a:Account)-[t:TRANSACTION {id: $tx_id}]->(b:Account)
        MATCH path = (b)-[:TRANSACTION*1..4]->(a)
        RETURN length(path) as path_len
        LIMIT 1
        """
        
        with self.graph_service.driver.session() as session:
            result = session.run(query, tx_id=transaction_id)
            record = result.single()
            if record:
                return f"Circular Flow Alert: Cycle of length {record['path_len'] + 1} detected involving this transaction."
            return None

    def get_gds_scores(self, tx_id: str):
        query = """
        MATCH (s:Client)-[t:TRANSACTED {txId: $txId}]->(r:Client)
        RETURN 
            s.pagerankScore AS sender_rank,
            s.communityId   AS sender_community,
            r.pagerankScore AS receiver_rank,
            r.communityId   AS receiver_community
        """
        with self.graph_service.driver.session() as session:
            result = session.run(query, txId=tx_id)
            record = result.single()

        if record:
            return dict(record)

        return {
            "sender_rank": 0,
            "receiver_rank": 0,
            "sender_community": "N/A",
            "receiver_community": "N/A"
        }


    def explain_fraud(self, tx_id: str):
        query = """
        MATCH (s:Client)-[t:TRANSACTED {txId: $txId}]->(r:Client)
        RETURN 
            s.id AS sender,
            r.id AS receiver,
            t.amount AS amount,
            t.type AS type,
            t.step AS step,
            t.isFraud AS isFraud,
            coalesce(t.ruleFlaggedFraud, false) AS ruleFlaggedFraud
        """

        with self.graph_service.driver.session() as session:
            result = session.run(query, txId=tx_id)
            record = result.single()

        if not record:
            return {
                "status": "NOT_FOUND",
                "explanation": "Transaction ID not found in graph.",
                "gds_scores": {},
            }

        # ✅ FETCH GDS SCORES
        gds_scores = self.get_gds_scores(tx_id)

        if record["isFraud"] == 1 or record["ruleFlaggedFraud"]:
            return {
                "status": "FRAUD",
                "explanation": "Transaction matches fraud indicators.",
                "data": dict(record),
                "gds_scores": gds_scores
            }

        return {
            "status": "CLEARED",
            "explanation": "No specific fraud patterns detected.",
            "data": dict(record),
            "gds_scores": gds_scores
        }



fraud_rules = FraudRules(graph_service)
