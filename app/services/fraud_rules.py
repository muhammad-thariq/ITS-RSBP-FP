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

    def get_gds_scores(self, transaction_id):
        """
        Fetch Louvain Community ID and PageRank Score for the sender and receiver.
        """
        query = """
        MATCH (sender:Account)-[t:TRANSACTION {id: $tx_id}]->(receiver:Account)
        RETURN sender.communityId as sender_community, sender.rankScore as sender_rank,
               receiver.communityId as receiver_community, receiver.rankScore as receiver_rank
        """
        with self.graph_service.driver.session() as session:
            result = session.run(query, tx_id=transaction_id)
            record = result.single()
            if record:
                return {
                    "sender_community": record["sender_community"],
                    "sender_rank": record["sender_rank"],
                    "receiver_community": record["receiver_community"],
                    "receiver_rank": record["receiver_rank"]
                }
            return {}

    def explain_fraud(self, transaction_id):
        reasons = []
        
        # Check Rules
        fan_in = self.detect_fan_in(transaction_id)
        if fan_in:
            reasons.append(fan_in)
            
        circular = self.detect_circular_flow(transaction_id)
        if circular:
            reasons.append(circular)
            
        # Check GDS Scores (Heuristic: High PageRank + High Community Modularity might indicate hubs)
        scores = self.get_gds_scores(transaction_id)
        if scores:
            reasons.append(f"GDS Analysis: Sender Rank: {scores.get('sender_rank', 0):.4f}, Receiver Rank: {scores.get('receiver_rank', 0):.4f}")
            if scores.get('sender_community') == scores.get('receiver_community'):
                 reasons.append(f"Community: Both parties are in Community {scores.get('sender_community')}")

        return {
            "transaction_id": transaction_id,
            "is_suspicious": bool(fan_in or circular),
            "reasons": reasons,
            "gds_scores": scores
        }

fraud_rules = FraudRules(graph_service)
