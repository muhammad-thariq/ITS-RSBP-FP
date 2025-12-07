from fastapi import APIRouter, Request, Depends
from fastapi.templating import Jinja2Templates
from app.services.fraud_rules import fraud_rules
from app.core.config import settings

router = APIRouter(
    prefix="/investigation",
    tags=["investigation"]
)

templates = Jinja2Templates(directory="app/templates")

@router.get("/{transaction_id}")
async def investigate_transaction(request: Request, transaction_id: str):
    # 1. Run Fraud Analysis
    fraud_context = fraud_rules.explain_fraud(transaction_id)
    
    # 2. Prepare Cypher for Neovis.js
    # We want to show the transaction, sender, receiver, and immediate neighbors
    cypher_query = f"""
    MATCH (t:TRANSACTION {{id: '{transaction_id}'}})
    MATCH (sender)-[t]->(receiver)
    OPTIONAL MATCH (sender)-[t1]-(n1)
    OPTIONAL MATCH (receiver)-[t2]-(n2)
    RETURN t, sender, receiver, t1, n1, t2, n2 LIMIT 50
    """
    
    return templates.TemplateResponse("investigation.html", {
        "request": request,
        "fraud_context": fraud_context,
        "cypher_query": cypher_query,
        "neo4j_uri": settings.NEO4J_URI,
        "neo4j_user": settings.NEO4J_USER,
        "neo4j_password": settings.NEO4J_PASSWORD
    })

@router.post("/run-gds")
async def run_gds_pipeline():
    """
    Trigger the Graph Data Science pipeline:
    1. Create Projection
    2. Run Louvain
    3. Run PageRank
    """
    try:
        from app.services.graph_service import graph_service
        graph_service.run_gds_pipeline()
        return {"status": "success", "message": "GDS Pipeline executed successfully."}
    except Exception as e:
        return {"status": "error", "message": str(e)}
