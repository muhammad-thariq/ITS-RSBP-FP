import os
import sys
from app.core.config import settings
from app.services.graph_service import GraphService
from app.services.fraud_rules import FraudRules

def check_file(path):
    if os.path.exists(path):
        print(f"[OK] Found {path}")
    else:
        print(f"[FAIL] Missing {path}")

def verify():
    print("Verifying Project Structure...")
    check_file("app/core/config.py")
    check_file("app/services/graph_service.py")
    check_file("app/services/fraud_rules.py")
    check_file("app/routers/investigation.py")
    check_file("app/templates/investigation.html")
    check_file("app/static/style.css")
    check_file("main.py")
    check_file("requirements.txt")

    print("\nVerifying Imports...")
    try:
        graph_service = GraphService()
        print("[OK] GraphService instantiated")
        
        rules = FraudRules(graph_service)
        print("[OK] FraudRules instantiated")
        
        # Don't actually connect to DB as it might not be running
        print("[INFO] Skipping actual DB connection test (requires running Neo4j)")
        
    except Exception as e:
        print(f"[FAIL] Import/Instantiation error: {e}")

if __name__ == "__main__":
    verify()
