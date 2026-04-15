import mysql.connector
from config import Config

def test_connections():
    registry = Config.CAMPUS_REGISTRY
    results = {}
    
    for code, branch in registry.items():
        print(f"Testing {code} ({branch['short_name']})...")
        if not branch.get('active'):
            print(f"  [SKIP] Branch is marked INACTIVE in config.")
            results[code] = "INACTIVE"
            continue
            
        try:
            conn = mysql.connector.connect(
                host=branch['koha_host'],
                user=branch['koha_user'],
                password=branch['koha_pass'],
                database=branch['koha_db'],
                connect_timeout=5
            )
            print(f"  [OK] Connected successfully.")
            conn.close()
            results[code] = "SUCCESS"
        except Exception as e:
            print(f"  [FAIL] {str(e)}")
            results[code] = f"FAILED: {str(e)}"
            
    print("\n--- Summary ---")
    for code, status in results.items():
        print(f"{code}: {status}")

if __name__ == "__main__":
    test_connections()
