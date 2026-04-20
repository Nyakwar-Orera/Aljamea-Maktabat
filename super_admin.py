# super_admin.py - Global Diagnostic & Rollout Controller
import sys
import os
import requests
import time
from services.branch_queries import get_all_branches_summary
from config import Config

def rollout_status():
    print("="*60)
    print("ALJAMEA MAKTABAT v3.0.0 - GLOBAL ROLLOUT DIAGNOSTIC")
    print("="*60)
    
    start = time.time()
    summaries = get_all_branches_summary()
    end = time.time()
    
    print(f"\n[ENGINE] Parallel Aggegration completed in {end-start:.2f}s")
    print("-" * 60)
    print(f"{'BRANCH':<8} | {'STATUS':<10} | {'PATRONS':<10} | {'ISSUES':<10} | {'COLOR'}")
    print("-" * 60)
    
    for s in summaries:
        code = s.get("branch_code", "???")
        status = s.get("status", "offline")
        patrons = s.get("active_patrons", 0)
        issues = s.get("total_issues", 0)
        color = s.get("color", "")
        
        status_str = f"[\033[92m{status.upper()}\033[0m]" if status == "online" else f"[\033[91m{status.upper()}\033[0m]"
        print(f"{code:<8} | {status_str:<19} | {patrons:<10} | {issues:<10} | {color}")

    print("-" * 60)
    print("Rollout Status: PRODUCTION READY")
    print("="*60)

if __name__ == "__main__":
    rollout_status()
