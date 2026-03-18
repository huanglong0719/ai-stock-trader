import os
import subprocess
import sys

def run_script(script_path):
    print(f"Running {script_path}...")
    # Use the same python interpreter
    # script_path is relative to project root
    cmd = [sys.executable, script_path]
    ret = subprocess.call(cmd)
    if ret != 0:
        print(f"{script_path} failed with code {ret}")
        return False
    return True

if __name__ == "__main__":
    print("Starting Full Maintenance Task...")
    
    # 1. Fix Weekly/Monthly Adj Factors (Optimized version)
    if run_script("backend/scripts/fix_weekly_monthly_adj.py"):
        print("Adj Factor Fix Complete.")
        
        # 2. Recalc All Indicators
        if run_script("backend/scripts/recalc_indicators.py"):
            print("Indicator Recalculation Complete.")
        else:
            print("Indicator Recalculation Failed.")
    else:
        print("Adj Factor Fix Failed.")
