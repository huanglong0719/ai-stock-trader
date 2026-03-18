
import sqlite3
import os

def check_industry_data_distribution():
    db_path = os.path.join('backend', 'aitrader.db')
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    target_date = '2026-01-09'
    
    print(f"Checking industry data for {target_date}...")
    
    # All unique industries
    cursor.execute("SELECT DISTINCT industry FROM industry_data")
    all_industries = [r[0] for r in cursor.fetchall()]
    print(f"Total unique industries in IndustryData: {len(all_industries)}")
    
    # Industries with data on target_date
    cursor.execute("SELECT DISTINCT industry FROM industry_data WHERE trade_date = ?", (target_date,))
    target_industries = [r[0] for r in cursor.fetchall()]
    print(f"Industries with data on {target_date}: {len(target_industries)}")
    
    # Industries with indicators on target_date
    cursor.execute("SELECT DISTINCT ts_code FROM stock_indicators WHERE ts_code LIKE 'IND_%' AND trade_date = ?", (target_date,))
    indicator_industries = [r[0][4:] for r in cursor.fetchall()]
    print(f"Industries with indicators on {target_date}: {len(indicator_industries)}")
    
    missing = set(target_industries) - set(indicator_industries)
    print(f"Missing industry indicators for {target_date}: {len(missing)}")
    if missing:
        print("Sample missing industries:")
        for m in list(missing)[:10]:
            # Check how many data points this industry has
            cursor.execute("SELECT count(*) FROM industry_data WHERE industry = ?", (m,))
            count = cursor.fetchone()[0]
            print(f"  {m}: {count} data points")
            
    conn.close()

if __name__ == "__main__":
    check_industry_data_distribution()
