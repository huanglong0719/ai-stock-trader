import sqlite3
import os

db_path = r'd:\木偶说\backend\aitrader.db'

def check_600686_macd():
    if not os.path.exists(db_path):
        print(f"Database not found at {db_path}")
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        query = """
            SELECT trade_date, macd_diff, macd_dea, macd, 
                   weekly_ma20, weekly_macd, 
                   monthly_ma20, monthly_macd
            FROM stock_indicators 
            WHERE ts_code = '600686.SH' 
            ORDER BY trade_date DESC LIMIT 10;
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        
        with open('check_result.txt', 'w', encoding='utf-8') as f:
            if not rows:
                f.write("No data found for 600686.SH in stock_indicators table.\n")
            else:
                f.write("Recent indicators for 600686.SH:\n")
                f.write("Date | MACD_Diff | MACD_Dea | MACD | W_MA20 | W_MACD | M_MA20 | M_MACD\n")
                for row in rows:
                    f.write(f"{row[0]} | {row[1]:.4f} | {row[2]:.4f} | {row[3]:.4f} | {row[4]:.2f} | {row[5]:.4f} | {row[6]:.2f} | {row[7]:.4f}\n")
        print("Done writing to check_result.txt")
                
    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    check_600686_macd()
