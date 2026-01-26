#!/usr/bin/env python3
"""
Audit script to review all orders and their purchased_units matches.
Checks for correctness of ASIN selection, quantities, and unit costs.
"""
import sqlite3
import json
import os
import sys

DB_PATH = os.getenv("DB_PATH", "/app/database.db")
if not os.path.exists(DB_PATH):
    DB_PATH = os.path.join(os.path.dirname(__file__), "..", "database.db")

def audit_orders():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    
    # Get all orders
    cursor = conn.execute('''
        SELECT 
            oh.order_id, oh.transaction_id, oh.item_title, oh.transaction_price, 
            oh.quantity_purchased, oh.created_time,
            oh.match1_asin, oh.match1_title, oh.match1_lot_breakdown, oh.match1_net_cost,
            oh.match2_asin, oh.match2_title, oh.match2_lot_breakdown, oh.match2_net_cost,
            oh.match3_asin, oh.match3_title, oh.match3_lot_breakdown, oh.match3_net_cost,
            oh.match4_asin, oh.match4_title, oh.match4_lot_breakdown, oh.match4_net_cost
        FROM order_history oh
        ORDER BY oh.created_time DESC
    ''')
    orders = cursor.fetchall()
    
    print("=" * 100)
    print("ORDER AUDIT REPORT")
    print("=" * 100)
    
    issues = []
    
    for order in orders:
        oid = order['order_id']
        tid = order['transaction_id']
        title = order['item_title'] or 'NO TITLE'
        price = float(order['transaction_price'] or 0)
        qty = order['quantity_purchased'] or 1
        date = (order['created_time'] or '')[:10]
        
        # Get purchased_units for this order
        pu_cursor = conn.execute('''
            SELECT color, asin, quantity, unit_cost, net_per_unit, model, capacity
            FROM purchased_units 
            WHERE order_id = ? AND transaction_id = ? AND color != 'unmatched'
        ''', (oid, tid))
        units = list(pu_cursor.fetchall())
        
        # Parse lot breakdowns to understand expected units
        expected_units = 0
        expected_colors = {}
        for i in range(1, 5):
            lb_str = order[f'match{i}_lot_breakdown']
            if lb_str:
                try:
                    lb = json.loads(lb_str)
                    if isinstance(lb, dict) and 'color_quantities' in lb:
                        for color, qty_c in lb['color_quantities'].items():
                            if color not in expected_colors:
                                expected_colors[color] = qty_c
                                expected_units += qty_c
                        break  # Use first valid breakdown
                except:
                    pass
        
        # Calculate totals
        total_matched_qty = sum(u['quantity'] for u in units)
        total_unit_cost = sum(u['unit_cost'] * u['quantity'] for u in units) if units else 0
        
        # Check for issues
        order_issues = []
        
        # Issue: No matches when matches were available
        if not units and order['match1_asin']:
            order_issues.append("NO_PURCHASED_UNITS_BUT_MATCHES_EXIST")
        
        # Issue: Unit cost doesn't match order price
        if units and abs(total_unit_cost - price) > 1.0:
            order_issues.append(f"COST_MISMATCH: paid=${price:.2f} but units sum=${total_unit_cost:.2f}")
        
        # Issue: Quantity mismatch
        if expected_units > 0 and total_matched_qty != expected_units:
            order_issues.append(f"QTY_MISMATCH: expected {expected_units} units, got {total_matched_qty}")
        
        # Print order
        print(f"\n{'⚠️ ' if order_issues else '✅ '}{date} | {oid[:18]}...")
        print(f"   Title: {title[:75]}{'...' if len(title) > 75 else ''}")
        print(f"   Paid: ${price:.2f} | eBay Qty: {qty}")
        
        if units:
            print(f"   UNITS ({len(units)}):")
            for u in units:
                net = u['net_per_unit'] or 0
                cost = u['unit_cost'] or 0
                profit = net - cost
                profit_color = "🟢" if profit > 0 else "🔴"
                print(f"      {u['color']:8} | {u['asin']} | qty:{u['quantity']} | paid:${cost:.2f} | net:${net:.2f} | {profit_color} profit:${profit:.2f}")
        else:
            print(f"   ❌ NO PURCHASED UNITS")
            # Show available matches
            for i in range(1, 5):
                asin = order[f'match{i}_asin']
                mtitle = order[f'match{i}_title']
                if asin:
                    print(f"      Match{i}: {asin} - {mtitle}")
        
        if order_issues:
            for issue in order_issues:
                print(f"   ⚠️  {issue}")
            issues.append((oid, order_issues))
    
    # Summary
    print("\n" + "=" * 100)
    print("SUMMARY")
    print("=" * 100)
    print(f"Total orders: {len(orders)}")
    print(f"Orders with issues: {len(issues)}")
    
    if issues:
        print("\nISSUES FOUND:")
        for oid, issue_list in issues[:20]:
            print(f"  {oid[:20]}: {', '.join(issue_list)}")
    
    conn.close()

if __name__ == "__main__":
    audit_orders()
