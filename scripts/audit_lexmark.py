#!/usr/bin/env python3
"""
Lexmark Products Audit Script

Analyzes Lexmark products for data quality issues:
- Duplicate ASINs
- Invalid ASIN formats
- Color/capacity mismatches with part numbers
- Model/part number inconsistencies
- Same part number with multiple ASINs (may be valid for multi-packs)

Outputs a detailed audit report.
"""

import os
import sys
import sqlite3
import csv
import re
from collections import defaultdict
from datetime import datetime

# Add project root to path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
sys.path.insert(0, PROJECT_ROOT)

DB_PATH = os.path.join(PROJECT_ROOT, "database.db")


def get_lexmark_products():
    """Fetch all Lexmark products from database."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.execute("""
        SELECT id, brand, model, part_number, color, capacity, asin, 
               variant_label, pack_size, net_cost, bsr, active
        FROM products 
        WHERE brand = 'lexmark'
        ORDER BY model, part_number, color
    """)
    products = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return products


def validate_asin(asin):
    """Check if ASIN format is valid (10 alphanumeric chars)."""
    if not asin:
        return False, "Missing ASIN"
    if len(asin) != 10:
        return False, f"ASIN length is {len(asin)}, expected 10"
    if not asin.isalnum():
        return False, "ASIN contains non-alphanumeric characters"
    return True, None


def extract_color_from_part_number(part_number):
    """
    Extract expected color from Lexmark part number suffix.
    
    Lexmark color codes (typically last 2 chars before final digit):
    - K, K0, KG = Black
    - C, C0, CG = Cyan
    - M, M0, MG = Magenta
    - Y, Y0, YG = Yellow
    - Z = Imaging/Photoconductor (usually Black)
    - V = CMY/Color set
    """
    if not part_number:
        return None
    
    pn = part_number.upper()
    
    # Check endings
    color_suffixes = {
        'K0': 'Black', 'C0': 'Cyan', 'M0': 'Magenta', 'Y0': 'Yellow',
        'KG': 'Black', 'CG': 'Cyan', 'MG': 'Magenta', 'YG': 'Yellow',
        'HK': 'Black', 'HC': 'Cyan', 'HM': 'Magenta', 'HY': 'Yellow',
        'XK': 'Black', 'XC': 'Cyan', 'XM': 'Magenta', 'XY': 'Yellow',
        'UK': 'Black', 'UC': 'Cyan', 'UM': 'Magenta', 'UY': 'Yellow',
        'SK': 'Black', 'SC': 'Cyan', 'SM': 'Magenta', 'SY': 'Yellow',
        'ZV': 'CMY', 'ZK': 'Black',
        'P00': 'Drum',
    }
    
    # Check 2-char suffix
    if len(pn) >= 2:
        suffix2 = pn[-2:]
        if suffix2 in color_suffixes:
            return color_suffixes[suffix2]
    
    # Check 3-char suffix
    if len(pn) >= 3:
        suffix3 = pn[-3:]
        if suffix3 in color_suffixes:
            return color_suffixes[suffix3]
    
    # Single char at end (less reliable)
    single_char = {
        'K': 'Black', 'C': 'Cyan', 'M': 'Magenta', 'Y': 'Yellow',
    }
    if pn and pn[-1] in single_char:
        # Only if preceded by number
        if len(pn) >= 2 and pn[-2].isdigit():
            return single_char[pn[-1]]
    
    return None


def extract_capacity_from_part_number(part_number):
    """
    Extract expected capacity from Lexmark part number.
    
    Capacity codes (usually 3rd from last or in middle):
    - H = High
    - X = Extra High
    - U = Ultra High
    - 0/1 at end = Standard
    - E = Economy/Return Program
    """
    if not part_number:
        return None
    
    pn = part_number.upper()
    
    # Look for capacity indicators
    # Format often: XXB1H00 where H=High, or XXB1X00 where X=Extra High
    
    # Check for specific patterns
    if 'XH' in pn or 'X0' in pn[-3:]:
        return 'Extra High'
    if 'UH' in pn or 'U0' in pn[-3:]:
        return 'Ultra High'
    
    # Check position -3 or -4 for capacity code
    for i in range(min(4, len(pn)-1), 1, -1):
        if i < len(pn):
            char = pn[-i]
            if char == 'H' and pn[-(i-1)].isdigit():
                return 'High'
            if char == 'X' and pn[-(i-1)].isdigit():
                return 'Extra High'  
            if char == 'U' and pn[-(i-1)].isdigit():
                return 'Ultra High'
    
    return None


def audit_products(products):
    """Run all audit checks on products."""
    
    issues = []      # Critical issues that need fixing
    warnings = []    # Potential issues to review
    info = []        # Informational notes
    
    # Tracking for duplicate detection
    asins_seen = defaultdict(list)
    part_numbers_seen = defaultdict(list)
    
    # Valid values
    VALID_COLORS = {'Black', 'Cyan', 'Magenta', 'Yellow', 'CMYK', 'Color', 'CMY', 
                    'Drum', 'Fuser', 'Maint', 'Imaging', 'Photoconductor', 'Ink', 'Grey'}
    VALID_CAPACITIES = {'Standard', 'High', 'Extra High', 'Ultra High', 'Super High', '', None}
    
    for p in products:
        pid = p['id']
        model = p['model'] or ''
        part_number = p['part_number'] or ''
        color = p['color'] or ''
        capacity = p['capacity'] or ''
        asin = p['asin'] or ''
        variant_label = p['variant_label'] or ''
        pack_size = p['pack_size'] or 1
        
        # Track for duplicates
        if asin:
            asins_seen[asin].append(p)
        if part_number:
            part_numbers_seen[part_number].append(p)
        
        # === Check 1: ASIN format ===
        valid, msg = validate_asin(asin)
        if not valid:
            issues.append({
                'type': 'INVALID_ASIN',
                'severity': 'ERROR',
                'product_id': pid,
                'part_number': part_number,
                'asin': asin,
                'message': msg,
                'suggestion': 'Verify and correct the ASIN'
            })
        
        # === Check 2: Color validation ===
        if color and color not in VALID_COLORS:
            warnings.append({
                'type': 'UNKNOWN_COLOR',
                'severity': 'WARNING',
                'product_id': pid,
                'part_number': part_number,
                'asin': asin,
                'color': color,
                'message': f'Unknown color value: "{color}"',
                'suggestion': f'Valid colors: {", ".join(sorted(VALID_COLORS))}'
            })
        
        # === Check 3: Capacity validation ===
        if capacity and capacity not in VALID_CAPACITIES:
            warnings.append({
                'type': 'UNKNOWN_CAPACITY',
                'severity': 'WARNING',
                'product_id': pid,
                'part_number': part_number,
                'asin': asin,
                'capacity': capacity,
                'message': f'Non-standard capacity: "{capacity}"',
                'suggestion': f'Standard capacities: Standard, High, Extra High, Ultra High'
            })
        
        # === Check 4: Part number vs color mismatch ===
        expected_color = extract_color_from_part_number(part_number)
        if expected_color and color and expected_color != color:
            # Skip certain types that don't follow color rules
            if color not in ['CMYK', 'CMY', 'Drum', 'Fuser', 'Maint', 'Imaging', 'Photoconductor', 'Ink']:
                warnings.append({
                    'type': 'COLOR_MISMATCH',
                    'severity': 'CHECK',
                    'product_id': pid,
                    'part_number': part_number,
                    'asin': asin,
                    'stated_color': color,
                    'expected_color': expected_color,
                    'message': f'Part number suggests "{expected_color}" but color is "{color}"',
                    'suggestion': 'Verify the correct color for this part number'
                })
        
        # === Check 5: Part number vs capacity mismatch ===
        expected_capacity = extract_capacity_from_part_number(part_number)
        if expected_capacity and capacity and expected_capacity != capacity:
            if 'Standard' not in [expected_capacity, capacity]:  # Standard is often inferred
                warnings.append({
                    'type': 'CAPACITY_MISMATCH',
                    'severity': 'CHECK',
                    'product_id': pid,
                    'part_number': part_number,
                    'asin': asin,
                    'stated_capacity': capacity,
                    'expected_capacity': expected_capacity,
                    'message': f'Part number suggests "{expected_capacity}" but capacity is "{capacity}"',
                    'suggestion': 'Verify the correct capacity for this part number'
                })
        
        # === Check 6: Model/Part number alignment ===
        if model and part_number:
            # Model should generally be related to part number
            model_clean = model.replace(' ', '').upper()
            pn_clean = part_number.replace(' ', '').upper()
            
            # Check if model is prefix or contained in part_number
            if not pn_clean.startswith(model_clean[:3]) and model_clean not in pn_clean:
                # Known exceptions
                known_exceptions = ['36XL & 37XL', 'CX735', 'X73X']
                if model not in known_exceptions:
                    info.append({
                        'type': 'MODEL_PART_MISMATCH',
                        'severity': 'INFO',
                        'product_id': pid,
                        'model': model,
                        'part_number': part_number,
                        'message': f'Model "{model}" may not align with part_number "{part_number}"',
                        'suggestion': 'Review if model family is correct'
                    })
        
        # === Check 7: Missing net_cost ===
        if not p['net_cost'] and active:
            info.append({
                'type': 'MISSING_NET_COST',
                'severity': 'INFO',
                'product_id': pid,
                'part_number': part_number,
                'asin': asin,
                'message': 'Active product missing net_cost',
                'suggestion': 'Import analyzer data to set net_cost'
            })
        
        # === Check 8: Variant label consistency ===
        if variant_label:
            # Check if variant_label matches color/capacity
            vl_lower = variant_label.lower()
            if color and color.lower() not in vl_lower and color not in ['CMYK', 'CMY']:
                if 'black' not in vl_lower and color == 'Black':
                    pass  # Often implied
                elif color.lower() not in vl_lower:
                    info.append({
                        'type': 'VARIANT_LABEL_MISMATCH',
                        'severity': 'INFO',
                        'product_id': pid,
                        'part_number': part_number,
                        'color': color,
                        'variant_label': variant_label,
                        'message': f'Color "{color}" not found in variant_label "{variant_label}"',
                        'suggestion': 'Update variant_label to include color'
                    })
    
    # === Check for duplicate ASINs ===
    for asin, prods in asins_seen.items():
        if len(prods) > 1:
            issues.append({
                'type': 'DUPLICATE_ASIN',
                'severity': 'ERROR',
                'asin': asin,
                'count': len(prods),
                'products': [
                    {
                        'id': p['id'],
                        'part_number': p['part_number'],
                        'color': p['color'],
                        'capacity': p['capacity'],
                        'pack_size': p['pack_size'],
                        'variant_label': p['variant_label']
                    }
                    for p in prods
                ],
                'message': f'ASIN {asin} used by {len(prods)} different products',
                'suggestion': 'Each product should have a unique ASIN. Remove duplicates or correct ASINs.'
            })
    
    # === Check for same part number with multiple ASINs ===
    for pn, prods in part_numbers_seen.items():
        if len(prods) > 1:
            pack_sizes = set(p['pack_size'] for p in prods)
            
            if len(pack_sizes) == len(prods):
                # Different pack sizes - this is valid
                info.append({
                    'type': 'MULTI_PACK_VARIANTS',
                    'severity': 'OK',
                    'part_number': pn,
                    'count': len(prods),
                    'pack_sizes': sorted(pack_sizes),
                    'message': f'{pn} has {len(prods)} ASINs with different pack sizes: {sorted(pack_sizes)}',
                    'suggestion': 'This is expected for multi-pack variants'
                })
            else:
                # Same pack sizes - potential issue
                warnings.append({
                    'type': 'DUPLICATE_PART_NUMBER',
                    'severity': 'WARNING',
                    'part_number': pn,
                    'count': len(prods),
                    'products': [
                        {
                            'id': p['id'],
                            'asin': p['asin'],
                            'pack_size': p['pack_size'],
                            'variant_label': p['variant_label']
                        }
                        for p in prods
                    ],
                    'message': f'{pn} has {len(prods)} ASINs with same/overlapping pack sizes',
                    'suggestion': 'Review if these are truly different products or duplicates'
                })
    
    return {
        'issues': issues,
        'warnings': warnings,
        'info': info
    }


def generate_report(products, audit_results):
    """Generate the audit report document."""
    
    issues = audit_results['issues']
    warnings = audit_results['warnings']
    info = audit_results['info']
    
    report = []
    report.append("=" * 80)
    report.append("LEXMARK PRODUCTS AUDIT REPORT")
    report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append("=" * 80)
    report.append("")
    
    # Summary
    report.append("## SUMMARY")
    report.append("-" * 40)
    report.append(f"Total Lexmark Products: {len(products)}")
    report.append(f"Sellable Products: {len([p for p in products if p['sellable']])}")
    report.append(f"Blocked Products: {len([p for p in products if not p['sellable']])}")
    report.append("")
    report.append(f"🔴 ERRORS (must fix): {len(issues)}")
    report.append(f"🟡 WARNINGS (should review): {len(warnings)}")
    report.append(f"🔵 INFO (for reference): {len(info)}")
    report.append("")
    
    # Errors
    if issues:
        report.append("")
        report.append("=" * 80)
        report.append("🔴 ERRORS - MUST FIX")
        report.append("=" * 80)
        
        # Group by type
        issues_by_type = defaultdict(list)
        for i in issues:
            issues_by_type[i['type']].append(i)
        
        for issue_type, items in issues_by_type.items():
            report.append("")
            report.append(f"### {issue_type} ({len(items)} issues)")
            report.append("-" * 40)
            
            for item in items:
                report.append(f"")
                report.append(f"  Message: {item['message']}")
                
                if issue_type == 'DUPLICATE_ASIN':
                    report.append(f"  ASIN: {item['asin']}")
                    report.append(f"  Affected Products:")
                    for p in item['products']:
                        report.append(f"    - Part#: {p['part_number']} | Color: {p['color']} | "
                                     f"Capacity: {p['capacity']} | Pack: {p['pack_size']}")
                        report.append(f"      Label: {p['variant_label']}")
                else:
                    report.append(f"  Part#: {item.get('part_number', 'N/A')}")
                    report.append(f"  ASIN: {item.get('asin', 'N/A')}")
                
                report.append(f"  Suggestion: {item['suggestion']}")
    
    # Warnings
    if warnings:
        report.append("")
        report.append("=" * 80)
        report.append("🟡 WARNINGS - SHOULD REVIEW")
        report.append("=" * 80)
        
        warnings_by_type = defaultdict(list)
        for w in warnings:
            warnings_by_type[w['type']].append(w)
        
        for warning_type, items in warnings_by_type.items():
            report.append("")
            report.append(f"### {warning_type} ({len(items)} warnings)")
            report.append("-" * 40)
            
            for item in items[:20]:  # Limit to first 20 per type
                report.append(f"")
                report.append(f"  Message: {item['message']}")
                report.append(f"  Part#: {item.get('part_number', 'N/A')}")
                report.append(f"  ASIN: {item.get('asin', 'N/A')}")
                
                if 'products' in item:
                    report.append(f"  Affected Products:")
                    for p in item['products']:
                        report.append(f"    - {p['asin']} | Pack: {p['pack_size']} | {p['variant_label']}")
                
                report.append(f"  Suggestion: {item['suggestion']}")
            
            if len(items) > 20:
                report.append(f"  ... and {len(items) - 20} more")
    
    # Info (condensed)
    if info:
        report.append("")
        report.append("=" * 80)
        report.append("🔵 INFORMATIONAL NOTES")
        report.append("=" * 80)
        
        info_by_type = defaultdict(list)
        for i in info:
            info_by_type[i['type']].append(i)
        
        for info_type, items in info_by_type.items():
            ok_items = [i for i in items if i.get('severity') == 'OK']
            other_items = [i for i in items if i.get('severity') != 'OK']
            
            report.append("")
            report.append(f"### {info_type}")
            
            if ok_items:
                report.append(f"  ✅ {len(ok_items)} items OK (e.g., multi-pack variants)")
            
            if other_items:
                report.append(f"  ℹ️  {len(other_items)} items to review")
                for item in other_items[:5]:
                    report.append(f"    - {item['message']}")
                if len(other_items) > 5:
                    report.append(f"    ... and {len(other_items) - 5} more")
    
    # Suggested CSV Changes
    report.append("")
    report.append("=" * 80)
    report.append("SUGGESTED CHANGES FOR CSV IMPORT")
    report.append("=" * 80)
    report.append("")
    report.append("Based on the audit, here are the recommended actions:")
    report.append("")
    
    # Collect all actionable changes
    changes = []
    
    for issue in issues:
        if issue['type'] == 'DUPLICATE_ASIN':
            for i, p in enumerate(issue['products']):
                if i > 0:  # Keep first, suggest removal/change for others
                    changes.append({
                        'action': 'REMOVE or CHANGE ASIN',
                        'asin': issue['asin'],
                        'part_number': p['part_number'],
                        'reason': 'Duplicate ASIN - either remove this row or assign different ASIN',
                        'current': p
                    })
    
    for warning in warnings:
        if warning['type'] == 'COLOR_MISMATCH':
            changes.append({
                'action': 'VERIFY COLOR',
                'asin': warning.get('asin'),
                'part_number': warning.get('part_number'),
                'reason': warning['message'],
                'current_color': warning.get('stated_color'),
                'suggested_color': warning.get('expected_color')
            })
        elif warning['type'] == 'CAPACITY_MISMATCH':
            changes.append({
                'action': 'VERIFY CAPACITY',
                'asin': warning.get('asin'),
                'part_number': warning.get('part_number'),
                'reason': warning['message'],
                'current_capacity': warning.get('stated_capacity'),
                'suggested_capacity': warning.get('expected_capacity')
            })
    
    if changes:
        report.append(f"Total suggested changes: {len(changes)}")
        report.append("")
        
        for i, change in enumerate(changes, 1):
            report.append(f"{i}. [{change['action']}]")
            report.append(f"   Part#: {change.get('part_number')}")
            report.append(f"   ASIN: {change.get('asin')}")
            report.append(f"   Reason: {change['reason']}")
            if 'suggested_color' in change:
                report.append(f"   Current: {change['current_color']} → Suggested: {change['suggested_color']}")
            if 'suggested_capacity' in change:
                report.append(f"   Current: {change['current_capacity']} → Suggested: {change['suggested_capacity']}")
            report.append("")
    else:
        report.append("No critical changes required. Database appears to be in good shape!")
    
    return "\n".join(report)


def main():
    print("Fetching Lexmark products from database...")
    products = get_lexmark_products()
    print(f"Found {len(products)} Lexmark products")
    
    print("\nRunning audit checks...")
    audit_results = audit_products(products)
    
    print("\nGenerating report...")
    report = generate_report(products, audit_results)
    
    # Save report
    report_path = os.path.join(PROJECT_ROOT, "docs", "LEXMARK_AUDIT_REPORT.md")
    with open(report_path, 'w') as f:
        f.write(report)
    
    print(f"\n✅ Report saved to: {report_path}")
    print("\n" + "=" * 60)
    print(report)


if __name__ == "__main__":
    main()
