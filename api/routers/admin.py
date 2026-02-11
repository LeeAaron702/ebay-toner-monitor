"""
admin.py
FastAPI router for the Admin Panel.

Serves HTML pages for:
- Product Management (CRUD, bulk upload)
- Analytics Dashboard (order history, spending)

Uses Jinja2 templates + HTMX for interactivity.
Session cookie authentication for single admin user.
"""

from __future__ import annotations

import os
import csv
import io
import json
import secrets
from datetime import datetime, timedelta
from functools import wraps
from typing import Optional, List, Dict, Any

from fastapi import APIRouter, Request, Response, HTTPException, Depends, Query, Form, UploadFile, File
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired
from pydantic import BaseModel

from db.products_db import (
    list_products,
    count_products,
    get_product,
    create_product,
    update_product,
    delete_product,
    bulk_upsert_products,
    VALID_BRANDS,
    get_overhead_pct,
    set_overhead_pct,
    get_target_profit,
    set_target_profit,
    get_all_settings,
    DEFAULT_OVERHEAD_PCT,
    DEFAULT_TARGET_PROFIT,
)
from db.listings_db import get_db_connection
from db.exclusions_db import (
    list_sellers as db_list_sellers,
    list_canon_keywords as db_list_canon_keywords,
    list_xerox_keywords as db_list_xerox_keywords,
)

# ---- Configuration ----
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "admin")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "changeme")  # Set in .env!
SECRET_KEY = os.getenv("SECRET_KEY", secrets.token_hex(32))
SESSION_MAX_AGE = 86400  # 24 hours

# ---- Template Setup ----
# Go up from api/routers/ to project root, then into templates/
TEMPLATES_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "templates")
templates = Jinja2Templates(directory=TEMPLATES_DIR)

# ---- Session Serializer ----
serializer = URLSafeTimedSerializer(SECRET_KEY)

router = APIRouter()


# =============================================================================
# Authentication Helpers
# =============================================================================

def create_session_token(username: str) -> str:
    """Create a signed session token."""
    return serializer.dumps({"username": username})


def verify_session_token(token: str) -> Optional[Dict]:
    """Verify session token, return payload or None."""
    try:
        return serializer.loads(token, max_age=SESSION_MAX_AGE)
    except (BadSignature, SignatureExpired):
        return None


def get_current_user(request: Request) -> Optional[str]:
    """Get current user from session cookie."""
    token = request.cookies.get("admin_session")
    if not token:
        return None
    payload = verify_session_token(token)
    return payload.get("username") if payload else None


def require_auth(request: Request) -> str:
    """Dependency that requires authentication."""
    user = get_current_user(request)
    if not user:
        # For browser requests, redirect to login
        raise HTTPException(
            status_code=303,
            headers={"Location": "/admin/login"}
        )
    return user


# =============================================================================
# Auth Routes
# =============================================================================

@router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request, error: str = None):
    """Login page."""
    return templates.TemplateResponse("login.html", {
        "request": request,
        "error": error
    })


@router.post("/login")
async def login(request: Request, username: str = Form(...), password: str = Form(...)):
    """Process login form."""
    if username == ADMIN_USERNAME and password == ADMIN_PASSWORD:
        token = create_session_token(username)
        response = RedirectResponse(url="/admin/", status_code=303)
        response.set_cookie(
            key="admin_session",
            value=token,
            httponly=True,
            samesite="lax",
            max_age=SESSION_MAX_AGE
        )
        return response
    return RedirectResponse(url="/admin/login?error=Invalid+credentials", status_code=303)


@router.get("/logout")
async def logout():
    """Log out and clear session."""
    response = RedirectResponse(url="/admin/login", status_code=303)
    response.delete_cookie("admin_session")
    return response


# =============================================================================
# Dashboard Routes
# =============================================================================

@router.get("/", response_class=HTMLResponse)
async def dashboard(request: Request, user: str = Depends(require_auth)):
    """Main dashboard page."""
    # Get quick stats
    total_products = count_products()
    canon_count = count_products(brand="canon")
    xerox_count = count_products(brand="xerox")
    lexmark_count = count_products(brand="lexmark")
    
    # Get recent matches count (last 7 days)
    conn = get_db_connection()
    try:
        conn.row_factory = None
        cursor = conn.execute("""
            SELECT COUNT(*) FROM matches m
            JOIN messages msg ON m.message_id = msg.id
            WHERE msg.timestamp > ?
        """, (int((datetime.now() - timedelta(days=7)).timestamp()),))
        recent_matches = cursor.fetchone()[0]
        
        # Get recent orders count (last 7 days) 
        cursor = conn.execute("""
            SELECT COUNT(DISTINCT order_id) FROM order_history
            WHERE created_time >= ?
        """, ((datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d'),))
        recent_orders = cursor.fetchone()[0]
    finally:
        conn.close()
    
    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "user": user,
        "total_products": total_products,
        "canon_count": canon_count,
        "xerox_count": xerox_count,
        "lexmark_count": lexmark_count,
        "recent_matches": recent_matches,
        "recent_orders": recent_orders,
    })


# =============================================================================
# Exclusions Management Routes
# =============================================================================

@router.get("/exclusions", response_class=HTMLResponse)
async def exclusions_page(request: Request, user: str = Depends(require_auth)):
    """Exclusions management page."""
    sellers = db_list_sellers()
    canon_keywords = db_list_canon_keywords()
    xerox_keywords = db_list_xerox_keywords()
    
    return templates.TemplateResponse("exclusions.html", {
        "request": request,
        "user": user,
        "sellers": sellers,
        "canon_keywords": canon_keywords,
        "xerox_keywords": xerox_keywords,
    })


# =============================================================================
# Product Management Routes
# =============================================================================

@router.get("/products", response_class=HTMLResponse)
async def products_page(
    request: Request,
    user: str = Depends(require_auth),
    brand: str = None,
    search: str = None,
    page: int = 1,
    per_page: int = 50
):
    """Products list page."""
    offset = (page - 1) * per_page
    
    # Get products with filters (search now handled in SQL)
    products = list_products(brand=brand, search=search, limit=per_page, offset=offset)
    
    total = count_products(brand=brand, search=search)
    total_pages = (total + per_page - 1) // per_page
    
    # Check if HTMX request (partial render)
    if request.headers.get("HX-Request"):
        return templates.TemplateResponse("partials/products_table.html", {
            "request": request,
            "products": products,
            "page": page,
            "total_pages": total_pages,
            "brand": brand,
            "search": search,
        })
    
    return templates.TemplateResponse("products.html", {
        "request": request,
        "user": user,
        "products": products,
        "brands": list(VALID_BRANDS),
        "selected_brand": brand,
        "search": search,
        "page": page,
        "per_page": per_page,
        "total_pages": total_pages,
        "total": total,
    })


@router.get("/products/new", response_class=HTMLResponse)
async def new_product_form(request: Request, user: str = Depends(require_auth)):
    """New product form."""
    return templates.TemplateResponse("product_form.html", {
        "request": request,
        "user": user,
        "product": None,
        "brands": list(VALID_BRANDS),
        "is_new": True,
    })


@router.post("/products/new")
async def create_new_product(
    request: Request,
    user: str = Depends(require_auth),
    brand: str = Form(...),
    model: str = Form(None),
    part_number: str = Form(None),
    color: str = Form(None),
    capacity: str = Form(None),
    pack_size: int = Form(1),
    asin: str = Form(...),
    amazon_sku: str = Form(None),
    sellable: int = Form(1),
    variant_label: str = Form(None),
    notes: str = Form(None),
):
    """Create a new product. Note: net_cost and bsr are managed by analyzer only."""
    try:
        product_data = {
            "brand": brand,
            "model": model or None,
            "part_number": part_number or None,
            "color": color or None,
            "capacity": capacity or None,
            "pack_size": pack_size,
            "asin": asin,
            "amazon_sku": amazon_sku or None,
            "net_cost": None,  # Only set by analyzer
            "bsr": None,       # Only set by analyzer
            "sellable": bool(sellable),
            "variant_label": variant_label or None,
            "notes": notes or None,
        }
        product_id = create_product(product_data)
        return RedirectResponse(url=f"/admin/products?success=Product+created", status_code=303)
    except Exception as e:
        return templates.TemplateResponse("product_form.html", {
            "request": request,
            "user": user,
            "product": None,
            "brands": list(VALID_BRANDS),
            "is_new": True,
            "error": str(e),
        })


@router.get("/products/{product_id}/edit", response_class=HTMLResponse)
async def edit_product_form(request: Request, product_id: str, user: str = Depends(require_auth)):
    """Edit product form."""
    product = get_product(product_id)
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    
    return templates.TemplateResponse("product_form.html", {
        "request": request,
        "user": user,
        "product": product,
        "brands": list(VALID_BRANDS),
        "is_new": False,
    })


@router.post("/products/{product_id}/edit")
async def update_existing_product(
    request: Request,
    product_id: str,
    user: str = Depends(require_auth),
    model: str = Form(None),
    part_number: str = Form(None),
    color: str = Form(None),
    capacity: str = Form(None),
    pack_size: int = Form(1),
    asin: str = Form(None),
    amazon_sku: str = Form(None),
    sellable: int = Form(1),
    variant_label: str = Form(None),
    notes: str = Form(None),
):
    """Update existing product. Note: net_cost and bsr are managed by analyzer only."""
    try:
        updates = {
            "model": model or None,
            "part_number": part_number or None,
            "color": color or None,
            "capacity": capacity or None,
            "pack_size": pack_size,
            "asin": asin,
            "amazon_sku": amazon_sku or None,
            "sellable": bool(sellable),
            "variant_label": variant_label or None,
            "notes": notes or None,
            # Note: net_cost and bsr are NOT included - they're analyzer-only
        }
        success = update_product(product_id, updates)
        if success:
            return RedirectResponse(url="/admin/products?success=Product+updated", status_code=303)
        raise HTTPException(status_code=404, detail="Product not found")
    except Exception as e:
        product = get_product(product_id)
        return templates.TemplateResponse("product_form.html", {
            "request": request,
            "user": user,
            "product": product,
            "brands": list(VALID_BRANDS),
            "is_new": False,
            "error": str(e),
        })


@router.delete("/products/{product_id}")
async def delete_product_route(product_id: str, user: str = Depends(require_auth)):
    """Delete a product permanently."""
    success = delete_product(product_id)
    if success:
        return {"success": True}
    raise HTTPException(status_code=404, detail="Product not found")


@router.get("/products/template")
async def download_csv_template(user: str = Depends(require_auth)):
    """Download CSV template for bulk upload."""
    output = io.StringIO()
    writer = csv.writer(output)
    
    # Headers
    headers = ["brand", "model", "part_number", "color", "capacity", "pack_size", 
               "asin", "amazon_sku", "net_cost", "bsr", "variant_label", "sellable", "notes"]
    writer.writerow(headers)
    
    # Example rows
    writer.writerow(["canon", "054H", "", "Black", "High", "1", "B07XYZ1234", "SKU-001", "45.99", "5000", "054H Black High Yield", "1", ""])
    writer.writerow(["xerox", "", "006R01512", "Cyan", "Standard", "1", "B08ABC5678", "SKU-002", "89.99", "8000", "WorkCentre Cyan", "1", ""])
    writer.writerow(["lexmark", "20N1", "20N1H10", "Black", "High", "2", "B09DEF9012", "SKU-003", "129.99", "12000", "20N1 Black High 2-Pack", "0", "Low margin"])
    
    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=products_template.csv"}
    )


@router.get("/products/upload", response_class=HTMLResponse)
async def upload_page(request: Request, user: str = Depends(require_auth)):
    """Bulk upload page."""
    return templates.TemplateResponse("upload.html", {
        "request": request,
        "user": user,
    })


@router.post("/products/upload/preview")
async def preview_upload(
    request: Request,
    user: str = Depends(require_auth),
    file: UploadFile = File(...)
):
    """Preview CSV upload before importing."""
    content = await file.read()
    try:
        text = content.decode('utf-8')
    except UnicodeDecodeError:
        text = content.decode('latin-1')
    
    reader = csv.DictReader(io.StringIO(text))
    rows = []
    errors = []
    
    for i, row in enumerate(reader):
        row_data = dict(row)
        row_errors = []
        
        # Validate brand
        brand = row_data.get('brand', '').lower()
        if not brand or brand not in VALID_BRANDS:
            row_errors.append(f"Invalid brand: {brand}")
        
        # Validate ASIN
        asin = row_data.get('asin', '')
        if not asin or asin.upper() in ('N/A', 'NO AMZ LISTING', ''):
            row_errors.append("Missing or invalid ASIN")
        
        # Convert numeric fields
        try:
            if row_data.get('pack_size'):
                row_data['pack_size'] = int(row_data['pack_size'])
        except ValueError:
            row_errors.append("Invalid pack_size")
        
        try:
            if row_data.get('net_cost'):
                row_data['net_cost'] = float(row_data['net_cost'])
        except ValueError:
            row_errors.append("Invalid net_cost")
        
        try:
            if row_data.get('bsr'):
                row_data['bsr'] = int(row_data['bsr'])
        except ValueError:
            row_errors.append("Invalid bsr")
        
        # Convert sellable to int (accepts 0/1, true/false, yes/no)
        try:
            sellable_val = row_data.get('sellable', '1').strip().lower()
            if sellable_val in ('0', 'false', 'no', 'n'):
                row_data['sellable'] = 0
            else:
                row_data['sellable'] = 1
        except (ValueError, AttributeError):
            row_data['sellable'] = 1
        
        row_data['_row_num'] = i + 2  # +2 for header row and 0-index
        row_data['_errors'] = row_errors
        rows.append(row_data)
        
        if row_errors:
            errors.append({'row': i + 2, 'errors': row_errors})
    
    return templates.TemplateResponse("partials/upload_preview.html", {
        "request": request,
        "rows": rows,
        "errors": errors,
        "total": len(rows),
        "error_count": len(errors),
    })


@router.post("/products/upload/confirm")
async def confirm_upload(
    request: Request,
    user: str = Depends(require_auth),
    file: UploadFile = File(...)
):
    """Execute the bulk upload."""
    content = await file.read()
    try:
        text = content.decode('utf-8')
    except UnicodeDecodeError:
        text = content.decode('latin-1')
    
    reader = csv.DictReader(io.StringIO(text))
    products = []
    
    for row in reader:
        # Parse sellable (accepts 0/1, true/false, yes/no)
        sellable_val = row.get('sellable', '1').strip().lower() if row.get('sellable') else '1'
        sellable = sellable_val not in ('0', 'false', 'no', 'n')
        
        product = {
            'brand': row.get('brand', '').lower(),
            'model': row.get('model') or None,
            'part_number': row.get('part_number') or None,
            'color': row.get('color') or None,
            'capacity': row.get('capacity') or None,
            'pack_size': int(row.get('pack_size', 1)) if row.get('pack_size') else 1,
            'asin': row.get('asin'),
            'amazon_sku': row.get('amazon_sku') or None,
            'net_cost': float(row.get('net_cost')) if row.get('net_cost') else None,
            'bsr': int(row.get('bsr')) if row.get('bsr') else None,
            'variant_label': row.get('variant_label') or None,
            'sellable': sellable,
            'notes': row.get('notes') or None,
        }
        products.append(product)
    
    stats = bulk_upsert_products(products)
    
    return templates.TemplateResponse("partials/upload_result.html", {
        "request": request,
        "stats": stats,
    })


# =============================================================================
# Analytics Routes
# =============================================================================

@router.get("/analytics", response_class=HTMLResponse)
async def analytics_page(request: Request, user: str = Depends(require_auth)):
    """Analytics dashboard page."""
    return templates.TemplateResponse("analytics.html", {
        "request": request,
        "user": user,
    })


@router.get("/analytics/orders", response_class=HTMLResponse)
async def orders_page(
    request: Request,
    user: str = Depends(require_auth),
    start_date: str = None,
    end_date: str = None,
    account: str = None,
    page: int = 1,
    per_page: int = 50
):
    """Order history and spending metrics page."""
    if not start_date:
        start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    if not end_date:
        end_date = datetime.now().strftime('%Y-%m-%d')
    
    offset = (page - 1) * per_page
    
    conn = get_db_connection()
    conn.row_factory = lambda c, r: dict(zip([col[0] for col in c.description], r))
    try:
        # Get list of accounts for filter dropdown
        cursor = conn.execute("SELECT DISTINCT account_label FROM order_history WHERE account_label IS NOT NULL AND account_label != '' ORDER BY account_label")
        accounts = [row['account_label'] for row in cursor.fetchall()]
        
        # Build account filter clause
        account_filter = ""
        params_base = [start_date, end_date + ' 23:59:59']
        if account and account != 'all':
            account_filter = " AND account_label = ?"
            params_base = [start_date, end_date + ' 23:59:59', account]
        
        # Orders list with account filter
        if account and account != 'all':
            cursor = conn.execute("""
                SELECT order_id, transaction_id, created_time, item_title, quantity_purchased, 
                       transaction_price, seller_user_id, order_total, account_label
                FROM order_history
                WHERE created_time >= ? AND created_time <= ? AND account_label = ?
                ORDER BY created_time DESC
                LIMIT ? OFFSET ?
            """, (start_date, end_date + ' 23:59:59', account, per_page, offset))
        else:
            cursor = conn.execute("""
                SELECT order_id, transaction_id, created_time, item_title, quantity_purchased, 
                       transaction_price, seller_user_id, order_total, account_label
                FROM order_history
                WHERE created_time >= ? AND created_time <= ?
                ORDER BY created_time DESC
                LIMIT ? OFFSET ?
            """, (start_date, end_date + ' 23:59:59', per_page, offset))
        orders = cursor.fetchall()
        
        # Get total count for pagination
        if account and account != 'all':
            cursor = conn.execute("""
                SELECT COUNT(*) FROM order_history
                WHERE created_time >= ? AND created_time <= ? AND account_label = ?
            """, (start_date, end_date + ' 23:59:59', account))
        else:
            cursor = conn.execute("""
                SELECT COUNT(*) FROM order_history
                WHERE created_time >= ? AND created_time <= ?
            """, (start_date, end_date + ' 23:59:59'))
        total = cursor.fetchone()['COUNT(*)']
        
        # Total spent and order count (deduplicated by order_id)
        cursor = conn.execute(f"""
            SELECT 
                COUNT(DISTINCT order_id) as order_count,
                SUM(CAST(order_total AS REAL)) as total_spent
            FROM (
                SELECT order_id, order_total,
                       ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY transaction_id) as rn
                FROM order_history
                WHERE created_time >= ? AND created_time <= ?{account_filter}
            )
            WHERE rn = 1
        """, params_base)
        totals = cursor.fetchone()
        
        total_spent = totals['total_spent'] or 0
        order_count = totals['order_count'] or 0
        avg_order_value = total_spent / order_count if order_count > 0 else 0
        
        # Daily spending for chart
        cursor = conn.execute(f"""
            SELECT 
                DATE(REPLACE(created_time, ' PST', '')) as date,
                SUM(CAST(order_total AS REAL)) as daily_spent
            FROM (
                SELECT order_id, order_total, created_time,
                       ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY transaction_id) as rn
                FROM order_history
                WHERE created_time >= ? AND created_time <= ?{account_filter}
            )
            WHERE rn = 1
            GROUP BY DATE(REPLACE(created_time, ' PST', ''))
            ORDER BY date
        """, params_base)
        daily_spending = cursor.fetchall()
        
        # Spending by brand (based on item_title keywords) - deduplicated by order
        cursor = conn.execute(f"""
            SELECT 
                CASE 
                    WHEN LOWER(item_title) LIKE '%canon%' THEN 'Canon'
                    WHEN LOWER(item_title) LIKE '%xerox%' THEN 'Xerox'
                    WHEN LOWER(item_title) LIKE '%lexmark%' THEN 'Lexmark'
                    ELSE 'Other'
                END as brand,
                SUM(CAST(order_total AS REAL)) as spent
            FROM (
                SELECT order_id, order_total, item_title,
                       ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY transaction_id) as rn
                FROM order_history
                WHERE created_time >= ? AND created_time <= ?{account_filter}
            )
            WHERE rn = 1
            GROUP BY brand
            ORDER BY spent DESC
        """, params_base)
        brand_spending = cursor.fetchall()
        
        # Account comparison (only when viewing all accounts)
        account_comparison = []
        if not account or account == 'all':
            cursor = conn.execute("""
                SELECT 
                    account_label as account,
                    COUNT(DISTINCT order_id) as order_count,
                    SUM(CAST(order_total AS REAL)) as total_spent
                FROM (
                    SELECT account_label, order_id, order_total,
                           ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY transaction_id) as rn
                    FROM order_history
                    WHERE created_time >= ? AND created_time <= ?
                      AND account_label IS NOT NULL AND account_label != ''
                )
                WHERE rn = 1
                GROUP BY account_label
                ORDER BY total_spent DESC
            """, (start_date, end_date + ' 23:59:59'))
            rows = cursor.fetchall()
            for row in rows:
                account_comparison.append({
                    'account': row['account'],
                    'order_count': row['order_count'],
                    'total_spent': row['total_spent'] or 0,
                    'avg_order': (row['total_spent'] or 0) / row['order_count'] if row['order_count'] > 0 else 0
                })
    finally:
        conn.close()
    
    total_pages = (total + per_page - 1) // per_page
    
    if request.headers.get("HX-Request"):
        return templates.TemplateResponse("partials/orders_table.html", {
            "request": request,
            "orders": orders,
            "page": page,
            "total_pages": total_pages,
        })
    
    return templates.TemplateResponse("orders.html", {
        "request": request,
        "user": user,
        "orders": orders,
        "start_date": start_date,
        "end_date": end_date,
        "account": account or "all",
        "accounts": accounts,
        "page": page,
        "total_pages": total_pages,
        "total": total,
        "total_spent": total_spent,
        "order_count": order_count,
        "avg_order_value": avg_order_value,
        "daily_spending": daily_spending,
        "brand_spending": brand_spending,
        "account_comparison": account_comparison,
    })


@router.get("/analytics/orders/export")
async def export_orders(
    user: str = Depends(require_auth),
    start_date: str = None,
    end_date: str = None
):
    """Export orders to CSV."""
    if not start_date:
        start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    if not end_date:
        end_date = datetime.now().strftime('%Y-%m-%d')
    
    conn = get_db_connection()
    conn.row_factory = lambda c, r: dict(zip([col[0] for col in c.description], r))
    try:
        cursor = conn.execute("""
            SELECT * FROM order_history
            WHERE created_time >= ? AND created_time <= ?
            ORDER BY created_time DESC
        """, (start_date, end_date + ' 23:59:59'))
        orders = cursor.fetchall()
    finally:
        conn.close()
    
    output = io.StringIO()
    if orders:
        writer = csv.DictWriter(output, fieldnames=orders[0].keys())
        writer.writeheader()
        writer.writerows(orders)
    
    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=orders_{start_date}_to_{end_date}.csv"}
    )


@router.get("/analytics/orders-by-item", response_class=HTMLResponse)
async def orders_by_item_page(
    request: Request,
    user: str = Depends(require_auth),
    start_date: str = None,
    end_date: str = None
):
    """Orders grouped by item."""
    if not start_date:
        start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    if not end_date:
        end_date = datetime.now().strftime('%Y-%m-%d')
    
    conn = get_db_connection()
    conn.row_factory = lambda c, r: dict(zip([col[0] for col in c.description], r))
    try:
        cursor = conn.execute("""
            SELECT 
                item_title,
                match1_asin as asin,
                COUNT(*) as times_purchased,
                SUM(CAST(quantity_purchased AS INTEGER)) as total_units,
                AVG(CAST(transaction_price AS REAL)) as avg_price,
                SUM(CAST(transaction_price AS REAL)) as total_spent
            FROM order_history
            WHERE created_time >= ? AND created_time <= ?
            GROUP BY item_title
            ORDER BY total_spent DESC
        """, (start_date, end_date + ' 23:59:59'))
        items = cursor.fetchall()
    finally:
        conn.close()
    
    return templates.TemplateResponse("orders_by_item.html", {
        "request": request,
        "user": user,
        "items": items,
        "start_date": start_date,
        "end_date": end_date,
    })


@router.get("/analytics/orders-by-item/{item_title}", response_class=HTMLResponse)
async def orders_for_item(
    request: Request,
    item_title: str,
    user: str = Depends(require_auth),
    start_date: str = None,
    end_date: str = None
):
    """Individual orders for a specific item."""
    if not start_date:
        start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    if not end_date:
        end_date = datetime.now().strftime('%Y-%m-%d')
    
    from urllib.parse import unquote
    item_title = unquote(item_title)
    
    conn = get_db_connection()
    conn.row_factory = lambda c, r: dict(zip([col[0] for col in c.description], r))
    try:
        cursor = conn.execute("""
            SELECT order_id, created_time, quantity_purchased, transaction_price, 
                   seller_user_id, order_total
            FROM order_history
            WHERE item_title = ? AND created_time >= ? AND created_time <= ?
            ORDER BY created_time DESC
        """, (item_title, start_date, end_date + ' 23:59:59'))
        orders = cursor.fetchall()
    finally:
        conn.close()
    
    return templates.TemplateResponse("partials/item_orders.html", {
        "request": request,
        "item_title": item_title,
        "orders": orders,
    })


@router.get("/api/order-matches/{order_id}/{transaction_id}", response_class=HTMLResponse)
async def get_order_matches(
    request: Request,
    order_id: str,
    transaction_id: str,
    user: str = Depends(require_auth)
):
    """Get matched products for an order (HTMX endpoint)."""
    from urllib.parse import unquote
    order_id = unquote(order_id)
    transaction_id = unquote(transaction_id)
    
    conn = get_db_connection()
    conn.row_factory = lambda c, r: dict(zip([col[0] for col in c.description], r))
    try:
        # Get overhead setting
        cursor = conn.execute("SELECT value FROM settings WHERE key = 'overhead_pct'")
        row = cursor.fetchone()
        overhead_pct = float(row['value']) if row else 15.0
        
        # Get purchased_units breakdown with product details
        # Each match_index represents one color, so we want all of them
        cursor = conn.execute("""
            SELECT 
                pu.model, pu.capacity, pu.color, pu.quantity, pu.unit_cost, 
                pu.asin, pu.net_per_unit,
                p.amazon_price, p.net_cost as product_net_cost, p.pack_size as product_pack_size
            FROM purchased_units pu
            LEFT JOIN products p ON pu.asin = p.asin
            WHERE pu.order_id = ? AND pu.transaction_id = ?
            ORDER BY pu.color
        """, (order_id, transaction_id))
        raw_units = cursor.fetchall()
        
        # Calculate values for each unit
        units = []
        for unit in raw_units:
            amazon_price = unit.get('amazon_price') or 0
            product_net_cost = unit.get('product_net_cost') or 0
            product_pack_size = unit.get('product_pack_size') or 1
            if product_pack_size < 1:
                product_pack_size = 1
            
            # CRITICAL: Divide product values by pack_size to get per-unit values
            # A 2-pack ASIN with $306 total net cost = $153/unit
            amazon_price_per_unit = amazon_price / product_pack_size
            seller_proceeds_per_unit = product_net_cost / product_pack_size if product_net_cost else (unit.get('net_per_unit') or 0)
            
            overhead_amount = amazon_price_per_unit * (overhead_pct / 100) if amazon_price_per_unit else 0
            net_after_overhead = seller_proceeds_per_unit - overhead_amount if seller_proceeds_per_unit else 0
            unit_cost = unit.get('unit_cost') or 0
            profit = net_after_overhead - unit_cost if net_after_overhead and unit_cost else 0
            
            units.append({
                'model': unit.get('model'),
                'capacity': unit.get('capacity'),
                'color': unit.get('color'),
                'quantity': unit.get('quantity') or 1,
                'asin': unit.get('asin'),
                'unit_cost': unit_cost,
                'amazon_price': amazon_price_per_unit,
                'seller_proceeds': seller_proceeds_per_unit,
                'overhead_pct': overhead_pct,
                'net_after_overhead': net_after_overhead,
                'profit': profit,
            })
    finally:
        conn.close()
    
    return templates.TemplateResponse("partials/order_matches.html", {
        "request": request,
        "units": units,
        "overhead_pct": overhead_pct,
        "has_data": bool(units),
    })


# =============================================================================
# API Endpoints (for HTMX)
# =============================================================================

@router.get("/api/spending-chart-data")
async def spending_chart_data(
    user: str = Depends(require_auth),
    start_date: str = None,
    end_date: str = None,
    interval: str = "daily",
    account: str = None
):
    """Get spending data for Chart.js."""
    if not start_date:
        start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    if not end_date:
        end_date = datetime.now().strftime('%Y-%m-%d')
    
    # Build account filter
    account_filter = ""
    params = [start_date, end_date + ' 23:59:59']
    if account and account != 'all':
        account_filter = " AND account_label = ?"
        params.append(account)
    
    conn = get_db_connection()
    conn.row_factory = lambda c, r: dict(zip([col[0] for col in c.description], r))
    try:
        if interval == "weekly":
            cursor = conn.execute(f"""
                SELECT 
                    strftime('%Y-W%W', REPLACE(created_time, ' PST', '')) as period,
                    SUM(CAST(order_total AS REAL)) as total
                FROM (
                    SELECT order_id, order_total, created_time,
                           ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY transaction_id) as rn
                    FROM order_history
                    WHERE created_time >= ? AND created_time <= ?{account_filter}
                )
                WHERE rn = 1
                GROUP BY period
                ORDER BY period
            """, params)
        else:
            cursor = conn.execute(f"""
                SELECT 
                    DATE(REPLACE(created_time, ' PST', '')) as period,
                    SUM(CAST(order_total AS REAL)) as total
                FROM (
                    SELECT order_id, order_total, created_time,
                           ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY transaction_id) as rn
                    FROM order_history
                    WHERE created_time >= ? AND created_time <= ?{account_filter}
                )
                WHERE rn = 1
                GROUP BY period
                ORDER BY period
            """, (start_date, end_date + ' 23:59:59'))
        
        data = cursor.fetchall()
    finally:
        conn.close()
    
    return {
        "labels": [d['period'] for d in data],
        "values": [d['total'] or 0 for d in data]
    }


@router.get("/api/profit-chart-data")
async def profit_chart_data(
    user: str = Depends(require_auth),
    start_date: str = None,
    end_date: str = None
):
    """Get profit data for Chart.js."""
    if not start_date:
        start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    if not end_date:
        end_date = datetime.now().strftime('%Y-%m-%d')
    
    start_ts = int(datetime.strptime(start_date, '%Y-%m-%d').timestamp())
    end_ts = int(datetime.strptime(end_date, '%Y-%m-%d').timestamp()) + 86400
    
    conn = get_db_connection()
    conn.row_factory = lambda c, r: dict(zip([col[0] for col in c.description], r))
    try:
        cursor = conn.execute("""
            SELECT 
                DATE(datetime(msg.timestamp, 'unixepoch', 'localtime')) as period,
                SUM(m.profit) as total
            FROM matches m
            JOIN messages msg ON m.message_id = msg.id
            WHERE msg.timestamp >= ? AND msg.timestamp <= ?
            GROUP BY period
            ORDER BY period
        """, (start_ts, end_ts))
        data = cursor.fetchall()
    finally:
        conn.close()
    
    return {
        "labels": [d['period'] for d in data],
        "values": [d['total'] or 0 for d in data]
    }


# =============================================================================
# Settings Routes
# =============================================================================

@router.get("/settings", response_class=HTMLResponse)
async def settings_page(request: Request, user: str = Depends(require_auth)):
    """Settings management page."""
    overhead_pct = get_overhead_pct()
    target_profit = get_target_profit()
    
    return templates.TemplateResponse("settings.html", {
        "request": request,
        "user": user,
        "overhead_pct": overhead_pct,
        "default_overhead_pct": DEFAULT_OVERHEAD_PCT,
        "target_profit": target_profit,
        "default_target_profit": DEFAULT_TARGET_PROFIT,
    })


@router.post("/settings/overhead")
async def update_overhead_setting(
    request: Request,
    user: str = Depends(require_auth),
    overhead_pct: float = Form(...)
):
    """Update overhead percentage setting."""
    # Validate range
    if overhead_pct < 0 or overhead_pct > 100:
        # For HTMX requests, return error message
        if request.headers.get("HX-Request"):
            return HTMLResponse(
                content='<div class="alert alert-error">Overhead must be between 0 and 100%</div>',
                status_code=400
            )
        return RedirectResponse(url="/admin/settings?error=Invalid+value", status_code=303)
    
    success = set_overhead_pct(overhead_pct)
    
    if request.headers.get("HX-Request"):
        if success:
            return HTMLResponse(
                content=f'<div class="alert alert-success">Overhead updated to {overhead_pct}%</div>'
            )
        return HTMLResponse(
            content='<div class="alert alert-error">Failed to save setting</div>',
            status_code=500
        )
    
    return RedirectResponse(url="/admin/settings", status_code=303)


@router.post("/settings/target-profit")
async def update_target_profit_setting(
    request: Request,
    user: str = Depends(require_auth),
    target_profit: float = Form(...)
):
    """Update target profit threshold setting."""
    # Validate range
    if target_profit < 0 or target_profit > 500:
        if request.headers.get("HX-Request"):
            return HTMLResponse(
                content='<div class="alert alert-error">Target profit must be between $0 and $500</div>',
                status_code=400
            )
        return RedirectResponse(url="/admin/settings?error=Invalid+value", status_code=303)
    
    success = set_target_profit(target_profit)
    
    if request.headers.get("HX-Request"):
        if success:
            return HTMLResponse(
                content=f'<div class="alert alert-success">Target profit updated to ${target_profit:.0f}</div>'
            )
        return HTMLResponse(
            content='<div class="alert alert-error">Failed to save setting</div>',
            status_code=500
        )
    
    return RedirectResponse(url="/admin/settings", status_code=303)


@router.get("/api/settings")
async def get_settings_api(user: str = Depends(require_auth)):
    """API endpoint to get all settings."""
    return {
        "overhead_pct": get_overhead_pct(),
        "target_profit": get_target_profit(),
        "all_settings": get_all_settings()
    }
