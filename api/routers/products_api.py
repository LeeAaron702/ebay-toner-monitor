"""
products_api.py
FastAPI router for products CRUD, bulk import, metrics import, and CSV template.

Mount this router in your FastAPI app:
    app.include_router(router, prefix="/api/v1/products", tags=["Products"])

Endpoints:
    CRUD:
        GET    /                     List products (paginated, filterable)
        GET    /{id}                 Get single product
        POST   /                     Create single product
        PUT    /{id}                 Update single product
        DELETE /{id}                 Soft delete product

    Bulk Operations:
        GET    /template             Download CSV template
        POST   /import               Bulk upsert from CSV upload
        POST   /metrics/import       Bulk update metrics from CSV

    Printable:
        GET    /groups               List all group_keys
        GET    /groups/{group_key}   Get products in a group/block
"""

from __future__ import annotations

import csv
import io
from typing import List, Optional

from fastapi import APIRouter, File, HTTPException, Query, UploadFile, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from db.products_db import (
    init_products_db,
    create_product,
    get_product,
    update_product,
    delete_product,
    list_products,
    count_products,
    bulk_upsert_products,
    bulk_update_metrics,
    get_all_group_keys,
    get_products_by_group_key,
    VALID_BRANDS,
)

# Initialize products table on import
init_products_db()

router = APIRouter()


# =============================================================================
# Pydantic Models
# =============================================================================

class ProductCreate(BaseModel):
    """Create a new product."""
    brand: str = Field(..., description="Brand: canon, xerox, or lexmark")
    model: Optional[str] = Field(None, description="Model family (Canon: '054H', Lexmark: '20N1')")
    capacity: Optional[str] = Field(None, description="Standard, High, Extra High, etc.")
    part_number: Optional[str] = Field(None, description="Part number for matching")
    variant_label: Optional[str] = Field(None, description="Display label: '4 Color High', 'Cyan Extra High Yield RP'")
    color: Optional[str] = Field(None, description="Black, Cyan, Magenta, Yellow, Color")
    pack_size: int = Field(1, description="Number of units in pack")
    asin: str = Field(..., description="Amazon ASIN (required)")
    amazon_sku: Optional[str] = Field(None, description="Amazon SKU")
    net_cost: Optional[float] = Field(None, description="Net cost after Amazon fees")
    bsr: Optional[int] = Field(None, description="Best Seller Rank")
    sellable: bool = Field(True, description="Is sellable on Amazon")
    source_tab: Optional[str] = Field(None, description="Original sheet tab name")
    is_model_block: Optional[bool] = Field(None, description="Xerox only: True for model blocks")


class ProductUpdate(BaseModel):
    """Update an existing product (all fields optional)."""
    model: Optional[str] = None
    capacity: Optional[str] = None
    part_number: Optional[str] = None
    variant_label: Optional[str] = None
    color: Optional[str] = None
    pack_size: Optional[int] = None
    asin: Optional[str] = None
    amazon_sku: Optional[str] = None
    net_cost: Optional[float] = None
    bsr: Optional[int] = None
    sellable: Optional[bool] = None
    is_model_block: Optional[bool] = None


class ProductOut(BaseModel):
    """Product response."""
    id: str
    brand: str
    model: Optional[str]
    capacity: Optional[str]
    group_key: Optional[str]
    part_number: Optional[str]
    variant_label: Optional[str]
    color: Optional[str]
    pack_size: int
    asin: Optional[str]
    amazon_sku: Optional[str]
    net_cost: Optional[float]
    bsr: Optional[int]
    sellable: bool
    source_tab: Optional[str]
    is_model_block: Optional[bool]
    created_at: Optional[str]
    updated_at: Optional[str]

    class Config:
        from_attributes = True


class ProductListResponse(BaseModel):
    """Paginated list response."""
    total: int
    limit: int
    offset: int
    items: List[ProductOut]


class BulkImportResult(BaseModel):
    """Result of bulk import operation."""
    total: int
    created: int
    updated: int
    skipped: int
    errors: List[dict]


class MetricsImportResult(BaseModel):
    """Result of metrics import operation."""
    total: int
    updated: int
    not_found: int
    errors: List[dict]


class MessageOut(BaseModel):
    """Simple message response."""
    message: str


# =============================================================================
# Helper Functions
# =============================================================================

def _db_to_response(product: dict) -> dict:
    """Convert DB row to response format."""
    if not product:
        return None
    return {
        **product,
        'sellable': bool(product.get('sellable')),
        'is_model_block': bool(product.get('is_model_block')) if product.get('is_model_block') is not None else None,
    }


def _parse_csv_row(row: dict) -> dict:
    """Parse a CSV row into a product dict."""
    product = {}
    
    # String fields
    for field in ['brand', 'model', 'capacity', 'part_number', 'variant_label', 
                  'color', 'asin', 'amazon_sku', 'source_tab']:
        if field in row and row[field]:
            product[field] = row[field].strip()
    
    # Integer fields
    if 'pack_size' in row and row['pack_size']:
        try:
            product['pack_size'] = int(row['pack_size'])
        except ValueError:
            product['pack_size'] = 1
    
    if 'bsr' in row and row['bsr']:
        try:
            # Handle commas in BSR
            bsr_str = row['bsr'].replace(',', '').strip()
            if bsr_str and bsr_str not in ('#N/A', 'N/A', '0'):
                product['bsr'] = int(bsr_str)
        except ValueError:
            pass
    
    # Float fields
    if 'net_cost' in row and row['net_cost']:
        try:
            cost_str = row['net_cost'].replace('$', '').replace(',', '').strip()
            if cost_str and cost_str not in ('#N/A', 'N/A'):
                product['net_cost'] = float(cost_str)
        except ValueError:
            pass
    
    # Boolean fields
    if 'sellable' in row and row['sellable']:
        val = row['sellable'].lower().strip()
        product['sellable'] = val in ('true', '1', 'yes', 'sellable')
    
    if 'is_model_block' in row and row['is_model_block']:
        val = row['is_model_block'].lower().strip()
        product['is_model_block'] = val in ('true', '1', 'yes')
    
    return product


# =============================================================================
# CRUD Endpoints
# =============================================================================

@router.get("", response_model=ProductListResponse)
async def get_products(
    brand: Optional[str] = Query(None, description="Filter by brand"),
    model: Optional[str] = Query(None, description="Filter by model"),
    group_key: Optional[str] = Query(None, description="Filter by group_key"),
    limit: int = Query(100, ge=1, le=1000, description="Max results"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
):
    """List products with optional filters."""
    if brand and brand.lower() not in VALID_BRANDS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid brand. Must be one of: {', '.join(VALID_BRANDS)}"
        )
    
    products = list_products(
        brand=brand,
        model=model,
        group_key=group_key,
        limit=limit,
        offset=offset,
    )
    total = count_products(brand=brand)
    
    return ProductListResponse(
        total=total,
        limit=limit,
        offset=offset,
        items=[_db_to_response(p) for p in products],
    )


# Static routes before dynamic /{product_id} routes - defined later in file

@router.post("", response_model=ProductOut, status_code=status.HTTP_201_CREATED)
async def create_new_product(product: ProductCreate):
    """Create a new product."""
    if product.brand.lower() not in VALID_BRANDS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid brand. Must be one of: {', '.join(VALID_BRANDS)}"
        )
    
    try:
        product_id = create_product(product.model_dump())
        created = get_product(product_id)
        return _db_to_response(created)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        if "UNIQUE constraint failed" in str(e):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Product with this ASIN already exists for this brand"
            )
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


# NOTE: Dynamic /{product_id} routes (GET, PUT, DELETE) are defined at the end of file
# to avoid matching before static routes like /template, /groups, /import


# =============================================================================
# Bulk Operations
# =============================================================================

CSV_TEMPLATE_HEADER = [
    'brand', 'model', 'capacity', 'part_number', 'variant_label', 'color',
    'pack_size', 'asin', 'amazon_sku', 'net_cost', 'bsr', 'sellable', 'is_model_block'
]

CSV_TEMPLATE_EXAMPLE = [
    'canon', '054H', 'High', '054H', '4 Color High', 'Color', '4',
    'B07WSH9ZWF', '1BJV-8Y41-5AE2', '352.00', '181110', 'true', ''
]


@router.get("/template", response_class=StreamingResponse)
async def download_csv_template():
    """Download CSV template for bulk import."""
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(CSV_TEMPLATE_HEADER)
    writer.writerow(CSV_TEMPLATE_EXAMPLE)
    
    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=products_template.csv"}
    )


@router.get("/export/asins", response_class=StreamingResponse)
async def export_asins_csv(
    brand: Optional[str] = Query(None, description="Filter by brand (optional)"),
):
    """
    Export all ASINs to CSV for analyzer.tools upload.
    
    The CSV contains just an 'asin' column which is what analyzer.tools expects.
    Optionally filter by brand.
    """
    # Get all products
    products = list_products(
        brand=brand.lower() if brand else None,
        limit=10000,  # High limit to get all
        offset=0,
    )
    
    # Extract unique ASINs
    asins = sorted(set(p['asin'] for p in products if p.get('asin')))
    
    if not asins:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No products with ASINs found"
        )
    
    # Generate CSV
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['asin'])  # Header
    for asin in asins:
        writer.writerow([asin])
    
    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=asins_for_analyzer.csv"}
    )


@router.post("/import", response_model=BulkImportResult)
async def bulk_import_csv(file: UploadFile = File(...)):
    """
    Bulk import/upsert products from CSV upload.
    
    Uses (brand, asin) as unique key:
    - If exists: updates all fields
    - If not: creates new product
    """
    if not file.filename.endswith('.csv'):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="File must be a CSV"
        )
    
    try:
        contents = await file.read()
        decoded = contents.decode('utf-8')
        reader = csv.DictReader(io.StringIO(decoded))
        
        products = []
        for row in reader:
            parsed = _parse_csv_row(row)
            if parsed.get('brand') and parsed.get('asin'):
                products.append(parsed)
        
        if not products:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No valid products found in CSV"
            )
        
        result = bulk_upsert_products(products)
        return BulkImportResult(**result)
    
    except UnicodeDecodeError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="File must be UTF-8 encoded"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Import failed: {str(e)}"
        )


@router.post("/metrics/import", response_model=MetricsImportResult)
async def bulk_import_metrics(file: UploadFile = File(...)):
    """
    Bulk update metrics (BSR, net_cost, sellable) from analyzer CSV.
    
    CSV should have columns: asin, net_cost, bsr, sellable
    Matches by ASIN across all brands.
    """
    if not file.filename.endswith('.csv'):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="File must be a CSV"
        )
    
    try:
        contents = await file.read()
        decoded = contents.decode('utf-8')
        reader = csv.DictReader(io.StringIO(decoded))
        
        metrics = []
        for row in reader:
            asin = row.get('asin', '').strip()
            if not asin:
                continue
            
            metric = {'asin': asin}
            
            if 'net_cost' in row and row['net_cost']:
                try:
                    cost_str = row['net_cost'].replace('$', '').replace(',', '').strip()
                    if cost_str and cost_str not in ('#N/A', 'N/A'):
                        metric['net_cost'] = float(cost_str)
                except ValueError:
                    pass
            
            if 'bsr' in row and row['bsr']:
                try:
                    bsr_str = row['bsr'].replace(',', '').strip()
                    if bsr_str and bsr_str not in ('#N/A', 'N/A', '0'):
                        metric['bsr'] = int(bsr_str)
                except ValueError:
                    pass
            
            if 'sellable' in row and row['sellable']:
                val = row['sellable'].lower().strip()
                metric['sellable'] = val in ('true', '1', 'yes', 'sellable')
            
            metrics.append(metric)
        
        if not metrics:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No valid metrics found in CSV"
            )
        
        result = bulk_update_metrics(metrics)
        return MetricsImportResult(**result)
    
    except UnicodeDecodeError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="File must be UTF-8 encoded"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Import failed: {str(e)}"
        )


# =============================================================================
# Grouping / Printable Endpoints
# =============================================================================

@router.get("/groups", response_model=List[str])
async def list_group_keys(
    brand: Optional[str] = Query(None, description="Filter by brand")
):
    """List all group_keys for printable blocks."""
    if brand and brand.lower() not in VALID_BRANDS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid brand. Must be one of: {', '.join(VALID_BRANDS)}"
        )
    
    return get_all_group_keys(brand=brand)


@router.get("/groups/{group_key:path}", response_model=List[ProductOut])
async def get_group_products(group_key: str):
    """Get all products in a specific block/group."""
    products = get_products_by_group_key(group_key)
    if not products:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No products found for this group_key"
        )
    return [_db_to_response(p) for p in products]


# =============================================================================
# Dynamic /{product_id} Routes - MUST be defined LAST
# =============================================================================

@router.get("/{product_id}", response_model=ProductOut)
async def get_product_by_id(product_id: str):
    """Get a single product by ID."""
    product = get_product(product_id)
    if not product:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Product not found"
        )
    return _db_to_response(product)


@router.put("/{product_id}", response_model=ProductOut)
async def update_existing_product(product_id: str, updates: ProductUpdate):
    """Update an existing product."""
    existing = get_product(product_id)
    if not existing:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Product not found"
        )
    
    # Filter out None values
    update_data = {k: v for k, v in updates.model_dump().items() if v is not None}
    
    if not update_data:
        return _db_to_response(existing)
    
    try:
        update_product(product_id, update_data)
        updated = get_product(product_id)
        return _db_to_response(updated)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.delete("/{product_id}", response_model=MessageOut)
async def delete_existing_product(
    product_id: str,
    hard: bool = Query(False, description="Permanently delete instead of soft delete")
):
    """Delete a product (soft delete by default)."""
    existing = get_product(product_id)
    if not existing:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Product not found"
        )
    
    delete_product(product_id, hard=hard)
    action = "permanently deleted" if hard else "deactivated"
    return MessageOut(message=f"Product {action}")
