"""
Unit tests for api/routers/products_api.py

Tests API endpoints for products:
- CRUD operations
- Bulk import
- Metrics import
- Grouping/printable endpoints
"""

import os
import sys
import tempfile
import pytest
from fastapi.testclient import TestClient
import io

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture(scope="function")
def client():
    """Create a test client with a temporary database."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        test_db_path = f.name
    
    # Patch the DB_PATH before importing modules
    import db.products_db as products_db
    original_path = products_db.DB_PATH
    products_db.DB_PATH = test_db_path
    
    # Initialize schema
    products_db.init_products_db()
    
    # Import and create test client
    from api.routers.products_api import router
    from fastapi import FastAPI
    
    app = FastAPI()
    app.include_router(router, prefix="/products")
    
    client = TestClient(app)
    
    yield client
    
    # Cleanup
    products_db.DB_PATH = original_path
    try:
        os.unlink(test_db_path)
    except:
        pass


# =============================================================================
# CRUD Endpoint Tests
# =============================================================================

class TestGetProducts:
    """Tests for GET /products endpoint."""
    
    def test_get_empty_list(self, client):
        """Get products returns empty list initially."""
        response = client.get("/products")
        assert response.status_code == 200
        data = response.json()
        assert data['total'] == 0
        assert data['items'] == []
    
    def test_get_products_paginated(self, client):
        """Get products with pagination."""
        # Create some products
        for i in range(15):
            client.post("/products", json={
                'brand': 'canon',
                'asin': f'B{i:05d}',
                'model': '054H',
            })
        
        # First page
        response = client.get("/products?limit=10&offset=0")
        assert response.status_code == 200
        data = response.json()
        assert data['total'] == 15
        assert len(data['items']) == 10
        
        # Second page
        response = client.get("/products?limit=10&offset=10")
        data = response.json()
        assert len(data['items']) == 5
    
    def test_get_products_filter_by_brand(self, client):
        """Get products filtered by brand."""
        client.post("/products", json={'brand': 'canon', 'asin': 'B1'})
        client.post("/products", json={'brand': 'xerox', 'asin': 'B2'})
        
        response = client.get("/products?brand=canon")
        assert response.status_code == 200
        data = response.json()
        assert data['total'] == 1
        assert data['items'][0]['brand'] == 'canon'
    
    def test_get_products_invalid_brand(self, client):
        """Get products with invalid brand returns 400."""
        response = client.get("/products?brand=hp")
        assert response.status_code == 400


class TestGetProductById:
    """Tests for GET /products/{id} endpoint."""
    
    def test_get_existing_product(self, client):
        """Get existing product by ID."""
        # Create a product
        create_response = client.post("/products", json={
            'brand': 'canon',
            'asin': 'B07WSH9ZWF',
            'model': '054H',
        })
        product_id = create_response.json()['id']
        
        # Get it
        response = client.get(f"/products/{product_id}")
        assert response.status_code == 200
        data = response.json()
        assert data['id'] == product_id
        assert data['asin'] == 'B07WSH9ZWF'
    
    def test_get_nonexistent_product(self, client):
        """Get nonexistent product returns 404."""
        response = client.get("/products/nonexistent-id")
        assert response.status_code == 404


class TestCreateProduct:
    """Tests for POST /products endpoint."""
    
    def test_create_product(self, client):
        """Create a new product."""
        response = client.post("/products", json={
            'brand': 'canon',
            'asin': 'B07WSH9ZWF',
            'model': '054H',
            'capacity': 'High',
            'variant_label': '4 Color High',
            'pack_size': 4,
            'net_cost': 352.00,
            'bsr': 181110,
            'sellable': True,
        })
        
        assert response.status_code == 201
        data = response.json()
        assert data['brand'] == 'canon'
        assert data['asin'] == 'B07WSH9ZWF'
        assert data['group_key'] == 'canon:054h:high'
    
    def test_create_product_invalid_brand(self, client):
        """Create with invalid brand returns 400."""
        response = client.post("/products", json={
            'brand': 'hp',
            'asin': 'B12345',
        })
        assert response.status_code == 400
    
    def test_create_duplicate_returns_409(self, client):
        """Create duplicate (brand, asin) returns 409."""
        client.post("/products", json={'brand': 'canon', 'asin': 'B12345'})
        response = client.post("/products", json={'brand': 'canon', 'asin': 'B12345'})
        assert response.status_code == 409


class TestUpdateProduct:
    """Tests for PUT /products/{id} endpoint."""
    
    def test_update_product(self, client):
        """Update existing product."""
        # Create
        create_response = client.post("/products", json={
            'brand': 'canon',
            'asin': 'B12345',
            'net_cost': 100.0,
        })
        product_id = create_response.json()['id']
        
        # Update
        response = client.put(f"/products/{product_id}", json={
            'net_cost': 150.0,
            'bsr': 50000,
        })
        
        assert response.status_code == 200
        data = response.json()
        assert data['net_cost'] == 150.0
        assert data['bsr'] == 50000
    
    def test_update_nonexistent_returns_404(self, client):
        """Update nonexistent product returns 404."""
        response = client.put("/products/nonexistent", json={'net_cost': 100.0})
        assert response.status_code == 404


class TestDeleteProduct:
    """Tests for DELETE /products/{id} endpoint."""
    
    def test_soft_delete(self, client):
        """Soft delete product."""
        create_response = client.post("/products", json={
            'brand': 'canon',
            'asin': 'B12345',
        })
        product_id = create_response.json()['id']
        
        # Delete
        response = client.delete(f"/products/{product_id}")
        assert response.status_code == 200
        assert 'deactivated' in response.json()['message']
        
        # Should not appear in active list
        list_response = client.get("/products?brand=canon")
        assert list_response.json()['total'] == 0
    
    def test_hard_delete(self, client):
        """Hard delete product."""
        create_response = client.post("/products", json={
            'brand': 'canon',
            'asin': 'B12345',
        })
        product_id = create_response.json()['id']
        
        # Delete
        response = client.delete(f"/products/{product_id}?hard=true")
        assert response.status_code == 200
        assert 'permanently deleted' in response.json()['message']
        
        # Should return 404
        get_response = client.get(f"/products/{product_id}")
        assert get_response.status_code == 404


# =============================================================================
# Bulk Operation Tests
# =============================================================================

class TestBulkImport:
    """Tests for POST /products/import endpoint."""
    
    def test_import_csv(self, client):
        """Import products from CSV."""
        csv_content = """brand,model,capacity,part_number,variant_label,color,pack_size,asin,amazon_sku,net_cost,bsr,sellable,is_model_block
canon,054H,High,054H,4 Color High,Color,4,B07WSH9ZWF,1BJV-8Y41-5AE2,352.00,181110,true,
canon,054H,High,054H,Black High,Black,1,B07QFYHY72,,84.00,7179,false,
xerox,,Standard,001R00610,Transfer Belt,,1,B007L0DI7Y,001r00610,195.00,456046,true,false
"""
        
        files = {'file': ('products.csv', io.BytesIO(csv_content.encode()), 'text/csv')}
        response = client.post("/products/import", files=files)
        
        assert response.status_code == 200
        data = response.json()
        assert data['total'] == 3
        assert data['created'] == 3
        assert data['errors'] == []
    
    def test_import_upserts_existing(self, client):
        """Import updates existing products."""
        # Create initial
        csv1 = """brand,asin,net_cost
canon,B12345,100.00
"""
        files = {'file': ('products.csv', io.BytesIO(csv1.encode()), 'text/csv')}
        client.post("/products/import", files=files)
        
        # Update
        csv2 = """brand,asin,net_cost
canon,B12345,150.00
"""
        files = {'file': ('products.csv', io.BytesIO(csv2.encode()), 'text/csv')}
        response = client.post("/products/import", files=files)
        
        data = response.json()
        assert data['updated'] == 1
        assert data['created'] == 0
    
    def test_import_invalid_file_type(self, client):
        """Import non-CSV returns 400."""
        files = {'file': ('products.txt', io.BytesIO(b'test'), 'text/plain')}
        response = client.post("/products/import", files=files)
        assert response.status_code == 400


class TestMetricsImport:
    """Tests for POST /products/metrics/import endpoint."""
    
    def test_import_metrics(self, client):
        """Import metrics updates."""
        # Create products first
        client.post("/products", json={'brand': 'canon', 'asin': 'B1', 'net_cost': 100.0})
        client.post("/products", json={'brand': 'canon', 'asin': 'B2', 'net_cost': 200.0})
        
        # Import metrics
        csv_content = """asin,net_cost,bsr,sellable
B1,150.00,50000,true
B2,250.00,75000,false
"""
        files = {'file': ('metrics.csv', io.BytesIO(csv_content.encode()), 'text/csv')}
        response = client.post("/products/metrics/import", files=files)
        
        assert response.status_code == 200
        data = response.json()
        assert data['updated'] == 2
    
    def test_import_metrics_not_found(self, client):
        """Import metrics for unknown ASIN."""
        csv_content = """asin,net_cost
UNKNOWN,100.00
"""
        files = {'file': ('metrics.csv', io.BytesIO(csv_content.encode()), 'text/csv')}
        response = client.post("/products/metrics/import", files=files)
        
        data = response.json()
        assert data['not_found'] == 1


# =============================================================================
# Template Download Test
# =============================================================================

class TestTemplateDownload:
    """Tests for GET /products/template endpoint."""
    
    def test_download_template(self, client):
        """Download CSV template."""
        response = client.get("/products/template")
        
        assert response.status_code == 200
        assert 'text/csv' in response.headers['content-type']
        assert 'attachment' in response.headers.get('content-disposition', '')
        
        content = response.content.decode()
        assert 'brand' in content
        assert 'asin' in content
        assert 'net_cost' in content


# =============================================================================
# Grouping Endpoint Tests
# =============================================================================

class TestGroupingEndpoints:
    """Tests for grouping/printable endpoints."""
    
    def test_list_group_keys(self, client):
        """List all group keys."""
        client.post("/products", json={'brand': 'canon', 'model': '054H', 'capacity': 'High', 'asin': 'B1'})
        client.post("/products", json={'brand': 'canon', 'model': '054H', 'capacity': 'Standard', 'asin': 'B2'})
        
        response = client.get("/products/groups")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
        assert 'canon:054h:high' in data
        assert 'canon:054h:standard' in data
    
    def test_list_group_keys_by_brand(self, client):
        """List group keys filtered by brand."""
        client.post("/products", json={'brand': 'canon', 'model': '054H', 'capacity': 'High', 'asin': 'B1'})
        client.post("/products", json={'brand': 'xerox', 'part_number': '001R00610', 'asin': 'B2'})
        
        response = client.get("/products/groups?brand=canon")
        data = response.json()
        assert len(data) == 1
        assert data[0].startswith('canon:')
    
    def test_get_products_by_group_key(self, client):
        """Get products in a specific group."""
        client.post("/products", json={'brand': 'canon', 'model': '054H', 'capacity': 'High', 'asin': 'B1'})
        client.post("/products", json={'brand': 'canon', 'model': '054H', 'capacity': 'High', 'asin': 'B2'})
        client.post("/products", json={'brand': 'canon', 'model': '054H', 'capacity': 'Standard', 'asin': 'B3'})
        
        response = client.get("/products/groups/canon:054h:high")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
    
    def test_get_products_by_group_key_not_found(self, client):
        """Get products for nonexistent group returns 404."""
        response = client.get("/products/groups/nonexistent:group:key")
        assert response.status_code == 404


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
