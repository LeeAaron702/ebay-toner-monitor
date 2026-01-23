"""
Unit tests for db/products_db.py

Tests database operations for the products table:
- Schema creation
- CRUD operations
- Bulk import/upsert
- Metrics updates
- Engine query helpers
"""

import os
import sys
import sqlite3
import tempfile
import pytest

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Override DB_PATH before importing products_db
@pytest.fixture(scope="function")
def test_db():
    """Create a temporary database for each test."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        test_db_path = f.name
    
    # Patch the DB_PATH
    import db.products_db as products_db
    original_path = products_db.DB_PATH
    products_db.DB_PATH = test_db_path
    
    # Initialize the schema
    products_db.init_products_db()
    
    yield test_db_path
    
    # Cleanup
    products_db.DB_PATH = original_path
    try:
        os.unlink(test_db_path)
    except:
        pass


# =============================================================================
# Group Key Generation Tests
# =============================================================================

class TestGroupKeyGeneration:
    """Tests for generate_group_key function."""
    
    def test_canon_group_key(self):
        """Canon: model + capacity."""
        from db.products_db import generate_group_key
        result = generate_group_key('canon', '054H', 'High', None)
        assert result == 'canon:054h:high'
    
    def test_canon_group_key_standard(self):
        """Canon: standard capacity."""
        from db.products_db import generate_group_key
        result = generate_group_key('canon', '054H', 'Standard', None)
        assert result == 'canon:054h:standard'
    
    def test_canon_group_key_extra_high(self):
        """Canon: extra high capacity."""
        from db.products_db import generate_group_key
        result = generate_group_key('canon', '046H', 'Extra High', None)
        assert result == 'canon:046h:extra-high'
    
    def test_lexmark_group_key(self):
        """Lexmark: model only (no capacity)."""
        from db.products_db import generate_group_key
        result = generate_group_key('lexmark', '50F', 'High', None)
        assert result == 'lexmark:50f'
    
    def test_lexmark_group_key_ignores_capacity(self):
        """Lexmark blocks contain mixed capacities."""
        from db.products_db import generate_group_key
        result1 = generate_group_key('lexmark', '50F', 'Standard', None)
        result2 = generate_group_key('lexmark', '50F', 'Extra High', None)
        assert result1 == result2 == 'lexmark:50f'
    
    def test_xerox_part_number_block(self):
        """Xerox part number block."""
        from db.products_db import generate_group_key
        result = generate_group_key('xerox', None, 'Standard', '001R00610', is_model_block=False)
        assert result == 'xerox:part:001r00610'
    
    def test_xerox_model_block(self):
        """Xerox model block (with capacity)."""
        from db.products_db import generate_group_key
        result = generate_group_key('xerox', '7500', 'High', None, is_model_block=True)
        assert result == 'xerox:model:7500:high'
    
    def test_xerox_auto_detect_part_number(self):
        """Xerox auto-detects part number pattern."""
        from db.products_db import is_xerox_model_block
        assert is_xerox_model_block('001R00610') == False  # Part number
        assert is_xerox_model_block('106R01512') == False  # Part number
        assert is_xerox_model_block('7500') == True        # Model
        assert is_xerox_model_block('C400') == True        # Model


class TestNormalizeCapacity:
    """Tests for normalize_capacity function."""
    
    def test_standard(self):
        from db.products_db import normalize_capacity
        assert normalize_capacity('Standard') == 'standard'
        assert normalize_capacity('standard capacity') == 'standard'
    
    def test_high(self):
        from db.products_db import normalize_capacity
        assert normalize_capacity('High') == 'high'
        assert normalize_capacity('High Capacity') == 'high'
        assert normalize_capacity('High Yield') == 'high'
    
    def test_extra_high(self):
        from db.products_db import normalize_capacity
        assert normalize_capacity('Extra High') == 'extra-high'
        assert normalize_capacity('Extra-High') == 'extra-high'
        assert normalize_capacity('Super High') == 'extra-high'
    
    def test_empty(self):
        from db.products_db import normalize_capacity
        assert normalize_capacity('') == 'standard'
        assert normalize_capacity(None) == 'standard'


# =============================================================================
# CRUD Operation Tests
# =============================================================================

class TestCreateProduct:
    """Tests for create_product function."""
    
    def test_create_basic_product(self, test_db):
        """Create a basic product."""
        from db.products_db import create_product, get_product
        
        product_id = create_product({
            'brand': 'canon',
            'model': '054H',
            'capacity': 'High',
            'part_number': '054H',
            'variant_label': '4 Color High',
            'color': 'Color',
            'pack_size': 4,
            'asin': 'B07WSH9ZWF',
            'net_cost': 352.00,
            'bsr': 181110,
            'sellable': True,
        })
        
        assert product_id is not None
        
        # Verify it was created
        product = get_product(product_id)
        assert product is not None
        assert product['brand'] == 'canon'
        assert product['model'] == '054H'
        assert product['asin'] == 'B07WSH9ZWF'
        assert product['group_key'] == 'canon:054h:high'
    
    def test_create_product_invalid_brand(self, test_db):
        """Creating with invalid brand raises ValueError."""
        from db.products_db import create_product
        
        with pytest.raises(ValueError, match="Invalid brand"):
            create_product({
                'brand': 'hp',  # Not valid
                'asin': 'B123456789',
            })
    
    def test_create_product_missing_brand(self, test_db):
        """Creating without brand raises ValueError."""
        from db.products_db import create_product
        
        with pytest.raises(ValueError, match="brand is required"):
            create_product({
                'asin': 'B123456789',
            })
    
    def test_create_duplicate_asin_fails(self, test_db):
        """Creating duplicate (brand, asin) raises IntegrityError."""
        from db.products_db import create_product
        
        create_product({
            'brand': 'canon',
            'asin': 'B07WSH9ZWF',
        })
        
        with pytest.raises(sqlite3.IntegrityError):
            create_product({
                'brand': 'canon',
                'asin': 'B07WSH9ZWF',  # Same ASIN
            })
    
    def test_same_asin_different_brands_ok(self, test_db):
        """Same ASIN for different brands is allowed."""
        from db.products_db import create_product
        
        id1 = create_product({'brand': 'canon', 'asin': 'B07WSH9ZWF'})
        id2 = create_product({'brand': 'xerox', 'asin': 'B07WSH9ZWF'})
        
        assert id1 != id2


class TestGetProduct:
    """Tests for get_product function."""
    
    def test_get_existing_product(self, test_db):
        """Get existing product by ID."""
        from db.products_db import create_product, get_product
        
        product_id = create_product({'brand': 'canon', 'asin': 'B12345'})
        product = get_product(product_id)
        
        assert product is not None
        assert product['id'] == product_id
    
    def test_get_nonexistent_product(self, test_db):
        """Get nonexistent product returns None."""
        from db.products_db import get_product
        
        product = get_product('nonexistent-id')
        assert product is None


class TestUpdateProduct:
    """Tests for update_product function."""
    
    def test_update_fields(self, test_db):
        """Update specific fields."""
        from db.products_db import create_product, update_product, get_product
        
        product_id = create_product({
            'brand': 'canon',
            'asin': 'B12345',
            'net_cost': 100.0,
            'bsr': 50000,
        })
        
        result = update_product(product_id, {
            'net_cost': 150.0,
            'bsr': 75000,
        })
        
        assert result == True
        
        product = get_product(product_id)
        assert product['net_cost'] == 150.0
        assert product['bsr'] == 75000
    
    def test_update_nonexistent_returns_false(self, test_db):
        """Updating nonexistent product returns False."""
        from db.products_db import update_product
        
        result = update_product('nonexistent', {'net_cost': 100.0})
        assert result == False
    
    def test_update_recalculates_group_key(self, test_db):
        """Updating model/capacity recalculates group_key."""
        from db.products_db import create_product, update_product, get_product
        
        product_id = create_product({
            'brand': 'canon',
            'model': '054H',
            'capacity': 'Standard',
            'asin': 'B12345',
        })
        
        product = get_product(product_id)
        assert product['group_key'] == 'canon:054h:standard'
        
        update_product(product_id, {'capacity': 'High'})
        
        product = get_product(product_id)
        assert product['group_key'] == 'canon:054h:high'


class TestDeleteProduct:
    """Tests for delete_product function."""
    
    def test_delete(self, test_db):
        """Delete removes the row permanently."""
        from db.products_db import create_product, delete_product, get_product
        
        product_id = create_product({'brand': 'canon', 'asin': 'B12345'})
        
        result = delete_product(product_id)
        assert result == True
        
        product = get_product(product_id)
        assert product is None
    
    def test_delete_nonexistent(self, test_db):
        """Delete nonexistent product returns False."""
        from db.products_db import delete_product
        
        result = delete_product('nonexistent-id')
        assert result == False


class TestListProducts:
    """Tests for list_products function."""
    
    def test_list_by_brand(self, test_db):
        """List products filtered by brand."""
        from db.products_db import create_product, list_products
        
        create_product({'brand': 'canon', 'asin': 'B1'})
        create_product({'brand': 'canon', 'asin': 'B2'})
        create_product({'brand': 'xerox', 'asin': 'B3'})
        
        canon_products = list_products(brand='canon')
        assert len(canon_products) == 2
        
        xerox_products = list_products(brand='xerox')
        assert len(xerox_products) == 1
    
    def test_list_pagination(self, test_db):
        """List with pagination."""
        from db.products_db import create_product, list_products
        
        for i in range(10):
            create_product({'brand': 'canon', 'asin': f'B{i}'})
        
        page1 = list_products(brand='canon', limit=5, offset=0)
        assert len(page1) == 5
        
        page2 = list_products(brand='canon', limit=5, offset=5)
        assert len(page2) == 5


# =============================================================================
# Bulk Operation Tests
# =============================================================================

class TestBulkUpsertProducts:
    """Tests for bulk_upsert_products function."""
    
    def test_bulk_create(self, test_db):
        """Bulk create new products."""
        from db.products_db import bulk_upsert_products, count_products
        
        products = [
            {'brand': 'canon', 'asin': 'B1', 'model': '054H'},
            {'brand': 'canon', 'asin': 'B2', 'model': '054H'},
            {'brand': 'xerox', 'asin': 'B3', 'part_number': '001R00610'},
        ]
        
        stats = bulk_upsert_products(products)
        
        assert stats['total'] == 3
        assert stats['created'] == 3
        assert stats['updated'] == 0
        assert count_products() == 3
    
    def test_bulk_update_existing(self, test_db):
        """Bulk update existing products."""
        from db.products_db import bulk_upsert_products, get_product_by_asin
        
        # Create initial
        bulk_upsert_products([
            {'brand': 'canon', 'asin': 'B1', 'net_cost': 100.0},
        ])
        
        # Update
        stats = bulk_upsert_products([
            {'brand': 'canon', 'asin': 'B1', 'net_cost': 150.0},
        ])
        
        assert stats['updated'] == 1
        assert stats['created'] == 0
        
        product = get_product_by_asin('canon', 'B1')
        assert product['net_cost'] == 150.0
    
    def test_bulk_skip_invalid(self, test_db):
        """Bulk skip invalid rows."""
        from db.products_db import bulk_upsert_products
        
        products = [
            {'brand': 'canon', 'asin': 'B1'},  # Valid
            {'brand': 'invalid', 'asin': 'B2'},  # Invalid brand
            {'brand': 'canon', 'asin': 'N/A'},  # Invalid ASIN
            {'brand': 'canon', 'asin': ''},  # Empty ASIN
        ]
        
        stats = bulk_upsert_products(products)
        
        assert stats['created'] == 1
        assert stats['skipped'] == 3


class TestBulkUpdateMetrics:
    """Tests for bulk_update_metrics function."""
    
    def test_update_metrics_by_asin(self, test_db):
        """Update BSR/net_cost by ASIN."""
        from db.products_db import create_product, bulk_update_metrics, get_product
        
        product_id = create_product({
            'brand': 'canon',
            'asin': 'B12345',
            'net_cost': 100.0,
            'bsr': 50000,
        })
        
        stats = bulk_update_metrics([
            {'asin': 'B12345', 'net_cost': 150.0, 'bsr': 75000},
        ])
        
        assert stats['updated'] == 1
        
        product = get_product(product_id)
        assert product['net_cost'] == 150.0
        assert product['bsr'] == 75000
    
    def test_update_metrics_not_found(self, test_db):
        """Metrics update for unknown ASIN."""
        from db.products_db import bulk_update_metrics
        
        stats = bulk_update_metrics([
            {'asin': 'UNKNOWN', 'net_cost': 100.0},
        ])
        
        assert stats['not_found'] == 1


# =============================================================================
# Engine Helper Tests
# =============================================================================

class TestEngineHelpers:
    """Tests for engine query helpers."""
    
    def test_get_products_for_engine(self, test_db):
        """Get products as DataFrame for engine."""
        from db.products_db import create_product, get_products_for_engine
        
        create_product({'brand': 'canon', 'asin': 'B1', 'sellable': True})
        create_product({'brand': 'canon', 'asin': 'B2', 'sellable': False})
        create_product({'brand': 'xerox', 'asin': 'B3', 'sellable': True})
        
        df = get_products_for_engine('canon')
        
        assert len(df) == 2
        assert 'asin' in df.columns
        assert 'sellable' in df.columns
        assert df['sellable'].dtype == bool
    
    def test_build_part_number_index(self, test_db):
        """Build part number lookup index."""
        from db.products_db import create_product, build_part_number_index
        
        create_product({
            'brand': 'xerox',
            'asin': 'B1',
            'part_number': '001R00610',
        })
        create_product({
            'brand': 'xerox',
            'asin': 'B2',
            'part_number': '106R01512',
        })
        
        index = build_part_number_index('xerox')
        
        assert '001r00610' in index
        assert '106r01512' in index
        assert index['001r00610']['asin'] == 'B1'


# =============================================================================
# Grouping Helper Tests
# =============================================================================

class TestGroupingHelpers:
    """Tests for printable/grouping helpers."""
    
    def test_get_all_group_keys(self, test_db):
        """Get distinct group keys."""
        from db.products_db import create_product, get_all_group_keys
        
        create_product({'brand': 'canon', 'model': '054H', 'capacity': 'High', 'asin': 'B1'})
        create_product({'brand': 'canon', 'model': '054H', 'capacity': 'High', 'asin': 'B2'})
        create_product({'brand': 'canon', 'model': '054H', 'capacity': 'Standard', 'asin': 'B3'})
        
        keys = get_all_group_keys()
        
        assert len(keys) == 2
        assert 'canon:054h:high' in keys
        assert 'canon:054h:standard' in keys
    
    def test_get_products_by_group_key(self, test_db):
        """Get products in a specific group."""
        from db.products_db import create_product, get_products_by_group_key
        
        create_product({'brand': 'canon', 'model': '054H', 'capacity': 'High', 'asin': 'B1'})
        create_product({'brand': 'canon', 'model': '054H', 'capacity': 'High', 'asin': 'B2'})
        create_product({'brand': 'canon', 'model': '054H', 'capacity': 'Standard', 'asin': 'B3'})
        
        products = get_products_by_group_key('canon:054h:high')
        
        assert len(products) == 2


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
