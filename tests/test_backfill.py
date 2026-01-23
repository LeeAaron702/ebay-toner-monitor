"""
Tests for incremental backfill functionality.

Tests the gap detection and incremental processing logic that ensures
purchased_units stays in sync with order_history.
"""

import pytest
import sqlite3
import tempfile
import os

# Patch DB_PATH before importing modules
@pytest.fixture(scope="function")
def test_db():
    """Create a temporary database for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        test_db_path = f.name
    
    # Patch DB_PATH in all relevant modules
    import db.listings_db as listings_db
    import db.products_db as products_db
    
    original_listings_path = listings_db.DB_PATH
    original_products_path = products_db.DB_PATH
    
    listings_db.DB_PATH = test_db_path
    products_db.DB_PATH = test_db_path
    
    # Initialize schemas
    listings_db.init_db()
    products_db.init_products_db()
    
    yield test_db_path
    
    # Restore and cleanup
    listings_db.DB_PATH = original_listings_path
    products_db.DB_PATH = original_products_path
    os.unlink(test_db_path)


class TestGetUnprocessedOrders:
    """Tests for get_unprocessed_orders() query."""
    
    def test_empty_tables_returns_empty(self, test_db):
        """When both tables are empty, returns empty list."""
        from db.listings_db import get_unprocessed_orders
        
        result = get_unprocessed_orders()
        assert result == []
    
    def test_all_orders_unprocessed(self, test_db):
        """When order_history has rows but purchased_units is empty, returns all."""
        from db.listings_db import get_unprocessed_orders, get_db_connection
        
        conn = get_db_connection()
        conn.execute('''
            INSERT INTO order_history (order_id, transaction_id, item_title)
            VALUES ('order1', 'txn1', 'Canon 046 Black'),
                   ('order2', 'txn2', 'Canon 054H Cyan')
        ''')
        conn.commit()
        conn.close()
        
        result = get_unprocessed_orders()
        assert len(result) == 2
        order_ids = {r['order_id'] for r in result}
        assert order_ids == {'order1', 'order2'}
    
    def test_some_orders_processed(self, test_db):
        """When some orders have purchased_units, only returns unprocessed."""
        from db.listings_db import get_unprocessed_orders, get_db_connection
        
        conn = get_db_connection()
        # Add orders
        conn.execute('''
            INSERT INTO order_history (order_id, transaction_id, item_title)
            VALUES ('order1', 'txn1', 'Canon 046 Black'),
                   ('order2', 'txn2', 'Canon 054H Cyan'),
                   ('order3', 'txn3', 'Canon 055 Magenta')
        ''')
        # Mark order1 as processed by adding purchased_units
        conn.execute('''
            INSERT INTO purchased_units (order_id, transaction_id, model, color, quantity)
            VALUES ('order1', 'txn1', '046', 'Black', 1)
        ''')
        conn.commit()
        conn.close()
        
        result = get_unprocessed_orders()
        assert len(result) == 2
        order_ids = {r['order_id'] for r in result}
        assert order_ids == {'order2', 'order3'}
    
    def test_all_orders_processed(self, test_db):
        """When all orders have purchased_units, returns empty."""
        from db.listings_db import get_unprocessed_orders, get_db_connection
        
        conn = get_db_connection()
        conn.execute('''
            INSERT INTO order_history (order_id, transaction_id, item_title)
            VALUES ('order1', 'txn1', 'Canon 046 Black')
        ''')
        conn.execute('''
            INSERT INTO purchased_units (order_id, transaction_id, model, color, quantity)
            VALUES ('order1', 'txn1', '046', 'Black', 1)
        ''')
        conn.commit()
        conn.close()
        
        result = get_unprocessed_orders()
        assert result == []


class TestGetBackfillStatus:
    """Tests for get_backfill_status() monitoring function."""
    
    def test_empty_tables(self, test_db):
        """Empty tables return zero counts."""
        from db.listings_db import get_backfill_status
        
        status = get_backfill_status()
        assert status['total_orders'] == 0
        assert status['processed_orders'] == 0
        assert status['unprocessed_orders'] == 0
    
    def test_partial_processing(self, test_db):
        """Correctly calculates gap between orders and processed."""
        from db.listings_db import get_backfill_status, get_db_connection
        
        conn = get_db_connection()
        # Add 3 orders
        conn.execute('''
            INSERT INTO order_history (order_id, transaction_id, item_title)
            VALUES ('order1', 'txn1', 'Canon 046 Black'),
                   ('order2', 'txn2', 'Canon 054H Cyan'),
                   ('order3', 'txn3', 'Canon 055 Magenta')
        ''')
        # Process 1 order
        conn.execute('''
            INSERT INTO purchased_units (order_id, transaction_id, model, color, quantity)
            VALUES ('order1', 'txn1', '046', 'Black', 1)
        ''')
        conn.commit()
        conn.close()
        
        status = get_backfill_status()
        assert status['total_orders'] == 3
        assert status['processed_orders'] == 1
        assert status['unprocessed_orders'] == 2


class TestCheckAndBackfill:
    """Tests for the check_and_backfill() coordinator function."""
    
    def test_no_action_when_no_orders(self, test_db):
        """Returns no_action status when no orders exist."""
        from backfill_matches import check_and_backfill
        
        result = check_and_backfill(verbose=False)
        assert result['status'] == 'no_action'
        assert result['processed'] == 0
    
    def test_no_action_when_all_processed(self, test_db):
        """Returns no_action status when all orders already processed."""
        from backfill_matches import check_and_backfill
        from db.listings_db import get_db_connection
        
        conn = get_db_connection()
        conn.execute('''
            INSERT INTO order_history (order_id, transaction_id, item_title)
            VALUES ('order1', 'txn1', 'Canon 046 Black')
        ''')
        conn.execute('''
            INSERT INTO purchased_units (order_id, transaction_id, model, color, quantity)
            VALUES ('order1', 'txn1', '046', 'Black', 1)
        ''')
        conn.commit()
        conn.close()
        
        result = check_and_backfill(verbose=False)
        assert result['status'] == 'no_action'


class TestBackfillIdempotency:
    """Tests that backfill is idempotent - safe to run multiple times."""
    
    def test_running_twice_same_result(self, test_db):
        """Running backfill twice doesn't create duplicates."""
        from backfill_matches import check_and_backfill
        from db.listings_db import get_backfill_status, get_db_connection
        
        conn = get_db_connection()
        conn.execute('''
            INSERT INTO order_history (order_id, transaction_id, item_title, transaction_price)
            VALUES ('order1', 'txn1', 'Canon 046 Black Toner', '25.00')
        ''')
        conn.commit()
        conn.close()
        
        # First run
        result1 = check_and_backfill(verbose=False)
        status1 = get_backfill_status()
        
        # Second run - should be no_action since already processed
        result2 = check_and_backfill(verbose=False)
        status2 = get_backfill_status()
        
        # Second run should do nothing
        assert result2['status'] == 'no_action'
        # Counts should be identical
        assert status1['processed_orders'] == status2['processed_orders']
