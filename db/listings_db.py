import json
import os
import sqlite3
import time
from typing import Optional, Dict, Any, List, Tuple

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DB_PATH = os.path.abspath(os.getenv("DB_PATH", os.path.join(REPO_ROOT, "database.db")))

SEEN_IDS_TABLE = 'seen_ids'
MESSAGES_TABLE = 'messages'
MATCHES_TABLE = 'matches'
ORDER_HISTORY_TABLE = 'order_history'
PURCHASED_UNITS_TABLE = 'purchased_units'
ROLLING_WINDOW_SEC = 72 * 3600  # 72 hours

def migrate_db():
	"""Rename seen_ids.db to database.db if it exists, and migrate schema if needed."""
	if os.path.exists('seen_ids.db') and not os.path.exists(DB_PATH):
		os.rename('seen_ids.db', DB_PATH)

def get_db_connection():
	conn = sqlite3.connect(DB_PATH)
	return conn

def init_db():
	migrate_db()
	conn = get_db_connection()
	with conn:
		# seen_ids table
		conn.execute(f'''
			CREATE TABLE IF NOT EXISTS {SEEN_IDS_TABLE} (
				id TEXT PRIMARY KEY,
				seen_ts INTEGER
			)
		''')
		# messages table
		conn.execute(f'''
			CREATE TABLE IF NOT EXISTS {MESSAGES_TABLE} (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				listing_id TEXT,
				timestamp INTEGER,
				listed_time TEXT,
				link TEXT,
				type TEXT,
				quantity TEXT,
				price REAL,
				shipping REAL,
				total REAL,
				message TEXT
			)
		''')
		# matches table
		conn.execute(f'''
			CREATE TABLE IF NOT EXISTS {MATCHES_TABLE} (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				message_id INTEGER,
				is_alternative INTEGER,
				title TEXT,
				asin TEXT,
				bsr INTEGER,
				sellable INTEGER,
				net_cost REAL,
				profit REAL,
				pack_size INTEGER,
				color TEXT,
				lot_breakdown TEXT,
				total_units INTEGER,
				is_mixed_lot INTEGER DEFAULT 0,
				FOREIGN KEY(message_id) REFERENCES {MESSAGES_TABLE}(id)
			)
		''')
		
		# Add new columns to existing matches table if they don't exist
		try:
			conn.execute(f'ALTER TABLE {MATCHES_TABLE} ADD COLUMN lot_breakdown TEXT')
		except sqlite3.OperationalError:
			pass  # Column already exists
		try:
			conn.execute(f'ALTER TABLE {MATCHES_TABLE} ADD COLUMN total_units INTEGER')
		except sqlite3.OperationalError:
			pass  # Column already exists
		try:
			conn.execute(f'ALTER TABLE {MATCHES_TABLE} ADD COLUMN is_mixed_lot INTEGER DEFAULT 0')
		except sqlite3.OperationalError:
			pass  # Column already exists
		# order_history table
		conn.execute(f'''
			CREATE TABLE IF NOT EXISTS {ORDER_HISTORY_TABLE} (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				account_label TEXT,
				order_id TEXT,
				transaction_id TEXT,
				order_status TEXT,
				checkout_status TEXT,
				cancel_status TEXT,
				cancel_reason TEXT,
				return_status TEXT,
				inquiry_status TEXT,
				refund_amount TEXT,
				refund_time TEXT,
				refund_type TEXT,
				refund_status TEXT,
				created_time TEXT,
				paid_time TEXT,
				shipped_time TEXT,
				actual_delivery_time TEXT,
				order_currency TEXT,
				order_amount_paid TEXT,
				order_subtotal TEXT,
				order_total TEXT,
				buyer_user_id TEXT,
				seller_user_id TEXT,
				item_id TEXT,
				item_title TEXT,
				item_sku TEXT,
				quantity_purchased TEXT,
				transaction_price TEXT,
				transaction_currency TEXT,
				shipping_service TEXT,
				shipping_service_cost TEXT,
				tracking_numbers TEXT,
				tracking_carriers TEXT,
				match1_title TEXT,
				match1_asin TEXT,
				match1_bsr TEXT,
				match1_sellable TEXT,
				match1_net_cost TEXT,
				match1_profit TEXT,
				match1_pack_size TEXT,
				match1_color TEXT,
				match1_is_alternative TEXT,
				match1_lot_breakdown TEXT,
				match1_total_units TEXT,
				match2_title TEXT,
				match2_asin TEXT,
				match2_bsr TEXT,
				match2_sellable TEXT,
				match2_net_cost TEXT,
				match2_profit TEXT,
				match2_pack_size TEXT,
				match2_color TEXT,
				match2_is_alternative TEXT,
				match2_lot_breakdown TEXT,
				match2_total_units TEXT,
				match3_title TEXT,
				match3_asin TEXT,
				match3_bsr TEXT,
				match3_sellable TEXT,
				match3_net_cost TEXT,
				match3_profit TEXT,
				match3_pack_size TEXT,
				match3_color TEXT,
				match3_is_alternative TEXT,
				match3_lot_breakdown TEXT,
				match3_total_units TEXT,
				match4_title TEXT,
				match4_asin TEXT,
				match4_bsr TEXT,
				match4_sellable TEXT,
				match4_net_cost TEXT,
				match4_profit TEXT,
				match4_pack_size TEXT,
				match4_color TEXT,
				match4_is_alternative TEXT,
				match4_lot_breakdown TEXT,
				match4_total_units TEXT,
				UNIQUE(order_id, transaction_id)
			)
		''')
		
		# Add new lot columns to order_history if they don't exist
		for i in range(1, 5):
			try:
				conn.execute(f'ALTER TABLE {ORDER_HISTORY_TABLE} ADD COLUMN match{i}_lot_breakdown TEXT')
			except sqlite3.OperationalError:
				pass
			try:
				conn.execute(f'ALTER TABLE {ORDER_HISTORY_TABLE} ADD COLUMN match{i}_total_units TEXT')
			except sqlite3.OperationalError:
				pass
		
		# purchased_units table - normalized per-color/per-unit analytics
		conn.execute(f'''
			CREATE TABLE IF NOT EXISTS {PURCHASED_UNITS_TABLE} (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				order_id TEXT NOT NULL,
				transaction_id TEXT NOT NULL,
				item_id TEXT,
				purchased_date TEXT,
				model TEXT,
				capacity TEXT,
				color TEXT,
				quantity INTEGER,
				unit_cost REAL,
				asin TEXT,
				bsr INTEGER,
				net_per_unit REAL,
				lot_type TEXT,
				match_index INTEGER,
				UNIQUE(order_id, transaction_id, match_index, model, color)
			)
		''')
		
		# Create index for common queries
		conn.execute(f'''
			CREATE INDEX IF NOT EXISTS idx_purchased_units_model_color 
			ON {PURCHASED_UNITS_TABLE} (model, color)
		''')
		conn.execute(f'''
			CREATE INDEX IF NOT EXISTS idx_purchased_units_date 
			ON {PURCHASED_UNITS_TABLE} (purchased_date)
		''')
				
	conn.close()

def add_seen_id(listing_id: str, timestamp: Optional[int] = None):
	if timestamp is None:
		timestamp = int(time.time())
	conn = get_db_connection()
	with conn:
		conn.execute(f'''
			INSERT OR REPLACE INTO {SEEN_IDS_TABLE} (id, seen_ts)
			VALUES (?, ?)
		''', (listing_id, timestamp))
	conn.close()

def is_id_seen(listing_id: str) -> bool:
	conn = get_db_connection()
	cur = conn.cursor()
	cur.execute(f'SELECT 1 FROM {SEEN_IDS_TABLE} WHERE id = ?', (listing_id,))
	result = cur.fetchone()
	conn.close()
	return result is not None

def gc_old_ids():
	cutoff = int(time.time()) - ROLLING_WINDOW_SEC
	conn = get_db_connection()
	with conn:
		conn.execute(f'DELETE FROM {SEEN_IDS_TABLE} WHERE seen_ts < ?', (cutoff,))
	conn.close()

def insert_message(listing_id: str, timestamp: int, listed_time: str, link: str, type_: str, quantity: str, price: float, shipping: float, total: float, message: str) -> int:
	conn = get_db_connection()
	with conn:
		cur = conn.execute(f'''
			INSERT INTO {MESSAGES_TABLE} (listing_id, timestamp, listed_time, link, type, quantity, price, shipping, total, message)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		''', (listing_id, timestamp, listed_time, link, type_, quantity, price, shipping, total, message))
		message_id = cur.lastrowid
	conn.close()
	return message_id

def insert_match(
	message_id: int, 
	is_alternative: int, 
	title: str, 
	asin: str, 
	bsr: Optional[int], 
	sellable: int, 
	net_cost: Optional[float], 
	profit: Optional[float], 
	pack_size: Optional[int], 
	color: str,
	lot_breakdown: Optional[str] = None,
	total_units: Optional[int] = None,
	is_mixed_lot: int = 0
):
	"""
	Insert a match record into the database.
	
	Args:
		message_id: FK to messages table
		is_alternative: 1 if this is an alternative match, 0 if primary
		title: Match title (e.g., "Canon GPR-53 Yellow")
		asin: Amazon ASIN
		bsr: Best Sellers Rank
		sellable: 1 if sellable, 0 if not
		net_cost: Net cost per unit
		profit: Calculated profit
		pack_size: Number of units in pack
		color: Color of the toner
		lot_breakdown: JSON string of LotBreakdown.to_dict() for mixed lots
		total_units: Total units in the lot (for mixed lots)
		is_mixed_lot: 1 if this is part of a mixed lot analysis, 0 otherwise
	"""
	conn = get_db_connection()
	with conn:
		conn.execute(f'''
			INSERT INTO {MATCHES_TABLE} (message_id, is_alternative, title, asin, bsr, sellable, net_cost, profit, pack_size, color, lot_breakdown, total_units, is_mixed_lot)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		''', (message_id, is_alternative, title, asin, bsr, sellable, net_cost, profit, pack_size, color, lot_breakdown, total_units, is_mixed_lot))
	conn.close()

def get_message_by_listing_id(listing_id: str) -> Optional[Dict[str, Any]]:
	conn = get_db_connection()
	cur = conn.cursor()
	cur.execute(f'SELECT * FROM {MESSAGES_TABLE} WHERE listing_id = ?', (listing_id,))
	row = cur.fetchone()
	conn.close()
	if row:
		columns = [desc[0] for desc in cur.description]
		return dict(zip(columns, row))
	return None

def get_matches_for_message(message_id: int) -> List[Dict[str, Any]]:
	conn = get_db_connection()
	cur = conn.cursor()
	cur.execute(f'SELECT * FROM {MATCHES_TABLE} WHERE message_id = ?', (message_id,))
	rows = cur.fetchall()
	columns = [desc[0] for desc in cur.description]
	conn.close()
	return [dict(zip(columns, row)) for row in rows]


# Order history columns (excluding auto-increment id)
ORDER_HISTORY_COLUMNS = [
	'account_label', 'order_id', 'transaction_id', 'order_status', 'checkout_status',
	'cancel_status', 'cancel_reason', 'return_status', 'inquiry_status',
	'refund_amount', 'refund_time', 'refund_type', 'refund_status',
	'created_time', 'paid_time', 'shipped_time', 'actual_delivery_time',
	'order_currency', 'order_amount_paid', 'order_subtotal', 'order_total',
	'buyer_user_id', 'seller_user_id', 'item_id', 'item_title', 'item_sku',
	'quantity_purchased', 'transaction_price', 'transaction_currency',
	'shipping_service', 'shipping_service_cost', 'tracking_numbers', 'tracking_carriers',
	'match1_title', 'match1_asin', 'match1_bsr', 'match1_sellable', 'match1_net_cost',
	'match1_profit', 'match1_pack_size', 'match1_color', 'match1_is_alternative',
	'match1_lot_breakdown', 'match1_total_units',
	'match2_title', 'match2_asin', 'match2_bsr', 'match2_sellable', 'match2_net_cost',
	'match2_profit', 'match2_pack_size', 'match2_color', 'match2_is_alternative',
	'match2_lot_breakdown', 'match2_total_units',
	'match3_title', 'match3_asin', 'match3_bsr', 'match3_sellable', 'match3_net_cost',
	'match3_profit', 'match3_pack_size', 'match3_color', 'match3_is_alternative',
	'match3_lot_breakdown', 'match3_total_units',
	'match4_title', 'match4_asin', 'match4_bsr', 'match4_sellable', 'match4_net_cost',
	'match4_profit', 'match4_pack_size', 'match4_color', 'match4_is_alternative',
	'match4_lot_breakdown', 'match4_total_units',
]


def upsert_order_history(rows: List[Dict[str, Any]]) -> Tuple[int, int]:
	"""
	Upsert order history rows to the database.
	Uses INSERT OR REPLACE keyed on (order_id, transaction_id).
	Returns tuple of (new_count, updated_count).
	"""
	if not rows:
		return 0, 0

	conn = get_db_connection()
	cur = conn.cursor()

	# Get existing order keys to track new vs updated
	cur.execute(f'SELECT order_id, transaction_id FROM {ORDER_HISTORY_TABLE}')
	existing_keys = {(row[0], row[1]) for row in cur.fetchall()}

	new_count = 0
	updated_count = 0

	placeholders = ', '.join(['?' for _ in ORDER_HISTORY_COLUMNS])
	columns_str = ', '.join(ORDER_HISTORY_COLUMNS)

	with conn:
		for row in rows:
			key = (row.get('order_id', ''), row.get('transaction_id', ''))
			values = [str(row.get(col, '')) if row.get(col) is not None else '' for col in ORDER_HISTORY_COLUMNS]

			conn.execute(f'''
				INSERT OR REPLACE INTO {ORDER_HISTORY_TABLE} ({columns_str})
				VALUES ({placeholders})
			''', values)

			if key in existing_keys:
				updated_count += 1
			else:
				new_count += 1
				existing_keys.add(key)

	conn.close()
	return new_count, updated_count


def get_order_history(limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
	"""Retrieve order history records with pagination."""
	conn = get_db_connection()
	conn.row_factory = sqlite3.Row
	cur = conn.cursor()
	cur.execute(f'''
		SELECT * FROM {ORDER_HISTORY_TABLE}
		ORDER BY created_time DESC
		LIMIT ? OFFSET ?
	''', (limit, offset))
	rows = cur.fetchall()
	conn.close()
	return [dict(row) for row in rows]


def get_order_by_id(order_id: str, transaction_id: str = '') -> Optional[Dict[str, Any]]:
	"""Retrieve a specific order by order_id and transaction_id."""
	conn = get_db_connection()
	conn.row_factory = sqlite3.Row
	cur = conn.cursor()
	cur.execute(f'''
		SELECT * FROM {ORDER_HISTORY_TABLE}
		WHERE order_id = ? AND transaction_id = ?
	''', (order_id, transaction_id))
	row = cur.fetchone()
	conn.close()
	return dict(row) if row else None


def get_daily_order_stats(date_str: str) -> List[Dict[str, Any]]:
	"""
	Get order stats for a specific date (PST), grouped by account.
	
	Args:
		date_str: Date in format 'YYYY-MM-DD' (e.g., '2025-12-03')
	
	Returns:
		List of dicts with 'account_label', 'order_count', 'total_spent'
	"""
	conn = get_db_connection()
	conn.row_factory = sqlite3.Row
	cur = conn.cursor()
	
	# created_time is stored as 'YYYY-MM-DD HH:MM:SS PST'
	# We match on the date prefix and group by account
	# Use DISTINCT order_id to count orders (not transactions)
	# Sum order_total only once per order using a subquery
	cur.execute(f'''
		SELECT 
			account_label,
			COUNT(DISTINCT order_id) as order_count,
			COALESCE(SUM(
				CASE 
					WHEN transaction_id = (
						SELECT MIN(transaction_id) 
						FROM {ORDER_HISTORY_TABLE} oh2 
						WHERE oh2.order_id = {ORDER_HISTORY_TABLE}.order_id
					) THEN CAST(order_total AS REAL)
					ELSE 0
				END
			), 0) as total_spent
		FROM {ORDER_HISTORY_TABLE}
		WHERE created_time LIKE ?
		GROUP BY account_label
	''', (f'{date_str}%',))
	
	rows = cur.fetchall()
	conn.close()
	return [dict(row) for row in rows]


def get_order_stats_for_time_range(start_time: str, end_time: str) -> List[Dict[str, Any]]:
	"""
	Get order stats for a specific time range, grouped by account.
	
	Args:
		start_time: Start time in format 'YYYY-MM-DD HH:MM:SS' (e.g., '2025-12-03 00:00:00')
		end_time: End time in format 'YYYY-MM-DD HH:MM:SS' (e.g., '2025-12-03 09:00:00')
	
	Returns:
		List of dicts with 'account_label', 'order_count', 'total_spent'
	"""
	conn = get_db_connection()
	conn.row_factory = sqlite3.Row
	cur = conn.cursor()
	
	# created_time is stored as 'YYYY-MM-DD HH:MM:SS PST'
	# We compare the time portion (stripping ' PST' suffix for comparison)
	# Use DISTINCT order_id to count orders (not transactions)
	# Sum order_total only once per order using a subquery
	cur.execute(f'''
		SELECT 
			account_label,
			COUNT(DISTINCT order_id) as order_count,
			COALESCE(SUM(
				CASE 
					WHEN transaction_id = (
						SELECT MIN(transaction_id) 
						FROM {ORDER_HISTORY_TABLE} oh2 
						WHERE oh2.order_id = {ORDER_HISTORY_TABLE}.order_id
					) THEN CAST(order_total AS REAL)
					ELSE 0
				END
			), 0) as total_spent
		FROM {ORDER_HISTORY_TABLE}
		WHERE REPLACE(created_time, ' PST', '') >= ?
		  AND REPLACE(created_time, ' PST', '') < ?
		GROUP BY account_label
	''', (start_time, end_time))
	
	rows = cur.fetchall()
	conn.close()
	return [dict(row) for row in rows]


# ============================================================================
# PURCHASED UNITS TABLE FUNCTIONS
# ============================================================================

def insert_purchased_unit(
	order_id: str,
	transaction_id: str,
	item_id: str,
	purchased_date: str,
	model: str,
	capacity: str,
	color: str,
	quantity: int,
	unit_cost: float,
	asin: str,
	bsr: int,
	net_per_unit: float,
	lot_type: str,
	match_index: int
) -> Optional[int]:
	"""
	Insert a single purchased unit row. Returns the row id or None if duplicate.
	"""
	conn = get_db_connection()
	try:
		with conn:
			cur = conn.execute(f'''
				INSERT OR IGNORE INTO {PURCHASED_UNITS_TABLE}
				(order_id, transaction_id, item_id, purchased_date, model, capacity, 
				 color, quantity, unit_cost, asin, bsr, net_per_unit, lot_type, match_index)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			''', (order_id, transaction_id, item_id, purchased_date, model, capacity,
				  color, quantity, unit_cost, asin, bsr, net_per_unit, lot_type, match_index))
			return cur.lastrowid if cur.rowcount > 0 else None
	finally:
		conn.close()


def insert_purchased_units_batch(units: List[Dict[str, Any]]) -> Tuple[int, int]:
	"""
	Insert multiple purchased unit rows. Returns (inserted_count, skipped_count).
	
	Each dict should have keys: order_id, transaction_id, item_id, purchased_date,
	model, capacity, color, quantity, unit_cost, asin, bsr, net_per_unit, lot_type, match_index
	"""
	conn = get_db_connection()
	inserted = 0
	skipped = 0
	try:
		with conn:
			for unit in units:
				cur = conn.execute(f'''
					INSERT OR IGNORE INTO {PURCHASED_UNITS_TABLE}
					(order_id, transaction_id, item_id, purchased_date, model, capacity, 
					 color, quantity, unit_cost, asin, bsr, net_per_unit, lot_type, match_index)
					VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
				''', (
					unit['order_id'], unit['transaction_id'], unit['item_id'], 
					unit['purchased_date'], unit['model'], unit['capacity'],
					unit['color'], unit['quantity'], unit['unit_cost'], 
					unit['asin'], unit['bsr'], unit['net_per_unit'], 
					unit['lot_type'], unit['match_index']
				))
				if cur.rowcount > 0:
					inserted += 1
				else:
					skipped += 1
	finally:
		conn.close()
	return inserted, skipped


def expand_order_to_purchased_units(order_row: Dict[str, Any]) -> List[Dict[str, Any]]:
	"""
	Expand an order_history row into purchased_units rows based on lot_breakdown.
	
	IMPORTANT: Only expands HIGH or MEDIUM confidence breakdowns.
	Low confidence entries are SKIPPED to avoid polluting analytics with guessed data.
	
	Parses match1-4_lot_breakdown JSON to create per-color entries.
	The lot_breakdown JSON has structure:
	{
		"model": "045",
		"capacity": "Standard", 
		"color_quantities": {"black": 2, "cyan": 1, ...},
		"total_units": 4,
		"confidence": "high" | "medium" | "low",
		...
	}
	
	Returns list of unit dicts ready for insert_purchased_units_batch.
	"""
	units = []
	order_id = order_row.get('order_id')
	transaction_id = order_row.get('transaction_id')
	item_id = order_row.get('item_id')
	created_time = order_row.get('created_time') or ''
	purchased_date = created_time[:10] if created_time else None  # Extract date portion
	
	# Get total transaction cost for unit cost calculation
	transaction_price = order_row.get('transaction_price')
	try:
		total_cost = float(transaction_price) if transaction_price else 0.0
	except (ValueError, TypeError):
		total_cost = 0.0
	
	for i in range(1, 5):
		lot_breakdown_str = order_row.get(f'match{i}_lot_breakdown')
		if not lot_breakdown_str:
			continue
		
		try:
			lot_data = json.loads(lot_breakdown_str) if isinstance(lot_breakdown_str, str) else lot_breakdown_str
		except (json.JSONDecodeError, TypeError):
			continue
		
		if not lot_data:
			continue
		
		# Extract color_quantities from the lot_breakdown structure
		# lot_data could be the full LotBreakdown dict or just color_quantities dict
		if isinstance(lot_data, dict):
			if 'color_quantities' in lot_data:
				# Full LotBreakdown structure
				color_quantities = lot_data.get('color_quantities', {})
				model_from_breakdown = lot_data.get('model', '')
				capacity_from_breakdown = lot_data.get('capacity', 'standard')
				lot_total_units = lot_data.get('total_units', 0)
				confidence = lot_data.get('confidence', 'high')  # Default to high for backwards compat
			else:
				# Assume it's just a color_quantities dict directly
				color_quantities = lot_data
				model_from_breakdown = ''
				capacity_from_breakdown = 'standard'
				lot_total_units = sum(color_quantities.values()) if color_quantities else 0
				confidence = 'high'  # Old format without confidence, assume high
		else:
			continue
		
		# SKIP LOW CONFIDENCE ENTRIES
		# These have ambiguous per-color quantities and would pollute analytics
		if confidence == 'low':
			continue
		
		if not color_quantities:
			continue
		
		# Get match details
		asin = order_row.get(f'match{i}_asin', '')
		bsr_str = order_row.get(f'match{i}_bsr', '')
		try:
			bsr = int(bsr_str) if bsr_str else 0
		except (ValueError, TypeError):
			bsr = 0
		
		net_cost_str = order_row.get(f'match{i}_net_cost', '')
		try:
			net_cost = float(net_cost_str) if net_cost_str else 0.0
		except (ValueError, TypeError):
			net_cost = 0.0
		
		# Use total_units from match column, fallback to lot_breakdown, then calculated
		total_units_str = order_row.get(f'match{i}_total_units', '')
		try:
			total_units = int(total_units_str) if total_units_str else lot_total_units
		except (ValueError, TypeError):
			total_units = lot_total_units
		
		if total_units == 0:
			total_units = sum(color_quantities.values())
		
		# Determine lot type
		is_mixed = len(color_quantities) > 1 or any(qty > 1 for qty in color_quantities.values())
		lot_type = 'mixed' if is_mixed else 'single'
		
		# Calculate unit cost
		unit_cost = total_cost / total_units if total_units > 0 else total_cost
		
		# Extract model from title or use from breakdown
		title = order_row.get(f'match{i}_title', '') or order_row.get('item_title', '')
		model = model_from_breakdown or extract_model_from_title(title)
		capacity = capacity_from_breakdown or extract_capacity_from_title(title)
		
		for color, qty in color_quantities.items():
			units.append({
				'order_id': order_id,
				'transaction_id': transaction_id,
				'item_id': item_id,
				'purchased_date': purchased_date,
				'model': model,
				'capacity': capacity,
				'color': color,
				'quantity': qty,
				'unit_cost': unit_cost,
				'asin': asin,
				'bsr': bsr,
				'net_per_unit': net_cost,  # This is already per-unit from match
				'lot_type': lot_type,
				'match_index': i
			})
	
	return units


def extract_model_from_title(title: str) -> str:
	"""Extract Canon model number from title (e.g., '045', '055H', 'CRG137')."""
	import re
	if not title:
		return ''
	
	# Match Canon model patterns
	patterns = [
		r'\b(CRG[-\s]?\d{2,3}[A-Z]?)\b',  # CRG054, CRG-137
		r'\b(\d{3}[HX]?)\b',  # 045, 055H, 046
		r'\bCanon\s+(\d{3}[HX]?)\b',  # Canon 045
	]
	
	for pattern in patterns:
		match = re.search(pattern, title, re.IGNORECASE)
		if match:
			return match.group(1).upper()
	
	return ''


def extract_capacity_from_title(title: str) -> str:
	"""Extract capacity (standard/high) from title."""
	if not title:
		return 'standard'
	
	title_lower = title.lower()
	if 'high' in title_lower or 'xl' in title_lower:
		return 'high'
	if any(c in title for c in ['H', 'X']) and any(char.isdigit() for char in title):
		# Check if model number ends with H or X (like 055H)
		import re
		if re.search(r'\d{3}[HX]\b', title, re.IGNORECASE):
			return 'high'
	return 'standard'


def populate_purchased_units_from_order_history() -> Tuple[int, int]:
	"""
	Scan all order_history rows and populate purchased_units table.
	Returns (total_inserted, total_skipped).
	"""
	conn = get_db_connection()
	conn.row_factory = sqlite3.Row
	cur = conn.cursor()
	
	cur.execute(f'SELECT * FROM {ORDER_HISTORY_TABLE}')
	rows = cur.fetchall()
	conn.close()
	
	all_units = []
	for row in rows:
		row_dict = dict(row)
		units = expand_order_to_purchased_units(row_dict)
		all_units.extend(units)
	
	if all_units:
		return insert_purchased_units_batch(all_units)
	return 0, 0


def get_unprocessed_orders() -> List[Dict[str, Any]]:
	"""
	Find order_history rows that have no corresponding purchased_units entries.
	
	Used for incremental backfill - only processes orders that haven't been
	matched yet. This handles:
	- Fresh DB after reset (all orders need processing)
	- New orders added since last backfill
	- Orders that failed to process previously
	
	Returns:
		List of order_history row dicts that need backfill processing
	"""
	conn = get_db_connection()
	conn.row_factory = sqlite3.Row
	cur = conn.cursor()
	
	# Find orders with no purchased_units entries
	# Using LEFT JOIN + NULL check is more efficient than NOT EXISTS for SQLite
	query = f'''
		SELECT oh.*
		FROM {ORDER_HISTORY_TABLE} oh
		LEFT JOIN {PURCHASED_UNITS_TABLE} pu 
			ON oh.order_id = pu.order_id AND oh.transaction_id = pu.transaction_id
		WHERE pu.id IS NULL
		ORDER BY oh.order_id
	'''
	
	cur.execute(query)
	rows = cur.fetchall()
	conn.close()
	
	return [dict(row) for row in rows]


def get_backfill_status() -> Dict[str, Any]:
	"""
	Get statistics on backfill status for monitoring/debugging.
	
	Returns:
		Dict with counts: total_orders, processed_orders, unprocessed_orders
	"""
	conn = get_db_connection()
	cur = conn.cursor()
	
	# Total orders
	cur.execute(f'SELECT COUNT(*) FROM {ORDER_HISTORY_TABLE}')
	total_orders = cur.fetchone()[0]
	
	# Orders with purchased_units (distinct by order_id + transaction_id)
	cur.execute(f'''
		SELECT COUNT(DISTINCT order_id || '-' || transaction_id) 
		FROM {PURCHASED_UNITS_TABLE}
	''')
	processed_orders = cur.fetchone()[0]
	
	conn.close()
	
	return {
		'total_orders': total_orders,
		'processed_orders': processed_orders,
		'unprocessed_orders': total_orders - processed_orders,
	}


# ============================================================================
# ANALYTICS QUERIES FOR PURCHASED UNITS
# ============================================================================

def get_avg_cost_by_model_color(model: Optional[str] = None, color: Optional[str] = None) -> List[Dict[str, Any]]:
	"""
	Get average unit cost grouped by model and color.
	Optional filters for specific model or color.
	"""
	conn = get_db_connection()
	conn.row_factory = sqlite3.Row
	cur = conn.cursor()
	
	query = f'''
		SELECT 
			model,
			color,
			SUM(quantity) as total_units,
			AVG(unit_cost) as avg_unit_cost,
			MIN(unit_cost) as min_unit_cost,
			MAX(unit_cost) as max_unit_cost,
			AVG(net_per_unit) as avg_net_per_unit,
			COUNT(DISTINCT order_id) as order_count
		FROM {PURCHASED_UNITS_TABLE}
		WHERE 1=1
	'''
	params = []
	
	if model:
		query += ' AND model = ?'
		params.append(model)
	if color:
		query += ' AND LOWER(color) = LOWER(?)'
		params.append(color)
	
	query += ' GROUP BY model, color ORDER BY model, color'
	
	cur.execute(query, params)
	rows = cur.fetchall()
	conn.close()
	return [dict(row) for row in rows]


def get_inventory_summary() -> List[Dict[str, Any]]:
	"""
	Get summary of all purchased units by model, capacity, and color.
	"""
	conn = get_db_connection()
	conn.row_factory = sqlite3.Row
	cur = conn.cursor()
	
	cur.execute(f'''
		SELECT 
			model,
			capacity,
			color,
			lot_type,
			SUM(quantity) as total_units,
			AVG(unit_cost) as avg_unit_cost,
			SUM(quantity * unit_cost) as total_spent,
			AVG(net_per_unit) as avg_net_per_unit,
			COUNT(DISTINCT order_id || '-' || transaction_id) as transaction_count
		FROM {PURCHASED_UNITS_TABLE}
		GROUP BY model, capacity, color, lot_type
		ORDER BY model, capacity, color, lot_type
	''')
	
	rows = cur.fetchall()
	conn.close()
	return [dict(row) for row in rows]


def get_cost_trend_by_date(model: Optional[str] = None, color: Optional[str] = None) -> List[Dict[str, Any]]:
	"""
	Get cost trend over time, optionally filtered by model/color.
	"""
	conn = get_db_connection()
	conn.row_factory = sqlite3.Row
	cur = conn.cursor()
	
	query = f'''
		SELECT 
			purchased_date,
			model,
			color,
			SUM(quantity) as units_purchased,
			AVG(unit_cost) as avg_unit_cost,
			SUM(quantity * unit_cost) as total_spent
		FROM {PURCHASED_UNITS_TABLE}
		WHERE purchased_date IS NOT NULL AND purchased_date != ''
	'''
	params = []
	
	if model:
		query += ' AND model = ?'
		params.append(model)
	if color:
		query += ' AND LOWER(color) = LOWER(?)'
		params.append(color)
	
	query += ' GROUP BY purchased_date, model, color ORDER BY purchased_date DESC'
	
	cur.execute(query, params)
	rows = cur.fetchall()
	conn.close()
	return [dict(row) for row in rows]


def get_profit_margin_analysis(model: Optional[str] = None) -> List[Dict[str, Any]]:
	"""
	Analyze profit margins by model and color.
	net_per_unit is the Amazon selling price minus fees.
	Profit = net_per_unit - unit_cost
	"""
	conn = get_db_connection()
	conn.row_factory = sqlite3.Row
	cur = conn.cursor()
	
	query = f'''
		SELECT 
			model,
			color,
			capacity,
			SUM(quantity) as total_units,
			AVG(unit_cost) as avg_cost,
			AVG(net_per_unit) as avg_net,
			AVG(net_per_unit - unit_cost) as avg_profit_per_unit,
			SUM(quantity * (net_per_unit - unit_cost)) as total_potential_profit,
			CASE 
				WHEN AVG(net_per_unit) > 0 
				THEN (AVG(net_per_unit - unit_cost) / AVG(net_per_unit)) * 100 
				ELSE 0 
			END as profit_margin_pct
		FROM {PURCHASED_UNITS_TABLE}
		WHERE net_per_unit > 0
	'''
	params = []
	
	if model:
		query += ' AND model = ?'
		params.append(model)
	
	query += ' GROUP BY model, color, capacity ORDER BY avg_profit_per_unit DESC'
	
	cur.execute(query, params)
	rows = cur.fetchall()
	conn.close()
	return [dict(row) for row in rows]


def get_lot_type_comparison() -> List[Dict[str, Any]]:
	"""
	Compare costs between single-color and mixed lots.
	"""
	conn = get_db_connection()
	conn.row_factory = sqlite3.Row
	cur = conn.cursor()
	
	cur.execute(f'''
		SELECT 
			lot_type,
			COUNT(DISTINCT order_id || '-' || transaction_id) as transaction_count,
			SUM(quantity) as total_units,
			AVG(unit_cost) as avg_unit_cost,
			AVG(net_per_unit) as avg_net_per_unit,
			AVG(net_per_unit - unit_cost) as avg_profit_per_unit
		FROM {PURCHASED_UNITS_TABLE}
		GROUP BY lot_type
	''')
	
	rows = cur.fetchall()
	conn.close()
	return [dict(row) for row in rows]
