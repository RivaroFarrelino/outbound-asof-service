import logging
import os
import re
import secrets
import sys
import time
from datetime import date, datetime
from decimal import Decimal
from functools import wraps

import mysql.connector
from flask import Flask, jsonify, request

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger('outbound-asof')

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = int(os.environ.get('MAX_BODY_MB', 64)) * 1024 * 1024

API_TOKEN = os.environ.get('API_TOKEN', '')
DB_NAME = os.environ.get('MYSQL_DATABASE', 'outbound_asof')
CHUNK = int(os.environ.get('INSERT_CHUNK', 1000))

if not re.fullmatch(r'[A-Za-z0-9_]+', DB_NAME):
    raise SystemExit('MYSQL_DATABASE tidak valid')

BASE_CONFIG = {
    'host': os.environ.get('MYSQL_HOST', 'localhost'),
    'port': int(os.environ.get('MYSQL_PORT', 3306)),
    'user': os.environ.get('MYSQL_USER', 'root'),
    'password': os.environ.get('MYSQL_PASSWORD', ''),
    'connection_timeout': int(os.environ.get('MYSQL_CONNECT_TIMEOUT', 30)),
    'charset': 'utf8mb4',
    'collation': 'utf8mb4_unicode_ci'
}

TABLE_OPTS = 'ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci'

SCHEMA = [
    f"""CREATE TABLE IF NOT EXISTS items (
        item_no VARCHAR(50) NOT NULL PRIMARY KEY,
        per_berat DECIMAL(18,6) NOT NULL DEFAULT 0,
        panjang DECIMAL(18,4) NOT NULL DEFAULT 0,
        unit_cost DECIMAL(20,5) NOT NULL DEFAULT 0,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) {TABLE_OPTS}""",
    f"""CREATE TABLE IF NOT EXISTS wh_entries (
        entry_no BIGINT NOT NULL PRIMARY KEY,
        reg_date DATE NOT NULL,
        location_code VARCHAR(20) NOT NULL,
        bin_code VARCHAR(30) NOT NULL,
        brand VARCHAR(30) NOT NULL,
        item_no VARCHAR(50) NOT NULL,
        qty DECIMAL(20,5) NOT NULL,
        KEY idx_wh_date_brand (reg_date, brand),
        KEY idx_wh_item (item_no)
    ) {TABLE_OPTS}""",
    f"""CREATE TABLE IF NOT EXISTS ile_entries (
        entry_no BIGINT NOT NULL PRIMARY KEY,
        posting_date DATE NOT NULL,
        location_code VARCHAR(20) NOT NULL,
        kategori VARCHAR(10) NOT NULL,
        item_no VARCHAR(50) NOT NULL,
        qty DECIMAL(20,5) NOT NULL,
        KEY idx_ile_kat_date (kategori, posting_date, location_code),
        KEY idx_ile_item (item_no)
    ) {TABLE_OPTS}""",
    f"""CREATE TABLE IF NOT EXISTS summary (
        cutoff_date DATE NOT NULL,
        report VARCHAR(20) NOT NULL,
        group_key VARCHAR(50) NOT NULL,
        total_berat DECIMAL(24,4) NULL,
        total_amount DECIMAL(24,2) NOT NULL,
        entry_count INT NOT NULL,
        generated_at DATETIME NOT NULL,
        PRIMARY KEY (cutoff_date, report, group_key),
        KEY idx_summary_report (report, cutoff_date)
    ) {TABLE_OPTS}""",
    f"""CREATE TABLE IF NOT EXISTS sync_log (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        run_id VARCHAR(40) NOT NULL,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        level VARCHAR(10) NOT NULL,
        step VARCHAR(20) NOT NULL,
        message VARCHAR(1000) NULL,
        row_count INT NULL,
        duration_ms INT NULL,
        KEY idx_log_run (run_id),
        KEY idx_log_created (created_at)
    ) {TABLE_OPTS}"""
]

TABLES = {
    'items': ['item_no', 'per_berat', 'panjang', 'unit_cost'],
    'wh_entries': ['entry_no', 'reg_date', 'location_code', 'bin_code', 'brand', 'item_no', 'qty'],
    'ile_entries': ['entry_no', 'posting_date', 'location_code', 'kategori', 'item_no', 'qty']
}

ENTRY_TABLES = {'wh_entries', 'ile_entries'}
DATE_RE = re.compile(r'^\d{4}-\d{2}-\d{2}$')

BERAT = 'SUM(x.qty * COALESCE(i.per_berat,0) * COALESCE(i.panjang,0) / 1000)'
AMOUNT = 'SUM(x.qty * COALESCE(i.unit_cost,0))'
INSERT_HEAD = 'INSERT INTO summary (cutoff_date, report, group_key, total_berat, total_amount, entry_count, generated_at) '
FROM_WH = 'FROM wh_entries x LEFT JOIN items i ON i.item_no = x.item_no WHERE x.reg_date BETWEEN %s AND %s '


def from_ile(kat):
    return f"FROM ile_entries x LEFT JOIN items i ON i.item_no = x.item_no WHERE x.kategori = '{kat}' AND x.posting_date BETWEEN %s AND %s "


def select_head(code, berat, key):
    return f"SELECT %s, '{code}', {key}, {berat}, {AMOUNT}, COUNT(*), NOW() "


SNAPSHOT = [
    (False, INSERT_HEAD + select_head('MF_ASOF', BERAT, 'x.brand') + FROM_WH + 'GROUP BY x.brand'),
    (True, INSERT_HEAD + select_head('MF_OUTBOUND', BERAT, 'x.brand') + FROM_WH + 'AND x.qty < 0 GROUP BY x.brand'),
    (False, INSERT_HEAD + select_head('WARNA_ASOF', BERAT, 'x.location_code') + from_ile('WARNA') + 'GROUP BY x.location_code'),
    (True, INSERT_HEAD + select_head('WARNA_OUTBOUND', BERAT, 'x.location_code') + from_ile('WARNA') + 'AND x.qty < 0 GROUP BY x.location_code'),
    (False, INSERT_HEAD + select_head('SITE_ASOF', BERAT, 'x.location_code') + from_ile('SITE') + 'GROUP BY x.location_code'),
    (True, INSERT_HEAD + select_head('ACC_OUTBOUND', 'NULL', 'x.location_code') + from_ile('ACC') + 'AND x.qty < 0 GROUP BY x.location_code')
]


class ApiError(Exception):
    def __init__(self, message, status=400):
        super().__init__(message)
        self.status = status


def connect(with_db=True):
    cfg = dict(BASE_CONFIG)
    if with_db:
        cfg['database'] = DB_NAME
    return mysql.connector.connect(**cfg)


def plain(value):
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value.strftime('%Y-%m-%d %H:%M:%S')
    if isinstance(value, date):
        return value.strftime('%Y-%m-%d')
    return value


def fetch_all(cursor):
    cols = [c[0] for c in cursor.description]
    return [{k: plain(v) for k, v in zip(cols, row)} for row in cursor.fetchall()]


def write_log(cursor, run_id, level, step, message, rows=None, ms=None):
    cursor.execute(
        'INSERT INTO sync_log (run_id, level, step, message, row_count, duration_ms) VALUES (%s,%s,%s,%s,%s,%s)',
        (str(run_id or '-')[:40], str(level)[:10], str(step)[:20], str(message or '')[:1000], rows, ms)
    )


def butuh_token(view):
    @wraps(view)
    def pembungkus(*args, **kwargs):
        if not API_TOKEN:
            return jsonify({'ok': False, 'error': 'API_TOKEN belum diatur di server'}), 503
        if not secrets.compare_digest(request.headers.get('X-API-Token', ''), API_TOKEN):
            return jsonify({'ok': False, 'error': 'unauthorized'}), 401
        mulai = time.time()
        try:
            hasil = view(*args, **kwargs)
            logger.info('%s %s OK %dms', request.method, request.path, int((time.time() - mulai) * 1000))
            return hasil
        except ApiError as error:
            logger.warning('%s %s DITOLAK :: %s', request.method, request.path, error)
            return jsonify({'ok': False, 'error': str(error)}), error.status
        except mysql.connector.Error as error:
            logger.exception('%s %s DB ERROR', request.method, request.path)
            return jsonify({'ok': False, 'error': 'db: ' + str(error)[:500]}), 500
        except Exception as error:
            logger.exception('%s %s ERROR', request.method, request.path)
            return jsonify({'ok': False, 'error': str(error)[:500]}), 500
    return pembungkus


def body():
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        raise ApiError('body harus JSON object')
    return data


def valid_dates(values):
    if not isinstance(values, list) or not all(isinstance(v, str) and DATE_RE.match(v) for v in values):
        raise ApiError('format tanggal harus YYYY-MM-DD')
    return values


@app.route('/api/setup', methods=['POST'])
@butuh_token
def api_setup():
    conn = connect(with_db=False)
    try:
        cur = conn.cursor()
        cur.execute(f'CREATE DATABASE IF NOT EXISTS `{DB_NAME}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci')
        cur.execute(f'USE `{DB_NAME}`')
        for sql in SCHEMA:
            cur.execute(sql)
        conn.commit()
    finally:
        conn.close()
    logger.info('setup schema OK db=%s', DB_NAME)
    return jsonify({'ok': True, 'database': DB_NAME})


@app.route('/api/upsert', methods=['POST'])
@butuh_token
def api_upsert():
    data = body()
    table = data.get('table')
    rows = data.get('rows')
    if table not in TABLES:
        raise ApiError('table tidak dikenal')
    cols = TABLES[table]
    if not isinstance(rows, list) or any(not isinstance(r, list) or len(r) != len(cols) for r in rows):
        raise ApiError(f'rows harus list of list dengan {len(cols)} kolom')
    mulai = time.time()
    conn = connect()
    try:
        cur = conn.cursor()
        if rows:
            sql = (f"INSERT INTO {table} ({','.join(cols)}) VALUES ({','.join(['%s'] * len(cols))}) "
                   f"ON DUPLICATE KEY UPDATE {','.join(c + '=VALUES(' + c + ')' for c in cols[1:])}")
            for i in range(0, len(rows), CHUNK):
                cur.executemany(sql, rows[i:i + CHUNK])
        ms = int((time.time() - mulai) * 1000)
        write_log(cur, data.get('run_id'), 'DEBUG', data.get('step') or table, f'upsert {table}', len(rows), ms)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    logger.info('upsert %s rows=%d %dms run=%s', table, len(rows), ms, data.get('run_id'))
    return jsonify({'ok': True, 'rows': len(rows), 'ms': ms})


@app.route('/api/max-entry', methods=['GET'])
@butuh_token
def api_max_entry():
    table = request.args.get('table')
    if table not in ENTRY_TABLES:
        raise ApiError('table tidak dikenal')
    conn = connect()
    try:
        cur = conn.cursor()
        cur.execute(f'SELECT COALESCE(MAX(entry_no),0) FROM {table}')
        nilai = int(cur.fetchone()[0])
    finally:
        conn.close()
    return jsonify({'ok': True, 'max': nilai})


@app.route('/api/cutoffs', methods=['GET'])
@butuh_token
def api_cutoffs():
    conn = connect()
    try:
        cur = conn.cursor()
        cur.execute('SELECT DISTINCT cutoff_date FROM summary ORDER BY cutoff_date DESC')
        cutoffs = [plain(r[0]) for r in cur.fetchall()]
    finally:
        conn.close()
    return jsonify({'ok': True, 'cutoffs': cutoffs})


@app.route('/api/snapshot', methods=['POST'])
@butuh_token
def api_snapshot():
    data = body()
    cutoffs = valid_dates(data.get('cutoffs'))
    start = valid_dates([data.get('start_date')])[0]
    mulai = time.time()
    hasil = {}
    conn = connect()
    try:
        cur = conn.cursor()
        for c in cutoffs:
            month_start = c[:8] + '01'
            cur.execute('DELETE FROM summary WHERE cutoff_date = %s', (c,))
            total = 0
            for outbound, sql in SNAPSHOT:
                cur.execute(sql, (c, month_start, c) if outbound else (c, start, c))
                total += cur.rowcount
            hasil[c] = total
        ms = int((time.time() - mulai) * 1000)
        write_log(cur, data.get('run_id'), 'INFO', 'SNAPSHOT', 'snapshot ' + ','.join(f'{k}={v}' for k, v in hasil.items()), sum(hasil.values()), ms)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    logger.info('snapshot %s %dms run=%s', hasil, ms, data.get('run_id'))
    return jsonify({'ok': True, 'rows': hasil, 'ms': ms})


@app.route('/api/log', methods=['POST'])
@butuh_token
def api_log():
    data = body()
    conn = connect()
    try:
        cur = conn.cursor()
        write_log(cur, data.get('run_id'), data.get('level') or 'INFO', data.get('step') or '-', data.get('message'),
                  data.get('rows'), data.get('ms'))
        conn.commit()
    finally:
        conn.close()
    return jsonify({'ok': True})


@app.route('/api/init', methods=['GET'])
@butuh_token
def api_init():
    conn = connect()
    try:
        cur = conn.cursor()
        cur.execute('SELECT DISTINCT cutoff_date FROM summary ORDER BY cutoff_date DESC')
        cutoffs = [plain(r[0]) for r in cur.fetchall()]
        cur.execute('SELECT cutoff_date AS c, report AS r, SUM(total_berat) AS b, SUM(total_amount) AS a '
                    'FROM summary GROUP BY cutoff_date, report ORDER BY cutoff_date')
        trend = fetch_all(cur)
    finally:
        conn.close()
    return jsonify({'ok': True, 'cutoffs': cutoffs, 'trend': trend})


@app.route('/api/dashboard', methods=['GET'])
@butuh_token
def api_dashboard():
    cutoff = valid_dates([request.args.get('cutoff', '')])[0]
    conn = connect()
    try:
        cur = conn.cursor()
        cur.execute('SELECT report, group_key, total_berat, total_amount, entry_count, generated_at FROM summary '
                    'WHERE cutoff_date = %s ORDER BY report, ABS(total_amount) DESC', (cutoff,))
        rows = fetch_all(cur)
    finally:
        conn.close()
    data = {}
    generated = None
    for r in rows:
        data.setdefault(r['report'], []).append({'k': r['group_key'], 'b': r['total_berat'], 'a': r['total_amount'], 'n': r['entry_count']})
        if not generated or r['generated_at'] > generated:
            generated = r['generated_at']
    return jsonify({'ok': True, 'cutoff': cutoff, 'generatedAt': generated, 'data': data})


@app.route('/health', methods=['GET'])
def health_check():
    try:
        conn = connect(with_db=False)
        conn.close()
        return jsonify({'status': 'ok', 'database': 'connected'})
    except Exception as error:
        logger.error('health check gagal: %s', error)
        return jsonify({'status': 'degraded', 'database': 'tidak bisa dihubungi'}), 503


if not API_TOKEN:
    logger.error('API_TOKEN belum diatur, semua endpoint /api akan menolak request')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
