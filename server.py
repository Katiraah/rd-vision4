import os
from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
import psycopg2.extras

app = Flask(__name__)
CORS(app)

DB_CONFIG = {
    "host":     os.environ.get("DB_HOST",     "pg-26875239-tooshadrack60.f.aivencloud.com"),
    "port":     os.environ.get("DB_PORT",     "13258"),
    "dbname":   os.environ.get("DB_NAME",     "defaultdb"),
    "user":     os.environ.get("DB_USER",     "avnadmin"),
    "password": os.environ.get("DB_PASSWORD", "AVNS_-aSNA6qAzg6h5-yFqZA"),
    "sslmode":  os.environ.get("DB_SSLMODE",  "require")
}

def get_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"❌ Database connection error: {e}")
        return None

@app.route("/api/health")
def health():
    return jsonify({"status": "ok", "message": "Road Vision API is running"})

@app.route("/api/roads")
def get_roads():
    conn = get_connection()
    if not conn:
        return jsonify({"success": False, "error": "Database connection failed"}), 500
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            WITH latest_inspections AS (
                SELECT road_gid, MAX(timestamp::date) AS latest_date
                FROM parc.roadpothole GROUP BY road_gid
            ),
            current_counts AS (
                SELECT p.road_gid, COUNT(p.id) AS pothole_count
                FROM parc.roadpothole p
                JOIN latest_inspections li ON p.road_gid = li.road_gid
                    AND p.timestamp::date = li.latest_date
                GROUP BY p.road_gid
            )
            SELECT
                r.gid, r.roadname, r.roadtype, r.roadagency, r.roadcode,
                r.roadclass, r.county,
                ROUND(r.length_km::numeric, 2) AS length_km,
                ROUND(r.length_m::numeric, 0)  AS length_m,
                COALESCE(cc.pothole_count, 0)  AS pothole_count,
                CASE WHEN r.length_km > 0
                     THEN ROUND((COALESCE(cc.pothole_count,0)/NULLIF(r.length_km,0))::numeric,2)
                     ELSE 0 END AS density_per_km,
                CASE WHEN r.length_km > 0
                     THEN ROUND(GREATEST(0,1.0-(COALESCE(cc.pothole_count,0)/NULLIF(r.length_km,0))/10.0)::numeric,2)
                     ELSE 1.0 END AS rci_value,
                CASE WHEN r.length_km>0 AND (COALESCE(cc.pothole_count,0)/NULLIF(r.length_km,0))=0    THEN 'Perfect'
                     WHEN r.length_km>0 AND (COALESCE(cc.pothole_count,0)/NULLIF(r.length_km,0))<=2.0 THEN 'Good'
                     WHEN r.length_km>0 AND (COALESCE(cc.pothole_count,0)/NULLIF(r.length_km,0))<=6.0 THEN 'Average'
                     ELSE 'Poor' END AS condition,
                CASE WHEN r.length_km>0 AND (COALESCE(cc.pothole_count,0)/NULLIF(r.length_km,0))=0    THEN '#228B22'
                     WHEN r.length_km>0 AND (COALESCE(cc.pothole_count,0)/NULLIF(r.length_km,0))<=2.0 THEN '#32CD32'
                     WHEN r.length_km>0 AND (COALESCE(cc.pothole_count,0)/NULLIF(r.length_km,0))<=6.0 THEN '#FFA500'
                     ELSE '#FF0000' END AS road_color,
                ST_AsGeoJSON(r.geom)::json AS geometry
            FROM parc.jujaroads r
            LEFT JOIN current_counts cc ON r.gid = cc.road_gid
            WHERE r.geom IS NOT NULL ORDER BY r.roadname ASC
        """)
        rows = cur.fetchall()
        cur.close(); conn.close()
        return jsonify({"success": True, "roads": [dict(r) for r in rows]})
    except Exception as e:
        print(f"❌ Error in get_roads: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/stats")
def get_stats():
    conn = get_connection()
    if not conn:
        return jsonify({"success": False, "error": "Database connection failed"}), 500
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT COUNT(*) as count FROM parc.jujaroads WHERE geom IS NOT NULL")
        total_roads = cur.fetchone()['count']
        cur.execute("""
            WITH latest_inspections AS (
                SELECT road_gid, MAX(timestamp::date) AS latest_date
                FROM parc.roadpothole GROUP BY road_gid
            ),
            current_counts AS (
                SELECT p.road_gid, COUNT(p.id) AS pothole_count
                FROM parc.roadpothole p
                JOIN latest_inspections li ON p.road_gid=li.road_gid
                    AND p.timestamp::date=li.latest_date
                GROUP BY p.road_gid
            )
            SELECT
                SUM(COALESCE(cc.pothole_count,0)) as total_potholes,
                COUNT(CASE WHEN (COALESCE(cc.pothole_count,0)/NULLIF(r.length_km,0))>6.0 THEN 1 END) as poor_roads,
                COUNT(CASE WHEN (COALESCE(cc.pothole_count,0)/NULLIF(r.length_km,0))>2.0
                           AND (COALESCE(cc.pothole_count,0)/NULLIF(r.length_km,0))<=6.0 THEN 1 END) as average_roads,
                COUNT(CASE WHEN COALESCE(cc.pothole_count,0)>0
                           AND (COALESCE(cc.pothole_count,0)/NULLIF(r.length_km,0))<=2.0 THEN 1 END) as good_roads,
                COUNT(CASE WHEN COALESCE(cc.pothole_count,0)=0 THEN 1 END) as perfect_roads
            FROM parc.jujaroads r
            LEFT JOIN current_counts cc ON r.gid=cc.road_gid
            WHERE r.length_km>0
        """)
        stats = cur.fetchone()
        cur.close(); conn.close()
        return jsonify({"success": True, "stats": {
            "total_roads":    total_roads or 0,
            "total_potholes": stats['total_potholes'] or 0,
            "poor_roads":     stats['poor_roads'] or 0,
            "average_roads":  stats['average_roads'] or 0,
            "good_roads":     stats['good_roads'] or 0,
            "perfect_roads":  stats['perfect_roads'] or 0
        }})
    except Exception as e:
        print(f"❌ Error in get_stats: {e}")
        import traceback; traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/juja_boundary")
def get_juja_boundary():
    conn = get_connection()
    if not conn:
        return jsonify({"success": False, "error": "Database connection failed"}), 500
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT ST_AsGeoJSON(ST_Collect(geom))::json as boundary_geojson FROM parc.juja WHERE geom IS NOT NULL")
        row = cur.fetchone()
        cur.close(); conn.close()
        if row and row['boundary_geojson']:
            return jsonify({"success": True, "boundary": row['boundary_geojson']})
        return jsonify({"success": False, "error": "No boundary data found"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/timeline")
def get_timeline():
    try:
        road_gid  = request.args.get('road_gid')
        date_from = request.args.get('from', '2020-01-01')
        date_to   = request.args.get('to',   '2099-12-31')
        if not road_gid:
            return jsonify({"success": False, "error": "road_gid is required"}), 400
        conn = get_connection()
        if not conn:
            return jsonify({"success": False, "error": "Database connection failed"}), 500
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT p.timestamp::date AS period, COUNT(p.id) AS pothole_count,
                CASE WHEN r.length_km>0
                     THEN ROUND(GREATEST(0,1.0-(COUNT(p.id)::float/r.length_km)/10.0)::numeric,2)
                     ELSE 1.0 END AS rci_value
            FROM parc.roadpothole p
            JOIN parc.jujaroads r ON p.road_gid=r.gid
            WHERE p.road_gid=%s AND p.timestamp>=%s AND p.timestamp<=%s
            GROUP BY p.timestamp::date, r.length_km ORDER BY period ASC
        """, (road_gid, date_from, date_to))
        timeline_points = cur.fetchall()
        cur.execute("""
            SELECT COUNT(p.id) AS current_potholes FROM parc.roadpothole p
            WHERE p.road_gid=%s AND p.timestamp>=%s AND p.timestamp<=%s
              AND p.timestamp::date=(
                  SELECT MAX(timestamp::date) FROM parc.roadpothole
                  WHERE road_gid=%s AND timestamp>=%s AND timestamp<=%s)
        """, (road_gid, date_from, date_to, road_gid, date_from, date_to))
        cr = cur.fetchone()
        cur.close(); conn.close()
        return jsonify({"success": True, "points": [dict(r) for r in timeline_points],
                        "current_potholes": cr['current_potholes'] if cr else 0})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/analysis")
def get_analysis():
    conn = get_connection()
    if not conn:
        return jsonify({"success": False, "error": "Database connection failed"}), 500
    try:
        road_gid  = request.args.get('road_gid')
        date_from = request.args.get('from', '2020-01-01')
        date_to   = request.args.get('to',   '2099-12-31')
        if not road_gid:
            return jsonify({"success": False, "error": "road_gid is required"}), 400

        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Road info
        cur.execute("""
            SELECT gid, roadname, roadtype, roadagency, roadcode, roadclass, length_km
            FROM parc.jujaroads WHERE gid = %s
        """, (road_gid,))
        road_info = cur.fetchone()
        if not road_info:
            return jsonify({"success": False, "error": "Road not found"}), 404

        # Monthly condition data
        cur.execute("""
            SELECT
                DATE_TRUNC('month', p.timestamp)      AS month,
                COUNT(p.id)                           AS pothole_count,
                r.length_km,
                CASE
                    WHEN r.length_km > 0 AND COUNT(p.id) = 0                               THEN 'Perfect'
                    WHEN r.length_km > 0 AND (COUNT(p.id)::float / r.length_km) <= 2.0     THEN 'Good'
                    WHEN r.length_km > 0 AND (COUNT(p.id)::float / r.length_km) <= 6.0     THEN 'Average'
                    ELSE 'Poor'
                END AS condition,
                ROUND(GREATEST(0, 1.0-(COUNT(p.id)::float/r.length_km)/10.0)::numeric, 2)  AS rci_value,
                ROUND((COUNT(p.id)::float / NULLIF(r.length_km,0))::numeric, 2)             AS density_per_km
            FROM parc.roadpothole p
            JOIN parc.jujaroads r ON p.road_gid = r.gid
            WHERE p.road_gid = %s
              AND p.timestamp >= %s
              AND p.timestamp <= %s
            GROUP BY DATE_TRUNC('month', p.timestamp), r.length_km, r.gid
            ORDER BY month ASC
        """, (road_gid, date_from, date_to))
        rows = cur.fetchall()
        cur.close(); conn.close()

        condition_counts = {'Perfect': 0, 'Good': 0, 'Average': 0, 'Poor': 0}
        for row in rows:
            c = row['condition']
            condition_counts[c] = condition_counts.get(c, 0) + 1

        dominant_condition = max(condition_counts, key=condition_counts.get) if rows else 'N/A'

        data = []
        for row in rows:
            potholes = int(row['pothole_count'])
            density  = float(row['density_per_km']) if row['density_per_km'] else 0
            rci      = float(row['rci_value'])       if row['rci_value']      else 1.0
            condition= row['condition']

            data.append({
                'month':         row['month'].strftime('%Y-%m'),
                'month_display': row['month'].strftime('%b %Y'),
                'potholes':      potholes,
                'density':       density,
                'rci':           rci,
                'condition':     condition
            })

        return jsonify({
            "success":            True,
            "road_info": {
                "name":      road_info['roadname'],
                "type":      road_info['roadtype'],
                "agency":    road_info['roadagency'],
                "code":      road_info['roadcode'],
                "class":     road_info['roadclass'],
                "length_km": float(road_info['length_km']) if road_info['length_km'] else 0
            },
            "data":               data,
            "months_tracked":     len(data),
            "dominant_condition": dominant_condition,
            "condition_counts":   condition_counts
        })
    except Exception as e:
        print(f"❌ Error in get_analysis: {e}")
        import traceback; traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    print(f"🚀 Starting Road Vision API on port {port}...")
    app.run(host="0.0.0.0", port=port)
