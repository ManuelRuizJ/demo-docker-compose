from flask import Flask, jsonify, request
import os
import time
import psycopg2
import redis
import psycopg2.extras

app = Flask(__name__)

DATABASE_URL = os.getenv('DATABASE_URL')
REDIS_HOST = os.getenv('REDIS_HOST', 'redis')

def wait_for_db(max_retires = 20):
    for _ in range(max_retires):
        try:
            conn = psycopg2.connect(DATABASE_URL)
            conn.close()
            return
        except Exception:
            time.sleep(1)
    raise RuntimeError("DB no respondio, esta muerto")

def init_db():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            email VARCHAR(100) UNIQUE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    conn.commit()
    cur.close()
    conn.close()

def incr_visits():
    r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
    return r.incr("visits")

@app.get("/")
def home():
    return jsonify({
        "message": "Hola desde Docker Compose!",
        "services": {
            "/health": "Verifica la salud de la aplicacion",
            "/visits": "Cuenta las visitas usando redis",
            "/users": "[GET] Lista todos los usuarios / [POST] Crea un nuevo usuario"
        }
    })

@app.get("/health")
def health():
    try:
        # Verificacr condicion de base de datos
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute("SELECT NOW();")
        now = cur.fetchone()[0]
        cur.close
        conn.close()

        # Verificar conexion a Redis
        r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
        pong = r.ping()

        return jsonify({
            "status": "ok",
            "db_time": str(now),
            "redis_ping": pong
        })
    except Exception as e:
        return jsonify({
            "statu": "error",
            "message": str(e)
        }), 500
    
@app.get("/visits")
def visits():
    try:
        r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
        count = r.get("visits")
        if count is None:
            count = 0
        return jsonify({
            "visits": int(count),
        })
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500
    
@app.post("/users")
def create_user():
    try:
        data = request.get_json()
        if not data or 'name' not in data or 'email' not in data:
            return jsonify({
                "error": "Faltan datos por enviar"
            }), 400
        
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO users (name, email) VALUES (%s, %s) RETURNING id, name, email, created_at;",
            (data['name'], data['email'])
        )
        new_user = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()

        visits_count = incr_visits()

        return jsonify({
            "message": "Usuario creado",
            "user": {
                "id": new_user[0],
                "name": new_user[1],
                "email": new_user[2],
                "created_at": new_user[3]
            },
            "visitas": visits_count
        }), 201
    except psycopg2.IntegrityError:
        return jsonify({
            "error": "El email ya existe"
        }), 400
    except Exception as e:
        return jsonify({
            "error": str(e)
        }), 500
    

@app.get("/users")
def list_users():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute("SELECT id, name, email, created_at FROM users ORDER BY id;")
        users = cur.fetchall()
        cur.close()
        conn.close()
        r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
        current_visits = r.get("visits") or 0

        # visits_count = incr_visits()

        return jsonify({
            "users": users,
            "total_visits": int(current_visits)
        })
    except Exception as e:
        return jsonify({
            "error": str(e)
        }), 500
    
if __name__ == "__main__":
    wait_for_db()
    init_db()
    app.run(host="0.0.0.0", port=8000)