from flask import Flask, jsonify, render_template, request
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
from sqlalchemy import text

# Iniciar App
app = Flask(__name__)

# Configurar todo
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://postgres:postgres@localhost:5432/MDS7103-FootballData'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

with app.app_context():
    db.session.execute(text("SET search_path TO 'MDS7103-Data'"))
    db.session.commit()

# Funciones auxilaires 

def get_ligas_domesticas():
    q = text("SELECT name FROM competitions WHERE type = 'domestic_league' ORDER BY name")
    return [row[0] for row in db.session.execute(q)]

def get_temporadas_valoraciones():
    q = text("SELECT DISTINCT EXTRACT(YEAR FROM date) FROM player_valuations ORDER BY 1 DESC")
    return [int(row[0]) for row in db.session.execute(q)]

def get_temporadas_jugadas():
    q = text("SELECT DISTINCT season FROM games ORDER BY season DESC")
    return [int(row[0]) for row in db.session.execute(q)]

def get_nacionalidades():
    q = text("""
        SELECT DISTINCT country_of_citizenship
        FROM players
        WHERE country_of_citizenship IS NOT NULL
        ORDER BY 1
    """)
    return [row[0] for row in db.session.execute(q)]

def get_posiciones_raw():
    q = text("SELECT DISTINCT position FROM players WHERE position IS NOT NULL ORDER BY position")
    return [row[0] for row in db.session.execute(q)]

def get_clubes_por_liga(liga):
    q = text("""
        SELECT c.club_id, c.name
        FROM clubs c
        JOIN competitions comp ON comp.competition_id = c.domestic_competition_id
        WHERE comp.name = :liga
        ORDER BY c.name
    """)
    rows = db.session.execute(q, {"liga": liga}).fetchall()
    return [{"id": r.club_id, "name": r.name} for r in rows]

# último resultado

@app.route('/')
def index():
    result = {
        "home": "Colo Colo",
        "away": "Unión la Calera",
        "home_logo": "https://upload.wikimedia.org/wikipedia/en/thumb/b/be/Colo-Colo.svg/278px-Colo-Colo.svg.png",
        "away_logo": "https://upload.wikimedia.org/wikipedia/commons/thumb/2/29/Uni%C3%B3n_la_Calera_2020.png/330px-Uni%C3%B1_la_Calera_2020.png",
        "score": "4 - 1",
        "date": "2025-11-23"
    }

    return render_template('index.html', result=result, year=datetime.now().year)

# Obtener clubes por liga

@app.route('/clubs_by_league')
def clubs_by_league_json():
    liga = request.args.get("liga", "")
    return jsonify(get_clubes_por_liga(liga))

# Consulta 1

@app.route('/players', methods=['GET', 'POST'])
def players():
    error = None
    results = []

    nacionalidades = get_nacionalidades()
    posiciones_raw = get_posiciones_raw()
    ligas = get_ligas_domesticas()
    temporadas = get_temporadas_valoraciones()

    pos_labels = {
        "Goalkeeper": "Arquero",
        "Defender": "Defensa",
        "Midfield": "Mediocampo",
        "Forward": "Delantero",
        "Missing": "-",
        "Attack": "Delantero",
        "Midfielder": "Mediocampista"
    }

    selected = {k: None for k in ["temporada", "nacionalidad", "posicion", "liga", "monto"]}

    if request.method == 'POST':
        temporada = request.form.get('temporada', '').strip()
        nacionalidad = request.form.get('nacionalidad', '').strip()
        posicion = request.form.get('posicion', '').strip()
        liga = request.form.get('liga', '').strip()
        monto = request.form.get('monto', '').strip()

        selected.update({
            "temporada": temporada,
            "nacionalidad": nacionalidad,
            "posicion": posicion,
            "liga": liga,
            "monto": monto
        })

        if not all([temporada, nacionalidad, posicion, liga, monto]):
            error = "Completa todos los campos."
        else:
            try:
                query = text("""
                    WITH latest_value AS (
                        SELECT 
                            pv.player_id,
                            pv.market_value_in_eur,
                            ROW_NUMBER() OVER (
                                PARTITION BY pv.player_id
                                ORDER BY pv.date DESC
                            ) AS rn
                        FROM player_valuations pv
                        WHERE EXTRACT(YEAR FROM pv.date) = :temporada
                    ),
                    club_in_season AS (
                        SELECT
                            a.player_id,
                            c.name AS club_name,
                            comp.name AS league_name,
                            ROW_NUMBER() OVER (
                                PARTITION BY a.player_id
                                ORDER BY g.date DESC
                            ) AS rn
                        FROM appearances a
                        JOIN games g ON g.game_id = a.game_id
                        JOIN clubs c ON c.club_id = a.player_club_id
                        JOIN competitions comp ON comp.competition_id = g.competition_id
                        WHERE g.season = :temporada
                    )
                    SELECT
                        p.name,
                        p.country_of_citizenship,
                        p.position,
                        cs.club_name AS current_season_club,
                        lv.market_value_in_eur,
                        cs.league_name
                    FROM players p
                    JOIN latest_value lv ON lv.player_id = p.player_id AND lv.rn = 1
                    JOIN club_in_season cs ON cs.player_id = p.player_id AND cs.rn = 1
                    WHERE p.country_of_citizenship = :nacionalidad
                      AND p.position = :posicion
                      AND lv.market_value_in_eur > :monto
                      AND cs.league_name = :liga
                    ORDER BY lv.market_value_in_eur DESC;
                """)

                results = db.session.execute(query, {
                    "temporada": int(temporada),
                    "nacionalidad": nacionalidad,
                    "posicion": posicion,
                    "liga": liga,
                    "monto": float(monto)
                }).fetchall()

            except Exception as e:
                error = f"Error: {e}"

    return render_template(
        "players.html",
        results=results,
        error=error,
        nacionalidades=nacionalidades,
        posiciones=posiciones_raw,
        pos_labels=pos_labels,
        ligas=ligas,
        temporadas=temporadas,
        selected=selected
    )

@app.route('/transfers', methods=['GET', 'POST'])
def transfers():
    error = None
    results = []

    # Solo hasta 2024
    años = [
        int(row[0]) for row in db.session.execute(
            text("""
                SELECT DISTINCT EXTRACT(YEAR FROM transfer_date)
                FROM transfers
                WHERE EXTRACT(YEAR FROM transfer_date) <= 2024
                ORDER BY 1 DESC
            """)
        )
    ]

    ligas = get_ligas_domesticas()
    tipos = ["entrada", "salida", "ambos"]
    ordenes = ["valor", "fecha"]

    selected = {
        "año": None, 
        "liga": None, 
        "club_id": None, 
        "club_name": None,
        "tipo": None, 
        "orden": None
    }

    if request.method == "POST":
        año = request.form.get("año", "").strip()
        liga = request.form.get("liga", "").strip()
        club_id = request.form.get("club_id", "").strip()
        tipo = request.form.get("tipo", "").strip()
        orden = request.form.get("orden", "").strip()

        # Obtener nombre real del club para mostrarlo en el select
        club_name = None
        if club_id:
            row = db.session.execute(
                text("SELECT name FROM clubs WHERE club_id = :id"),
                {"id": int(club_id)}
            ).fetchone()
            club_name = row[0] if row else None

        selected.update({
            "año": año,
            "liga": liga,
            "club_id": club_id,
            "club_name": club_name,
            "tipo": tipo,
            "orden": orden
        })

        if not all([año, liga, club_id, tipo, orden]):
            error = "Completa todos los campos."
        else:
            query = text("""
                WITH club_league AS (
                    SELECT 
                        c.club_id,
                        c.name AS club_name,
                        comp.name AS league_name
                    FROM clubs c
                    JOIN competitions comp 
                        ON comp.competition_id = c.domestic_competition_id
                    WHERE comp.type = 'domestic_league'
                ),
                transfer_clean AS (
                    SELECT 
                        t.player_name,
                        t.transfer_date,
                        t.transfer_fee,
                        t.from_club_id,
                        t.to_club_id
                    FROM transfers t
                    WHERE EXTRACT(YEAR FROM t.transfer_date) = :año
                      AND EXTRACT(YEAR FROM t.transfer_date) <= 2024
                )
                SELECT
                    tc.player_name,
                    clf.club_name AS from_club,
                    clt.club_name AS to_club,
                    tc.transfer_date,
                    tc.transfer_fee
                FROM transfer_clean tc
                LEFT JOIN club_league clf ON clf.club_id = tc.from_club_id
                LEFT JOIN club_league clt ON clt.club_id = tc.to_club_id
                WHERE
                    (:liga = clf.league_name OR :liga = clt.league_name)
                    AND (
                        (:tipo = 'entrada' AND tc.to_club_id = :club_id)
                        OR 
                        (:tipo = 'salida' AND tc.from_club_id = :club_id)
                        OR 
                        -- CORRECCIÓN APLICADA AQUÍ:
                        (:tipo = 'ambos' AND (tc.to_club_id = :club_id OR tc.from_club_id = :club_id))
                    )
                ORDER BY
                    CASE WHEN :orden = 'valor' THEN tc.transfer_fee END DESC,
                    CASE WHEN :orden = 'fecha' THEN tc.transfer_date END DESC;
            """)

            results = db.session.execute(query, {
                "año": int(año),
                "liga": liga,
                "club_id": int(club_id),
                "tipo": tipo,
                "orden": orden
            }).fetchall()

            if len(results) == 0:
                error = f"No se encontraron transferencias para {liga} ({año})."

    return render_template(
        "transfers.html",
        results=results,
        años=años,
        ligas=ligas,
        tipos=tipos,
        ordenes=ordenes,
        selected=selected,
        error=error
    )

# Consulta 3

@app.route('/comparador', methods=['GET', 'POST'])
def comparador():
    error = None
    results = []
    mensaje = None

    ligas = get_ligas_domesticas()
    temporadas = get_temporadas_jugadas()

    clubs = [row[0] for row in db.session.execute(text("SELECT name FROM clubs ORDER BY name"))]

    selected = {"temporada": None, "liga": None, "club1": None, "club2": None}

    if request.method == "POST":
        temporada = request.form.get("temporada", "").strip()
        liga = request.form.get("liga", "").strip()
        club1 = request.form.get("club1", "").strip()
        club2 = request.form.get("club2", "").strip()

        selected.update({
            "temporada": temporada,
            "liga": liga,
            "club1": club1,
            "club2": club2
        })

        if not all([temporada, liga, club1, club2]):
            error = "Completa todos los campos."
        elif club1 == club2:
            error = "Debes escoger dos clubes distintos."
        else:
            query = text("""
                WITH latest_player_value AS (
                    SELECT
                        pv.player_id,
                        pv.market_value_in_eur,
                        ROW_NUMBER() OVER (
                            PARTITION BY pv.player_id
                            ORDER BY pv.date DESC
                        ) AS rn
                    FROM player_valuations pv
                ),
                club_squad_value AS (
                    SELECT
                        p.current_club_id AS club_id,
                        SUM(lpv.market_value_in_eur) AS squad_value
                    FROM players p
                    LEFT JOIN latest_player_value lpv
                        ON lpv.player_id = p.player_id AND lpv.rn = 1
                    GROUP BY p.current_club_id
                ),
                club_stats AS (
                    SELECT
                        cl.club_id,
                        cl.name AS club_name,
                        SUM(
                            CASE
                                WHEN g.home_club_id = cl.club_id THEN g.home_club_goals
                                WHEN g.away_club_id = cl.club_id THEN g.away_club_goals
                            END
                        ) AS goals_for,
                        SUM(
                            CASE
                                WHEN g.home_club_id = cl.club_id THEN g.away_club_goals
                                WHEN g.away_club_id = cl.club_id THEN g.home_club_goals
                            END
                        ) AS goals_against
                    FROM clubs cl
                    JOIN games g
                        ON g.season = :season
                       AND g.competition_id = (
                            SELECT competition_id
                            FROM competitions
                            WHERE name = :league
                       )
                       AND (g.home_club_id = cl.club_id OR g.away_club_id = cl.club_id)
                    GROUP BY cl.club_id, cl.name
                )
                SELECT
                    cs.club_name AS club,
                    cs.goals_for,
                    cs.goals_against,
                    csv.squad_value
                FROM club_stats cs
                LEFT JOIN club_squad_value csv ON csv.club_id = cs.club_id
                WHERE cs.club_name IN (:club1, :club2)
                ORDER BY cs.club_name;
            """)

            results = db.session.execute(query, {
                "season": int(temporada),
                "league": liga,
                "club1": club1,
                "club2": club2
            }).fetchall()

            if len(results) == 0:
                mensaje = f"Estos clubes no se enfrentaron durante el año {temporada}."
            elif len(results) == 1:
                falta = club1 if results[0].club != club1 else club2
                mensaje = f"El club {falta} no registró partidos en {temporada}. Prueba buscando otro!"

    return render_template(
        "comparador.html",
        error=error,
        mensaje=mensaje,
        results=results,
        ligas=ligas,
        temporadas=temporadas,
        clubs=clubs,
        selected=selected
    )

# Consulta 4

@app.route('/top-players', methods=['GET', 'POST'])
def top_players():
    error = None
    results = []

    ligas = get_ligas_domesticas()
    temporadas = get_temporadas_jugadas()
    ordenes = ["ASC", "DESC"]

    selected = {"liga": None, "temporada": None, "orden": None, "n": None}

    if request.method == "POST":
        liga = request.form.get("liga", "").strip()
        temporada = request.form.get("temporada", "").strip()
        orden = request.form.get("orden", "").strip()
        n = request.form.get("n", "").strip()

        selected.update({
            "liga": liga,
            "temporada": temporada,
            "orden": orden,
            "n": n
        })

        if not all([liga, temporada, orden, n]):
            error = "Completa todos los campos."
        else:
            try:
                query = text(f"""
                    SELECT
                        p.player_id,
                        p.name,
                        SUM(a.goals) AS goals,
                        c.name AS club
                    FROM appearances a
                    JOIN players p ON p.player_id = a.player_id
                    JOIN clubs c ON c.club_id = a.player_club_id
                    JOIN games g ON g.game_id = a.game_id
                    JOIN competitions comp ON comp.competition_id = g.competition_id
                    WHERE comp.name = :league
                      AND g.season = :season
                    GROUP BY p.player_id, p.name, c.name
                    ORDER BY goals {orden}
                    LIMIT :N;
                """)

                results = db.session.execute(query, {
                    "league": liga,
                    "season": int(temporada),
                    "N": int(n)
                }).fetchall()

            except Exception as e:
                error = f"Error al ejecutar consulta: {e}"

    return render_template(
        "top_players.html",
        ligas=ligas,
        temporadas=temporadas,
        ordenes=ordenes,
        results=results,
        selected=selected,
        error=error
    )


if __name__ == '__main__':
    app.run(debug=True)
