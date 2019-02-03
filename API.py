import flask
from flask import jsonify
from sqlalchemy import *
from db_and_functions import engine
from flask import Flask, Markup, render_template
from pandas import DataFrame
app = flask.Flask(__name__)
app.config["DEBUG"] = True
app.config['JSON_SORT_KEYS'] = False
QUERYavg = """
        SELECT typ_pomiaru, jednostka_pomiaru, AVG(wartosc_pomiaru)
FROM pomiary
WHERE data >= NOW() - interval :interval AND id_czujnika=:id_czuj
GROUP BY typ_pomiaru, jednostka_pomiaru;"""


QUERY="""
        select dt.id,dt.id_lokalizacji,dt.dl_geograficzna,dt.szer_geograficzna, dt.distance, lokalizacja.kraj, lokalizacja.miasto, lokalizacja.ulica, lokalizacja.nr
from (
    select *, ( 6371 * acos( cos( radians(:szerokosc) ) * cos( radians( szer_geograficzna ) ) * cos( radians( dl_geograficzna ) - radians(:dlugosc) ) + sin( radians(:szerokosc) ) * sin( radians( szer_geograficzna ) ) ) ) as distance
    from czujniki
) as dt
LEFT JOIN lokalizacja ON dt.id_lokalizacji = lokalizacja.id
where dt.distance < :promien
order by distance asc;"""

QUERYavgALL="""
            select pomiary.typ_pomiaru, pomiary.jednostka_pomiaru, AVG(pomiary.wartosc_pomiaru)
from (
    select *, ( 6371 * acos( cos( radians(:szerokosc) ) * cos( radians( szer_geograficzna ) ) * cos( radians( dl_geograficzna ) - radians(:dlugosc) ) + sin( radians(:szerokosc) ) * sin( radians( szer_geograficzna ) ) ) ) as distance
    from czujniki
) as dt
LEFT JOIN pomiary ON dt.id = pomiary.id_czujnika
where data >= NOW() - interval :interval AND dt.distance < :promien
GROUP BY pomiary.typ_pomiaru, pomiary.jednostka_pomiaru;"""

QUERYavgHOUR="""
            select
  date_trunc('hour', data - interval '1 minute') as interv_start,
  date_trunc('hour', data - interval '1 minute')  + interval '1 hours' as interv_end,
  typ_pomiaru,
  jednostka_pomiaru,
 avg(wartosc_pomiaru)
  from pomiary
  	where data >= NOW() - interval :interval AND id_czujnika=:id_czuj
    	group by date_trunc('hour', data - interval '1 minute'), typ_pomiaru, jednostka_pomiaru
order by interv_start;"""

QUERYogl="""
        SELECT data,  wartosc_pomiaru, jednostka_pomiaru
  FROM pomiary
 where data >= NOW() - interval :interval AND id_czujnika= :id_czuj AND typ_pomiaru=:typ_pomiaru
 GROUP BY data,  wartosc_pomiaru, jednostka_pomiaru;"""


QUERYtypes="""
        SELECT distinct typ_pomiaru from pomiary
where id_czujnika=:id_czuj
"""

QueryWYKRES = '''

	select
  	date_trunc('hour', data - interval '1 minute') as interv_start,
  	date_trunc('hour', data - interval '1 minute')  + interval '1 hours' as interv_end,
  	typ_pomiaru,
 	avg(wartosc_pomiaru)
  	from pomiary
  		where data >= NOW() - interval :interval AND id_czujnika=:id_czuj and typ_pomiaru=:typ_pomiaru
    		group by date_trunc('hour', data - interval '1 minute'), typ_pomiaru
	order by interv_start;'''

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


@app.route('/', methods=['GET'])
def home():
    return '''<h1>Distant Reading Archive</h1>
<p>A prototype API for distant reading of science fiction novels.</p>'''


@app.route('/location_area/lat=<szerokosc>/lon=<dlugosc>/rad=<promien>', methods=['GET'])
def api_all(szerokosc, dlugosc, promien):
    result = engine.execute(text(QUERY), szerokosc=szerokosc, dlugosc=dlugosc, promien=promien)

    result_dict = {}
    counter = 1
    for row in result:
        result_dict[counter] = {
            "latitude": row['dl_geograficzna'],
            "longitude": row['szer_geograficzna'],
            "country": row['kraj'],
            "city": row['miasto'],
            "street": row['ulica'],
            "nr": row['nr'],
            "distance": row['distance'],
            "sensor_id": row['id']
        }
        counter += 1
    return jsonify(result_dict)

@app.errorhandler(404)
def page_not_found(e):
    return "<h1>404</h1><p>The resource could not be found.</p>", 404

@app.route('/average/interval=<interval>/id_czuj=<id_czuj>', methods=['GET'])
def api_alls(interval, id_czuj):
    result = engine.execute(text(QUERYavg), interval=interval, id_czuj=id_czuj)

    result_dict = {}
    counter = 1
    for row in result:
        result_dict[counter] = {
            "type": row['typ_pomiaru'],
            "avg": row['avg'],
            "unit": row['jednostka_pomiaru']

        }
        counter += 1
    return jsonify(result_dict)


@app.route('/average_all/lat=<szerokosc>/lon=<dlugosc>/rad=<promien>/interval=<interval>', methods=['GET'])
def api_allss(szerokosc, dlugosc, promien, interval):
    result = engine.execute(text(QUERYavgALL), szerokosc=szerokosc, dlugosc=dlugosc, promien=promien, interval=interval)
    result_dict = {}
    counter = 1
    for row in result:
        result_dict[counter] = {
            "type": row['typ_pomiaru'],
            "avg": row['avg'],
            "unit": row['jednostka_pomiaru']

        }
        counter += 1
    return jsonify(result_dict)


@app.route('/average_hour/interval=<interval>/id_czuj=<id_czuj>', methods=['GET'])
def api_allssd(id_czuj, interval):
    result = engine.execute(text(QUERYavgHOUR), id_czuj=id_czuj, interval=interval)
    result_dict = {}
    counter = 1
    for row in result:
        result_dict[counter] = {
            "interv_start": row['interv_start'],
            "interv_end": row['interv_end'],
            "type": row['typ_pomiaru'],
            "avg": row['avg'],
            "unit": row['jednostka_pomiaru']
            #"ulica": row['ulica'],
            #"nr": row['nr']
            #"distance": row['distance']
        }
        counter += 1
    return jsonify(result_dict)

@app.route('/data/id_czuj=<id_czuj>/interval=<interval>/typ_pomiaru=<typ_pomiaru>', methods=['GET'])
def api_data(id_czuj, interval, typ_pomiaru):
    result = engine.execute(text(QUERYogl), id_czuj=id_czuj, interval=interval, typ_pomiaru=typ_pomiaru)

    result_dict = {}
    counter = 1

    for row in result:
        result_dict[counter] = {
            "date": row['data'],
            "value": row['wartosc_pomiaru'],
            "unit": row['jednostka_pomiaru']
            #"ulica": row['ulica'],
            #"nr": row['nr']
            #"distance": row['distance']
        }
        counter += 1
    return jsonify(result_dict)


@app.route('/types/id_czuj=<id_czuj>', methods=['GET'])
def api_types(id_czuj):
    result = engine.execute(text(QUERYtypes), id_czuj=id_czuj)

    result_dict = {}
    counter = 1

    for row in result:
        result_dict[counter] = {

            "type": row['typ_pomiaru']

            # "ulica": row['ulica'],
            # "nr": row['nr']
            # "distance": row['distance']
        }
        counter += 1
    return jsonify(result_dict)


@app.route('/chart/id_czuj=<id_czuj>/interval=<interval>/typ_pomiaru=<typ_pomiaru>')
def line(interval, id_czuj, typ_pomiaru):

    result = engine.execute(text(QueryWYKRES), interval=interval, id_czuj=id_czuj, typ_pomiaru=typ_pomiaru)
    df = DataFrame(result.fetchall())
    valX = df[1]
    valY = df[3]
    maks = df.loc[df[3].idxmax()]
    mak = maks[3]
    zakres = mak + (0.05 * mak)
    labels = valX
    values = valY
    line_labels=labels
    line_values=values

    return render_template('line_chart.html', title='', max=zakres,  labels=line_labels, values=line_values)

@app.errorhandler(404)
def page_not_found(e):
    return "<h1>404</h1><p>The resource could not be found.</p>", 404
app.run()