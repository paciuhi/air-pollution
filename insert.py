from db_and_functions import engine
import requests
from sqlalchemy.orm import sessionmaker

url = 'http://api.luftdaten.info/static/v1/data.json'
json = requests.get(url).json()

# lokalizacja = []
# for i, result in enumerate(json):
#     row = {}
#     #row['id'] = i
#     row['id'] = result['location']['id']
#     row['kraj'] = result['location']['country']
#     S = sessionmaker(bind=engine)
#     s = S()
#     s.execute("SELECT add_to_lokalizacjaNOWE(" + str(row['id'])+","+"'"+str(row['kraj'])+"'"+ ")")
#     s.commit()
'''insert danych'''
czujniki = []
for i, result in enumerate(json):
    row = {}
    row['id'] = result['sensor']['id']
    row['id_lokalizacji'] = result['location']['id']
    #print(result['location']['longitude'])

    row['szer_geograficzna'] = result['location']['latitude']
    row['nazwa_serwisu'] = 'luftdaten.info'
    if not result['location']['longitude']:
        row['dl_geograficzna'] = 0
    else:
        row['dl_geograficzna'] =  result['location']['longitude']
    print(row['dl_geograficzna'])

    S = sessionmaker(bind=engine)
    s = S()
    s.execute("SELECT add_to_czujnikiNOWE(" + str(row['id']) + "," + str(row['id_lokalizacji']) + ","+"'" + str(
        row['dl_geograficzna'])+"'" + "," + str(row['szer_geograficzna']) + "," + "'" + str(
        row['nazwa_serwisu']) + "'" + ")")
    s.commit()
pomiary = []
for i, result in enumerate(json):
    idCzujnika = result['sensor']['id']
    data = result['timestamp']
    for data_item in result['sensordatavalues']:
        row = {}
        row['id_czujnika'] = idCzujnika
        row['data'] = data
        row['wartosc_pomiaru'] = data_item['value']
        #jednostka = 'jed'
        row['typ_pomiaru'] = data_item['value_type']
        if "P1" in row['typ_pomiaru']:
            row['typ_pomiaru'] = row['typ_pomiaru'].replace("P1", "PM10")
        if "P2" in row['typ_pomiaru']:
            row['typ_pomiaru'] = row['typ_pomiaru'].replace("P2", "PM2.5")

        if row['typ_pomiaru'] == "PM10" or row['typ_pomiaru'] == "PM2.5":
            jednostka = 'µg/m3'
        if row['typ_pomiaru'] == "pressure" or row['typ_pomiaru'] ==  "pressure_at_sealevel":
            jednostka = 'Pa'
        if row['typ_pomiaru'] == "humidity":
            jednostka = '%'
        if row['typ_pomiaru'] == "temperature":
            jednostka = '°C'



        S = sessionmaker(bind=engine)
        s = S()
        s.execute("CREATE UNIQUE INDEX IF NOT EXISTS pomiar_uidx ON pomiary(id_czujnika, data, wartosc_pomiaru, typ_pomiaru, jednostka_pomiaru);")
        s.execute("SELECT add_to_pomiaryNOWE(" +str(row['id_czujnika']) + ","  + "'"+ str(row['data']) + "'" + "," + str(
            row['wartosc_pomiaru']) + "," + "'" + str(row['typ_pomiaru']) + "'" + "," + "'" + str(jednostka) + "'" + ")")
        s.commit()
