import sqlalchemy
from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from geopy.geocoders import Nominatim
import requests
import math
import time
import geocoder
engine = create_engine('postgresql+psycopg2://postgres:post1@localhost:5432')
geolocator = Nominatim(timeout=100, user_agent='geo')
#for i in range(6500):
url = 'http://api.luftdaten.info/static/v1/data.json'
jsonH = requests.get(url).json()

'''insert danych do tabeli lokalizacja wraz z uzupełnieniem adresu czujnika'''

for i, result in enumerate(jsonH):
    row = {}

    id = result['location']['id']
    #dlugoscD = result['location']['longitude']
    szerokoscS = result['location']['latitude']
    if not result['location']['longitude']:
        dlugoscD = '0'
    else:
        dlugoscD = result['location']['longitude']
    print(dlugoscD)
    dlugosc = float((dlugoscD))
    szerokosc = float((szerokoscS))

    # g = geocoder.geocodefarm([52.509669, 13.376294], method='reverse', language='en')
    #g = geocoder.locationiq([szerokosc, dlugosc],key='d2550e071f4dbe', method='reverse', language='en')
   # json = g.json

    location = geolocator.reverse(szerokoscS + "," + dlugoscD, language='en')
    #print(dlugosc)
    #print(szerokosc)


    if ('country' in location.raw['address']):

        kraj = location.raw['address']['country']
        #print(kraj)
        if "'" in kraj:
            kraj=kraj.replace("'", "''")
    else:
        kraj='unavailable'
    if ('city' in location.raw['address']):

        miejscowosc = location.raw['address']['city']

        if "'" in miejscowosc:
            miejscowosc=miejscowosc.replace("'", "''")
    else:
        miejscowosc='unavailable'

    if ('house_number' in location.raw['address']):


        nr = location.raw['address']['house_number']
        if "'" in nr:
            nr=nr.replace("'", "''")
    else:
        nr='unavailable'

    if ('road' in location.raw['address']):

        ulica = location.raw['address']['road']

        if "'" in ulica:
            ulica=ulica.replace("'", "''")
        #print(ulica)
    else:
        ulica='unavailable'

    S = sessionmaker(bind=engine)
    s = S()
    s.execute("SELECT adresy(" + str(id)  + "," + "'" + str(kraj) + "'" + "," + "'" + miejscowosc + "'" + "," + "'" + ulica + "'" + "," + "'"+
        nr+ "'" + ")")
    s.commit()

    time.sleep(1.1)
