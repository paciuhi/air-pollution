from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
import psycopg2

#tworzenie silnika połączenia z bazą danych:
engine = create_engine('postgresql+psycopg2://postgres:post1@localhost:5432')

metadata = MetaData()
metadata.reflect(bind=engine)

Base = declarative_base()
Session = sessionmaker(bind=engine)
session = Session()




#tworzenie tabel:
class Lokalizacja(Base):
    __tablename__ = "lokalizacja"
    id = Column(Integer, primary_key=True)
    kraj = Column(VARCHAR(55))
    miasto = Column(VARCHAR(60))
    ulica = Column(VARCHAR(100))
    nr = Column(VARCHAR(50))
    loki = relationship('Czujniki')

class Czujniki(Base):
    __tablename__ = "czujniki"
    id = Column(Integer, primary_key=True)
    id_lokalizacji = Column(Integer, ForeignKey('lokalizacja.id'))
    dl_geograficzna = Column(Float)
    szer_geograficzna = Column(Float)
    nazwa_serwisu = Column(VARCHAR(20))
    czujki = relationship('Pomiary')

class Pomiary(Base):
    __tablename__ = "pomiary"
    id = Column(Integer,  primary_key=True)
    id_czujnika = Column(Integer, ForeignKey('czujniki.id'))
    data = Column(DateTime(timezone=True))
    wartosc_pomiaru = Column(Float)
    typ_pomiaru = Column(VARCHAR(25))
    jednostka_pomiaru = Column(VARCHAR(10))

Lokalizacja.__table__.create(bind=engine, checkfirst=True)
Czujniki.__table__.create(bind=engine, checkfirst=True)
Pomiary.__table__.create(bind=engine, checkfirst=True)


#funkcje insert:

QUERY_lokalizacja="""
CREATE OR REPLACE FUNCTION add_to_lokalizacjaNOWE( _id INT, _kraj VARCHAR(55) ) RETURNS VOID AS $$
BEGIN																															   
	IF NOT EXISTS(SELECT d.id FROM lokalizacja d WHERE d.id=_id) THEN
		INSERT INTO lokalizacja VALUES (_id, _kraj);
    END IF;
END
$$
	LANGUAGE 'plpgsql';"""

QUERY_czujniki="""
CREATE OR REPLACE FUNCTION add_to_czujnikiNOWE( _id INT, _id_lokalizacji INT, _dl_geograficzna NUMERIC, _szer_geograficzna NUMERIC, _nazwa_serwisu VARCHAR(20) ) RETURNS VOID AS $$
BEGIN																															   
	IF NOT EXISTS(SELECT c.id FROM czujniki c WHERE c.id=_id) THEN
		INSERT INTO czujniki VALUES (_id, _id_lokalizacji, _dl_geograficzna, _szer_geograficzna, _nazwa_serwisu);
    END IF;
END
$$
	LANGUAGE 'plpgsql';"""

QUERY_pomiary="""
CREATE OR REPLACE FUNCTION add_to_pomiaryNOWE(
       _id_czujnika INT,
       _data TIMESTAMP WITH TIME ZONE,
       _wartosc_pomiaru NUMERIC,
       _typ_pomiaru VARCHAR(25),
       _jednostka_pomiaru VARCHAR(5))
RETURNS VOID AS $$
BEGIN
        INSERT INTO pomiary(
               id_czujnika,
               data,
               wartosc_pomiaru,
               typ_pomiaru,
	       jednostka_pomiaru
        )
        VALUES(
               _id_czujnika,
               _data,
               _wartosc_pomiaru,
               _typ_pomiaru,
               _jednostka_pomiaru
       )
       ON CONFLICT DO NOTHING;
END
$$
LANGUAGE 'plpgsql';"""

engine.execute(text(QUERY_lokalizacja))
engine.execute(text(QUERY_czujniki))
engine.execute(text(QUERY_pomiary))