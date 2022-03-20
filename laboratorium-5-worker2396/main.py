import numpy as np
import pickle

import psycopg2 as pg
import pandas.io.sql as psql
import pandas as pd

from typing import Union, List, Tuple

connection = pg.connect(host='pgsql-196447.vipserv.org', port=5432, dbname='wbauer_adb', user='wbauer_adb', password='adb2020');

def film_in_category(category:Union[int,str])->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o tytuł filmu, język, oraz kategorię dla zadanego:
        - id: jeżeli categry jest int
        - name: jeżeli category jest str, dokładnie taki jak podana wartość
    Przykład wynikowej tabeli:
    |   |title          |languge    |category|
    |0	|Amadeus Holy	|English	|Action|
    
    Tabela wynikowa ma być posortowana po tylule filmu i języku.
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
    
    Parameters:
    category (int,str): wartość kategorii po id (jeżeli typ int) lub nazwie (jeżeli typ str)  dla którego wykonujemy zapytanie
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if type(category) == int:
        df = pd.read_sql("SELECT film.title title, language.name languge, category.name category FROM film " +
                         "INNER JOIN film_category ON film.film_id = film_category.film_id " +
                         "INNER JOIN category ON film_category.category_id = category.category_id " +
                         "INNER JOIN language ON film.language_id = language.language_id " +
                         "WHERE category.category_id = '{x}' ".format(x=category) +
                         "ORDER BY film.title, language.name", con=connection)
        return df
    elif type(category) == str:
        df = pd.read_sql("Select film.title title, language.name languge, category.name category from film " +
                         "INNER JOIN film_category ON film.film_id = film_category.film_id " +
                         "INNER JOIN category ON film_category.category_id = category.category_id " +
                         "INNER JOIN language ON film.language_id = language.language_id " +
                         "WHERE category.name ~ '^{x}$' ".format(x=category) +
                         "ORDER BY film.title, language.name", con=connection)
        return df
    else:
        return None
    
def film_in_category_case_insensitive(category:Union[int,str])->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o tytuł filmu, język, oraz kategorię dla zadanego:
        - id: jeżeli categry jest int
        - name: jeżeli category jest str
    Przykład wynikowej tabeli:
    |   |title          |languge    |category|
    |0	|Amadeus Holy	|English	|Action|
    
    Tabela wynikowa ma być posortowana po tylule filmu i języku.
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
    
    Parameters:
    category (int,str): wartość kategorii po id (jeżeli typ int) lub nazwie (jeżeli typ str)  dla którego wykonujemy zapytanie
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if type(category) == int:
        df = pd.read_sql("Select film.title title, language.name languge, category.name category from film " +
                         "INNER JOIN film_category ON film.film_id = film_category.film_id " +
                         "INNER JOIN category ON film_category.category_id = category.category_id " +
                         "INNER JOIN language ON film.language_id = language.language_id " +
                         "WHERE category.category_id = '{x}' ".format(x=category) +
                         "ORDER BY film.title, language.name", con=connection)

        return df
    elif type(category) == str:
        df = pd.read_sql("Select film.title title, language.name languge, category.name category from film " +                         
                         "INNER JOIN film_category ON film.film_id = film_category.film_id " +
                         "INNER JOIN category ON film_category.category_id = category.category_id " +
                         "INNER JOIN language ON film.language_id = language.language_id " +
                         "WHERE category.name ~* '^{x}$' ".format(x=category) +
                         "ORDER BY film.title, language.name", con=connection)
        return df

    else:
        return None
    
def film_cast(title:str)->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o obsadę filmu o dokładnie zadanym tytule.
    Przykład wynikowej tabeli:
    |   |first_name |last_name  |
    |0	|Greg       |Chaplin    | 
    
    Tabela wynikowa ma być posortowana po nazwisku i imieniu klienta.
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    title (int): wartość id kategorii dla którego wykonujemy zapytanie
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if type(title) == str:
        df = pd.read_sql("SELECT actor.first_name, actor.last_name FROM film " +
                         "INNER JOIN film_actor ON film.film_id = film_actor.film_id " +
                         "INNER JOIN actor ON film_actor.actor_id = actor.actor_id " +
                         "WHERE film.title ~ '^{x}$' ".format(x=title) +
                         "ORDER BY actor.last_name, actor.first_name", con=connection)

        return df
    else:
        return None
    

def film_title_case_insensitive(words:list) :
    ''' Funkcja zwracająca wynik zapytania do bazy o tytuły filmów zawierających conajmniej jedno z podanych słów z listy words.
    Przykład wynikowej tabeli:
    |   |title              |
    |0	|Crystal Breaking 	| 
    
    Tabela wynikowa ma być posortowana po nazwisku i imieniu klienta.

    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    words(list): wartość minimalnej długości filmu
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if type(words) == list:
        regex = ''
        for el in words:
            regex += el + '|'
        regex = regex[:-1]
        regex = '(^| )(' + regex + ')($| )'

        df = pd.read_sql("Select title from film " +
                         "WHERE film.title ~* '{x}' ".format(x=regex) +
                         "ORDER BY film.title", con=connection)
        return df
    else:
        return None