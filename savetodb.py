import psycopg2
import csv

connection = psycopg2.connect(host='localhost',
                              user='postgres',
                              password='mysecretpassword',
                              database='pokemon')

with connection:
    with connection.cursor() as cursor:

        # ポケモンテーブルを初期化
        sql = "DROP TABLE IF EXISTS monsters;"
        cursor.execute(sql)

        sql = "CREATE TABLE monsters (\
                id serial PRIMARY KEY, \
                pokedex_number smallint, \
                name varchar(255), \
                base_total smallint, \
                attack smallint, \
                defense smallint, \
                sp_attack smallint, \
                sp_defense smallint, \
                speed smallint, \
                hp smallint \
                );"

        cursor.execute(sql)

        # タイプテーブルを初期化
        sql = "DROP TABLE IF EXISTS types;"
        cursor.execute(sql)

        sql = "CREATE TABLE types (\
                id serial PRIMARY KEY, \
                pokedex_number smallint, \
                type varchar(255) \
                );"

        cursor.execute(sql)

        # ポケモンテーブルにデータを挿入
        with open('pokemon.csv') as f:
            for row in csv.reader(f):
                monsterinfo = row[:9]
                typeinfo = row[-2:]

                if monsterinfo[0] == 'pokedex_number':
                    continue

                sql = "INSERT INTO monsters (pokedex_number, name, base_total, attack, defense, sp_attack, sp_defense, speed, hp) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
                cursor.execute(sql, monsterinfo)

                for t in typeinfo:
                    if t == '':
                        continue

                    sql = "INSERT INTO types (pokedex_number, type) VALUES (%s, %s)"
                    cursor.execute(sql, (monsterinfo[0], t))

        # 相性テーブルを初期化
        sql = "DROP TABLE IF EXISTS affinities;"
        cursor.execute(sql)

        sql = "CREATE TABLE affinities (\
                id serial PRIMARY KEY, \
                attacker varchar(255), \
                defender varchar(255), \
                magnifier real \
                );"

        cursor.execute(sql)

        # 相性テーブルにデータ挿入
        with open('affinity.csv') as f:
            header = []
            for i, row in enumerate(csv.reader(f)):
                if i == 0:
                    header = [r.strip() for r in row]
                    continue

                t0 = row[0].strip()
                for t1 in header[1:]:
                    t1 = t1.strip()
                    mag = float(row[header.index(t1)])

                    sql = "INSERT INTO affinities (attacker, defender, magnifier) VALUES (%s, %s, %s)"

                    cursor.execute(sql, (t0, t1, mag))

    connection.commit()
