import xml.etree.ElementTree as ET
import psycopg2
import os

try:
    # подключение к ДБ
    connection = psycopg2.connect(
        user="postgres",
        password="12345",
        host="localhost",
        port="5432",
        database="db_ersmsp"
    )
    with connection.cursor() as cursor:
        msp_ent_counter = 0
        msp_ind_counter = 0
        os.chdir("msp_xml")
        for filename in os.listdir("."):
            tree = ET.parse(filename)
            root = tree.getroot()
            for doc in root:
                # юр.лица
                if 'ИННЮЛ' in doc[0].attrib.keys():
                    msp_ent_counter += 1
                    if 'ССЧР' in doc.attrib.keys():
                        cursor.execute(
                            "INSERT INTO msp_entities_sschr (msp_ent_id, date_inclusion, msp_ent_name, msp_ent_sschr, "
                            "msp_ent_inn, msp_ent_ogrn) VALUES (%s, %s, %s, %s, %s, %s)",
                            (msp_ent_counter, doc.attrib['ДатаВклМСП'], doc[0].attrib['НаимОрг'], doc.attrib['ССЧР'],
                             doc[0].attrib['ИННЮЛ'], doc[0].attrib['ОГРН'])
                        )
                        connection.commit()
                    else:
                        cursor.execute(
                            "INSERT INTO msp_entities_sschr (msp_ent_id, date_inclusion, msp_ent_name, msp_ent_sschr, "
                            "msp_ent_inn, msp_ent_ogrn) VALUES (%s, %s, %s, %s, %s, %s)",
                            (msp_ent_counter, doc.attrib['ДатаВклМСП'], doc[0].attrib['НаимОрг'], None,
                             doc[0].attrib['ИННЮЛ'], doc[0].attrib['ОГРН'])
                        )
                        connection.commit()
                if 'ИННФЛ' in doc[0].attrib.keys():
                    msp_ind_counter += 1
                    if len(doc[0][0].attrib) == 3:  # с отчеством
                        if 'ССЧР' in doc.attrib.keys():
                            cursor.execute(
                                "INSERT INTO msp_individuals_sschr(msp_ind_id, date_inclusion, msp_ind_initials,"
                                " msp_ind_sschr, msp_ind_inn, msp_ind_ogrnip) VALUES (%s, %s, %s, %s, %s, %s)",
                                (msp_ind_counter, doc.attrib['ДатаВклМСП'], doc[0][0].attrib['Фамилия'] + " "
                                 + doc[0][0].attrib['Имя'] + " " + doc[0][0].attrib['Отчество'], doc.attrib['ССЧР'],
                                 doc[0].attrib['ИННФЛ'], doc[0].attrib['ОГРНИП'])
                            )
                            connection.commit()
                        else:
                            cursor.execute(
                                "INSERT INTO msp_individuals_sschr(msp_ind_id, date_inclusion, msp_ind_initials, "
                                "msp_ind_sschr, msp_ind_inn, msp_ind_ogrnip) VALUES (%s, %s, %s, %s, %s, %s)",
                                (msp_ind_counter, doc.attrib['ДатаВклМСП'], doc[0][0].attrib['Фамилия'] + " "
                                 + doc[0][0].attrib['Имя'] + " " + doc[0][0].attrib['Отчество'], None,
                                 doc[0].attrib['ИННФЛ'], doc[0].attrib['ОГРНИП'])
                            )
                            connection.commit()
                    else:
                        if 'ССЧР' in doc.attrib.keys():
                            cursor.execute(
                                "INSERT INTO msp_individuals_sschr(msp_ind_id, date_inclusion, msp_ind_initials,"
                                " msp_ind_sschr, msp_ind_inn, msp_ind_ogrnip) VALUES (%s, %s, %s, %s, %s, %s)",
                                (msp_ind_counter, doc.attrib['ДатаВклМСП'], doc[0][0].attrib['Фамилия'] + " "
                                 + doc[0][0].attrib['Имя'], doc.attrib['ССЧР'],
                                 doc[0].attrib['ИННФЛ'], doc[0].attrib['ОГРНИП'])
                            )
                            connection.commit()
                        else:
                            cursor.execute(
                                "INSERT INTO msp_individuals_sschr(msp_ind_id, date_inclusion, msp_ind_initials, "
                                "msp_ind_sschr, msp_ind_inn, msp_ind_ogrnip) VALUES (%s, %s, %s, %s, %s, %s)",
                                (msp_ind_counter, doc.attrib['ДатаВклМСП'], doc[0][0].attrib['Фамилия'] + " "
                                 + doc[0][0].attrib['Имя'], None,
                                 doc[0].attrib['ИННФЛ'], doc[0].attrib['ОГРНИП'])
                            )
                            connection.commit()

except Exception as _ex:
    print(_ex)
    print("[INFO] Error while working with PostgreSQL")
finally:
    if connection:
        connection.close()
        print("[INFO] PostgreSQL connection closed")
