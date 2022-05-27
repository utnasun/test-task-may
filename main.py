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
        ent_counter = 0
        ind_counter = 0
        supp_counter = 0
        os.chdir("data_xml")
        for filename in os.listdir("."):
            tree = ET.parse(filename)
            root = tree.getroot()
            ent_flag = 0
            ind_flag = 0
            for doc in root:
                for elem in doc:
                    if len(elem.attrib) == 2:  # юр.лицо
                        if 'ИННЮЛ' in elem.attrib.keys():
                            if ind_flag:
                                ind_flag = 0
                            ent_counter += 1
                            ent_flag = 1
                            # print(elem.attrib['ИННЮЛ'], elem.attrib['НаимОрг'])
                            cursor.execute(
                                "INSERT INTO entities (entity_id, entity_inn, entity_name) VALUES (%s, %s, %s)",
                                (ent_counter, elem.attrib['ИННЮЛ'], elem.attrib['НаимОрг'])
                            )
                            connection.commit()

                    if len(elem.attrib) == 1:  # физ.лицо
                        if 'ИННФЛ' in elem.attrib.keys():
                            ind_counter += 1
                            if ent_flag:
                                ent_flag = 0
                            ind_flag = 1
                            # print(elem.attrib['ИННФЛ'])
                            # print(elem[0].attrib['Фамилия'])
                            if len(elem[0].attrib) == 3:  # с отчеством
                                cursor.execute(
                                    "INSERT INTO individuals (ind_id, ind_inn, ind_initials) VALUES (%s, %s, %s)",
                                    (ind_counter, elem.attrib['ИННФЛ'], elem[0].attrib['Фамилия'] + " "
                                     + elem[0].attrib['Имя'] + " " + elem[0].attrib['Отчество'])
                                )
                                connection.commit()
                            if len(elem[0].attrib) == 2:  # без отчества
                                cursor.execute(
                                    "INSERT INTO individuals (ind_id, ind_inn, ind_initials) VALUES (%s, %s, %s)",
                                    (ind_counter, elem.attrib['ИННФЛ'], elem[0].attrib['Фамилия'] + " "
                                     + elem[0].attrib['Имя'])
                                )
                                connection.commit()
                    if len(elem.attrib) == 6:  # без даты прекращения
                        supp_counter += 1
                        # Есть некорректные данные без информации о нарушениях, их не используем
                        if 'ИнфНаруш' not in elem[3].attrib.keys():
                            break
                        if ent_flag:
                            cursor.execute(
                                "INSERT INTO supporters (supp_id, s_type, supp_org, supp_inn, category, date_term,"
                                " date_acceptance, code_form, supp_name, code_type, type_name, total, amount, "
                                "entity_id, ind_id, info_violation, info_inexpedient) "
                                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                                (supp_counter, elem.attrib['ВидПП'], elem.attrib['НаимОрг'], elem.attrib['ИННЮЛ'],
                                 elem.attrib['КатСуб'], elem.attrib['СрокПод'], elem.attrib['ДатаПрин'],
                                 elem[0].attrib['КодФорм'], elem[0].attrib['НаимФорм'],
                                 elem[1].attrib['КодВид'], elem[1].attrib['НаимВид'],
                                 elem[2].attrib['РазмПод'], elem[2].attrib['ЕдПод'],
                                 ent_counter, None,
                                 elem[3].attrib['ИнфНаруш'], elem[3].attrib['ИнфНецел'])
                            )
                            connection.commit()
                        if ind_flag:
                            cursor.execute(
                                "INSERT INTO supporters (supp_id, s_type, supp_org, supp_inn, category, date_term,"
                                " date_acceptance, code_form, supp_name, code_type, type_name, total, amount, "
                                "entity_id, ind_id, info_violation, info_inexpedient) "
                                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                                (supp_counter, elem.attrib['ВидПП'], elem.attrib['НаимОрг'], elem.attrib['ИННЮЛ'],
                                 elem.attrib['КатСуб'], elem.attrib['СрокПод'], elem.attrib['ДатаПрин'],
                                 elem[0].attrib['КодФорм'], elem[0].attrib['НаимФорм'],
                                 elem[1].attrib['КодВид'], elem[1].attrib['НаимВид'],
                                 elem[2].attrib['РазмПод'], elem[2].attrib['ЕдПод'],
                                 None, ind_counter,
                                 elem[3].attrib['ИнфНаруш'], elem[3].attrib['ИнфНецел'])
                            )
                            connection.commit()

                    if len(elem.attrib) == 7:  # с датой прекращения
                        supp_counter += 1
                        if 'ИнфНаруш' not in elem[3].attrib.keys():
                            break
                        if ent_flag:
                            cursor.execute(
                                "INSERT INTO supporters (supp_id, s_type, supp_org, supp_inn, category, date_term,"
                                "date_acceptance, date_end, code_form, supp_name, code_type, type_name, total, amount, "
                                "entity_id, ind_id, info_violation, info_inexpedient) "
                                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                                (supp_counter, elem.attrib['ВидПП'], elem.attrib['НаимОрг'], elem.attrib['ИННЮЛ'],
                                 elem.attrib['КатСуб'], elem.attrib['СрокПод'],
                                 elem.attrib['ДатаПрин'], elem.attrib['ДатаПрекр'],
                                 elem[0].attrib['КодФорм'], elem[0].attrib['НаимФорм'],
                                 elem[1].attrib['КодВид'], elem[1].attrib['НаимВид'],
                                 elem[2].attrib['РазмПод'], elem[2].attrib['ЕдПод'],
                                 ent_counter, None,
                                 elem[3].attrib['ИнфНаруш'], elem[3].attrib['ИнфНецел'])
                            )
                            connection.commit()
                        if ind_flag:
                            cursor.execute(
                                "INSERT INTO supporters (supp_id, s_type, supp_org, supp_inn, category, date_term,"
                                "date_acceptance, date_end, code_form, supp_name, code_type, type_name, total, amount, "
                                "entity_id, ind_id, info_violation, info_inexpedient) "
                                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                                (supp_counter, elem.attrib['ВидПП'], elem.attrib['НаимОрг'], elem.attrib['ИННЮЛ'],
                                 elem.attrib['КатСуб'], elem.attrib['СрокПод'],
                                 elem.attrib['ДатаПрин'], elem.attrib['ДатаПрекр'],
                                 elem[0].attrib['КодФорм'], elem[0].attrib['НаимФорм'],
                                 elem[1].attrib['КодВид'], elem[1].attrib['НаимВид'],
                                 elem[2].attrib['РазмПод'], elem[2].attrib['ЕдПод'],
                                 None, ind_counter,
                                 elem[3].attrib['ИнфНаруш'], elem[3].attrib['ИнфНецел'])
                            )
                            connection.commit()

except Exception as _ex:
    print(_ex)
    print("[INFO] Error while working with PostgreSQL")
finally:
    if connection:
        connection.close()
        print("[INFO] PostgreSQL connection closed")
