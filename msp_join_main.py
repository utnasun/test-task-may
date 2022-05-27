import psycopg2


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
        # Берем объединение тех юр.лиц, кто получал помощь
        cursor.execute(
            "CREATE TABLE msp_ent_with_support AS SELECT * FROM msp_entities_sschr "
            "JOIN entities ON entities.entity_inn = msp_entities_sschr.msp_ent_inn"
        )
        connection.commit()
        # Берем объединение тех физ.лиц, кто получал помощь
        cursor.execute(
            "CREATE TABLE msp_ind_with_support AS SELECT * FROM msp_individuals_sschr "
            "JOIN individuals ON individuals.ind_inn = msp_individuals_sschr.msp_ind_inn"
        )
        connection.commit()
        # Берем только тех юр.лиц, которые не получали помощь
        cursor.execute(
            "CREATE TABLE msp_ent_without_support AS SELECT * FROM msp_entities_sschr "
            "LEFT JOIN entities ON msp_entities_sschr.msp_ent_inn = entities.entity_inn"
            " WHERE entities.entity_inn IS NULL;"
        )
        connection.commit()
        # Берем только тех физ.лиц, которые не получали помощь
        cursor.execute(
            "CREATE TABLE msp_ind_without_support AS SELECT * FROM msp_individuals_sschr "
            "LEFT JOIN individuals ON individuals.ind_inn = msp_individuals_sschr.msp_ind_inn"
            " WHERE individuals.ind_inn IS NULL;"
        )
        connection.commit()
except Exception as _ex:
    print(_ex)
    print("[INFO] Error while working with PostgreSQL")
finally:
    if connection:
        connection.close()
        print("[INFO] PostgreSQL connection closed")
