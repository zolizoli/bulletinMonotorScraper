from sqlalchemy import create_engine, MetaData, event
from sqlalchemy.sql import text


def copy_db():
    src_engine = create_engine("sqlite:///bulletin.db")
    src_metadata = MetaData(bind=src_engine)

    tgt_engine = create_engine("sqlite:///bulletin_search.db")
    tgt_metadata = MetaData(bind=tgt_engine)


    @event.listens_for(src_metadata, "column_reflect")
    def genericize_datatypes(inspector, tablename, column_dict):
        column_dict["type"] = column_dict["type"].as_generic(allow_nulltype=True)

    tgt_conn = tgt_engine.connect()
    tgt_metadata.reflect()

    tgt_metadata.clear()
    tgt_metadata.reflect()
    src_metadata.reflect()

    for table in src_metadata.sorted_tables:
        table.create(bind=tgt_engine)

    tgt_metadata.clear()
    tgt_metadata.reflect()

    for table in tgt_metadata.sorted_tables:
        src_table = src_metadata.tables[table.name]
        stmt = table.insert()
        for index, row in enumerate(src_table.select().execute()):
            print("table =", table.name, "Inserting row", index)
            stmt.execute(row._asdict())
    try:
        statement = text(
            """CREATE VIRTUAL TABLE FULLTEXTS USING FTS5(text, lemmatized, tokenize="unicode61 remove_diacritics 2")""")
        tgt_conn.execute(statement)
    except Exception as e:
        print(e)
        pass
    tgt_conn.close()
