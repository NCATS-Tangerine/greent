import os

def write_block(prefix,outf):
    tablename = 'omnicorp.{}'.format(prefix)
    outf.write('CREATE TABLE {} ( pubmedid int, curie varchar(255) );\n'.format(tablename))
    outf.write("\copy {} FROM '../../omnicorp/{}' DELIMITER E'\\t' CSV\n".format(tablename,prefix))
    outf.write("CREATE INDEX ON {} (pubmedid);\n".format(tablename))
    outf.write("CREATE INDEX ON {} (curie);\n".format(tablename))

with open('create_omnicorp.sql','w') as sqlfile:
    sqlfile.write('CREATE SCHEMA omnicorp;\n')
    files = os.listdir('../../omnicorp')
    for prefix in files:
        write_block(prefix,sqlfile)

#python generate_omnicorp_table_script.py
#psql -U murphy robokop -f create_omnicorp.sql
