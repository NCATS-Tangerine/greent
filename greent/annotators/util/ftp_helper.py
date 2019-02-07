from ftplib import FTP
from io import BytesIO
from json import loads
from gzip import decompress

def pull_via_ftp(ftpsite, ftpdir, ftpfile):
    ftp = FTP(ftpsite)
    ftp.login()
    ftp.cwd(ftpdir)
    with BytesIO() as data:
        ftp.retrbinary(f'RETR {ftpfile}', data.write)
        binary = data.getvalue()
    ftp.quit()
    return binary

def pull_hgnc_json():
    """Get the HGNC json file & convert to python"""
    data = pull_via_ftp('ftp.ebi.ac.uk', '/pub/databases/genenames/new/json', 'hgnc_complete_set.json')
    hgnc_json = loads( data.decode() )
    return hgnc_json
