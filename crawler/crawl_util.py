from ftplib import FTP
from greent.util import Text
from builder.question import LabeledID
from io import BytesIO
from greent.rosetta import Rosetta

import pickle

def pull_via_ftp(ftpsite, ftpdir, ftpfile):
    ftp = FTP(ftpsite)
    ftp.login()
    ftp.cwd(ftpdir)
    with BytesIO() as data:
        ftp.retrbinary(f'RETR {ftpfile}', data.write)
        binary = data.getvalue()
    ftp.quit()
    return binary

def glom(conc_set, newgroups, unique_prefixes=[]):
    """We want to construct sets containing equivalent identifiers.
    conc_set is a dictionary where the values are these equivalent identifier sets and
    the keys are all of the elements in the set.   For each element in a set, there is a key
    in the dictionary that points to the set.
    newgroups is an iterable that of new equivalence groups (expressed as sets,tuples,or lists)
    with which we want to update conc_set."""
    for group in newgroups:
        #Find all the equivalence sets that already correspond to any of the identifiers in the new set.
        p=False
        existing_sets = [ conc_set[x] for x in group if x in conc_set ]
        if p:
            print(existing_sets)
        #All of these sets are now going to be combined through the equivalence of our new set.
        newset=set().union(*existing_sets)
        if p:
            print(newset)
        #put all the new stuff in it.  Do it element-wise, cause we don't know the type of the new group
        for element in group:
            newset.add(element)
        #make sure we didn't combine anything we want to keep separate
        setok = True
        for up in unique_prefixes:
            if len([1 for e in newset if e.startswith(up)]) > 1:
                setok = False
                break
        if not setok:
            continue
        #Now make all the elements point to this new set:
        if p:
            print(newset)
        for element in newset:
            conc_set[element] = newset

def dump_cache(concord,rosetta,outf=None):
    for element in concord:
        if isinstance(element,LabeledID):
            element_id = element.identifier
        else:
            element_id = element
        key = f"synonymize({Text.upper_curie(element_id)})"
        value = concord[element]
        if outf is not None:
            outf.write(f'{key}: {value}\n')
        rosetta.cache.set(key,value)


############
# Gets a simple array of sequence variant ids
#
# param: Rosetts object
# returns: a list of sequence variant IDs
############
def get_variant_list(rosetta: Rosetta, limit: int = None) -> list:
    # get a connection to the graph database
    db_conn = rosetta.type_graph.driver

    # init the returned variant id list
    var_list = []

    # open a db session
    with db_conn.session() as session:
        # this query will get the node id and synonymized inro
        query = 'match (s:sequence_variant) return distinct s.id, s.equivalent_identifiers'

        # if we got an optional limit of returned data
        if limit is not None:
            query += f' limit {limit}'

        # execute the query, get the results
        response = session.run(query)

    # did we get a valid response
    if response is not None:
        # de-queue the returned data into a list for iteration
        rows = list(response)

        # go through each record and save only what we need (id, synonymizations) into a simple list
        for r in rows:
            # append only the data we need to the returned list
            var_list.append([r[0], r[1]])

    # return the simple array to the caller
    return var_list

