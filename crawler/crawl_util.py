from ftplib import FTP
from greent.util import Text
from builder.question import LabeledID
from io import BytesIO
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
def get_variant_list(rosetta: object) -> list:
    # get a connection to the graph database
    db_conn = rosetta.type_graph.driver

    # init the returned variant id list
    var_list = []

    # open a db session
    with db_conn.session() as session:
        # execute the query, get the results
        response = session.run('match (a:sequence_variant) return distinct a.id as id limit 10')

    # did we get a valid response
    if response is not None:
        # de-queue the returned data into a list for iteration
        rows = list(response)

        # go through each record and save only what we need (the id) into a simple array
        for r in rows:
            # append the id to the list
            var_list.append(r[0])

    # return the simple array to the caller
    return var_list


#######
# process_variant_annotation_cache - processes an array of un-cached variant nodes.
#######
def prepopulate_variant_annotation_cache(cache, myvariant, batch_of_nodes: list):
    # get a batch of variants
    batch_annotations = myvariant.batch_sequence_variant_to_gene(batch_of_nodes)

    # open a connection to the redis cache DB
    with cache.redis.pipeline() as redis_pipe:
        # for each variant
        for seq_var_curie, annotations in batch_annotations.items():
            # assemble the redis key
            key = f'myvariant.sequence_variant_to_gene({seq_var_curie})'

            # add the key and data to the list to execute
            redis_pipe.set(key, pickle.dumps(annotations))

        # write the records out to the cache DB
        redis_pipe.execute()
