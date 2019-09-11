from gzip import decompress, GzipFile
from greent.util import LoggingUtil, Text
from greent.graph_components import LabeledID
import logging
import os
from crawler.mesh_unii import refresh_mesh_pubchem
from crawler.crawl_util import glom, dump_cache, pull_via_ftp
from functools import partial
import pickle
import requests
import greent.annotators.util.async_client as async_client
import asyncio
from greent.annotators.chemical_annotator import ChemicalAnnotator
import ftplib
import pandas

logger = LoggingUtil.init_logging("robokop-interfaces.crawler.chemicals", logging.DEBUG, format='medium', logFilePath=f'{os.environ["ROBOKOP_HOME"]}/logs/')


def pull(location, directory, filename):
    data = pull_via_ftp(location, directory, filename)
    rdf = decompress(data).decode()
    return rdf


def make_mesh_id(mesh_uri):
    return f"mesh:{mesh_uri.split('/')[-1][:-1]}"


def pull_mesh_chebi():
    url = 'https://query.wikidata.org/sparql?format=json&query=SELECT ?chebi ?mesh WHERE { ?compound wdt:P683 ?chebi . ?compound wdt:P486 ?mesh. }'
    results = requests.get(url).json()
    pairs = [(f'MESH:{r["mesh"]["value"]}', f'CHEBI:{r["chebi"]["value"]}')
             for r in results['results']['bindings']
             if not r['mesh']['value'].startswith('M')]
    with open('mesh_chebi.txt', 'w') as outf:
        for m, c in pairs:
            outf.write(f'{m}\t{c}\n')
    return pairs


def load_chemicals(rosetta, refresh=False):
    # Build if need be
    if refresh:
        refresh_mesh_pubchem(rosetta)
    # Get all the simple stuff
    print('UNICHEM')
    concord = load_unichem()
    # DO MESH/UNII
    print('MESH/UNII')
    mesh_unii_file = os.path.join(os.path.dirname(__file__), 'mesh_to_unii.txt')
    mesh_unii_pairs = load_pairs(mesh_unii_file, 'UNII')
    glom(concord, mesh_unii_pairs)
    # DO MESH/PUBCHEM
    print('MESH/PUBCHEM')
    mesh_pc_file = os.path.join(os.path.dirname(__file__), 'mesh_to_pubchem.txt')
    mesh_pc_pairs = load_pairs(mesh_pc_file, 'PUBCHEM')
    glom(concord, mesh_pc_pairs)
    # DO MESH/CHEBI, but don't combine any chebi's into a set with it
    print('MESH/CHEBI')
    mesh_chebi = pull_mesh_chebi()
    glom(concord, mesh_chebi, ['CHEBI'])
    # Add labels to CHEBIs, CHEMBLs, and MESHes
    print('LABEL')
    label_chebis(concord)
    label_chembls(concord)
    label_meshes(concord)
    # Dump
    with open('chemconc.txt', 'w') as outf:
        for key in concord:
            outf.write(f'{key}\t{concord[key]}\n')
    # dump_cache(concord,rosetta)


def get_chebi_label(ident):
    res = requests.get(f'https://uberonto.renci.org/label/{ident}/').json()
    return res['label']


def get_chembl_label(ident):
    res = requests.get(f'https://www.ebi.ac.uk/chembl/api/data/molecule/{Text.un_curie(ident)}.json').json()
    return res['pref_name']


def get_dict_label(ident, labels):
    try:
        return labels[ident]
    except KeyError:
        return None


def get_mesh_label(ident, labels):
    return labels[Text.un_curie(ident)]


###

def label_chebis(concord):
    print('READ CHEBI')
    chebiobo = pull_via_ftp('ftp.ebi.ac.uk', '/pub/databases/chebi/ontology', 'chebi_lite.obo').decode()
    lines = chebiobo.split('\n')
    chebi_labels = {}
    for line in lines:
        if line.startswith('[Term]'):
            tid = None
            label = None
        elif line.startswith('id:'):
            tid = line[3:].strip()
        elif line.startswith('name:'):
            label = line[5:].strip()
            chebi_labels[tid] = label
    print('LABEL CHEBI')
    label_compounds(concord, 'CHEBI', partial(get_dict_label, labels=chebi_labels))
    # label_compounds(concord,'CHEBI',get_chebi_label)


def process_chunk(lines, label_dict):
    if len(lines) == 0:
        return
    if not lines[0].startswith('chembl_molecule'):
        return
    chemblid = f"CHEMBL:{lines[0].split()[0].split(':')[1]}"
    label = None
    for line in lines[1:]:
        s = line.strip()
        if s.startswith('rdfs:label'):
            label = s.split()[1]
            if label.startswith('"'):
                label = label[1:]
            if label.endswith('"'):
                label = label[:-1]
    if label is not None:
        label_dict[chemblid] = label


def label_chembls(concord):
    print('READ CHEMBL')
    fname = 'chembl_24.1_molecule.ttl.gz'
    # uncomment if you need a new one
    # data=pull_via_ftp('ftp.ebi.ac.uk', '/pub/databases/chembl/ChEMBL-RDF/24.1/',fname)
    # with open(fname,'wb') as outf:
    #    outf.write(data)
    chembl_labels = {}
    chunk = []
    with GzipFile(fname, 'r') as inf:
        for line in inf:
            l = line.decode().strip()
            if len(l) == 0:
                process_chunk(chunk, chembl_labels)
                chunk = []
            elif l.startswith('@'):
                pass
            else:
                chunk.append(l)
    print('LABEL CHEMBL', len(chembl_labels))
    label_compounds(concord, 'CHEMBL', partial(get_dict_label, labels=chembl_labels))
    # label_compounds(concord,'CHEMBL',get_chembl_label)


def label_meshes(concord):
    print('LABEL MESH')
    labelname = os.path.join(os.path.dirname(__file__), 'meshlabels.pickle')
    with open(labelname, 'rb') as inf:
        mesh_labels = pickle.load(inf)
    label_compounds(concord, 'MESH', partial(get_mesh_label, labels=mesh_labels))


###

def label_compounds(concord, prefix, get_label):
    foundlabels = {}
    for k, v in concord.items():
        to_remove = []
        to_add = []
        for ident in v:
            if Text.get_curie(ident) == prefix:
                if not ident in foundlabels:
                    label = get_label(ident)
                    if label is not None:
                        lid = LabeledID(ident, get_label(ident))
                        foundlabels[ident] = lid
                    else:
                        foundlabels[ident] = None
                label = foundlabels[ident]
                if label is not None:
                    to_remove.append(ident)
                    to_add.append(foundlabels[ident])
        for r in to_remove:
            v.remove(r)
        for r in to_add:
            v.add(r)


def remove_ticks(s):
    if s.startswith("'"):
        s = s[1:]
    if s.endswith("'"):
        s = s[:-1]
    return s


def load_pairs(fname, prefix):
    pairs = []
    with open(fname, 'r') as inf:
        for line in inf:
            x = line.strip().split('\t')
            mesh = f"MESH:{x[0]}"
            if x[1].startswith('['):
                pre_ids = x[1][1:-1].split(',')
                pre_ids = [remove_ticks(pids.strip()) for pids in pre_ids]  # remove spaces and ' marks around ids
            else:
                pre_ids = [x[1]]
            ids = [f'{prefix}:{pid}' for pid in pre_ids]
            for identifier in ids:
                pairs.append((mesh, identifier))
    return pairs


def uni_glom(unichem_data, prefix1, prefix2, chemdict):
    print(f'{prefix1}/{prefix2}')
    n = unichem_data.split('\n')[1:]
    if len(n[-1]) == 0:
        n = n[:-1]
    pairs = [ni.split('\t') for ni in n]
    for p in pairs:
        if p[0].startswith("'") or p[1].startswith("'"):
            print('UNI_GLOM {prefix1} {prefix2} {p}')
    curiepairs = [(f'{prefix1}:{p[0]}', f'{prefix2}:{p[1]}') for p in pairs]
    glom(chemdict, curiepairs)


def load_unichem_deprecated():
    chemcord = {}
    prefixes = {1: 'CHEMBL', 2: 'DRUGBANK', 4: 'GTOPDB', 6: 'KEGG.COMPOUND', 7: 'CHEBI', 14: 'UNII', 18: 'HMDB', 22: 'PUBCHEM'}
    #
    keys = list(prefixes.keys())
    keys.sort()
    for i in range(len(keys)):
        for j in range(i + 1, len(keys)):
            ki = keys[i]
            kj = keys[j]
            prefix_i = prefixes[ki]
            prefix_j = prefixes[kj]
            dr = f'pub/databases/chembl/UniChem/data/wholeSourceMapping/src_id{ki}'
            fl = f'src{ki}src{kj}.txt.gz'
            pairs = pull('ftp.ebi.ac.uk', dr, fl)
            uni_glom(pairs, prefix_i, prefix_j, chemcord)
    return chemcord


#########################
# load_unichem() - Loads a dict object with targeted chemical substance cureies for synonymization
#
# return: dict - The cross referenced curies ready for inserting into the the redis cache
#
# The xref file from unichem
# ftp.ebi.ac.uk/pub/databases/chembl/UniChem/data/oracleDumps/UDRI<the latest>/UC_XREF.txt.gz
# cols: uci   src_id    src_compound_id   assignment   last_release_u_when_current   created   lastupdated   userstamp   aux_src
#
# The structure file from unichem
# ftp.ebi.ac.uk/pub/databases/chembl/UniChem/data/oracleDumps/UDRI<the latest>/UC_STRUCTURE.txt.gz
# cols: uci   standardinchi   standardinchikey   created   username   fikhb


def load_unichem(xref_file=None, struct_file=None) -> dict:
    logger.info(f'Start of Unichem loading...')

    # init the returned list
    synonyms: dict = {}

    # init a counter
    counter: int = 0

    # declare the unichem ids tfor the target data
    data_sources: dict = {1: 'CHEMBL', 2: 'DRUGBANK', 4: 'GTOPDB', 6: 'KEGG.COMPOUND', 7: 'CHEBI', 14: 'UNII', 18: 'HMDB', 22: 'PUBCHEM'}

    try:
        # get a handle to the ftp directory
        ftp = ftplib.FTP("ftp.ebi.ac.uk")

        # login
        ftp.login()

        # move to the target directory
        ftp.cwd('/pub/databases/chembl/UniChem/data/oracleDumps')

        # get the directory listing
        files: list = ftp.nlst()

        # init the starting point
        target_dir_index = 0

        # parse the list to determine the most recently updated dir
        for f in files:
            # is this file greater that the previous
            if "UDRI" in f:
                # convert the suffix into an int and compare it to the previous one
                if int(f[4:]) > target_dir_index:
                    # save this as our new highest value
                    target_dir_index = int(f[4:])

        # move the the target directory
        ftp.cwd(f'UDRI{target_dir_index}')
        logger.info(f'Target unichem sub-directory: UDRI{target_dir_index}. Creating xref iterator...')

        # TODO: get the XREF and STRUCTURE archive files and unzip them onto the file system
        if xref_file is None:
            #xref_file = 'C:\\Users\\powen\\Desktop\\xref_100k_subset.txt'
            xref_file = './crawler/xref_1000k_subset.txt'

        if struct_file is None:
            #struct_file = 'C:\\Users\\powen\\Desktop\\struct_100k_subset.txt'
            struct_file = './crawler/struct_1000k_subset.txt'

        # get an iterator to loop through the xref data
        xref_iter = pandas.read_csv(xref_file, dtype={"uci": int, "src_id": int, "src_compound_id": str},
                                    sep='\t', header=None, usecols=[0, 1, 2], names=['uci', 'src_id', 'src_compound_id'], iterator=True, chunksize=100000)
        logger.debug(f'Xref iterator created using file {xref_file}. Loading/filtering xrefs by source type...')

        # parse the records, creating a data frame with only the wanted source types
        df_source_xrefs = pandas.concat(xref_element[xref_element['src_id'].isin(list(data_sources.keys()))] for xref_element in xref_iter)
        logger.debug(f'Xref data frame filtered by source type created. {len(df_source_xrefs)} records found. Grouping xrefs...')

        # group the data by unichem id and filter out singletons
        df_grouped_xrefs = df_source_xrefs.groupby(['uci'])
        logger.debug(f'Xref data frame grouped by unichem id created. Filtering out xref singletons...')

        df_filtered_xrefs = df_grouped_xrefs.filter(lambda x: len(x) > 1)
        logger.debug(f'Xref data frame filtered by non-singletons created. {len(df_filtered_xrefs)} records found. Creating structure iterator...')

        # get an iterator to loop through the xref data
        structure_iter = pandas.read_csv(struct_file, dtype={"uci": int, "standardinchikey": str},
                                         sep='\t', header=None, usecols=[0, 2], names=['uci', 'standardinchikey'], iterator=True, chunksize=100000)
        logger.debug(f'Structure iterator created using file {struct_file}. Loading/filtering data frame by filtered xref unichem ids...')

        # load it into a data frame
        df_structures = pandas.concat(struct_element[struct_element['uci'].isin(df_filtered_xrefs.uci)] for struct_element in structure_iter)
        logger.debug(f'Structure data frame filtered by xref unichem ids created. {len(df_structures)} records loaded. Processing data...')

        # for each of the structured records use the uci to get the xref records
        for struct_index, structure_element in df_structures.iterrows():
            # get the xref records for this uci
            df_structure_xrefs = pandas.DataFrame(df_filtered_xrefs[df_filtered_xrefs.uci == structure_element.uci])

            # did we get anything
            if len(df_structure_xrefs) > 0:
                # init a set for the curie elements
                new_synonym: set = {'INCHIKEY:' + structure_element.standardinchikey}

                # for each xref returned create a record with curie identifiers
                for xref_index, xref_element in df_structure_xrefs.iterrows():
                    # get the curie name by the src_id
                    curie: str = data_sources[xref_element.src_id] + ':' + xref_element.src_compound_id

                    # append this element to the array of curies
                    new_synonym.add(curie)

                # now loop through the array that was just created and pull out each value for the key
                for element in new_synonym:
                    # save the new synonymized unichem entry
                    synonyms[element] = new_synonym

                    # increment the counter
                    counter += 1

                    # output some feedback for the user
                    if (counter % 10000) == 0:
                        logger.info(f'Processed {counter} unichem synonymizations...')

    except Exception as e:
        logger.error(f'Exception caught. Exception: {e}')

    logger.info(f'Load complete. Processed {counter} unichem synonymizations.')

    # return the resultant list set to the caller
    return synonyms


async def make_uberon_role_queries(chebi_ids, chemical_annotator):
    tasks = []
    for id in chebi_ids:
        tasks.append(chemical_annotator.get_chemical_roles(id))
    results = await asyncio.gather(*tasks)

    reformatted_result = {}
    for result in results:
        for chebi_id in result:
            reformatted_result[chebi_id] = list(map(lambda x: x['role_label'], result[chebi_id]))
    return reformatted_result


def merge_roles_and_annotations(chebi_role_data, chebi_annotation_data):
    """
    Merges roles into the bigger annotation dict as roles key.
    """
    for chebi_id in chebi_role_data:
        for key in chebi_role_data[chebi_id]:
            chebi_annotation_data[chebi_id][key] = True
        yield (chebi_id, chebi_annotation_data[chebi_id])


def annotate_from_chebi(rosetta):
    chebisdf = pull('ftp.ebi.ac.uk', '/pub/databases/chebi/SDF/', 'ChEBI_complete_3star.sdf.gz')
    chunk = []
    logger.debug('caching chebi annotations')
    # grab a bunch of them to make use of concurrent execution for fetching roles from Uberon
    result_buffer = {}
    num_request_per_round = 500
    loop = asyncio.new_event_loop()
    chemical_annotator = ChemicalAnnotator(rosetta)
    interesting_keys = chemical_annotator.config['CHEBI']['keys']
    lines = chebisdf.split('\n')
    count = 0
    for line in lines:
        if '$$$$' in line:
            chebi_set = chebi_sdf_entry_to_dict(chunk, interesting_keys=interesting_keys)
            chunk = []
            result_buffer[chebi_set[0]] = chebi_set[1]
            if len(result_buffer) == num_request_per_round:
                chebi_role_data = loop.run_until_complete(make_uberon_role_queries(result_buffer.keys(), chemical_annotator))
                for entry in merge_roles_and_annotations(chebi_role_data, result_buffer):
                    # entry[0] is the chebi id
                    rosetta.cache.set(f'annotation({Text.upper_curie(entry[0])})', entry[1])
                    # clear buffer
                    count += 1
                result_buffer = {}
                logger.debug(f'cached {count} entries... ')
        else:
            if line != '\n':
                line = line.strip('\n')
                chunk += [line]

    if len(result_buffer) != 0:
        # deal with the last pieces left in the buffer
        chebi_role_data = loop.run_until_complete(result_buffer.keys())
        for entry in merge_roles_and_annotations(chebi_role_data, result_buffer):
            rosetta.cache.set(f'annotation({Text.upper_curie(entry[0])})', entry[1])
    logger.debug('done caching chebi annotations...')
    loop.close()


def chebi_sdf_entry_to_dict(sdf_chunk, interesting_keys={}):
    """
    Converts each SDF entry to a dictionary
    """
    final_dict = {}
    current_key = 'mol_file'
    chebi_id = ''
    for line in sdf_chunk:
        if len(line):
            if '>' == line[0]:
                current_key = line.replace('>', '').replace('<', '').strip().replace(' ', '').lower()
                current_key = 'formula' if current_key == 'formulae' else current_key
                if current_key in interesting_keys:
                    final_dict[interesting_keys[current_key]] = ''
                continue
            if current_key == 'chebiid':
                chebi_id = line
            if current_key in interesting_keys:
                final_dict[interesting_keys[current_key]] += line
    return (chebi_id, final_dict)


async def make_multiple_chembl_requests(num_requests=100, start=0):
    """
    Fetches 1000 records per request beginning from 'start' till 'num_requests' * 1000
    """
    tasks = []
    for i in range(0, num_requests):
        offset = i * 1000 + start  # chebml api returns 1000 records max
        url = f"https://www.ebi.ac.uk/chembl/api/data/molecule?format=json&limit=0&offset={offset}"
        tasks.append(async_client.async_get_json(url, {}))
    results = await asyncio.gather(*tasks)
    return results


def annotate_from_chembl(rosetta):
    """
    Gets and caches chembl annotations.
    """
    j = 100  # assume first that we can finish the whole thing with 100 rounds of 100 request for each round
    all_results = []
    logger.debug('annotating chembl data')
    annotator = ChemicalAnnotator(rosetta)
    for i in range(0, j):
        # open the loop
        loop = asyncio.new_event_loop()
        num_requests = 100
        start = (num_requests * 1000) * i
        results = loop.run_until_complete(make_multiple_chembl_requests(num_requests=num_requests, start=start))
        loop.close()
        if i == 0:
            # determine the actual number of records to not just guess when we should stop
            total_count = results[0]['page_meta']['total_count']
            j = round(total_count / (1000 * num_requests))
        for result in results:
            extract_chebml_data_add_to_cache(result, annotator, rosetta)
        logger.debug(f'done annotating {(i / j) * 100} % of chembl')

    logger.debug('caching chebml stuff done...')


def extract_chebml_data_add_to_cache(result, annotator, rosetta):
    """
    Helper function to parse out and extract useful info form a single request result from chebml api.
    """
    molecules = result['molecules']
    for molecule in molecules:
        extract = annotator.extract_chembl_data(molecule, annotator.get_prefix_config('CHEMBL')['keys'])
        logger.debug(extract)
        chembl_id = molecule['molecule_chembl_id']
        rosetta.cache.set(f"annotation({Text.upper_curie(chembl_id)})", extract)


def load_annotations_chemicals(rosetta):
    annotate_from_chebi(rosetta)
    annotate_from_chembl(rosetta)


#######
# Main - Stand alone entry point for testing
#######
if __name__ == '__main__':
    # load_unichem_deprecated()
    import sys

    the_list = load_unichem(sys.argv[1], sys.argv[2])

    with open('./output.txt', 'w') as f:
        for k, v in the_list.items():
            f.write(str(k) + ' >>> ' + str(v) + '\n')

    logger.info('Done.')
