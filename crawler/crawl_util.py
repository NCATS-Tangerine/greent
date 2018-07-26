def glom(conc_set, newgroups):
    """We want to construct sets containing equivalent identifiers.
    conc_set is a dictionary where the values are these equivlent identifier sets and
    the keys are all of the elements in the set.   For each element in a set, there is a key
    in the dictionary that points to the set.
    newgroups is an iterable that of new equivalence groups (expressed as sets,tuples,or lists)
    with which we want to update conc_set."""
    for group in newgroups:
        #Find all the equivalence sets that already correspond to any of the identifiers in the new set.
        existing_sets = [ conc_set[x] for x in group if x in conc_set ]
        #All of these sets are now going to be combined through the equivalence of our new set.
        newset=set().union(*existing_sets)
        #put all the new stuff in it.  Do it element-wise, cause we don't know the type of the new group
        for element in group:
            newset.add(element)
        #Now make all the elements point to this new set:
        for element in newset:
            conc_set[element] = newset

def dump_cache(concord,rosetta):
    for chem_id in concord:
        key = f"synonymize({chem_id})"
        value = concord[chem_id]
        rosetta.cache.set(key,value)