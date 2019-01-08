
from abc import ABCMeta
from aiida.common.extendeddicts import Enumerate
from aiida.orm import Node, Group, Computer, User
from aiida.orm.node.process import CalculationNode
from aiida.orm.querybuilder import QueryBuilder
import numpy as np
import six

MODES = Enumerate(('APPEND', 'REPLACE'))
VALID_CLASSES = (Node, Group)

class EntitySet(object):
    """
    Instances of this class reference a subset of entities in a databases via a unique identifier.
    There are also a few operators defined, for simplicity, to do set-additions (unions) and deletions.
    The underlying Python-class is **set**, which means that adding an instance again to an EntitySet
    will not create a duplicate.
    """
    def __init__(self, aiida_cls):
        """
        :param aiida_cls: A valid AiiDA ORM class, i.e. Node, Group, Computer
        """
        if not aiida_cls in VALID_CLASSES:
            raise TypeError("aiida_cls has to be among:{}".format(
                    VALID_CLASSES))
        # Done with checks, saving to attributes:
        self._aiida_cls = aiida_cls
        # The _set is the set where keys are set:
        self._set = set()
        # the identifier for the key, when I get instance classes
        # it has a type that I check as well
        self._identifier ='id' # TODO: Customize this
        self._identifier_type = int # and this

    def __len__(self):
        return len(self._set)

    def _check_self_and_other(self, other):
        if self.aiida_cls != other.aiida_cls:
            raise TypeError("The two instances do not have the same aiida type!")
        if self.identifier != other.identifier:
            raise ValueError("The two instances do not have the same identifier!")
        if self._identifier_type != other._identifier_type:
            raise TypeError("The two instances do not have the same identifier type!")
        return True

    def __add__(self, other):
        self._check_self_and_other(other)
        new = EntitySet(self.aiida_cls) # , identifier=self.identifier)
        new._set_key_set_nocheck(self._set.union(other._set))
        return new

    def __iadd__(self, other):
        """
        Adding inplace!
        """
        self._check_self_and_other(other)
        self._set_key_set_nocheck(self._set.union(other._set))
        return self

    def __sub__(self, other):
        self._check_self_and_other(other)
        new = EntitySet(self.aiida_cls) #, identifier=self.identifier)
        new._set_key_set_nocheck(self._set.difference(other._set))
        return new

    def __isub__(self, other):
        """
        subtracting inplace!
        """
        self._check_self_and_other(other)
        self._set = self._set.difference(other._set)
        return self

    def __str__(self):
        return str(self._set)

    def __eq__(self,  other):
        return self._set == other._set

    def __ne__(self,  other):
        return not(self==other)

    @property
    def identifier(self):
        return self._identifier

    @property
    def aiida_cls(self):
        return self._aiida_cls


    def _check_input_for_set(self, input_for_set):
        """
        """
        if isinstance(input_for_set, self._aiida_cls):
            return getattr(input_for_set, self._identifier)
        elif isinstance(input_for_set, self._identifier_type):
            return input_for_set
        else:
            raise ValueError("{} is not a valid input\n"
                "You can either pass an AiiDA instance or a key to an instance that"
                "matches the identifier you defined ({})".format(input_for_set, self._identifier_type))

    def set_entities(self, new_entitites):
        self._set = set(map(self._check_input_for_set, new_entitites))

    def add_entities(self, new_entitites):
        self._set = self._set.union(map(self._check_input_for_set, new_entitites))

    def get_keys(self):
        return self._set

    def _set_key_set_nocheck(self, _set):
        """
        Use with care! If you know that the new set is valid, call this function!
        """
        self._set = _set

    def empty(self):
        self._set = set()

    def copy(self, with_data=True):
        """
        Create a new instance, with the attributes defining being the same.
        :param bool with_data: Whether to copy also the data.
        """
        new = EntitySet(aiida_cls=self.aiida_cls) #
        #  , identifier=self.identifier, identifier_type=self._identifier_type)
        if with_data:
            new._set_key_set_nocheck(self._set.copy())
        return new



class AiidaEntitySets():
    """
    AiidaEntitySets are a container for several EntitySet.
    In the current implementation, they contain a Node "set" and a Group "set".
    :TODO: Computers and Users!
    """
    def __init__(self, nodes=None, groups=None):
        """
        :param nodes: An EntitySet of Node
        :param groups: An EntitySet of Group
        """
        if nodes is None:
            nodes = EntitySet(Node) #, identifier='id', identifier_type=int)
        elif isinstance(nodes, EntitySet):
            pass
        else:
            raise TypeError("nodes has to be an instance of EntitySet")
        if groups is None:
            groups = EntitySet(Group) #, identifier='id', identifier_type=int)
        elif isinstance(groups, EntitySet):
            pass
        else:
            raise TypeError("groups has to be an instance of EntitySet")
        for inp, should_cls in ((nodes, Node), (groups, Group)):
            if not inp.aiida_cls == should_cls:
                raise TypeError("{} does not have {} as its class, but {}".format(
                        inp, should_cls, inp.aiida_cls))
        self._dict = dict(nodes=nodes, groups=groups)

    @property
    def sets(self):
        return zip(*sorted(self.dict.items()))[1]

    @property
    def dict(self):
        return self._dict

    @property
    def nodes(self):
        return self._dict['nodes']

    @property
    def groups(self):
        return self._dict['groups']

    def __getitem__(self,  key):
        return self._dict[key]

    def __setitem__(self,  key, val):
        self._dict[key] = val

    def __add__(self, other):
        new_dict = {}
        for key in self._dict.keys():
            new_dict[key] =  val[key]+other[key]
        return AiidaEntitySets(**new_dict)
 
    def __iadd__(self, other):
        for key in self._dict.keys():
            self[key] +=  other[key]
        return self
 
    def __sub__(self, other):
        new_dict = {}
        for key in self._dict.keys():
            new_dict[key] =  self[key] - other[key]
        return AiidaEntitySets(**new_dict)
 
    def __isub__(self, other):
        for key in other._dict.keys():
            self[key] -=  other[key]
        return self

    def __len__(self):
        return sum([len(s) for s in self.sets])

    def __eq__(self, other):
        for key in self._dict.keys():
            if self[key] != other[key]:
                return False
        return True

    def __ne__(self, other):
        return not(self==other)

    def __str__(self):
        ret_str = ''
        for key, val in self._dict.items():
            ret_str += '  ' + key + ': '
            ret_str += str(val) + '\n'
        return ret_str

    def empty(self):
        """
        Empty every subset from its content
        """
        for set_ in self._dict.values():
            set_.empty()

    def copy(self, with_data=True):
        new_dict = dict()
        for key, val in self._dict.items():
            new_dict[key] = val.copy(with_data=with_data)
        return AiidaEntitySets(**new_dict)


@six.add_metaclass(ABCMeta)
class Operation(object):
    def __init__(self, mode, max_iterations, track_edges, track_visits):
        assert mode in MODES, 'You have to pass an option of {}'.format(MODES)
        self._mode = mode
        self.set_max_iterations(max_iterations)
        self._track_edges = track_edges
        self._track_visits = track_visits
        self._walkers = None
        self._visits = None
        self._iterations_done = None

    def _init_run(self, entity_set):
        pass
    def _check(self, entity_set):
        if not isinstance(entity_set, AiidaEntitySets):
            raise TypeError("You need to set the walkers with an AiidaEntitySet")

    def set_max_iterations(self, max_iterations):
        if max_iterations is np.inf:
            pass
        elif not isinstance(max_iterations, int):
            raise TypeError("max iterations has to be an integer")
        self._maxiter = max_iterations

    def set_walkers(self, walkers):
        self._check(walkers)
        self._walkers = walkers

    def get_iterations_done(self):
        return self._iterations_done

    def get_walkers(self):
        return self._walkers #.copy(with_data=True)

    def set_visits(self, entity_set):
        self._check(entity_set)
        self._visits = entity_set

    def get_visits(self):
        return self._visits

    def run(self, walkers=None, visits=None, iterations=None):
        if walkers is not None:
            self.set_walkers(walkers)
        else:
            if self._walkers is None:
                raise RuntimeError("Walkers have not been set for this instance")
        if visits is not None:
            self.set_visits(visits)
        if self._track_visits and self._visits is None:
            #raise RuntimeError("Supposed to track visits, but visits have not been "
            #        "set for this instance")
            # If nothing is set, I do it here!
            self.set_visits(self._walkers.copy())
        if iterations is not None:
            self.set_iterations(iterations)

        self._init_run(self._walkers)
        # The active walkers are all workers where this rule-instance have
        # not been applied, yet
        active_walkers = self._walkers.copy()
        # The new_set is where I can put the results of the query
        # It starts empty.
        new_results = self._walkers.copy(with_data=False)
        # I also need somewhere to store everything I've walked to
        # with_data is set to True, since the active walkers are of course being visited
        # even before we start the iterations!
        visited_this_rule = self._walkers.copy(with_data=True) # w
        iterations = 0
        while (active_walkers and iterations < self._maxiter):
            iterations += 1
            # loading results into new_results set:
            self._load_results(new_results, active_walkers)
            # It depends on the mode, how I update the walkers
            # I set the active walkers to all results that have not been visited yet.
            active_walkers = new_results - visited_this_rule
            # The visited is augmented:
            visited_this_rule += active_walkers
        self._iterations_done = iterations
        if self._mode == MODES.APPEND:
            self._walkers += visited_this_rule
        elif self._mode == MODES.REPLACE:
            self._walkers = active_walkers

        if self._track_visits:
            self._visits += visited_this_rule
        return self._walkers
        
        if self._mode == MODES.APPEND:
            # The visited_set is all the instances I visited during the application of this rule.
            # Basically a memory of all the vertices traversed
            visited_set = entity_set.copy()
            while (operational_set and iterations < self._maxiter):
                iterations += 1
                new_set = self._get_result(new_set, operational_set)
                operational_set = new_set - visited_set
                visited_set +=  new_set
            return visited_set
        elif self._mode == MODES.REPLACE:
            while (iterations < self._maxiter):
                iterations += 1
                new_set = self._get_result(new_set, operational_set )
                operational_set = new_set
                if not new_set:
                    # I don't have anything in my new set, so it will be empty queries hencon
                    # I can break the loop here
                    break
            return new_set
        else:
            raise RuntimeError("Unknown mode {}".format(self._mode))


class UpdateRule(Operation):
    def __init__(self, querybuilder, mode=MODES.APPEND, max_iterations=1,
            track_edges=False, track_visits=True):
        def get_spec_from_path(queryhelp, idx):
            if (queryhelp['path'][idx]['type'].startswith('node') or
                    queryhelp['path'][idx]['type'].startswith('data') or
                    queryhelp['path'][idx]['type'] == ''):
                return 'nodes'
            elif queryhelp['path'][idx]['type'] == 'group':
                return 'groups'
            else:
                raise Exception("not understood entity from ( {} )".format(queryhelp['path'][0]['type']))
                
        queryhelp = querybuilder.get_json_compatible_queryhelp()
        for pathspec in queryhelp['path']:
            if not pathspec['type']:
                pathspec['type'] = 'node.Node.'
        self._querybuilder = QueryBuilder(**queryhelp)
        queryhelp = self._querybuilder.get_json_compatible_queryhelp()
        self._first_tag = queryhelp['path'][0]['tag']
        self._last_tag = queryhelp['path'][-1]['tag']

        self._entity_from = get_spec_from_path(queryhelp, 0)
        self._entity_to = get_spec_from_path(queryhelp, -1)
        super(UpdateRule, self).__init__(mode, max_iterations, 
                track_edges=track_edges, track_visits=track_visits)


    def _load_results(self, target_set, operational_set):
        """
        :param target_set: The set to load the results into
        :param operational_set: Where the results originate from (walkers)
        """
        # I check that I have primary keys
        primkeys = operational_set[self._entity_from].get_keys()
        # Empty the target set, so that only these results are inside
        target_set.empty()
        if primkeys:
            self._querybuilder.add_filter(self._first_tag, {
                        operational_set[self._entity_from].identifier:{'in':primkeys}})
            # These are the new results returned by the query
            target_set[self._entity_to].add_entities(
                        [item[self._last_tag][operational_set[self._entity_to].identifier] 
                            for item in self._querybuilder.iterdict()])
        # Everything is changed in place, no need to return anything


    def _init_run(self, entity_set):
        # Removing all other projections in the QueryBuilder instance:
        for tag, projectio in self._querybuilder._projections.items():
            self._querybuilder._projections[tag] = []
        # priming querybuilder to add projection on the key I need:
        self._querybuilder.add_projection(self._last_tag, entity_set[self._entity_to].identifier)



class RuleSaveWalkers(Operation):
    def __init__(self, stash):
        self._stash = stash
        super(RuleSaveWalkers, self).__init__(mode=MODES.REPLACE, 
                max_iterations=1, track_edges=True, track_visits=True)

    def _load_results(self, target_set, operational_set):
        self._stash += self._walkers

class RuleSetWalkers(Operation):
    def __init__(self, stash):
        self._stash = stash
        super(RuleSetWalkers, self).__init__(mode=MODES.REPLACE, 
                max_iterations=1, track_edges=True, track_visits=True)

    def _load_results(self, target_set, operational_set):
        self._walkers.empty()
        self._walkers += self._stash

class RuleSequence(Operation):
    def __init__(self, rules, mode=MODES.APPEND, max_iterations=1,
            track_edges=False, track_visits=True):
        for rule in rules:
            if not isinstance(rule, Operation):
                print(rule)
                raise TypeError("rule has to be an instance of Operation-subclass")
        self._rules = rules
        super(RuleSequence, self).__init__(mode, max_iterations, 
                track_edges=track_edges, track_visits=track_visits)


    def _load_results(self, target_set, active_walkers):
        target_set.empty()
        for irule, rule in enumerate(self._rules):
            # I iterate the operational_set through all the rules:
            #rule.set_walkers(active_walkers)
            rule.set_visits(self._visits)
            rule.set_walkers(self._walkers)
            target_set += rule.run()



def get_entity_sets(node_ids=None, group_ids=None, *args):
    node_set = EntitySet(Node) #, identifier='id', identifier_type=int)
    if node_ids:
        node_set.set_entities(node_ids)
    group_set = EntitySet(Group) #, identifier='id', identifier_type=int)
    if group_ids:
        group_set.set_entities(group_ids)
    nodes = [a for a in args if isinstance(a,Node)]
    node_set.add_entities(nodes)

    groups = [a for a in args if isinstance(a,Group)]
    group_set.add_entities(nodes)
    return AiidaEntitySets(nodes=node_set, groups=group_set)

if __name__  == '__main__':
    print(QueryBuilder().append(Node, project='id').all())

