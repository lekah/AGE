from aiida.orm import Node, Group

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
