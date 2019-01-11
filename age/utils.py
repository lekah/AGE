from aiida.orm.data import Data
from aiida.orm.node.process import CalculationNode, WorkflowNode
from aiida.common.links import LinkType
import numpy as np


def create_tree(max_depth=3, branching=3, starting_cls=Data, draw=False):
    """
    
    """
    if starting_cls not in (Data, CalculationNode):
        raise TypeError("The starting_cls has to be either Data or CalculationNode")

    # The number of nodes I will create:
    nr_of_nodes = sum([branching**d for d in range(max_depth)])

    parent = starting_cls().store() # This is the ancestor of every Node I create later

    depth_dict = {} # This where I save the descendants, by depth (depth is the key).
    # I'm including original node as a descendant of depth 0.
    depth_dict[0] = set([parent.id])

    previous_cls = starting_cls # The previous_cls is needed to be able to alternate
    # between Data and Calculations.
    # If previous_cls is Data, add calculations, and vice versa
    previous_ins = [parent] # The previous_ins list stores all the instance created in the previous
    # iteration
    previous_indices = [0] # The previous_indicies list stores where the ids of previous_ins are
    # stored in all_instances:

    # all_instances saves all the instances EVER created"
    all_instances = np.zeros(nr_of_nodes, dtype=int)
    all_instances[0] = parent.id
    adjacency = np.zeros((nr_of_nodes, nr_of_nodes), dtype=int)
    # Iterating over the depth of the tree
    counter = 1
    for depth in range(1, max_depth):
        # I'm at new depth, create new set:
        depth_dict[depth] = set()

        # Here I decide what class to create this level of descendants, and what the
        # link type is:
        cls = Data if previous_cls is CalculationNode else CalculationNode
        ltype = LinkType.CREATE if cls is Data else LinkType.INPUT_CALC

        # The new instances I create are saved in this list:
        new_ins = []
        new_indices = []

        # Iterating over previous instances
        for (pins,pins_idx) in zip(previous_ins, previous_indices):
            # every previous instances gets a certain number of children:
            for ioutgoing in range(branching):
                new = cls().store()
                new.add_incoming(pins, link_type=ltype, link_label='{}'.format(ioutgoing))
                new_ins.append(new)
                all_instances[counter] = new.id
                adjacency[pins_idx, counter] = 1
                new_indices.append(counter)

                depth_dict[depth].add(new.id)
                counter += 1
        # Everything done, loading new instances to previous instances, and previous class
        # to this class:
        previous_ins = new_ins
        previous_indices = new_indices
        previous_cls = cls

    if draw:
        from aiida.utils.ascii_vis import draw_children
        print('\n\n\n The tree created:')
        print(draw_children(parent, dist=max_depth+1,
                follow_links_of_type=(LinkType.INPUT_CALC, LinkType.CREATE)))
        print('\n\n\n')

    return {'parent':parent, 'depth_dict':depth_dict, 'instances':all_instances,
            'adjacency':adjacency}
