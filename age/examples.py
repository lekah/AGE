
from aiida.orm import Node, Group, Computer, User
from aiida.orm.data import Data
from aiida.orm.node.process import CalculationNode, WorkflowNode
from aiida.common.links import LinkType
import numpy as np
from age.rules import (UpdateRule, RuleSequence, get_entity_sets, MODES,
        RuleSaveWalkers, RuleSetWalkers)

def example1():
    parent = Data().store()
    print (parent)
    desc_dict = {}
    desc_dict[0] = set([parent.id])
    previous_cls = Data
    previous_ins = [parent]
    all_instances = set([parent.id])
    nr_of_descs = 2
    for depth in range(1,4):
        desc_dict[depth] = set()
        cls = Data if previous_cls is CalculationNode else CalculationNode
        ltype = LinkType.CREATE if cls is Data else LinkType.INPUT_CALC
        new_ins = []
        for pins in previous_ins:
            for idesc in range(nr_of_descs):
                new = cls().store()
                new.add_incoming(pins, link_type=ltype, link_label='{}'.format(idesc))
                new_ins.append(new)
                all_instances.add(new.id)
                desc_dict[depth].add(new.id)
        previous_ins = new_ins
        previous_cls = cls

    es = get_entity_sets(node_ids=(parent.id,))

    qb = QueryBuilder().append(Node).append(Node)

    for depth in range(1, 4):


        rule = UpdateRule(qb, mode=MODES.APPEND, max_iterations=depth)
        res = rule.run(es.copy())['nodes']._set

        should_set = set()
        [[should_set.add(s) for s in desc_dict[d]] for d in range(depth+1)]
        print(depth, res, should_set)

        assert not(res.difference(should_set) or should_set.difference(res))

        rule = UpdateRule(qb, mode=MODES.REPLACE, max_iterations=depth)
        res = rule.run(es.copy())['nodes']._set
        should_set = desc_dict[d]
        print(depth, res, should_set)
        assert not(res.difference(should_set) or should_set.difference(res))


def example2():
    groups = []
    N = 10
    rand = np.random.randint(10000)
    for igroup in range(N):
        name='g-{}-{}'.format(rand, igroup)
        g = Group(name=name)
        g.store()
        groups.append(g)
    nodes = []
    for inode in range(1, N):
        d = Data().store()
        groups[inode].add_nodes(d)
        groups[inode-1].add_nodes(d)
        nodes.append(d)


    nodes_set = set([n.id for n in nodes])
    groups_set = set([g.id for g in groups])

    # Now I want rule that gives me all the data starting from the last node, with links being
    # belonging to the same group:
    qb = QueryBuilder()
    qb.append(Data, tag='d')
    qb.append(Group, with_node='d', tag='g', filters={'type':''}    )
    qb.append(Data, with_group='g')

    es = get_entity_sets(node_ids=(d.id,))
    rule = UpdateRule(qb, max_iterations=np.inf)
    res = rule.run(es.copy())['nodes']._set
    print('example2a')
    print(res, nodes_set, rule.get_visits()['nodes']._set)
    assert not(res.difference(nodes_set) or nodes_set.difference(res))
    assert rule.get_visits()['nodes']._set == res, ""

    print('example2b')
    qb1=QueryBuilder().append(Node, tag='n').append(Group, with_node='n', filters={'type':''})
    qb2=QueryBuilder().append(Group, tag='n').append(Node, with_group='n')
    rule1 = UpdateRule(qb1)
    rule2 = UpdateRule(qb2)
    seq = RuleSequence((rule1, rule2), max_iterations=np.inf)
    res = seq.run(es.copy())
    for should_set, is_set  in ((nodes_set.copy(), res['nodes']._set), (groups_set,res['groups']._set)):
        print(should_set, is_set)
        assert not(is_set.difference(should_set) or should_set.difference(is_set))
    

def example3():
    rules = []
    # linking all processes to input data:
    qb = QueryBuilder()
    qb.append(Data, tag='predecessor')
    qb.append(ProcessNode, with_incoming='predecessor',
              edge_filters={'type': {'in': [LinkType.INPUT_CALC.value, LinkType.INPUT_WORK.value]}})
    rules.append(UpdateRule(qb))

    # CREATE/RETURN(ProcessNode, Data) - Forward
    qb = QueryBuilder()
    qb.append(ProcessNode, tag='predecessor')
    qb.append(Data, with_incoming='predecessor', edge_filters={
                  'type': {'in': [LinkType.CREATE.value, LinkType.RETURN.value]}})
    rules.append(UpdateRule(qb))

    # CALL(ProcessNode, ProcessNode) - Forward
    qb = QueryBuilder()
    qb.append(ProcessNode, tag='predecessor')
    qb.append(ProcessNode, with_incoming='predecessor',
        edge_filters={'type': {'in': [LinkType.CALL_CALC.value, LinkType.CALL_WORK.value]}})
    rules.append(UpdateRule(qb))

    # CREATE(ProcessNode, Data) - Reversed
    if create_reversed:
        qb = QueryBuilder()
        qb.append(ProcessNode, tag='predecessor', project=['id'])
        qb.append(Data, with_incoming='predecessor', edge_filters={'type': {'in': [LinkType.CREATE.value]}})
        rules.append(UpdateRule(qb))
    # Case 3:
    # RETURN(ProcessNode, Data) - Reversed
    if return_reversed:
        qb = QueryBuilder()
        qb.append(ProcessNode, tag='predecessor',)
        qb.append(Data, output_of='predecessor', edge_filters={'type': {'in': [LinkType.RETURN.value]}})
        rules.append(UpdateRule(qb))


    seq = RuleSequence(rules, max_iterations=np.inf )


def example4():
    """
    Creating a cycle:
    """
    d = Data().store()
    c = WorkflowNode().store()
    c.add_incoming(d, link_type=LinkType.INPUT_WORK, link_label='lala')
    d.add_incoming(c, link_type=LinkType.RETURN, link_label='lala')
    qb = QueryBuilder().append(Node).append(Node)
    rule = UpdateRule(qb, max_iterations=np.inf)
    es = get_entity_sets(node_ids=(d.id,))
    print(rule.run(es)['nodes'] )

def example5():
    c = CalculationNode().store()
    dins = set()
    for i in range(3):
        data_in = Data().store()
        dins.add(data_in.id)
        c.add_incoming(data_in, link_type=LinkType.INPUT_CALC, link_label='lala-{}'.format(i))
    douts = set()
    for i in range(4):
        data_out = Data().store()
        douts.add(data_out.id)
        data_out.add_incoming(c, link_type=LinkType.CREATE, link_label='lala-{}'.format(i))
    # adding another calculation, with one input from c's outputs, and one input from c's inputs
    c2 = CalculationNode().store()
    c2.add_incoming(data_in, link_type=LinkType.INPUT_CALC, link_label='b')
    c2.add_incoming(data_out, link_type=LinkType.INPUT_CALC, link_label='c')
    es = get_entity_sets(node_ids=(c.id,))
    rule_in = UpdateRule(QueryBuilder().append(Node, tag='n').append(Node, with_outgoing='n'))
    rule_out = UpdateRule(QueryBuilder().append(Node, tag='n').append(Node, with_incoming='n'))

    is_set = rule_in.run(es.copy())['nodes']._set
    assert (is_set == dins.union({c.id}))

    is_set = rule_out.run(es.copy())['nodes']._set
    assert (is_set == douts.union({c.id}))
    
    rs1 = RuleSequence((rule_out, rule_in))
    is_set = rs1.run(es.copy())['nodes']._set
    assert (is_set == douts.union(dins).union({c.id}))

    rs2 = RuleSequence((rule_in, rule_out))
    is_set = rs2.run(es.copy())['nodes']._set
    assert (is_set == douts.union(dins).union({c.id, c2.id}))

    stash = es.copy(with_data=False)
    rsave = RuleSaveWalkers(stash)
    assert (rsave.run(es.copy())==es.copy(with_data=False))
    assert (stash==es)
    rs2 = RuleSequence((RuleSaveWalkers(stash), rule_in, RuleSetWalkers(stash) ,rule_out))
    is_set = rs2.run(es.copy())['nodes']._set

    assert (is_set == douts.union(dins).union({c.id}))

    
    
    
    
    
if __name__ == '__main__':
    example1()
    example2()
    example4()
    example5()

