
from age.entities import get_basket
from age.rules import (UpdateRule, RuleSequence, MODES, RuleSaveWalkers, RuleSetWalkers)

from aiida.backends.testbase import AiidaTestCase, check_if_tests_can_run
from aiida.common.exceptions import TestsNotAllowedError
from aiida.common.links import LinkType
from aiida.orm import Node
from aiida.orm.data import Data
from aiida.orm.node.process import CalculationNode, WorkflowNode


import numpy as np



class TestNodes(AiidaTestCase):
    # Hardcoding here how deep I go
    DEPTH = 4
    # Hardcoding the branching at every level, i.e. the number
    # of children per parent Node.
    NR_OF_CHILDREN = 2

    def runTest(self):
        """
        Just wrapping the other functions
        """
        self.test_data_provenance()
        # ~ self.test_returns_calls()
        self.test_cycle()
        self.test_stash()

    def test_data_provenance(self):
        """
        Creating a parent (Data) node.
        Attaching a sequence of Calculation/Data to create a "provenance".
        """
        from age.utils import create_tree
        created_dict = create_tree(self.DEPTH, self.NR_OF_CHILDREN)
        parent = created_dict['parent']
        desc_dict = created_dict['depth_dict']

        # Created all the nodes, tree. 
        #Now testing whether I find all the descendants
        # Using the utility function to create the starting entity set:
        es = get_basket(node_ids=(parent.id,))
        qb = QueryBuilder().append(Node).append(Node)

        for depth in range(0, self.DEPTH):
            #print('At depth {}'.format(depth))

            rule = UpdateRule(qb, mode=MODES.REPLACE, max_iterations=depth)
            res = rule.run(es.copy())['nodes']._set
            #print('   Replace-mode results: {}'.format(', '.join(map(str, sorted(res)))))
            should_set = desc_dict[depth]
            self.assertTrue(not(res.difference(should_set) or should_set.difference(res)))

            rule = UpdateRule(qb, mode=MODES.APPEND, max_iterations=depth)
            res = rule.run(es.copy())['nodes']._set
            #print('   Append-mode  results: {}'.format(', '.join(map(str, sorted(res)))))
            should_set = set()
            [[should_set.add(s) for s in desc_dict[d]] for d in range(depth+1)]

            self.assertTrue(not(res.difference(should_set) or should_set.difference(res)))


    def test_cycle(self):
        """
        Creating a cycle: A data-instance is both input to and returned by a WorkFlowNode
        """
        d = Data().store()
        c = WorkflowNode().store()
        c.add_incoming(d, link_type=LinkType.INPUT_WORK, link_label='lala')
        d.add_incoming(c, link_type=LinkType.RETURN, link_label='lala')
        qb = QueryBuilder().append(Node).append(Node)
        rule = UpdateRule(qb, max_iterations=np.inf)
        es = get_basket(node_ids=(d.id,))
        res = rule.run(es)
        self.assertEqual( res['nodes']._set, set([d.id, c.id]))

    
    
    def test_stash(self):
        """
        Here I'm testing the 'stash'
        """
        # creatin a first calculation with 3 input data:
        c = CalculationNode().store()
        dins = set() # To compare later, dins is a set of the input data pks.
        for i in range(3):
            data_in = Data().store()
            dins.add(data_in.id)
            c.add_incoming(data_in, 
                    link_type=LinkType.INPUT_CALC,
                    link_label='lala-{}'.format(i))

        # Creating output data to that calculation:
        douts = set() # Similar to dins, this is the set of data output pks
        for i in range(4):
            data_out = Data().store()
            douts.add(data_out.id)
            data_out.add_incoming(c,
                    link_type=LinkType.CREATE,
                    link_label='lala-{}'.format(i))
        #print(draw_children

        # adding another calculation, with one input from c's outputs,
        # and one input from c's inputs
        c2 = CalculationNode().store()
        c2.add_incoming(data_in, link_type=LinkType.INPUT_CALC, link_label='b')
        c2.add_incoming(data_out, link_type=LinkType.INPUT_CALC, link_label='c')


        # ALso here starting with a set that only contains the starting the calculation:
        es = get_basket(node_ids=(c.id,))
        # Creating the rule for getting input nodes:
        rule_in = UpdateRule(QueryBuilder().append(
                Node, tag='n').append(
                Node, with_outgoing='n'))
        # Creating the rule for getting output nodes
        rule_out = UpdateRule(QueryBuilder().append(
                Node, tag='n').append(
                Node, with_incoming='n'))
        #, edge_filters={'type':LinkType.CREATE.value}))


        # I'm testing the input rule. Since I'm updating, I should
        # have the input and the calculation itself:
        is_set = rule_in.run(es.copy())['nodes']._set
        self.assertEqual(is_set, dins.union({c.id}))

        # Testing the output rule, also here, output + calculation c is expected:
        is_set = rule_out.run(es.copy())['nodes']._set
        self.assertEqual(is_set, douts.union({c.id}))

        # Now I'm  testing the rule sequence.
        # I first apply the rule to get outputs, than the rule to get inputs
        rs1 = RuleSequence((rule_out, rule_in))
        is_set = rs1.run(es.copy())['nodes']._set
        # I expect the union of inputs, outputs, and the calculation:
        self.assertEqual(is_set, douts.union(dins).union({c.id}))

        # If the order of the rules is exchanged, I end up of also attaching c2 to the results.
        # This is because c and c2 share one data-input:
        rs2 = RuleSequence((rule_in, rule_out))
        is_set = rs2.run(es.copy())['nodes']._set
        self.assertEqual(is_set, douts.union(dins).union({c.id, c2.id}))

        # Testing similar rule, but with the possibility to stash the results:
        stash = es.copy(with_data=False)
        rsave = RuleSaveWalkers(stash)
        # Checking whether Rule does the right thing i.e If I stash the result,
        # the active walkers should be an empty set:
        self.assertEqual(rsave.run(es.copy()), es.copy(with_data=False))
        # Whereas the stash contains the same data as the starting point:
        self.assertEqual(stash,es)
        rs2 = RuleSequence((
                RuleSaveWalkers(stash), rule_in,
                RuleSetWalkers(stash) ,rule_out))
        is_set = rs2.run(es.copy())['nodes']._set
        # NOw I test whether the stash does the right thing,
        # namely not including c2 in the results:
        self.assertEqual(is_set, douts.union(dins).union({c.id}))
    
        
    def test_returns_calls(self ):
        rules = []
        # linking all processes to input data:
        qb = QueryBuilder()
        qb.append(Data, tag='predecessor')
        qb.append(ProcessNode, with_incoming='predecessor',
                  edge_filters={'type': {'in': [
                        LinkType.INPUT_CALC.value,
                        LinkType.INPUT_WORK.value]}})
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
            edge_filters={'type': {'in': [
                    LinkType.CALL_CALC.value,
                    LinkType.CALL_WORK.value]}})
        rules.append(UpdateRule(qb))
    
        # CREATE(ProcessNode, Data) - Reversed
        if create_reversed:
            qb = QueryBuilder()
            qb.append(ProcessNode, tag='predecessor', project=['id'])
            qb.append(Data, 
                    with_incoming='predecessor',
                    edge_filters={'type': {'in': [LinkType.CREATE.value]}})
            rules.append(UpdateRule(qb))
        # Case 3:
        # RETURN(ProcessNode, Data) - Reversed
        if return_reversed:
            qb = QueryBuilder()
            qb.append(ProcessNode, tag='predecessor',)
            qb.append(Data,
                    output_of='predecessor',
                    edge_filters={'type': {'in': [LinkType.RETURN.value]}})
            rules.append(UpdateRule(qb))
    
    
        seq = RuleSequence(rules, max_iterations=np.inf )


class TestGroups(AiidaTestCase):
    N_GROUPS = 10
    def runTest(self):
        """
        Testing whether groups and nodes can be traversed with the Graph explorer:
        """
        # I create a certain number of groups and save them in this list:
        groups = []
        for igroup in range(self.N_GROUPS):
            name='g-{}'.format(igroup) # Name has to be unique
            groups.append(Group(name=name).store())
        # Same with nodes: Create 1 node less than I have groups
        nodes = []
        for inode in range(1, self.N_GROUPS):
            d = Data().store()
            # The node I create, I added both to the group of
            # same index and the group of index - 1
            groups[inode].add_nodes(d)
            groups[inode-1].add_nodes(d)
            nodes.append(d)

        # Creating sets for the test:
        nodes_set = set([n.id for n in nodes])
        groups_set = set([g.id for g in groups])

        # Now I want rule that gives me all the data starting
        # from the last node, with links being
        # belonging to the same group:
        qb = QueryBuilder()
        qb.append(Data, tag='d')
        qb.append(Group, with_node='d', tag='g', filters={'type':''} ) # The filter here is
        # there for avoiding problems with autogrouping. Depending how the test
        # exactly is run, nodes can be put into autogroups.
        qb.append(Data, with_group='g')

        es = get_basket(node_ids=(d.id,))
        rule = UpdateRule(qb, max_iterations=np.inf)
        res = rule.run(es.copy())['nodes']._set
        # checking whether this updateRule above really visits all the nodes I created:
        self.assertEqual(res, nodes_set)
        # The visits:
        self.assertEqual(rule.get_visits()['nodes']._set,res)

        # I can do the same with 2 rules chained into a RuleSequence:
        qb1=QueryBuilder().append(Node, tag='n').append(
                Group, with_node='n', filters={'type':''})
        qb2=QueryBuilder().append(Group, tag='n').append(
                Node, with_group='n')
        rule1 = UpdateRule(qb1)
        rule2 = UpdateRule(qb2)
        seq = RuleSequence((rule1, rule2), max_iterations=np.inf)
        res = seq.run(es.copy())
        for should_set, is_set in (
                (nodes_set.copy(), res['nodes']._set),
                (groups_set,res['groups']._set)):
            self.assertEqual(is_set, should_set)

class TestEdges(AiidaTestCase):
    DEPTH = 4
    NR_OF_CHILDREN = 2

    def runTest(self):
        """
        Testing whether nodes (and nodes) can be traversed with the Graph explorer,
        with the links being stored
        """
        from age.utils import create_tree
        # I create a certain number of groups and save them in this list:
        created_dict = create_tree(self.DEPTH, self.NR_OF_CHILDREN, draw=True)
        instances = created_dict['instances']
        adjacency = created_dict['adjacency']

        es = get_basket(node_ids=(created_dict['parent'].id,))


        qb = QueryBuilder().append(Node).append(Node)

        rule = UpdateRule(qb, mode=MODES.APPEND, max_iterations=self.DEPTH-1, track_edges=True)
        res = rule.run(es.copy())
        #print('   Append-mode  results: {}'.format(', '.join(map(str, sorted(res)))))
        should_set = set()
        [[should_set.add(s) for s in created_dict['depth_dict'][d]] for d in range(self.DEPTH)]

        self.assertEqual(res['nodes']._set, should_set) #) or should_set.difference(res)))


        touples_should = set((instances[i],instances[j]) for  i, j in zip(*np.where(adjacency)))
        touples_are = set(zip(*zip(*res['nodes_nodes']._set)[:2]))

        self.assertEqual(touples_are, touples_should)


        rule = UpdateRule(qb, mode=MODES.REPLACE, max_iterations=self.DEPTH-1, track_edges=True)
        res = rule.run(es.copy())
        # Since I apply the replace rule, the last set of links should appear:

        instances = created_dict['instances']
        adjacency = created_dict['adjacency']

        touples_should = set()
        [touples_should.add((pk1, pk2))
                for idx1,pk1 in enumerate(instances)
                for idx2,pk2 in enumerate(instances)
                if adjacency[idx1, idx2]
                and pk1 in created_dict['depth_dict'][self.DEPTH-2]
                and pk2 in created_dict['depth_dict'][self.DEPTH-1]
            ]

        touples_are = set(zip(*zip(*res['nodes_nodes']._set)[:2]))
        self.assertEqual(touples_are, touples_should)

if __name__ == '__main__':
    from unittest import TestSuite, TextTestRunner
    try:
        check_if_tests_can_run()
    except TestsNotAllowedError as e:
        print >> sys.stderr, e.message
        sys.exit(1)

    test_suite = TestSuite()
    test_suite.addTest(TestNodes())
    test_suite.addTest(TestGroups())
    test_suite.addTest(TestEdges())
    results = TextTestRunner(failfast=False, verbosity=2).run(test_suite)
