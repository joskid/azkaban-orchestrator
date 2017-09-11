from azkaban_orchestrator import diagram
from mock import patch
import unittest
import os

MODULE_PATH = os.path.dirname(os.path.abspath(__file__))


class TestDiagram(unittest.TestCase):

    def test_diagram1(self):
        """
        a(x=1| y) -> b(x)
        b(x) .> c
        """

        diagram_file_name = "{0}/{1}/{2}".format(MODULE_PATH, 'data', 'diagram1')
        d = diagram.Diagram("diagram", diagram_file_name)
        edges, clusters = d.parse_diagram()

        expected_edges = [
            {
                'head': {
                    'pretty_params': 'x = 1, y',
                    'unique_name': '',
                    'params': [{'name': 'x', 'value': '1'}, {'name': 'y', 'value': ''}],
                    'name': 'a'
                },
                'style': 'hard',
                'tail': {
                    'pretty_params': 'x',
                    'unique_name': '',
                    'params': [{'name': 'x', 'value': ''}],
                    'name': 'b'
                }
            },
            {
                'head': {
                    'pretty_params': 'x',
                    'unique_name': '',
                    'params': [{'name': 'x', 'value': ''}],
                    'name': 'b'
                }, 'style': 'soft',
                'tail': {
                    'pretty_params': '',
                    'unique_name': '',
                    'params': [],
                    'name': 'c'
                }
            }
        ]

        def remove_unique_name(edge):
            edge['head']['unique_name'] = ''
            edge['tail']['unique_name'] = ''

        map(remove_unique_name, edges)

        self.assertEqual(edges, expected_edges)

    def test_diagram2(self):
        """
        s1: a,b
        s2: a(x=1)
        s1 -> s2
        """

        diagram_file_name = "{0}/{1}/{2}".format(MODULE_PATH, 'data', 'diagram2')
        d = diagram.Diagram("diagram", diagram_file_name)
        edges, clusters = d.parse_diagram()

        expected_clusters = {
            's2': [
                {
                    'pretty_params': 'x = 1',
                    'unique_name': '',
                    'params': [{'name': 'x', 'value': '1'}],
                    'name': 'a'
                }
            ],
            's1': [
                {
                    'pretty_params': '',
                    'unique_name': '',
                    'params': [],
                    'name': 'a'
                },
                {
                    'pretty_params': '',
                    'unique_name': '',
                    'params': [], 'name': 'b'
                }
            ]
        }

        def remove_unique_name(nodes):
            for node in nodes:
                node['unique_name'] = ''

        map(remove_unique_name, clusters.values())

        self.assertEqual(clusters, expected_clusters)

