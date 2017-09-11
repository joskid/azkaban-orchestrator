import unittest
import json
import os
from random import randint
import logging
from mock import patch
from azkaban_orchestrator import graph, diagram

MODULE_PATH = os.path.dirname(os.path.abspath(__file__))


class TestGraph(unittest.TestCase):

    def setUp(self):
        self.logger = logging.getLogger(__name__)

    def run_pipeline_mock(project, flow, params):
        return randint(0, 1000)

    @patch('azkaban_orchestrator.azkaban.Client.check_pipeline', return_value=True)
    @patch('azkaban_orchestrator.azkaban.Client.run_pipeline', side_effect=run_pipeline_mock)
    def test_edges1(self, run_pipeline, check_pipeline):
        """
        a -> b -> c
        """

        with open('{}/data/edges1.json'.format(MODULE_PATH)) as edges_file:
            edges = json.load(edges_file)

        clusters = {}
        g = graph.Graph(
            edges=edges,
            clusters=clusters,
            params=[],
            host='',
            username='',
            password='',
            logger=self.logger
        )

        path = [node['name'] for node in g.traverse()]

        self.assertEqual(path, ['a', 'b', 'c'])

    @patch('azkaban_orchestrator.azkaban.Client.check_pipeline', return_value=True)
    @patch('azkaban_orchestrator.azkaban.Client.run_pipeline', side_effect=run_pipeline_mock)
    def test_edges2(self, run_pipeline, check_pipeline):
        """
        a -> b -> d
        |     ___/|
        |    |    |
        v    v    v
        c -> e <- f
        |
        v
        g
        """

        with open('{}/data/edges2.json'.format(MODULE_PATH)) as edges_file:
            edges = json.load(edges_file)

        clusters = {}
        g = graph.Graph(
            edges=edges,
            clusters=clusters,
            params=[],
            host='',
            username='',
            password='',
            logger=self.logger
        )

        path = [node['name'] for node in g.traverse()]

        self.assertEqual(path, ['a', 'b', 'c', 'd', 'g', 'f', 'e'])

    @patch('azkaban_orchestrator.azkaban.Client.check_pipeline', return_value=False)
    @patch('azkaban_orchestrator.azkaban.Client.run_pipeline', side_effect=run_pipeline_mock)
    def test_traverse_failed(self, run_pipeline, check_pipeline):
        """
        a(failed) -> b -> c
        """
        with open('{}/data/edges1.json'.format(MODULE_PATH)) as edges_file:
            edges = json.load(edges_file)

        clusters = {}
        g = graph.Graph(
            edges=edges,
            clusters=clusters,
            params=[],
            host='',
            username='',
            password='',
            logger=self.logger
        )

        self.assertRaises(Exception, g.traverse)

    @patch('azkaban_orchestrator.azkaban.Client.check_pipeline', return_value=True)
    @patch('azkaban_orchestrator.azkaban.Client.run_pipeline', side_effect=run_pipeline_mock)
    def test_edges3(self, run_pipeline, check_pipeline):
        """
        ----------
        |  a  b  | s1
        ----------
            |
            v
        ----------
        |  c  d  | s2
        ----------
            |
            v
            e
        """

        with open('{}/data/edges3.json'.format(MODULE_PATH)) as edges_file:
            edges = json.load(edges_file)

        with open('{}/data/clusters3.json'.format(MODULE_PATH)) as clusters_file:
            clusters = json.load(clusters_file)

        g = graph.Graph(
            edges=edges,
            clusters=clusters,
            params=[],
            host='',
            username='',
            password='',
            logger=self.logger
        )

        path = [node['name'] for node in g.traverse()]

        self.assertEqual(path, ['s1', 's2', 'e'])
