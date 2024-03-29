import os
import sys
import subprocess
import time

from pathlib import Path

from rest.test import TransactionTestCase as TestCase
from django.conf import settings

from otl_interpreter.interpreter_db import node_commands_manager
from otl_interpreter.interpreter_db.models import NodeCommand, ComputingNode

from mock_computing_node.computing_node_config import read_computing_node_config, read_otl_command_syntax
from base_classes import BaseTearDown

plugins_dir = settings.PLUGINS_DIR
base_rest_dir = settings.BASE_DIR

dispatcher_dir = Path(plugins_dir) / 'otl_interpreter' / 'dispatcher'
dispatcher_main = dispatcher_dir / 'main.py'

test_dir = Path(__file__).parent.parent.resolve()

dispatcher_proc_env = os.environ.copy()
dispatcher_proc_env["PYTHONPATH"] = f'{base_rest_dir}:{plugins_dir}'

computing_node_env = os.environ.copy()
computing_node_env["PYTHONPATH"] = f'{base_rest_dir}:{plugins_dir}:{str(test_dir)}'


class TestCoumputingNodeRegister(BaseTearDown, TestCase):

    def setUp(self):

        self.dispatcher_process = subprocess.Popen(
            [sys.executable, '-u', dispatcher_main, 'core.settings.test', 'use_test_database'],
            env=dispatcher_proc_env
        )

        time.sleep(5)

        self.computing_node_process = subprocess.Popen(
            [sys.executable, '-m', 'mock_computing_node', 'spark1.json', 'spark_commands1.json'],
            env=computing_node_env
        )
        time.sleep(5)

    def test_simple_register(self):
        guids_list = node_commands_manager.get_active_nodes_uuids()
        self.assertEqual(len(guids_list), 1)
        node_conf = read_computing_node_config('spark1.json')
        self.assertEqual(guids_list[0].hex, node_conf['uuid'])


class TestAnotherNodeRegister(BaseTearDown, TestCase):

    def setUp(self):

        self.dispatcher_process = subprocess.Popen(
            [sys.executable, '-u', dispatcher_main, 'core.settings.test', 'use_test_database'],
            env=dispatcher_proc_env
        )

        time.sleep(5)

        self.computing_node_process = subprocess.Popen(
            [sys.executable, '-m', 'mock_computing_node', 'another_name.json', 'spark_commands1.json'],
            env=computing_node_env
        )
        time.sleep(5)

    def test_simple_register_with_another_name(self):
        guids_list = node_commands_manager.get_active_nodes_uuids()
        self.assertEqual(len(guids_list), 1)
        node_conf = read_computing_node_config('another_name.json')
        self.assertEqual(guids_list[0].hex, node_conf['uuid'])


class TestCoumputingNodeUnRegister(BaseTearDown, TestCase):

    def setUp(self):

        self.dispatcher_process = subprocess.Popen(
            [sys.executable, '-u', dispatcher_main, 'core.settings.test'],
            env=dispatcher_proc_env
        )

        time.sleep(5)

        self.computing_node_process = subprocess.Popen(
            [sys.executable, '-m', 'mock_computing_node', 'spark_10_sec_lifetime.json', 'spark_commands1.json'],
            env=computing_node_env
        )
        time.sleep(3)

    def test_unregister(self):
        guids_list = node_commands_manager.get_active_nodes_uuids()
        self.assertEqual(len(guids_list), 1)
        node_conf = read_computing_node_config('spark_10_sec_lifetime.json')
        self.assertEqual(guids_list[0].hex, node_conf['uuid'])
        time.sleep(15)
        guids_list = node_commands_manager.get_active_nodes_uuids()
        self.assertEqual(len(guids_list), 0)
        computing_node = ComputingNode.objects.get(uuid=node_conf['uuid'])
        self.assertEqual(computing_node.active, False)
        node_commands = NodeCommand.objects.filter(node=computing_node)
        for node_command in node_commands:
            self.assertEqual(node_command.active, False)

        # test reregister
        # restart the node
        self.computing_node_process = subprocess.Popen(
            [sys.executable, '-m', 'mock_computing_node', 'spark_10_sec_lifetime.json', 'spark_commands1.json'],
            env=computing_node_env
        )
        time.sleep(5)
        guids_list = node_commands_manager.get_active_nodes_uuids()
        self.assertEqual(len(guids_list), 1)
        node_conf = read_computing_node_config('spark_10_sec_lifetime.json')
        self.assertEqual(guids_list[0].hex, node_conf['uuid'])
        guids_list = node_commands_manager.get_active_nodes_uuids()
        self.assertEqual(len(guids_list), 1)
        computing_node = ComputingNode.objects.get(uuid=node_conf['uuid'])
        self.assertEqual(computing_node.active, True)
        node_commands = NodeCommand.objects.filter(node=computing_node)
        for node_command in node_commands:
            self.assertEqual(node_command.active, True)




