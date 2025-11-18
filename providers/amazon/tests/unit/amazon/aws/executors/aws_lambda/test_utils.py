# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import datetime
from unittest import mock

import pytest

from airflow.models.taskinstancekey import TaskInstanceKey
from airflow.providers.amazon.aws.executors.aws_lambda.utils import (
    CONFIG_GROUP_NAME,
    INVALID_CREDENTIALS_EXCEPTIONS,
    AllLambdaConfigKeys,
    CommandType,
    ExecutorConfigType,
    InvokeLambdaKwargsConfigKeys,
    LambdaQueuedTask,
)


class TestLambdaQueuedTask:
    """Test cases for LambdaQueuedTask dataclass."""
    
    def test_lambda_queued_task_creation(self):
        """Test that LambdaQueuedTask can be created with all required attributes."""
        # Create a mock TaskInstanceKey
        mock_key = mock.Mock(spec=TaskInstanceKey)
        
        # Define test values
        command: CommandType = ["airflow", "tasks", "run"]
        queue = "test-queue"
        executor_config: ExecutorConfigType = {"test": "config"}
        attempt_number = 1
        next_attempt_time = datetime.datetime.now()
        
        # Create the task
        task = LambdaQueuedTask(
            key=mock_key,
            command=command,
            queue=queue,
            executor_config=executor_config,
            attempt_number=attempt_number,
            next_attempt_time=next_attempt_time
        )
        
        # Verify all attributes are set correctly
        assert task.key == mock_key
        assert task.command == command
        assert task.queue == queue
        assert task.executor_config == executor_config
        assert task.attempt_number == attempt_number
        assert task.next_attempt_time == next_attempt_time

    def test_lambda_queued_task_with_detailed_mock(self):
        """Test LambdaQueuedTask with properly configured mock TaskInstanceKey."""
        # Create a detailed mock with specific attributes
        mock_key = mock.Mock(spec=TaskInstanceKey)
        mock_key.dag_id = "test_dag"
        mock_key.task_id = "test_task"
        mock_key.run_id = "test_run"
        mock_key.try_number = 1
        mock_key.map_index = -1
        
        task = LambdaQueuedTask(
            key=mock_key,
            command=["airflow", "tasks", "run", "test_dag", "test_task"],
            queue="default",
            executor_config={"max_attempts": 3},
            attempt_number=0,  # Initial attempt
            next_attempt_time=datetime.datetime.now()
        )
        
        # Verify mock attributes are accessible through the task
        assert task.key.dag_id == "test_dag"
        assert task.key.task_id == "test_task"
        assert task.key.run_id == "test_run"
        assert task.attempt_number == 0

    def test_lambda_queued_task_minimal_attempt_number(self):
        """Test LambdaQueuedTask with minimum valid attempt number."""
        mock_key = mock.Mock(spec=TaskInstanceKey)
        
        task = LambdaQueuedTask(
            key=mock_key,
            command=["test"],
            queue="test-queue",
            executor_config={},
            attempt_number=0,  # Minimum valid attempt number
            next_attempt_time=datetime.datetime.now()
        )
        
        assert task.attempt_number == 0

    def test_lambda_queued_task_with_empty_executor_config(self):
        """Test LambdaQueuedTask with empty executor config."""
        mock_key = mock.Mock(spec=TaskInstanceKey)
        
        task = LambdaQueuedTask(
            key=mock_key,
            command=["airflow", "version"],
            queue="empty-config-queue",
            executor_config={},  # Empty config
            attempt_number=1,
            next_attempt_time=datetime.datetime.now()
        )
        
        assert task.executor_config == {}
        assert isinstance(task.executor_config, dict)


class TestInvokeLambdaKwargsConfigKeys:
    """Test cases for InvokeLambdaKwargsConfigKeys class."""
    
    def test_invoke_lambda_kwargs_config_keys_constants(self):
        """Test that InvokeLambdaKwargsConfigKeys has the expected constants."""
        assert hasattr(InvokeLambdaKwargsConfigKeys, "FUNCTION_NAME")
        assert hasattr(InvokeLambdaKwargsConfigKeys, "QUALIFIER")
        
        # Verify constant values
        assert InvokeLambdaKwargsConfigKeys.FUNCTION_NAME == "function_name"
        assert InvokeLambdaKwargsConfigKeys.QUALIFIER == "function_qualifier"

    def test_invoke_lambda_kwargs_config_keys_iteration(self):
        """Test that InvokeLambdaKwargsConfigKeys can be iterated over."""
        config_keys = list(InvokeLambdaKwargsConfigKeys())
        
        # Verify all expected keys are present
        assert "function_name" in config_keys
        assert "function_qualifier" in config_keys
        assert len(config_keys) == 2

    def test_invoke_lambda_kwargs_config_keys_are_strings(self):
        """Test that all InvokeLambdaKwargsConfigKeys values are strings."""
        config_keys = InvokeLambdaKwargsConfigKeys()
        
        for key_name in config_keys:
            key_value = getattr(InvokeLambdaKwargsConfigKeys, key_name.upper())
            assert isinstance(key_value, str)
            assert len(key_value) > 0


class TestAllLambdaConfigKeys:
    """Test cases for AllLambdaConfigKeys class."""
    
    def test_all_lambda_config_keys_inheritance(self):
        """Test that AllLambdaConfigKeys inherits from InvokeLambdaKwargsConfigKeys."""
        # Check inheritance of parent class attributes
        assert hasattr(AllLambdaConfigKeys, "FUNCTION_NAME")
        assert hasattr(AllLambdaConfigKeys, "QUALIFIER")
        
        # Check own attributes
        assert hasattr(AllLambdaConfigKeys, "AWS_CONN_ID")
        assert hasattr(AllLambdaConfigKeys, "CHECK_HEALTH_ON_STARTUP")
        assert hasattr(AllLambdaConfigKeys, "MAX_INVOKE_ATTEMPTS")
        assert hasattr(AllLambdaConfigKeys, "REGION_NAME")
        assert hasattr(AllLambdaConfigKeys, "QUEUE_URL")
        assert hasattr(AllLambdaConfigKeys, "DLQ_URL")
        assert hasattr(AllLambdaConfigKeys, "END_WAIT_TIMEOUT")

    def test_all_lambda_config_keys_constants(self):
        """Test that AllLambdaConfigKeys has the expected constants with correct values."""
        # Parent class constants
        assert AllLambdaConfigKeys.FUNCTION_NAME == "function_name"
        assert AllLambdaConfigKeys.QUALIFIER == "function_qualifier"
        
        # Own constants
        assert AllLambdaConfigKeys.AWS_CONN_ID == "conn_id"
        assert AllLambdaConfigKeys.CHECK_HEALTH_ON_STARTUP == "check_health_on_startup"
        assert AllLambdaConfigKeys.MAX_INVOKE_ATTEMPTS == "max_run_task_attempts"
        assert AllLambdaConfigKeys.REGION_NAME == "region_name"
        assert AllLambdaConfigKeys.QUEUE_URL == "queue_url"
        assert AllLambdaConfigKeys.DLQ_URL == "dead_letter_queue_url"
        assert AllLambdaConfigKeys.END_WAIT_TIMEOUT == "end_wait_timeout"

    def test_all_lambda_config_keys_iteration(self):
        """Test that AllLambdaConfigKeys can be iterated over and contains all expected keys."""
        config_keys = list(AllLambdaConfigKeys())
        
        expected_keys = {
            "function_name",
            "function_qualifier",
            "conn_id",
            "check_health_on_startup",
            "max_run_task_attempts",
            "region_name",
            "queue_url",
            "dead_letter_queue_url",
            "end_wait_timeout"
        }
        
        assert set(config_keys) == expected_keys
        assert len(config_keys) == 9

    def test_all_lambda_config_keys_are_strings(self):
        """Test that all AllLambdaConfigKeys values are non-empty strings."""
        for key_name in AllLambdaConfigKeys():
            key_value = getattr(AllLambdaConfigKeys, key_name.upper())
            assert isinstance(key_value, str), f"Key {key_name} is not a string"
            assert len(key_value) > 0, f"Key {key_name} is empty"


class TestConstants:
    """Test cases for module-level constants."""
    
    def test_config_group_name(self):
        """Test that CONFIG_GROUP_NAME has the expected value."""
        assert CONFIG_GROUP_NAME == "aws_lambda_executor"
        assert isinstance(CONFIG_GROUP_NAME, str)
        assert len(CONFIG_GROUP_NAME) > 0

    def test_invalid_credentials_exceptions(self):
        """Test that INVALID_CREDENTIALS_EXCEPTIONS has the expected values."""
        expected_exceptions = [
            "ExpiredTokenException",
            "InvalidClientTokenId",
            "UnrecognizedClientException",
        ]
        assert INVALID_CREDENTIALS_EXCEPTIONS == expected_exceptions
        
        # Additional validation
        assert isinstance(INVALID_CREDENTIALS_EXCEPTIONS, list)
        assert len(INVALID_CREDENTIALS_EXCEPTIONS) == 3
        
        for exception in INVALID_CREDENTIALS_EXCEPTIONS:
            assert isinstance(exception, str)
            assert len(exception) > 0


class TestTypeAliases:
    """Test cases for type aliases."""
    
    def test_command_type(self):
        """Test that CommandType is a Sequence of strings."""
        # Test with list of strings
        command: CommandType = ["airflow", "tasks", "run", "dag_id", "task_id"]
        assert isinstance(command, list)
        assert all(isinstance(item, str) for item in command)
        assert len(command) == 5

    def test_command_type_with_single_command(self):
        """Test CommandType with a single command element."""
        command: CommandType = ["single_command"]
        assert isinstance(command, list)
        assert len(command) == 1
        assert command[0] == "single_command"

    def test_executor_config_type(self):
        """Test that ExecutorConfigType is a dict with string keys and any values."""
        # Test with various value types
        config: ExecutorConfigType = {
            "string_key": "test_value",
            "int_key": 123,
            "bool_key": True,
            "list_key": ["item1", "item2"],
            "dict_key": {"nested": "value"}
        }
        
        assert isinstance(config, dict)
        assert all(isinstance(key, str) for key in config.keys())
        assert config["string_key"] == "test_value"
        assert config["int_key"] == 123
        assert config["bool_key"] is True
        assert config["list_key"] == ["item1", "item2"]
        assert config["dict_key"] == {"nested": "value"}

    def test_executor_config_type_empty(self):
        """Test ExecutorConfigType with empty dictionary."""
        config: ExecutorConfigType = {}
        assert isinstance(config, dict)
        assert len(config) == 0


class TestIntegration:
    """Integration tests combining multiple components."""
    
    def test_lambda_queued_task_with_realistic_values(self):
        """Test LambdaQueuedTask with realistic values that would be used in production."""
        mock_key = mock.Mock(spec=TaskInstanceKey)
        mock_key.dag_id = "example_dag"
        mock_key.task_id = "example_task"
        mock_key.run_id = "scheduled_2024_01_01T00_00_00"
        
        # Realistic command that Airflow would execute
        command: CommandType = [
            "airflow", "tasks", "run",
            "example_dag", "example_task",
            "scheduled_2024_01_01T00_00_00"
        ]
        
        # Realistic executor config for Lambda
        executor_config: ExecutorConfigType = {
            "function_name": "my-airflow-lambda-function",
            "conn_id": "aws_lambda_connection",
            "region_name": "us-east-1"
        }
        
        task = LambdaQueuedTask(
            key=mock_key,
            command=command,
            queue="lambda_queue",
            executor_config=executor_config,
            attempt_number=1,
            next_attempt_time=datetime.datetime.now() + datetime.timedelta(minutes=5)
        )
        
        # Verify the task contains all expected data
        assert task.key.dag_id == "example_dag"
        assert len(task.command) == 7
        assert "airflow" in task.command
        assert "example_dag" in task.command
        assert task.executor_config["function_name"] == "my-airflow-lambda-function"
        assert task.attempt_number == 1
