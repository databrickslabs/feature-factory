import unittest
from framework.feature_factory.feature import Feature, FeatureSet, CompositeFeature
from framework.feature_factory.feature_dict import ImmutableDictBase
from framework.feature_factory import Feature_Factory
from framework.feature_factory.helpers import Helpers
import pyspark.sql.functions as f
import json
from pyspark.sql.types import StructType
from test.local_spark_singleton import SparkSingleton
from framework.feature_factory.catalog import CatalogBase
from enum import IntEnum
from framework.feature_factory.llm_tools import *


class TestLLMTools(unittest.TestCase):

    def test_llamaindex_reader(self):
        doc_reader =  LlamaIndexDocReader()
        doc_reader.create()
        docs = doc_reader.apply("test/data/sample.pdf")
        assert len(docs) == 2
        
    def test_llamaindex_splitter(self):
        doc_reader =  LlamaIndexDocReader()
        doc_reader.create()
        docs = doc_reader.apply("test/data/sample.pdf")

        doc_splitter = LlamaIndexDocSplitter()
        doc_splitter.create()
        chunks = doc_splitter.apply(docs=docs)
        assert len(chunks) == 2

    def test_recursive_splitter(self):
        doc_splitter = LangChainRecursiveCharacterTextSplitter(chunk_size = 200, chunk_overlap=10)
        doc_splitter.create()
        txt = "a"*200 + "b"*30
        chunks = doc_splitter.apply(txt)
        assert len(chunks) == 2 and len(chunks[0]) == 200 and len(chunks[1]) == (30+10)

    def test_recursive_splitter_llamaindex_docs(self):
        doc_reader =  LlamaIndexDocReader()
        docs = doc_reader.apply("test/data/sample.pdf")

        doc_splitter = LangChainRecursiveCharacterTextSplitter(chunk_size = 200, chunk_overlap=10)
        chunks = doc_splitter.apply(docs=docs)
        assert len(chunks) > 0
        assert doc_splitter._require_init() == False

