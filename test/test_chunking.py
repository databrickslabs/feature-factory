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

    def test_process_docs(self):
        doc_reader =  LlamaIndexDocReader()
        doc_splitter = LlamaIndexDocSplitter()
        llm_feature = LLMFeature("test_llm", reader=doc_reader, splitter=doc_splitter)
        chunks = LLMUtils.process_docs(["test/data/sample.pdf"], llmFeat=llm_feature)
        for chunk in chunks:
            assert len(chunk) == 1

    def test_wrap_docs(self):
        doc = LCDocument(page_content="test", metadata={"filepath": "/tmp/filename"})
        docs = DocSplitter._wrap_docs(doc)
        assert docs[0] == doc

        doc = Document(txt="test")
        docs = DocSplitter._wrap_docs(doc)
        assert docs[0] == doc

        docs = DocSplitter._wrap_docs("test")
        assert docs == "test"


    def test_convert_to_text(self):
        assert DocSplitter._to_text(None) == ""
        doc = LCDocument(page_content="test", metadata={"filepath": "/tmp/filename"})
        txt = DocSplitter._to_text([doc])
        assert txt == "test"

        doc = Document(text="test")
        txt = DocSplitter._to_text([doc])
        assert txt == "test"

        txt = DocSplitter._to_text("test")
        assert txt == "test"

    def test_convert_to_document(self):
        assert DocSplitter._to_documents(None) is None
        doc = LCDocument(page_content="test", metadata={"filepath": "/tmp/filename"})
        new_docs = DocSplitter._to_documents([doc])
        assert new_docs[0].text == "test"

        doc = Document(text="test")
        new_docs = DocSplitter._to_documents([doc])
        assert new_docs[0].text == "test"

        new_docs = DocSplitter._to_documents("test")
        assert new_docs[0].text == "test"

    def test_convert_to_lcdocument(self):
        assert DocSplitter._to_lcdocuments(None) is None
        doc = LCDocument(page_content="test", metadata={"filepath": "/tmp/filename"})
        new_docs = DocSplitter._to_lcdocuments([doc])
        assert new_docs[0].page_content == "test"

        doc = Document(text="test")
        new_docs = DocSplitter._to_lcdocuments([doc])
        assert new_docs[0].page_content == "test"

        new_docs = DocSplitter._to_lcdocuments("test")
        assert new_docs[0].page_content == "test"

    def test_token_splitter(self):
        doc_reader = LlamaIndexDocReader()
        docs = doc_reader.apply("./test/data/sample.pdf")
        assert len(docs) > 0
        doc_splitter = TokenizerTextSpliter(chunk_size=1024, chunk_overlap=32, pretrained_tokenizer_path="hf-internal-testing/llama-tokenizer")
        chunks = doc_splitter.apply(docs)
        assert len(chunks) == 1
    