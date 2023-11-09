from abc import ABC, abstractmethod
from typing import List, Union
from llama_index import SimpleDirectoryReader
from llama_index.node_parser import SimpleNodeParser
from llama_index.node_parser.extractors import (
    MetadataExtractor,
    TitleExtractor
)
from llama_index.text_splitter import TokenTextSplitter
from llama_index.schema import MetadataMode, Document
from langchain.text_splitter import RecursiveCharacterTextSplitter


class LLMTool(ABC):

    @abstractmethod
    def create(self):
        ...

    @abstractmethod
    def apply(self):
        ...


class DocReader(LLMTool):

    def apply(self, filename: str) -> Union[str, List[Document]]:
        ...

class DocSplitter(LLMTool):
    
    def apply(self, docs: Union[str, List[Document]]) -> List[str]:
        ...


class LlamaIndexDocReader(DocReader):

    def create(self):
        ...

    def apply(self, filename: str):
        documents = SimpleDirectoryReader(input_files=[filename]).load_data()
        return documents


class LLMDef(LLMTool):

    def __init__(self) -> None:
        self._instance = None

    def get_instance(self):
        return self._instance
    


class LlamaIndexDocSplitter(DocSplitter):

    def __init__(self, chunk_size:int=1024, chunk_overlap:int=64, llm:LLMDef=None) -> None:
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.llm = llm

    def create(self):
        text_splitter = TokenTextSplitter(
            separator=" ", chunk_size=self.chunk_size, chunk_overlap=self.chunk_overlap
        )
        if self.llm:
            self.llm.create()
            metadata_extractor = MetadataExtractor(
                extractors=[
                    TitleExtractor(nodes=5, llm=self.llm.get_instance())
                ],
                in_place=False,
            )
        else:
            metadata_extractor = None

        self.node_parser = SimpleNodeParser.from_defaults(
            text_splitter=text_splitter,
            metadata_extractor=metadata_extractor,
        )
        return None
    
    def apply(self, docs):
        doc_nodes = self.node_parser.get_nodes_from_documents(docs)
        chunks = [node.get_content(metadata_mode=MetadataMode.LLM) for node in doc_nodes]
        return chunks
        

class LangChainRecursiveCharacterTextSplitter(DocSplitter):

    def __init__(self, chunk_size=1024, chunk_overlap=64) -> None:
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
    
    def create(self):
        self.text_splitter = RecursiveCharacterTextSplitter(
            chunk_size = self.chunk_size, 
            chunk_overlap = self.chunk_overlap
        )
    
    def apply(self, docs):
        chunks = self.text_splitter.split_text(docs)
        return chunks


class LLMFeature(LLMTool):
    
    def __init__(self, name: str, reader: DocReader, splitter: DocSplitter) -> None:
        self.name = name
        self.reader = reader
        self.splitter = splitter
    
    def create(self):
        self.reader.create()
        self.splitter.create()

    def apply(self, filename: str):
        docs = self.reader.apply(filename)
        return self.splitter.apply(docs)
        
