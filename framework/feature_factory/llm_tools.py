from abc import ABC, abstractmethod
from typing import List, Union, Tuple
from llama_index import SimpleDirectoryReader
from llama_index.node_parser import SimpleNodeParser
from llama_index.node_parser.extractors import (
    MetadataExtractor,
    TitleExtractor
)
from llama_index.text_splitter import TokenTextSplitter
from llama_index.schema import MetadataMode, Document as Document
from langchain.docstore.document import Document as LCDocument
from langchain.text_splitter import RecursiveCharacterTextSplitter
from transformers import AutoTokenizer
from langchain.document_loaders import UnstructuredPDFLoader


class LLMTool(ABC):

    def __init__(self) -> None:
        self._initialized = False

    def _require_init(self) -> bool:
        if self._initialized:
            return False
        else:
            self._initialized = True
            return True
        
    @abstractmethod
    def apply(self):
        ...

    @abstractmethod
    def create(self):
        ...


class DocReader(LLMTool):

    def create(self):
        ...

    def apply(self, filename: str) -> Union[str, List[Document]]:
        ...


class DocSplitter(LLMTool):
    
    def __init__(self) -> None:
        super().__init__()

    @classmethod
    def _wrap_docs(cls, docs: Union[Document, LCDocument]):
        if isinstance(docs, Document):
            docs = [docs]
        elif isinstance(docs, LCDocument):
            docs = [docs]
        return docs

    @classmethod
    def _to_text(cls, docs: Union[str, List[Document], List[LCDocument]]):
        if not docs: 
            return ""
        if isinstance(docs, str):
            return docs
        docs = cls._wrap_docs(docs)
        if isinstance(docs[0], Document):
            texts = [doc.get_content(metadata_mode=MetadataMode.LLM) for doc in docs]
            return "\n\n".join(texts)
        if isinstance(docs[0], LCDocument):
            texts = [doc.page_content for doc in docs]
            return "\n\n".join(texts)
        raise ValueError("doc type is not defined.")

    @classmethod
    def _to_documents(cls, txt: Union[str, List[Document]]):
        if not txt:
            return None
        txt = cls._wrap_docs(txt)
        if isinstance(txt, str):
            doc = Document(text=txt)
            return [doc]
        if isinstance(txt[0], Document):
            return txt
        if isinstance(txt[0], LCDocument):
            new_docs = []
            for doc in txt:
                new_doc = Document(text=doc.page_content, metadata=doc.metadata)
                new_docs.append(new_doc)
            return new_docs
    
    @classmethod
    def _to_lcdocuments(cls, docs: Union[str, List[Document], List[LCDocument]]):
        if not docs:
            return None
        docs = cls._wrap_docs(docs)
        if isinstance(docs, str):
            new_docs = LCDocument(page_content=docs)
            return [new_docs]
        if isinstance(docs[0], LCDocument):
            return docs
        if isinstance(docs[0], Document):
            new_docs = []
            for doc in docs:
                metadata = doc.metadata or {}
                new_doc = LCDocument(page_content=doc.text, metadata=metadata)
                new_docs.append(new_doc)
            return new_docs


    def apply(self, docs: Union[str, List[Document]]) -> List[str]:
        ...


class LlamaIndexDocReader(DocReader):

    def __init__(self) -> None:
        super().__init__()

    def apply(self, filename: str) -> List[Document]:
        documents = SimpleDirectoryReader(input_files=[filename]).load_data()
        return documents


class UnstructuredDocReader(DocReader):

    def __init__(self, allowedCategories: Tuple[str]=('NarrativeText', 'ListItem')) -> None:
        super().__init__()
        self.categories = allowedCategories

    def apply(self, filename: str) -> str:
        loader = UnstructuredPDFLoader(filename, mode="elements")
        docs = loader.load()
        filtered_docs = []
        for d in docs:
            if d.metadata.get('category') in self.categories:
                filtered_docs.append(d.page_content)
        combined_text = "\n\n".join(filtered_docs)
        
        return combined_text
    


class LLMDef(LLMTool):

    def __init__(self) -> None:
        self._instance = None

    def get_instance(self):
        return self._instance
    


class LlamaIndexDocSplitter(DocSplitter):

    def __init__(self, chunk_size:int=1024, chunk_overlap:int=64, llm:LLMDef=None) -> None:
        super().__init__()
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.llm = llm

    def create(self):
        if super()._require_init():
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
    
    def apply(self, docs: List[Document]):
        docs = DocSplitter._to_documents(docs)
        self.create()
        doc_nodes = self.node_parser.get_nodes_from_documents(docs)
        chunks = [node.get_content(metadata_mode=MetadataMode.LLM) for node in doc_nodes]
        return chunks


class LangChainRecursiveCharacterTextSplitter(DocSplitter):

    def __init__(self, chunk_size=1024, chunk_overlap=64, pretrained_model_path: str=None) -> None:
        super().__init__()
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.pretrained_model_path = pretrained_model_path
    
    def create(self):
        if super()._require_init():
            if self.pretrained_model_path:
                tokenizer = AutoTokenizer.from_pretrained(self.pretrained_model_path)
                self.text_splitter = RecursiveCharacterTextSplitter.from_huggingface_tokenizer(tokenizer, 
                    chunk_size=self.chunk_size, 
                    chunk_overlap=self.chunk_overlap)
            else:
                self.text_splitter = RecursiveCharacterTextSplitter(
                    chunk_size = self.chunk_size, 
                    chunk_overlap = self.chunk_overlap
                )
    
    def apply(self, docs):
        self.create()
        txt = DocSplitter._to_text(docs)
        chunks = self.text_splitter.split_text(txt)
        return chunks


class LLMFeature(LLMTool):
    
    def __init__(self, name: str, reader: DocReader, splitter: DocSplitter) -> None:
        super().__init__()
        self.name = name
        self.reader = reader
        self.splitter = splitter
    
    def create(self):
        if super()._require_init():
            self.reader.create()
            self.splitter.create()

    def apply(self, filename: str):
        self.create()
        docs = self.reader.apply(filename)
        return self.splitter.apply(docs)
        

class LLMUtils:

    @classmethod
    def split_docs(cls, fileName: str, llmFeat: LLMFeature):
        print(fileName)
        chunks = llmFeat.apply(fileName)
        return (chunks,)
        
    @classmethod
    def process_docs(cls, partitionData, llmFeat):
        llmFeat.create()
        for row in partitionData:
            yield cls.split_docs(row, llmFeat)