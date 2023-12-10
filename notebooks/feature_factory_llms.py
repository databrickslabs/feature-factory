# Databricks notebook source
# MAGIC %pip install llama-index==0.8.61 pypdf

# COMMAND ----------

# MAGIC %pip install typing_extensions==4.7.1

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %pip list

# COMMAND ----------

from framework.feature_factory.llm_tools import LLMFeature, LlamaIndexDocReader, LlamaIndexDocSplitter, LLMTool, LangChainRecursiveCharacterTextSplitter, LlamaIndexTitleExtractor
from framework.feature_factory import Feature_Factory
import torch
from llama_index.llms import HuggingFaceLLM

# COMMAND ----------

ff = Feature_Factory()

# COMMAND ----------

class MPT7b(LLMTool):
  def create(self):
    torch.cuda.empty_cache()
    generate_params = {
      "temperature": 1.0, 
      "top_p": 1.0, 
      "top_k": 50, 
      "use_cache": True, 
      "do_sample": True, 
      "eos_token_id": 0, 
      "pad_token_id": 0
    }

    llm = HuggingFaceLLM(
      max_new_tokens=256,
      generate_kwargs=generate_params,
      # system_prompt=system_prompt,
      # query_wrapper_prompt=query_wrapper_prompt,
      tokenizer_name="mosaicml/mpt-7b-instruct",
      model_name="mosaicml/mpt-7b-instruct",
      device_map="auto",
      tokenizer_kwargs={"max_length": 1024},
      model_kwargs={"torch_dtype": torch.float16, "trust_remote_code": True}
    )
    self._instance = llm
    return llm
  
  def apply(self):
    ...

# COMMAND ----------

TITLE_NODE_TEMPLATE = "Below is an instruction that describes a task. Write a response that appropriately completes the request.\n### Instruction:\nGive a title that summarizes this paragraph: {context_str}.\n### Response:\n"

# COMMAND ----------

TITLE_COMBINE_TEMPLATE = "Below is an instruction that describes a task. Write a response that appropriately completes the request.\n### Instruction:\nGive a title that summarizes the following: {context_str}.\n### Response:\n"

# COMMAND ----------

title_extractor = LlamaIndexTitleExtractor(
  nodes=5, 
  llm_def = MPT7b(),
  prompt_template = TITLE_NODE_TEMPLATE,
  combine_template = TITLE_COMBINE_TEMPLATE
)

# COMMAND ----------

doc_splitter = LlamaIndexDocSplitter(
  chunk_size = 1024,
  chunk_overlap = 32,
  extractors = [title_extractor]
)

# COMMAND ----------

# doc_splitter = LangChainRecursiveCharacterTextSplitter(
#   chunk_size = 1024,
#   chunk_overlap = 32
# )

# COMMAND ----------

llm_feature = LLMFeature (
  name = "chunks",
  reader = LlamaIndexDocReader(),
  splitter = doc_splitter
)

# COMMAND ----------

partition_num = 2

# COMMAND ----------

df = ff.assemble_llm_feature(spark, srcDirectory= "directory to your documents", llmFeature=llm_feature, partitionNum=partition_num)

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("<catalog>.<database>.<table>")

# COMMAND ----------

# MAGIC %sql select * from liyu_demo.va.chunks

# COMMAND ----------


