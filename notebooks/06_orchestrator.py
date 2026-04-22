# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — Orchestrator Agent (A-00)
# MAGIC Runs the full LangGraph pipeline end-to-end.
# MAGIC Schedule this notebook via **Databricks Workflows** on a 15-minute trigger (POC-2).

# COMMAND ----------

import sys, os
sys.path.insert(0, "/Workspace/Repos/maubrymusic@gmail.com/poc-wc-defect-analytics")
os.environ["HF_API_TOKEN"] = dbutils.secrets.get(scope="nike-wc-poc", key="hf_api_token")

# COMMAND ----------

%pip install "typing_extensions>=4.9.0" langgraph langchain langchain-community huggingface-hub pyyaml python-dotenv scikit-learn --quiet

# COMMAND ----------
# MAGIC %md ## Run the pipeline

# COMMAND ----------

from agents.orchestrator import run_pipeline

result = run_pipeline()
print(result)

# COMMAND ----------
# MAGIC %md ## View run log

# COMMAND ----------

spark.sql("SELECT * FROM gold.run_log ORDER BY run_timestamp DESC LIMIT 5").display()

# COMMAND ----------
# MAGIC %md ## View escalation queue (items requiring human review)

# COMMAND ----------

spark.sql("""
  SELECT agent_id, escalated_at, question, context_json
  FROM gold.escalation_queue
  WHERE resolved = false
  ORDER BY escalated_at DESC
""").display()

# COMMAND ----------
# MAGIC %md ## LangGraph — visualise the agent graph

# COMMAND ----------

from agents.orchestrator import build_graph
graph = build_graph()
# Display Mermaid diagram of the compiled graph
print(graph.get_graph().draw_mermaid())
