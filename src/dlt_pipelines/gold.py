import sys, os

from pyspark import pipelines as dp
from pyspark.sql.functions import expr


@dp.table(comment="AI-enriched tickets with sentiment and summary")
def tickets_gold_ai():
    return (
        dp.read("jira_tickets_silver")
        .withColumn("ai_analysis", expr("""
            ai_query(
                'databricks-meta-llama-3-1-8b-instruct',
                concat('Analyze this ticket. Provide: 1. Sentiment (Positive/Negative/Neutral) 2. A 1-sentence summary. Ticket: ', raw_description)
            )
        """))
    )


@dp.table(name="jira_tickets_categorized")
def jira_tickets_categorized():
    return (
        dp.read("jira_tickets_silver")
        .withColumn("ai_output", expr("""
            ai_query(
                'databricks-meta-llama-3-1-8b-instruct',
                concat(
                    'Classify this Jira ticket into one category: [Technical Debt, User Experience, Critical Bug, Documentation]. ',
                    'Also provide a sentiment score between -1 and 1. ',
                    'Ticket Summary: ', summary, 
                    ' Ticket Description: ', raw_description
                )
            )
        """))
    )