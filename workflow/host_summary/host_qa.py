from pathlib import Path
from typing import Union
import pandas as pd
import sys

sys.path.append(str(Path(__file__).parents[1]))
# sys.path.append(str(Path(__file__).parents[1].joinpath('vanna', 'src')))

from vanna.base import VannaBase
from vanna.chromadb import ChromaDB_VectorStore
from host_summary.openai_client import openai_setup
from airbnb_trino_client import create_client

hive_client = None

class HostSummaryLLM(VannaBase):
  def __init__(self, config=None):
    self.client = openai_setup()
    self.model_name = "gpt-4o-mini"
    pass

  def submit_prompt(self, prompt, **kwargs) -> str:
    response = self.client.chat.completions.create(
      model=self.model_name,
      messages=prompt
    )
    return response.choices[0].message.content

class HostSummaryQA(ChromaDB_VectorStore, HostSummaryLLM):
    def __init__(self, config=None):
        ChromaDB_VectorStore.__init__(self, config=config)
        HostSummaryLLM.__init__(self, config=config)

    def system_message(self, message: str) -> any:
        return {"role": "system", "content": message}

    def user_message(self, message: str) -> any:
        return {"role": "user", "content": message}

    def assistant_message(self, message: str) -> any:
        return {"role": "assistant", "content": message}
    
    def connect_to_airbnb_hive(self):
        """
        Connect to the Airbnb Hive database.
        """
        global hive_client
        if hive_client is None:
            hive_client = create_client()

            res = hive_client.fetchone("describe homes.listing__dim_active")
            assert res, "Failed to connect to Airbnb Hive database."

        def run_sql_airbnb_hive(query: str) -> Union[pd.DataFrame, None]:

            try:
                # res = hive_client.fetchall(query)
                # replace \n with space in the query to avoid issues with multi-line queries
                query = query.replace("\n", " ")  # replace newlines with spaces
                query = query.replace(";", "") # remove trailing semicolon if any
                query = query.replace("\t", " ")  # replace tabs with spaces

                cursor = hive_client.conn.cursor()
                cursor.execute(query)
                res = cursor.fetchall()

                df = pd.DataFrame(res, columns=[col[0] for col in cursor.description])
                return df
            except Exception as e:
                print(f"Error executing query: {e}")
                return None
            
        self.run_sql = run_sql_airbnb_hive
        self.run_sql_is_set = True

    def register_hive_table(self, table_name:str):

        if table_name == 'itx.dim_salesforce_account_update':
            self.train(ddl="CREATE TABLE itx.dim_salesforce_account_update ( host_id_external bigint, m_active_listings bigint, m_deactive_listings bigint ds varchar)")
            self.train(question="how many active listings does the host 217570714 have as of 2025-07-01?",
                                            sql="SELECT m_active_listings FROM itx.dim_salesforce_account_update WHERE host_id_external = 217570714 and ds = '2025-07-01'")

            self.train(question="how many listings does the host 217570714 have?",
                                            sql="SELECT m_active_listings + m_deactive_listings as total_listings FROM itx.dim_salesforce_account_update WHERE host_id_external = 217570714 and ds = '2025-07-01'")
        elif table_name == 'host_quality.listing__dim_quality_scores_v3':
            self.train(ddl="CREATE TABLE host_quality.listing__dim_quality_scores_v3 (id_listing bigint, id_host bigint, quality_score_name varchar, quality_score double, weight_version varchar, ds varchar, version varchar)")
            self.train(question="how many Prime listing does host 217570714 have?",
                                        sql="SELECT COUNT(*) as prime_listing_count FROM host_quality.listing__dim_quality_scores_v3 WHERE id_host = 217570714 AND quality_score > 1.94 and ds = '2025-07-01' and version = 'QS_GF.2.1'")

            self.train(question="What is the average listing quality does the host 217570714 have?",
                                        sql="SELECT AVG(quality_score) as average_quality FROM host_quality.listing__dim_quality_scores_v3 WHERE id_host = 217570714 AND ds = '2025-07-01' AND version = 'QS_GF.2.1'")

            self.train(question=f"what is the quality trend since 2025-06-01 of host 217570714?", sql="""
                                    SELECT
                                        AVG(quality_score) as average_quality,
                                        MIN(quality_score) as min_quality,
                                        MAX(quality_score) as max_quality,
                                        STDDEV(quality_score) as stddev_quality
                                    FROM host_quality.listing__dim_quality_scores_v3
                                    WHERE id_host = 217570714 AND ds > '2025-06-01' AND version = 'QS_GF.2.1'
                                    """)
        elif table_name == 'host_growth.listing__fct_deactivation_types_and_reasons':
            self.train(ddl="CREATE TABLE host_growth.listing__fct_deactivation_types_and_reasons (id_listing bigint, id_host bigint, dim_type varchar, dim_reason varchar, dim_details varchar, ds varchar)")
            self.train(question="what are the deactivation type and reason of host 123456789 since 2025-07-01?",
                                        sql="SELECT dim_type, dim_reason FROM host_growth.listing__fct_deactivation_types_and_reasons WHERE id_host = 123456789 and ds >= '2025-07-01'")
            
            self.train(question="what are the top deactivation types since 2025-01-01 for all hosts?", sql="""
                        SELECT dim_type, count(*)
                        FROM host_growth.listing__fct_deactivation_types_and_reasons
                        where ds > '2025-01-01'
                        group by dim_type
                        order by count(*) desc  """)

            self.train(question="which host deactivated the most listings since 2025-07-01?", sql="""            
                        SELECT id_host, count(*) as deactivation_count
                        FROM host_growth.listing__fct_deactivation_types_and_reasons    
                        where ds > '2025-07-01'
                        group by id_host
                        order by count(*) desc    """)         
        else:
            raise ValueError(f"Unsupported table name: {table_name}")

    def ask(
        self,
        question: Union[str, None] = None,
        print_results: bool = False,
        auto_train: bool = False,
        visualize: bool = False,  
        allow_llm_to_see_data: bool = True) -> Union[str, pd.DataFrame]:

        if visualize:
            print_results = True

        result = super().ask(question, auto_train=auto_train, visualize=visualize,
                           allow_llm_to_see_data=allow_llm_to_see_data, print_results=print_results)
        
        prompt = [
            self.system_message(f"""You are a helpful assistant that answers question based on pandas dataframe returned from query. 
                                You answer should be direct and concise. For example, when asked "how many active listings does the host 217570714 have as of 2025-07-01?",
                                you should answer with "The host has xxxx active listings", without any additional context.
                                Query = {result[0]}, Result = {result[1]}"""),
            self.user_message(question),
        ]
        response = self.submit_prompt(prompt=prompt)
        return response


def get_hive_ddl(table_name):
    """
    Get the DDL statement for a given table.
    """
    ddl = hive_client.fetchone(f"show create table {table_name}")

    if ddl: # remove icehouse. from the DDL
        cleaned_string = ddl[0].replace('icehouse.', '')
        return cleaned_string.split("\nWITH")[0]

    return None

# Cache for the HostSummaryQA instance
_host_summary_qa_cache = None

def get_host_summary_qa(config=None):
    """
    Get or create a cached HostSummaryQA instance.
    Returns the same trained instance on subsequent calls for efficiency.
    """
    global _host_summary_qa_cache
    
    if _host_summary_qa_cache is None:
        if config is None:
            config = {'path': str(Path(__file__).parent.joinpath('vector_db/chroma.db'))}
        
        _host_summary_qa_cache = HostSummaryQA(config=config)
        _host_summary_qa_cache.connect_to_airbnb_hive()

    return _host_summary_qa_cache

if __name__ == "__main__":
    # Example usage
    qa = get_host_summary_qa()

    qa.register_hive_table('itx.dim_salesforce_account_update')
    qa.register_hive_table('host_quality.listing__dim_quality_scores_v3')
    qa.register_hive_table('host_growth.listing__fct_deactivation_types_and_reasons')

    host_ids = [217570714,199055975,263502162,122382567,242933544,145244100, 218928815,
            1684752, 1982737, 5266238, 5710846, 6940936, 11256892, 1236025]
    
    result = qa.ask("what are the top 10 common deactivation types since 2025-07-01?")
    print(f"---{result}")

    result = qa.ask(f"which host deactivated the most listings since 2025-07-01?")
    print(f"---{result}")

    result = qa.ask(f"what are the deactivation type and reason for the host 674118442 since 2025-07-01?")
    print (f"---{result}")

    result = qa.ask(f"what is the quality trend since 2025-06-01 of host {host_ids[0]}? please include statistics such as average, min, max, and standard deviation of the quality score.")
    print (f"---{result}")


    result = qa.ask(f"how many active listings does the host {host_ids[0]} have as of 2025-07-01?")
    print (f"---{result}")

    result = qa.ask(f"what percentage of active listings is there for the host {host_ids[0]} as of 2025-07-01?")
    print (f"---{result}")


    result = qa.ask(f"what's total listing, percentage of active listings, and percentage of inactive listings is there for the host {host_ids[0]} as of 2025-07-01?")
    print (f"---{result}")

    result = qa.ask(f"what is the average listing quality does the host {host_ids[0]} have?")
    print (f"---{result}")

    result = qa.ask(f"how many prime listings does the host {host_ids[0]} have?")
    print (f"---{result}")
