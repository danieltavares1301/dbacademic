
import streamlit as st
#import requests
#import pandas as pd
#import plotly.express as px

import datadotworld as dw

st.set_page_config(layout = 'wide')

st.title('Consultas')

st.sidebar.title('Consultas ')
sparql = st.sidebar.text_area(
    "Consulta Sparql",
    """
PREFIX ccso: <https://w3id.org/ccso/ccso#> 
PREFIX dbp: <http://pt.dbpedia.org/property/>
PREFIX dbo: <http://dbpedia.org/ontology/>
prefix foaf: <http://xmlns.com/foaf/0.1/>

SELECT ?s ?p ?o where {

    ?s ?p ?o
}
limit 100
   """,
   height=300

    )

ds = dw.load_dataset('dbacademic/dbacademic', force_update=True)




results = dw.query('dbacademic/dbacademic', sparql, query_type='sparql')
df = results.dataframe

st.dataframe(df)