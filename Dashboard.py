
import streamlit as st
import requests
import pandas as pd
import plotly.express as px

import datadotworld as dw

st.title('DbAcademic')

st.sidebar.title('Filtros')
nome_curso = st.sidebar.text_input("Nome do Curso")

ds = dw.load_dataset('dbacademic/dbacademic', force_update=True)

sparql_docentes = '''
prefix ccso: <https://w3id.org/ccso/ccso#> 
PREFIX dbp: <http://pt.dbpedia.org/property/>
PREFIX dbo: <http://dbpedia.org/ontology/>
prefix foaf: <http://xmlns.com/foaf/0.1/>

SELECT ?o (count (DISTINCT ?s) as ?qtdocente) where {

    ?s a ccso:ProgramofStudy.
    ?s ccso:belongsTo ?o.

}
GROUP BY ?o

'''

sparql = f"""
PREFIX : <https://dbacademic.linked.data.world/d/dbacademic/>
PREFIX ds-institutos: <https://dbacademic.linked.data.world/d/institutos/>
PREFIX ds-universidades: <https://dbacademic.linked.data.world/d/universidades/>
prefix ccso: <https://w3id.org/ccso/ccso#>

SELECT ?name (COUNT(distinct ?cursos) AS ?qtcursos)

WHERE {{
      ?cursos a ccso:ProgramofStudy.
      ?cursos  ccso:psName ?name.

       FILTER regex(?name, "{nome_curso}", "i" )
}}

GROUP BY ?name
"""

results = dw.query('dbacademic/dbacademic', sparql, query_type='sparql')
df = results.dataframe.sort_values("qtcursos",ascending=False)

st.dataframe(df.head(10))