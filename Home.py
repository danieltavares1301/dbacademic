import streamlit as st

st.set_page_config(
    page_title="Painel Acadêmico Brasileiro",
    page_icon="🎓",
    layout="wide"
)

st.markdown("""
# 🎓 Painel Acadêmico Brasileiro

Bem-vindo(a) ao **Painel Acadêmico Brasileiro**!

Este aplicativo apresenta uma análise interativa de dados de **cursos** e **docentes** das universidades brasileiras, utilizando dados abertos conectados do [DbAcademic](https://data.world/) e [DBpedia](https://dbpedia.org/).

Aqui você pode:

- Visualizar a distribuição de cursos por universidade, estado e região.
- Consultar rankings dos cursos mais ofertados.
- Explorar o perfil dos docentes por estado, formação acadêmica e gênero.
- Comparar indicadores de diferentes regiões e instituições.

Navegue pelo menu lateral para acessar as diferentes análises e dashboards disponíveis.

---

💡 **Dica:** Clique nas opções do menu à esquerda para começar!

---
""")