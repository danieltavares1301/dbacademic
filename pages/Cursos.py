import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import datadotworld as dw
import numpy as np
import shutil
import os
import re

# Configuração da página
st.set_page_config(
    page_title="Cursos - Análise Acadêmica",
    page_icon="📚",
    layout="wide"
)

# Funções para executar consultas SPARQL
@st.cache_data(ttl=3600)
def get_cursos_por_universidade():
    """Consulta cursos por universidade"""
    try:
        ds = dw.load_dataset('dbacademic/dbacademic', force_update=True)
        
        sparql_query = """
        prefix ccso: <https://w3id.org/ccso/ccso#> 
        PREFIX dbp: <http://dbpedia.org/property/> 
        PREFIX dbo: <http://dbpedia.org/ontology/> 
        prefix foaf: <http://xmlns.com/foaf/0.1/>
        prefix owl: <http://www.w3.org/2002/07/owl#>

        SELECT ?Universidade (count (DISTINCT ?s) as ?Cursos) where {
            ?s a ccso:ProgramofStudy. 
            ?s ccso:belongsTo ?url_pt. 
            ?url_pt owl:sameAs ?url_eng.
            
            service <http://dbpedia.org/sparql> { 
                ?url_eng dbp:name ?Universidade. 
            }
        } 
        GROUP BY ?Universidade 
        ORDER BY DESC (?Cursos)
        """
        
        results = dw.query('dbacademic/dbacademic', sparql_query, query_type='sparql')
        return results.dataframe, sparql_query
        
    except Exception as e:
        # Tenta limpar o cache se erro for de pasta existente
        if "already exists" in str(e):
            cache_path = os.path.expanduser("~/.dw/cache/dbacademic/dbacademic")
            if os.path.exists(cache_path):
                shutil.rmtree(cache_path)
                st.warning("Cache local limpo automaticamente. Recarregue a página.")
        st.error(f"Erro ao consultar cursos por universidade: {str(e)}")
        return pd.DataFrame(), ""

@st.cache_data(ttl=3600)
def get_quantidade_cursos():
    """Consulta quantidade total de cursos"""
    try:
        ds = dw.load_dataset('dbacademic/dbacademic', force_update=True)
        
        sparql_query = """
        PREFIX : <https://dbacademic.linked.data.world/d/dbacademic/> 
        PREFIX ds-institutos: <https://dbacademic.linked.data.world/d/institutos/> 
        PREFIX ds-universidades: <https://dbacademic.linked.data.world/d/universidades/> 
        prefix ccso: <https://w3id.org/ccso/ccso#>
        
        SELECT (COUNT(distinct ?cursos) AS ?qtcursos)
        WHERE { 
            ?cursos a ccso:ProgramofStudy.
        }
        """
        
        results = dw.query('dbacademic/dbacademic', sparql_query, query_type='sparql')
        return results.dataframe, sparql_query
        
    except Exception as e:
        st.error(f"Erro ao consultar quantidade de cursos: {str(e)}")
        return pd.DataFrame(), ""

@st.cache_data(ttl=3600)
def get_cursos_engenharia_computacao():
    """Consulta cursos de engenharia de computação"""
    try:
        ds = dw.load_dataset('dbacademic/dbacademic', force_update=True)
        
        sparql_query = """
        PREFIX : <https://dbacademic.linked.data.world/d/dbacademic/> 
        PREFIX ds-institutos: <https://dbacademic.linked.data.world/d/institutos/> 
        PREFIX ds-universidades: <https://dbacademic.linked.data.world/d/universidades/> 
        prefix ccso: <https://w3id.org/ccso/ccso#>
        
        SELECT ?cursos ?name ?u
        WHERE { 
            ?cursos a ccso:ProgramofStudy. 
            ?cursos ccso:psName ?name. 
            ?cursos ccso:belongsTo ?u.
            FILTER regex(?name, "ENGENHARIA D. COMPUTAÇÃO", "i" )
        }
        """
        
        results = dw.query('dbacademic/dbacademic', sparql_query, query_type='sparql')
        return results.dataframe, sparql_query
        
    except Exception as e:
        st.error(f"Erro ao consultar cursos de engenharia de computação: {str(e)}")
        return pd.DataFrame(), ""

@st.cache_data(ttl=3600)
def get_cursos_engenharia_por_estado(estado="São Paulo"):
    """Consulta cursos de engenharia por estado"""
    try:
        ds = dw.load_dataset('dbacademic/dbacademic', force_update=True)
        
        sparql_query = f"""
        PREFIX : <https://dbacademic.linked.data.world/d/dbacademic/> 
        PREFIX ds-institutos: <https://dbacademic.linked.data.world/d/institutos/> 
        PREFIX ds-universidades: <https://dbacademic.linked.data.world/d/universidades/> 
        PREFIX ccso: <https://w3id.org/ccso/ccso#> 
        PREFIX owl: <http://www.w3.org/2002/07/owl#> 
        PREFIX dbp: <http://dbpedia.org/property/> 
        PREFIX dbo: <http://dbpedia.org/ontology/>
        
        SELECT ?name (COUNT(?cursos) AS ?qtd) 
        WHERE {{ 
            ?cursos a ccso:ProgramofStudy . 
            ?cursos ccso:psName ?name . 
            ?cursos ccso:belongsTo ?u . 
            ?u owl:sameAs ?u_dbpedia .
            FILTER regex(?name, "engenharia", "i")
            SERVICE <http://dbpedia.org/sparql> {{ 
                OPTIONAL {{ ?u_dbpedia dbp:state ?estado . }} 
                FILTER (lcase(str(?estado)) = "{estado.lower()}") 
            }} 
        }} 
        GROUP BY ?name 
        ORDER BY DESC(?qtd) 
        LIMIT 50
        """
        
        results = dw.query('dbacademic/dbacademic', sparql_query, query_type='sparql')
        return results.dataframe, sparql_query
        
    except Exception as e:
        st.error(f"Erro ao consultar cursos de engenharia por estado: {str(e)}")
        return pd.DataFrame(), ""

@st.cache_data(ttl=3600)
def get_cursos_por_nome():
    """Consulta quantidade de cursos por nome - versão completa"""
    try:
        ds = dw.load_dataset('dbacademic/dbacademic', force_update=True)
        
        sparql_query = """
        PREFIX : <https://dbacademic.linked.data.world/d/dbacademic/> 
        PREFIX ds-institutos: <https://dbacademic.linked.data.world/d/institutos/> 
        PREFIX ds-universidades: <https://dbacademic.linked.data.world/d/universidades/> 
        PREFIX ccso: <https://w3id.org/ccso/ccso#> 
        
        SELECT ?name (COUNT(?cursos) AS ?qtd) 
        WHERE {     
            ?cursos a ccso:ProgramofStudy .     
            ?cursos ccso:psName ?name .     
            ?cursos ccso:belongsTo ?u . 
        } 
        GROUP BY ?name 
        ORDER BY DESC(?qtd)
        """
        
        results = dw.query('dbacademic/dbacademic', sparql_query, query_type='sparql')
        return results.dataframe, sparql_query
        
    except Exception as e:
        st.error(f"Erro ao consultar cursos por nome: {str(e)}")
        return pd.DataFrame(), ""

@st.cache_data(ttl=3600)
def get_cursos_completos_com_universidade():
    """Consulta cursos com informações de universidade e estado"""
    try:
        ds = dw.load_dataset('dbacademic/dbacademic', force_update=True)
        
        sparql_query = """
        prefix ccso: <https://w3id.org/ccso/ccso#> 
        PREFIX dbp: <http://dbpedia.org/property/> 
        PREFIX dbo: <http://dbpedia.org/ontology/> 
        prefix foaf: <http://xmlns.com/foaf/0.1/>
        prefix owl: <http://www.w3.org/2002/07/owl#>

        SELECT ?NomeCurso ?Universidade ?Estado WHERE {
            ?curso a ccso:ProgramofStudy. 
            ?curso ccso:psName ?NomeCurso.
            ?curso ccso:belongsTo ?url_pt. 
            ?url_pt owl:sameAs ?url_eng.
            
            service <http://dbpedia.org/sparql> { 
                ?url_eng dbp:name ?Universidade.
                OPTIONAL {
                    ?url_eng dbo:state ?state.
                    ?state dbp:name ?Estado.
                }
            }
        }
        LIMIT 1000
        """
        
        results = dw.query('dbacademic/dbacademic', sparql_query, query_type='sparql')
        return results.dataframe, sparql_query
        
    except Exception as e:
        st.error(f"Erro ao consultar cursos completos: {str(e)}")
        return pd.DataFrame(), ""

# Funções de processamento de dados melhoradas
def process_universidade_data(df):
    """Processar dados de universidade com análise estatística"""
    if df.empty:
        return df
    
    df['Cursos'] = pd.to_numeric(df['Cursos'], errors='coerce')
    df = df.dropna().reset_index(drop=True)
    df = df.sort_values('Cursos', ascending=False).reset_index(drop=True)
    df['Posição'] = range(1, len(df) + 1)
    df['Percentual'] = (df['Cursos'] / df['Cursos'].sum() * 100).round(2)
    
    # Adicionar categoria de tamanho
    df['Categoria'] = pd.cut(df['Cursos'], 
                           bins=[0, 5, 15, 50, float('inf')], 
                           labels=['Pequena', 'Média', 'Grande', 'Muito Grande'])
    
    return df

def process_curso_nome_data(df):
    """Processar dados de cursos por nome"""
    if df.empty:
        return df
    
    df['qtd'] = pd.to_numeric(df['qtd'], errors='coerce')
    df = df.dropna().reset_index(drop=True)
    df = df.sort_values('qtd', ascending=False).reset_index(drop=True)
    df['Posição'] = range(1, len(df) + 1)
    df['Percentual'] = (df['qtd'] / df['qtd'].sum() * 100).round(2)
    
    return df

def process_engenharia_data(df):
    """Processar dados de cursos de engenharia"""
    if df.empty:
        return df
    
    if 'qtd' in df.columns:
        df['qtd'] = pd.to_numeric(df['qtd'], errors='coerce')
        df = df.dropna().reset_index(drop=True)
        df = df.sort_values('qtd', ascending=False).reset_index(drop=True)
        df['Posição'] = range(1, len(df) + 1)
        df['Percentual'] = (df['qtd'] / df['qtd'].sum() * 100).round(2)
    
    return df

def format_university_name(url_or_name):
    """Formatação melhorada de nomes de universidades mantendo nomes oficiais em inglês"""
    if pd.isna(url_or_name):
        return "Não informado"
    
    if isinstance(url_or_name, str):
        # Se for URL, extrair o nome
        if 'http' in url_or_name:
            parts = url_or_name.split('/')
            name = parts[-1] if parts else url_or_name
        else:
            name = url_or_name
        
        # Se já estiver bem formatado (nomes das universidades federais em inglês), manter
        if any(prefix in name for prefix in ['Federal University', 'University of', 'University for']):
            return name
        
        # Caso contrário, limpar e formatar
        name = name.replace('_', ' ').replace('-', ' ')
        name = ' '.join(word.capitalize() for word in name.split())
        
        return name
    
    return str(url_or_name)

def mapear_regiao_brasil(universidade):
    """Mapeamento completo de universidades para regiões brasileiras (português e inglês)"""
    univ_lower = universidade.lower()
    
    # Sudeste
    sudeste = [
        # Nomes em português
        'usp', 'unicamp', 'unesp', 'ufrj', 'uerj', 'uff', 'ufmg', 'puc-mg', 'ufes', 'unifesp', 'puc-sp', 'mackenzie',
        'são carlos', 'viçosa', 'itajubá', 'são joão del-rei',
        # Nomes em inglês das universidades federais
        'federal university of viçosa', 'federal university of são carlos', 'federal university of itajubá',
        'federal university of são joão del-rei', 'federal fluminense university', 'fluminense university',
        'university of são paulo', 'federal university of minas gerais', 'federal university of rio de janeiro',
        'federal university of espírito santo'
    ]
    if any(univ in univ_lower for univ in sudeste):
        return 'Sudeste'
    
    # Sul
    sul = [
        # Nomes em português
        'ufrgs', 'ufpr', 'ufsc', 'puc-rs', 'unisinos', 'furb', 'udesc', 'uem', 'uel', 'pelotas', 'fronteira sul',
        'utfpr', 'paraná',
        # Nomes em inglês das universidades federais
        'federal university of pelotas', 'federal university of technology – paraná', 'federal university of paraná',
        'federal university of rio grande do sul', 'federal university of santa catarina',
        'federal university of fronteira sul', 'federal university of health sciences of porto',
        'health sciences of porto'
    ]
    if any(univ in univ_lower for univ in sul):
        return 'Sul'
    
    # Nordeste  
    nordeste = [
        # Nomes em português
        'ufba', 'ufpe', 'ufc', 'ufpb', 'ufal', 'ufrn', 'ufse', 'ufpi', 'ufma', 'uece', 'maranhão', 'ceará', 'bahia',
        'pernambuco', 'piauí', 'rio grande do norte', 'paraíba', 'alagoas', 'sergipe',
        # Nomes em inglês das universidades federais
        'federal university of maranhão', 'ceará federal university', 'federal university of ceará',
        'federal university of bahia', 'federal university of pernambuco', 'federal university of piauí',
        'federal university of rio grande do norte', 'university for international integration',
        'federal university of paraíba', 'federal university of alagoas', 'federal university of sergipe'
    ]
    if any(univ in univ_lower for univ in nordeste):
        return 'Nordeste'
    
    # Centro-Oeste
    centro_oeste = [
        # Nomes em português
        'unb', 'ufg', 'ufmt', 'ufms', 'ucb', 'goiás', 'mato grosso', 'brasília',
        # Nomes em inglês das universidades federais
        'federal university of goiás', 'federal university of mato grosso do sul',
        'federal university of mato grosso', 'university of brasília'
    ]
    if any(univ in univ_lower for univ in centro_oeste):
        return 'Centro-Oeste'
    
    # Norte
    norte = [
        # Nomes em português
        'ufam', 'ufpa', 'ufac', 'ufrr', 'unir', 'ufap', 'uft', 'amazonas', 'pará', 'acre', 'tocantins',
        'rondônia', 'roraima', 'amapá',
        # Nomes em inglês das universidades federais
        'federal university of amazonas', 'federal university of pará', 'federal university of acre',
        'federal university of roraima', 'federal university of tocantins', 'federal university of amapá',
        'federal university of rondônia'
    ]
    if any(univ in univ_lower for univ in norte):
        return 'Norte'
    
    return 'Não Identificado'

# Interface principal
st.title("📚 Dashboard Avançado de Cursos Acadêmicos")
st.markdown("""
🎯 **Análise Completa e Inteligente** da distribuição de cursos por universidades brasileiras 
e regiões geográficas.

📊 Dados obtidos do **DbAcademic** integrados ao **DBpedia** com análises multidimensionais e visualizações interativas.
""")

# Sidebar para navegação
st.sidebar.title("🧭 Navegação")
page = st.sidebar.radio(
    "Escolha uma análise:",
    [
        "🏛️ Panorama Universitário",
        "📈 Ranking de Cursos",
        "🔬 Engenharias por Estado",
        "💻 Engenharia de Computação"
    ]
)

# Botão para recarregar dados
if st.sidebar.button("🔄 Atualizar Base de Dados"):
    st.cache_data.clear()
    st.success("✅ Cache limpo! Dados serão recarregados.")
    st.rerun()

# Carregar dados básicos
with st.spinner("🔄 Carregando estatísticas gerais..."):
    df_qtd_cursos_raw, _ = get_quantidade_cursos()
    total_cursos = int(df_qtd_cursos_raw['qtcursos'].iloc[0]) if not df_qtd_cursos_raw.empty else 0

# Métricas globais no topo
st.markdown("### 📊 Estatísticas Gerais")
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("📚 Total de Cursos", f"{total_cursos:,}", 
             help="Número total de cursos cadastrados na base")

with col2:
    # Carregar universidades para métrica
    df_univ_temp, _ = get_cursos_por_universidade()
    total_universidades = len(df_univ_temp) if not df_univ_temp.empty else 0
    st.metric("🏛️ Universidades", f"{total_universidades:,}",
             help="Universidades com cursos cadastrados")

with col3:
    if not df_univ_temp.empty:
        media_cursos = df_univ_temp['Cursos'].mean()
        st.metric("📊 Média por Universidade", f"{media_cursos:.1f}",
                 help="Número médio de cursos por universidade")

with col4:
    if not df_univ_temp.empty:
        max_cursos = df_univ_temp['Cursos'].max()
        st.metric("🏆 Máximo por Universidade", f"{int(max_cursos):,}",
                 help="Maior número de cursos em uma universidade")

st.markdown("---")

# === PÁGINA: PANORAMA UNIVERSITÁRIO ===
if page == "🏛️ Panorama Universitário":
    st.header("🏛️ Panorama Universitário Brasileiro")
    
    # Carregar dados
    with st.spinner("📊 Carregando dados universitários..."):
        df_universidade_raw, query_universidade = get_cursos_por_universidade()
    
    if df_universidade_raw.empty:
        st.error("❌ Não foi possível carregar os dados de universidades.")
        st.stop()
    
    # Processar dados
    df_universidade = process_universidade_data(df_universidade_raw)
    df_universidade['Região'] = df_universidade['Universidade'].apply(mapear_regiao_brasil)
    
    # Filtros interativos
    st.subheader("🎛️ Controles Interativos")
    
    col1, col2 = st.columns(2)
    
    with col1:
        filtro_regiao = st.selectbox(
            "🌎 Filtrar por Região:",
            ['Todas'] + sorted(df_universidade['Região'].unique().tolist())
        )
   
    
    with col2:
        top_n = st.slider("📊 Quantidade para exibir:", 5, min(100, len(df_universidade)), 25)
    
    # Aplicar filtros
    df_filtrado = df_universidade.copy()
    
    if filtro_regiao != 'Todas':
        df_filtrado = df_filtrado[df_filtrado['Região'] == filtro_regiao]
    
    
    # Gráfico principal - ranking universitário
    st.subheader(f"🏆 Top {top_n} Universidades" + (f" - {filtro_regiao}" if filtro_regiao != 'Todas' else ""))
    
    top_universidades = df_filtrado.head(top_n)
    
    fig_universidades = px.bar(
        top_universidades,
        y='Universidade',
        x='Cursos',
        orientation='h',
        color='Região',
        title=f"Ranking de Universidades por Número de Cursos",
        labels={'Cursos': 'Número de Cursos', 'Universidade': 'Universidade'},
        text='Cursos',
        color_discrete_sequence=px.colors.qualitative.Set1
    )
    
    fig_universidades.update_layout(
        height=max(600, top_n * 20),
        yaxis={'categoryorder': 'total ascending'},
        showlegend=True
    )
    fig_universidades.update_traces(texttemplate='%{text}', textposition='outside')
    
    st.plotly_chart(fig_universidades, use_container_width=True)
    
    # Análise regional detalhada
    st.subheader("🌎 Análise Regional Detalhada")
    
    regiao_stats = df_universidade.groupby('Região').agg({
        'Cursos': ['sum', 'count', 'mean', 'std', 'min', 'max']
    }).round(2)
    
    regiao_stats.columns = ['Total Cursos', 'Qtd Universidades', 'Média', 'Desvio Padrão', 'Mínimo', 'Máximo']
    regiao_stats = regiao_stats.reset_index().sort_values('Total Cursos', ascending=False)
    regiao_stats['Participação'] = (regiao_stats['Total Cursos'] / regiao_stats['Total Cursos'].sum() * 100).round(2)
    
    col1, col2 = st.columns(2)
    
    
    with col1:
        # Gráfico de barras comparativo
        fig_regiao_comp = px.bar(
            regiao_stats,
            x='Região',
            y=['Total Cursos'],
            title="Comparativo Regional",
            barmode='group',
            color_discrete_sequence=['#FF6B6B', '#4ECDC4']
        )
        st.plotly_chart(fig_regiao_comp, use_container_width=True)
    
    # Tabela interativa com estatísticas completas
    st.subheader("📊 Estatísticas Regionais Completas")
    
    # Formatar dados para exibição
    regiao_display = regiao_stats.copy()
    for col in ['Total Cursos', 'Qtd Universidades', 'Mínimo', 'Máximo']:
        regiao_display[col] = regiao_display[col].apply(lambda x: f"{int(x):,}")
    
    regiao_display['Participação'] = regiao_display['Participação'].apply(lambda x: f"{x:.1f}%")
    
    st.dataframe(
        regiao_display,
        column_config={
            'Região': st.column_config.TextColumn('🌎 Região'),
            'Total Cursos': st.column_config.TextColumn('📚 Total'),
            'Qtd Universidades': st.column_config.TextColumn('🏛️ Universidades'),
            'Média': st.column_config.NumberColumn('📊 Média', format="%.1f"),
            'Desvio Padrão': st.column_config.NumberColumn('📈 Desvio', format="%.1f"),
            'Mínimo': st.column_config.TextColumn('📉 Min'),
            'Máximo': st.column_config.TextColumn('📈 Max'),
            'Participação': st.column_config.TextColumn('🥧 %')
        },
        hide_index=True,
        use_container_width=True
    )

# === PÁGINA: RANKING DE CURSOS ===
elif page == "📈 Ranking de Cursos":
    st.header("📈 Análise Avançada do Ranking de Cursos")
    
    # Carregar dados
    with st.spinner("📊 Carregando dados completos de cursos..."):
        df_cursos_nome_raw, query_nome = get_cursos_por_nome()
    
    if df_cursos_nome_raw.empty:
        st.error("❌ Não foi possível carregar os dados de cursos.")
        st.stop()
    
    # Processar dados
    df_cursos_nome = process_curso_nome_data(df_cursos_nome_raw)
    
    # Filtros avançados
    st.subheader("🎛️ Filtros Inteligentes")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        min_cursos = st.number_input(
            "🔢 Mínimo de ofertas:",
            min_value=1,
            max_value=int(df_cursos_nome['qtd'].max()),
            value=1
        )
    
    with col2:
        top_n_cursos = st.slider(
            "📊 Quantidade para mostrar:",
            5, 100, 30
        )
    
    with col3:
        busca_curso = st.text_input(
            "🔍 Buscar curso:",
            placeholder="Digite parte do nome..."
        )
    
    # Aplicar filtros
    df_filtrado = df_cursos_nome.copy()
    
    if min_cursos > 1:
        df_filtrado = df_filtrado[df_filtrado['qtd'] >= min_cursos]
    
    if busca_curso:
        mask = df_filtrado['name'].str.contains(busca_curso, case=False, na=False)
        df_filtrado = df_filtrado[mask]
    
    st.info(f"📋 Exibindo {len(df_filtrado)} cursos de {len(df_cursos_nome)} totais")
    
    # Análise estatística básica
    st.subheader("📊 Análise Estatística dos Cursos")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Estatísticas descritivas
        stats_cursos = {
            'Total de Ofertas': f"{df_cursos_nome['qtd'].sum():,}",
            'Média de Ofertas': f"{df_cursos_nome['qtd'].mean():.1f}",
            'Mediana': f"{df_cursos_nome['qtd'].median():.0f}",
            'Desvio Padrão': f"{df_cursos_nome['qtd'].std():.1f}",
            'Máximo': f"{df_cursos_nome['qtd'].max():,}",
            'Mínimo': f"{df_cursos_nome['qtd'].min():,}"
        }
        
        st.markdown("**📊 Estatísticas Descritivas**")
        for key, value in stats_cursos.items():
            st.metric(key, value)
    
    with col2:
        # Histograma de distribuição
        fig_hist = px.histogram(
            df_cursos_nome,
            x='qtd',
            nbins=30,
            title="Distribuição da Frequência de Cursos",
            labels={'qtd': 'Número de Ofertas', 'count': 'Frequência'},
            color_discrete_sequence=['#FF6B6B']
        )
        fig_hist.update_layout(showlegend=False)
        st.plotly_chart(fig_hist, use_container_width=True)
    
    # Ranking principal
    st.subheader(f"🏆 Top {top_n_cursos} Cursos")
    
    top_cursos = df_filtrado.head(top_n_cursos)
    
    # Gráfico de barras horizontal
    fig_cursos = px.bar(
        top_cursos,
        y='name',
        x='qtd',
        orientation='h',
        title=f"Ranking dos Cursos Mais Ofertados",
        labels={'qtd': 'Número de Ofertas', 'name': 'Nome do Curso'},
        text='qtd',
        color='qtd',
        color_continuous_scale='Viridis'
    )
    
    fig_cursos.update_layout(
        height=max(700, len(top_cursos) * 25),
        yaxis={'categoryorder': 'total ascending'},
        showlegend=False
    )
    fig_cursos.update_traces(texttemplate='%{text}', textposition='outside')
    
    st.plotly_chart(fig_cursos, use_container_width=True)
    
    # Tabela detalhada com busca
    st.subheader("📋 Tabela Detalhada de Cursos")
    
    # Preparar dados para exibição
    display_cursos = df_filtrado.head(100)[['Posição', 'name', 'qtd', 'Percentual']].copy()
    display_cursos['qtd'] = display_cursos['qtd'].apply(lambda x: f"{int(x):,}")
    display_cursos['Percentual'] = display_cursos['Percentual'].apply(lambda x: f"{x:.2f}%")
    
    st.dataframe(
        display_cursos,
        column_config={
            'Posição': st.column_config.NumberColumn('🏆 Rank', width="small"),
            'name': st.column_config.TextColumn('📚 Nome do Curso'),
            'qtd': st.column_config.TextColumn('🔢 Ofertas', width="small"),
            'Percentual': st.column_config.TextColumn('📊 %', width="small")
        },
        hide_index=True,
        use_container_width=True,
        height=400
    )

# === PÁGINA: ENGENHARIAS POR ESTADO ===
elif page == "🔬 Engenharias por Estado":
    st.header("🔬 Análise Detalhada de Engenharias por Estado")
    
    # Lista completa de estados brasileiros
    estados_brasil = [
        "Acre", "Alagoas", "Amapá", "Amazonas", "Bahia", "Ceará", "Distrito Federal",
        "Espírito Santo", "Goiás", "Maranhão", "Mato Grosso", "Mato Grosso do Sul",
        "Minas Gerais", "Pará", "Paraíba", "Paraná", "Pernambuco", "Piauí",
        "Rio de Janeiro", "Rio Grande do Norte", "Rio Grande do Sul", "Rondônia",
        "Roraima", "Santa Catarina", "São Paulo", "Sergipe", "Tocantins"
    ]
    
    # Controles
    col1, col2 = st.columns(2)
    
    with col1:
        estado_selecionado = st.selectbox(
            "🗺️ Selecione um estado:",
            estados_brasil,
            index=estados_brasil.index("Maranhão")
        )
    
    with col2:
        comparar_estados = st.multiselect(
            "📊 Comparar com outros estados:",
            [e for e in estados_brasil if e != estado_selecionado],
            default=[]
        )
    
    # Carregar dados do estado principal
    with st.spinner(f"📊 Carregando engenharias de {estado_selecionado}..."):
        df_eng_estado_raw, query_eng_estado = get_cursos_engenharia_por_estado(estado_selecionado)
    
    if df_eng_estado_raw.empty:
        st.error(f"❌ Não foram encontrados cursos de engenharia em {estado_selecionado}.")
        st.info("💡 Tente selecionar outro estado ou verificar a conectividade.")
        st.stop()
    
    # Processar dados principais
    df_eng_estado = process_engenharia_data(df_eng_estado_raw)
    
    # Carregar dados dos estados para comparação
    df_comparacao = {}
    if comparar_estados:
        for estado_comp in comparar_estados:
            with st.spinner(f"📊 Carregando {estado_comp}..."):
                df_comp_raw, _ = get_cursos_engenharia_por_estado(estado_comp)
                if not df_comp_raw.empty:
                    df_comp_processed = process_engenharia_data(df_comp_raw)
                    df_comparacao[estado_comp] = df_comp_processed
    
    # Métricas principais
    st.subheader(f"📊 Panorama das Engenharias em {estado_selecionado}")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("🔬 Total de Engenharias", f"{len(df_eng_estado):,}")
    
    with col2:
        st.metric("🎯 Total de Ofertas", f"{df_eng_estado['qtd'].sum():,}")
    
    with col3:
        eng_lider = df_eng_estado.iloc[0]['name']
        st.metric("🥇 Engenharia Líder", eng_lider[:20] + "..." if len(eng_lider) > 20 else eng_lider)
    
    with col4:
        qtd_eng_lider = int(df_eng_estado.iloc[0]['qtd'])
        st.metric("📊 Ofertas da Líder", f"{qtd_eng_lider:,}")
    
    # Ranking detalhado das engenharias
    st.subheader(f"🏆 Ranking Completo das Engenharias em {estado_selecionado}")
    
    fig_eng_ranking = px.bar(
        df_eng_estado,
        y='name',
        x='qtd',
        orientation='h',
        title=f"Todas as Engenharias em {estado_selecionado}",
        labels={'qtd': 'Número de Ofertas', 'name': 'Curso de Engenharia'},
        text='qtd',
        color='qtd',
        color_continuous_scale='Viridis'
    )
    
    fig_eng_ranking.update_layout(
        height=max(800, len(df_eng_estado) * 20),
        yaxis={'categoryorder': 'total ascending'},
        showlegend=True
    )
    fig_eng_ranking.update_traces(texttemplate='%{text}', textposition='outside')
    
    st.plotly_chart(fig_eng_ranking, use_container_width=True)
    
    # Análise comparativa com outros estados
    if df_comparacao:
        st.subheader("📊 Análise Comparativa entre Estados")
        
        # Preparar dados para comparação
        dados_comparacao = []
        
        # Adicionar estado principal
        for _, row in df_eng_estado.iterrows():
            dados_comparacao.append({
                'Estado': estado_selecionado,
                'Engenharia': row['name'],
                'Ofertas': row['qtd']
            })
        
        # Adicionar estados de comparação
        for estado_comp, df_comp in df_comparacao.items():
            for _, row in df_comp.iterrows():
                dados_comparacao.append({
                    'Estado': estado_comp,
                    'Engenharia': row['name'],
                    'Ofertas': row['qtd']
                })
        
        df_comp_final = pd.DataFrame(dados_comparacao)
        
        # Gráfico comparativo de engenharias entre estados
        top_eng = df_comp_final.groupby('Engenharia')['Ofertas'].sum().sort_values(ascending=False).head(10).index
        df_top_comp = df_comp_final[df_comp_final['Engenharia'].isin(top_eng)]
        
        fig_comp_eng = px.bar(
            df_top_comp,
            x='Engenharia',
            y='Ofertas',
            color='Estado',
            title="Comparação das Principais Engenharias entre Estados",
            barmode='group',
            color_discrete_sequence=px.colors.qualitative.Set1
        )
        fig_comp_eng.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig_comp_eng, use_container_width=True)
        
        # Tabela comparativa
        eng_totais = df_comp_final.groupby('Estado')['Ofertas'].sum().reset_index()
        st.dataframe(
            eng_totais.sort_values('Ofertas', ascending=False),
            column_config={
                'Estado': st.column_config.TextColumn('🗺️ Estado'),
                'Ofertas': st.column_config.NumberColumn('📊 Total de Ofertas')
            },
            hide_index=True,
            use_container_width=True
        )
    
    # Tabela detalhada
    st.subheader("📋 Detalhamento Completo")
    
    display_eng = df_eng_estado[['Posição', 'name', 'qtd', 'Percentual']].copy()
    display_eng['qtd'] = display_eng['qtd'].apply(lambda x: f"{int(x):,}")
    display_eng['Percentual'] = display_eng['Percentual'].apply(lambda x: f"{x:.2f}%")
    
    st.dataframe(
        display_eng,
        column_config={
            'Posição': st.column_config.NumberColumn('🏆 Rank', width="small"),
            'name': st.column_config.TextColumn('🔬 Curso de Engenharia'),
            'qtd': st.column_config.TextColumn('🔢 Ofertas', width="small"),
            'Percentual': st.column_config.TextColumn('📊 %', width="small")
        },
        hide_index=True,
        use_container_width=True
    )

# === PÁGINA: ENGENHARIA DE COMPUTAÇÃO ===
elif page == "💻 Engenharia de Computação":
    st.header("💻 Análise Profunda de Engenharia de Computação")
    
    # Carregar dados
    with st.spinner("📊 Carregando dados de Engenharia de Computação..."):
        df_eng_comp_raw, query_eng_comp = get_cursos_engenharia_computacao()
    
    if df_eng_comp_raw.empty:
        st.error("❌ Não foram encontrados cursos de Engenharia de Computação.")
        st.info("💡 Verifique a conectividade ou tente recarregar os dados.")
        st.stop()
    
    # Processar dados - ADICIONADO CÓDIGO PARA REMOVER DUPLICATAS
    df_eng_comp = df_eng_comp_raw.copy()
    
    # Remover duplicatas com base no nome do curso e universidade
    df_eng_comp = df_eng_comp.drop_duplicates(subset=['name', 'u'])
    
    # Continuar o processamento
    df_eng_comp['Universidade_Nome'] = df_eng_comp['u'].apply(format_university_name)
    df_eng_comp['Região'] = df_eng_comp['u'].apply(lambda x: mapear_regiao_brasil(format_university_name(x)))
    
    # Análise de variações do nome
    variações_nome = df_eng_comp['name'].value_counts()
    
    # Adicionar métricas sobre duplicatas removidas
    total_antes = len(df_eng_comp_raw)
    total_depois = len(df_eng_comp)
    duplicatas_removidas = total_antes - total_depois
    
    # Métricas detalhadas
    st.subheader("📊 Estatísticas Gerais")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("💻 Total de Cursos", f"{len(df_eng_comp):,}")
    
    with col2:
        univ_unicas = df_eng_comp['Universidade_Nome'].nunique()
        st.metric("🏛️ Universidades", f"{univ_unicas:,}")
    
    with col3:
        regioes_com_curso = df_eng_comp['Região'].nunique()
        st.metric("🌎 Regiões Atendidas", f"{regioes_com_curso}/5")
    
    with col4:
        variações_nome_count = len(variações_nome)
        st.metric("📝 Variações do Nome", f"{variações_nome_count:,}")
    
    
    # Análise das variações do nome
    st.subheader("📝 Análise das Variações do Nome do Curso")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Gráfico das variações mais comuns
        top_variacoes = variações_nome.head(10)
        fig_variacoes = px.bar(
            x=top_variacoes.values,
            y=top_variacoes.index,
            orientation='h',
            title="Top 10 Variações do Nome",
            labels={'x': 'Frequência', 'y': 'Nome do Curso'},
            color=top_variacoes.values,
            color_continuous_scale='Blues'
        )
        fig_variacoes.update_layout(height=400)
        st.plotly_chart(fig_variacoes, use_container_width=True)
    
    # Distribuição geográfica
    st.subheader("🌎 Distribuição Geográfica")
    
    regiao_counts = df_eng_comp['Região'].value_counts()
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Gráfico de pizza regional
        fig_regiao_pie = px.pie(
            values=regiao_counts.values,
            names=regiao_counts.index,
            title="Distribuição por Região",
            color_discrete_sequence=px.colors.qualitative.Set2,
            hole=0.3
        )
        fig_regiao_pie.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig_regiao_pie, use_container_width=True)
    
    with col2:
        # Análise de concentração regional
        regiao_stats = df_eng_comp.groupby('Região').agg({
            'Universidade_Nome': 'nunique',
            'name': 'count'
        }).reset_index()
        regiao_stats.columns = ['Região', 'Universidades', 'Total Cursos']
        regiao_stats['Cursos por Universidade'] = (regiao_stats['Total Cursos'] / regiao_stats['Universidades']).round(2)
        
        fig_concentracao = px.scatter(
            regiao_stats,
            x='Universidades',
            y='Total Cursos',
            size='Cursos por Universidade',
            color='Região',
            title="Concentração Regional",
            hover_name='Região',
            size_max=50
        )
        st.plotly_chart(fig_concentracao, use_container_width=True)
    
  
    
    # Visualização em cards das universidades
    st.subheader("🏫 Galeria de Universidades")
    
    # Agrupar por região para melhor organização
    for regiao in df_eng_comp['Região'].unique():
        if regiao != 'Não Identificado':
            st.markdown(f"### 🌎 {regiao}")
            
            df_regiao = df_eng_comp[df_eng_comp['Região'] == regiao]
            univ_regiao = df_regiao['Universidade_Nome'].value_counts()
            
            # Mostrar em colunas de 3
            for i in range(0, len(univ_regiao), 3):
                cols = st.columns(3)
                for j in range(3):
                    if i+j < len(univ_regiao):
                        with cols[j]:
                            univ_nome = univ_regiao.index[i+j]
                            qtd_cursos = univ_regiao.iloc[i+j]
                            
                            st.markdown(f"""
                            <div style="padding:15px;border-radius:10px;border:2px solid #4ECDC4;background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);color:white;height:120px;margin:5px">
                                <h6 style="margin:0;color:white">{univ_nome}</h6>
                                <p style="margin:5px 0;"><b>📚 Cursos:</b> {qtd_cursos}</p>
                                <p style="margin:5px 0;"><b>🌎 Região:</b> {regiao}</p>
                            </div>
                            """, unsafe_allow_html=True)
    
    # Tabela completa e pesquisável
    st.subheader("📋 Base Completa de Dados")
    
    # Preparar dados para exibição
    df_display = df_eng_comp[['name', 'Universidade_Nome', 'Região']].copy()
    df_display.columns = ['Nome do Curso', 'Universidade', 'Região']
    
    # Adicionar filtro de busca
    busca_univ = st.text_input("🔍 Buscar universidade:", placeholder="Digite o nome da universidade...")
    
    if busca_univ:
        mask = df_display['Universidade'].str.contains(busca_univ, case=False, na=False)
        df_display = df_display[mask]
    
    st.dataframe(
        df_display,
        column_config={
            'Nome do Curso': st.column_config.TextColumn('💻 Nome do Curso'),
            'Universidade': st.column_config.TextColumn('🏛️ Universidade'),
            'Região': st.column_config.TextColumn('🌎 Região', width="medium")
        },
        hide_index=True,
        use_container_width=True,
        height=400
    )

# Sidebar - Downloads e informações
st.sidebar.markdown("---")
st.sidebar.subheader("📥 Downloads Inteligentes")

# Downloads específicos por página
if page == "🏛️ Panorama Universitário":
    if 'df_universidade' in locals() and not df_universidade.empty:
        csv_data = df_universidade.to_csv(index=False).encode('utf-8')
        st.sidebar.download_button(
            label="🏛️ Baixar Dados Universitários",
            data=csv_data,
            file_name=f'panorama_universitario_{pd.Timestamp.now().strftime("%Y%m%d_%H%M")}.csv',
            mime='text/csv'
        )

elif page == "📈 Ranking de Cursos":
    if 'df_cursos_nome' in locals() and not df_cursos_nome.empty:
        csv_data = df_cursos_nome.to_csv(index=False).encode('utf-8')
        st.sidebar.download_button(
            label="📈 Baixar Ranking de Cursos",
            data=csv_data,
            file_name=f'ranking_cursos_{pd.Timestamp.now().strftime("%Y%m%d_%H%M")}.csv',
            mime='text/csv'
        )

elif page == "🔬 Engenharias por Estado":
    if 'df_eng_estado' in locals() and not df_eng_estado.empty:
        csv_data = df_eng_estado.to_csv(index=False).encode('utf-8')
        st.sidebar.download_button(
            label="🔬 Baixar Dados de Engenharia",
            data=csv_data,
            file_name=f'engenharias_{estado_selecionado.replace(" ", "_")}_{pd.Timestamp.now().strftime("%Y%m%d_%H%M")}.csv',
            mime='text/csv'
        )

elif page == "💻 Engenharia de Computação":
    if 'df_eng_comp' in locals() and not df_eng_comp.empty:
        csv_data = df_eng_comp.to_csv(index=False).encode('utf-8')
        st.sidebar.download_button(
            label="💻 Baixar Dados de Computação",
            data=csv_data,
            file_name=f'engenharia_computacao_{pd.Timestamp.now().strftime("%Y%m%d_%H%M")}.csv',
            mime='text/csv'
        )

# Informações técnicas aprimoradas
st.sidebar.markdown("---")
st.sidebar.markdown("### ⚙️ Informações Técnicas")
st.sidebar.markdown("""
**🔧 Stack Tecnológico:**
- **Query Engine:** SPARQL 1.1 Federado
- **Data Sources:** DbAcademic + DBpedia
- **Frontend:** Streamlit + Plotly
- **Caching:** TTL 1 hora
- **Processing:** Pandas + NumPy

**📊 Características dos Dados:**
- ✅ Tempo real via SPARQL
- ✅ Integração semântica
- ✅ Análise multidimensional
- ✅ Mapeamento geográfico

**🎯 Funcionalidades Avançadas:**
- 📈 Análise estatística avançada
- 🎨 Visualizações interativas
- 🔍 Filtros inteligentes
- 📥 Exports personalizados
- 🌎 Mapeamento regional
""")

# Sidebar - Estatísticas em tempo real
st.sidebar.markdown("---")
st.sidebar.markdown("### 📊 Status do Sistema")

col1, col2 = st.sidebar.columns(2)
with col1:
    st.metric("🔄 Cache", "Ativo", "1h TTL")
with col2:
    st.metric("📡 SPARQL", "Online", "Federado")

# Rodapé aprimorado
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #666; padding: 30px; background: linear-gradient(90deg, #667eea 0%, #764ba2 100%); border-radius: 10px; margin: 20px 0;'>
    <h3 style='color: white; margin: 0;'>📚 Dashboard Avançado de Cursos Acadêmicos</h3>
    <p style='color: #f0f0f0; margin: 10px 0;'>
        Dados em tempo real do <strong>DbAcademic</strong> integrados ao <strong>DBpedia</strong>
    </p>
</div>
""", unsafe_allow_html=True)