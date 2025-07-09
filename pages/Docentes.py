import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import datadotworld as dw
import numpy as np
import shutil
import os

# Configuração da página
st.set_page_config(
    page_title="Docentes - Estado e Formação",
    page_icon="🎓",
    layout="wide"
)

# Funções para executar consultas SPARQL
@st.cache_data(ttl=3600)
def get_docentes_por_estado():
    """Consulta docentes por estado"""
    try:
        ds = dw.load_dataset('dbacademic/dbacademic', force_update=True)
        
        sparql_query = """
        prefix CCSO: <https://w3id.org/ccso/ccso#>
        PREFIX dbp: <http://dbpedia.org/property/>
        PREFIX dbo: <http://dbpedia.org/ontology/>
        prefix foaf: <http://xmlns.com/foaf/0.1/>
        prefix owl: <http://www.w3.org/2002/07/owl#>

        SELECT ?Estado (count (DISTINCT ?s) as ?Docentes)  where {

            ?s a CCSO:Professor.
            ?s CCSO:worksFor ?url_pt.
            ?url_pt owl:sameAs ?url_eng.

            service <http://dbpedia.org/sparql> {
                ?url_eng dbo:state ?state.
                ?state dbp:name ?Estado.
            }
        }
        GROUP BY ?Estado
        ORDER BY DESC (?Docentes)
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
        st.error(f"Erro ao consultar docentes por estado: {str(e)}")
        return pd.DataFrame(), ""

@st.cache_data(ttl=3600)
def get_docentes_por_degree():
    """Consulta docentes por grau de formação"""
    try:
        ds = dw.load_dataset('dbacademic/dbacademic', force_update=True)
        
        sparql_query = """
        prefix CCSO: <https://w3id.org/ccso/ccso#>
        PREFIX dbp: <http://dbpedia.org/property/>
        PREFIX dbo: <http://dbpedia.org/ontology/>
        prefix foaf: <http://xmlns.com/foaf/0.1/>
        prefix owl: <http://www.w3.org/2002/07/owl#>

        SELECT ?GrauFormacao (COUNT(DISTINCT ?s) AS ?Docentes) WHERE {
            ?s a CCSO:Professor.
            ?s CCSO:hasDegree ?GrauFormacao.
        }
        GROUP BY ?GrauFormacao
        ORDER BY DESC(?Docentes)
        """
        
        results = dw.query('dbacademic/dbacademic', sparql_query, query_type='sparql')
        return results.dataframe, sparql_query
        
    except Exception as e:
        st.error(f"Erro ao consultar docentes por grau: {str(e)}")
        return pd.DataFrame(), ""

@st.cache_data(ttl=3600)
def get_docentes_estado_degree():
    """Consulta docentes por estado e grau de formação"""
    try:
        ds = dw.load_dataset('dbacademic/dbacademic', force_update=True)
        
        sparql_query = """
        prefix CCSO: <https://w3id.org/ccso/ccso#>
        PREFIX dbp: <http://dbpedia.org/property/>
        PREFIX dbo: <http://dbpedia.org/ontology/>
        prefix foaf: <http://xmlns.com/foaf/0.1/>
        prefix owl: <http://www.w3.org/2002/07/owl#>

        SELECT ?Estado ?GrauFormacao (COUNT(DISTINCT ?s) AS ?Docentes) WHERE {
            ?s a CCSO:Professor.
            ?s CCSO:hasDegree ?GrauFormacao.
            ?s CCSO:worksFor ?url_pt.
            ?url_pt owl:sameAs ?url_eng.

            SERVICE <http://dbpedia.org/sparql> {
                ?url_eng dbo:state ?state.
                ?state dbp:name ?Estado.
            }
        }
        GROUP BY ?Estado ?GrauFormacao
        ORDER BY ?Estado DESC(?Docentes)
        """
        
        results = dw.query('dbacademic/dbacademic', sparql_query, query_type='sparql')
        return results.dataframe, sparql_query
        
    except Exception as e:
        st.error(f"Erro ao consultar docentes por estado e grau: {str(e)}")
        return pd.DataFrame(), ""

@st.cache_data(ttl=3600)
def get_docentes_por_sexo():
    """Consulta docentes por sexo para filtros"""
    try:
        ds = dw.load_dataset('dbacademic/dbacademic', force_update=True)
        
        sparql_query = """
        prefix CCSO: <https://w3id.org/ccso/ccso#>
        prefix foaf: <http://xmlns.com/foaf/0.1/>
        PREFIX dbp: <http://dbpedia.org/property/>
        PREFIX dbo: <http://dbpedia.org/ontology/>
        prefix owl: <http://www.w3.org/2002/07/owl#>

        SELECT ?Estado ?Sexo (COUNT(DISTINCT ?s) AS ?Docentes) WHERE {
            ?s a CCSO:Professor.
            ?s CCSO:worksFor ?url_pt.
            ?url_pt owl:sameAs ?url_eng.
            
            OPTIONAL { ?s foaf:gender ?Sexo . }

            SERVICE <http://dbpedia.org/sparql> {
                ?url_eng dbo:state ?state.
                ?state dbp:name ?Estado.
            }
        }
        GROUP BY ?Estado ?Sexo
        ORDER BY ?Estado DESC(?Docentes)
        """
        
        results = dw.query('dbacademic/dbacademic', sparql_query, query_type='sparql')
        return results.dataframe, sparql_query
        
    except Exception as e:
        st.error(f"Erro ao consultar docentes por sexo: {str(e)}")
        return pd.DataFrame(), ""

def format_degree_name(degree_uri):
    """Formatar nomes de graus acadêmicos"""
    if pd.isna(degree_uri):
        return "Não informado"
    
    degree_map = {
        "https://w3id.org/ccso/ccso#Doctorate": "Doutorado",
        "https://w3id.org/ccso/ccso#Masters": "Mestrado", 
        "https://w3id.org/ccso/ccso#Bachelors": "Graduação",
        "https://w3id.org/ccso/ccso#PostDoc": "Pós-Doutorado"
    }
    
    return degree_map.get(degree_uri, degree_uri.split('#')[-1] if '#' in degree_uri else degree_uri)

def process_estado_data(df):
    """Processar dados de estado"""
    if df.empty:
        return df
    
    df['Docentes'] = pd.to_numeric(df['Docentes'], errors='coerce')
    df = df.dropna().reset_index(drop=True)
    df = df.sort_values('Docentes', ascending=False).reset_index(drop=True)
    df['Posição'] = range(1, len(df) + 1)
    df['Percentual'] = (df['Docentes'] / df['Docentes'].sum() * 100).round(2)
    
    return df

def process_degree_data(df):
    """Processar dados de grau de formação"""
    if df.empty:
        return df
    
    df['Docentes'] = pd.to_numeric(df['Docentes'], errors='coerce')
    df = df.dropna().reset_index(drop=True)
    df['GrauFormacao_Formatado'] = df['GrauFormacao'].apply(format_degree_name)
    df = df.sort_values('Docentes', ascending=False).reset_index(drop=True)
    df['Percentual'] = (df['Docentes'] / df['Docentes'].sum() * 100).round(2)
    
    return df

def process_combined_data(df):
    """Processar dados combinados estado + grau"""
    if df.empty:
        return df
    
    df['Docentes'] = pd.to_numeric(df['Docentes'], errors='coerce')
    df = df.dropna().reset_index(drop=True)
    df['GrauFormacao_Formatado'] = df['GrauFormacao'].apply(format_degree_name)
    
    return df

def process_gender_data(df):
    """Processar dados de gênero"""
    if df.empty:
        return df
    
    df['Docentes'] = pd.to_numeric(df['Docentes'], errors='coerce')
    df = df.dropna(subset=['Docentes']).reset_index(drop=True)
    
    # Verificar se a coluna 'Sexo' existe, caso contrário tentar encontrar a coluna correta
    if 'Sexo' not in df.columns:
        # Procurar por colunas que possam conter dados de gênero
        gender_columns = [col for col in df.columns if 'gender' in col.lower() or 'sexo' in col.lower()]
        if gender_columns:
            df = df.rename(columns={gender_columns[0]: 'Sexo'})
        else:
            # Retornar DataFrame vazio se não encontrar coluna de gênero
            return pd.DataFrame()
    
    # Tratar valores nulos na coluna Sexo como "Sem informação"
    df['Sexo'] = df['Sexo'].fillna('N')  # N = Não informado
    
    # Formatar gênero incluindo a nova categoria
    df['Sexo_Formatado'] = df['Sexo'].map({
        'M': 'Masculino', 
        'F': 'Feminino',
        'N': 'Sem sexo registrado'
    })
    
    # Garantir que valores não mapeados também sejam tratados
    df['Sexo_Formatado'] = df['Sexo_Formatado'].fillna('Sem sexo registrado')
    
    return df

# Interface principal
st.title("🎓 Análise de Docentes: Estado e Formação Acadêmica")
st.markdown("""
Dashboard para análise da distribuição de docentes por estado brasileiro e grau de formação acadêmica, 
com dados obtidos do DbAcademic integrados ao DBpedia.
""")

# Sidebar para navegação
st.sidebar.title("🧭 Navegação")
page = st.sidebar.radio(
    "Selecione uma análise:",
    [
        "🗺️ Docentes por Estado",
        "🎓 Docentes por Formação", 
        "📊 Análise Combinada",
        "⚖️ Análise por Gênero"
    ]
)

# Botão para recarregar dados
if st.sidebar.button("🔄 Recarregar Todos os Dados"):
    st.cache_data.clear()
    st.rerun()

# === PÁGINA: DOCENTES POR ESTADO ===
if page == "🗺️ Docentes por Estado":
    st.header("🗺️ Distribuição de Docentes por Estado")
    
    # Carregar dados
    with st.spinner("📊 Carregando dados por estado..."):
        df_estado_raw, query_estado = get_docentes_por_estado()
        df_genero_raw, _ = get_docentes_por_sexo()
    
    if df_estado_raw.empty:
        st.error("❌ Não foi possível carregar os dados por estado.")
        st.stop()
    
    # Processar dados
    df_estado = process_estado_data(df_estado_raw)
    df_genero = process_gender_data(df_genero_raw)
    
    # Métricas principais
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("🗺️ Total de Estados", len(df_estado))
    
    with col2:
        st.metric("👥 Total de Docentes", f"{int(df_estado['Docentes'].sum()):,}")
    
    with col3:
        estado_lider = df_estado.iloc[0]
        st.metric("🥇 Estado Líder", estado_lider['Estado'])
    
    with col4:
        st.metric("📊 Docentes no Líder", f"{int(estado_lider['Docentes']):,}")
    
    # Filtro por gênero
    st.subheader("🔧 Filtros")
    
    # Verificar se dados de gênero estão disponíveis e válidos
    genero_disponivel = not df_genero.empty and 'Sexo' in df_genero.columns
    
    if genero_disponivel:
        col1, col2 = st.columns(2)
        
        with col1:
            filtro_genero = st.selectbox(
                "Filtrar por gênero:",
                ["Todos", "Masculino", "Feminino", "Sem sexo registrado"],
                help="Filtrar a visualização por gênero dos docentes"
            )
            mostrar_apenas_com_dados = False
        
       
    else:
        st.warning("⚠️ Dados de gênero não disponíveis para filtragem")
        filtro_genero = "Todos"
        mostrar_apenas_com_dados = True
    
    # Remover o slider - usar valor fixo máximo de 27
    top_n_estados = min(27, len(df_estado))
    
    # Aplicar filtro de gênero se disponível
    if filtro_genero != "Todos" and genero_disponivel:
        try:
            # Mapear filtro para código usado na base
            if filtro_genero == "Masculino":
                genero_code = "M"
            elif filtro_genero == "Feminino":
                genero_code = "F"
            else:  # "Sem sexo registrado"
                genero_code = "N"
            
            # Filtrar dados por gênero
            df_genero_filtrado = df_genero[df_genero['Sexo'] == genero_code]
            
            if not df_genero_filtrado.empty:
                # Agrupar por estado
                df_genero_agrupado = df_genero_filtrado.groupby('Estado')['Docentes'].sum().reset_index()
                
                if mostrar_apenas_com_dados:
                    # Mostrar apenas estados com dados de gênero
                    df_filtrado = df_genero_agrupado.sort_values('Docentes', ascending=False).reset_index(drop=True)
                    df_filtrado['Posição'] = range(1, len(df_filtrado) + 1)
                    
                    if df_filtrado['Docentes'].sum() > 0:
                        df_filtrado['Percentual'] = (df_filtrado['Docentes'] / df_filtrado['Docentes'].sum() * 100).round(2)
                    else:
                        df_filtrado['Percentual'] = 0
                    
                    info_msg = f"📊 Mostrando {len(df_filtrado)} estados com dados de {filtro_genero.lower()}"
                else:
                    # Mostrar todos os estados, preenchendo com 0 onde não há dados
                    df_todos_estados = df_estado[['Estado']].copy()
                    df_filtrado = df_todos_estados.merge(df_genero_agrupado, on='Estado', how='left')
                    df_filtrado['Docentes'] = df_filtrado['Docentes'].fillna(0)
                    df_filtrado = df_filtrado.sort_values('Docentes', ascending=False).reset_index(drop=True)
                    df_filtrado['Posição'] = range(1, len(df_filtrado) + 1)
                    
                    if df_filtrado['Docentes'].sum() > 0:
                        df_filtrado['Percentual'] = (df_filtrado['Docentes'] / df_filtrado['Docentes'].sum() * 100).round(2)
                    else:
                        df_filtrado['Percentual'] = 0
                    
                    estados_com_dados = len(df_filtrado[df_filtrado['Docentes'] > 0])
                    info_msg = f"📊 Mostrando todos os {len(df_filtrado)} estados ({estados_com_dados} com dados de {filtro_genero.lower()})"
                
                # Adicionar coluna de região
                regioes_map = {
                    'São Paulo': 'Sudeste', 'Rio de Janeiro': 'Sudeste', 'Minas Gerais': 'Sudeste', 'Espírito Santo': 'Sudeste',
                    'Rio Grande do Sul': 'Sul', 'Paraná': 'Sul', 'Santa Catarina': 'Sul',
                    'Bahia': 'Nordeste', 'Pernambuco': 'Nordeste', 'Ceará': 'Nordeste', 'Paraíba': 'Nordeste',
                    'Maranhão': 'Nordeste', 'Alagoas': 'Nordeste', 'Sergipe': 'Nordeste', 'Rio Grande do Norte': 'Nordeste', 'Piaui': 'Nordeste',
                    'Goiás': 'Centro-Oeste', 'Mato Grosso': 'Centro-Oeste', 'Mato Grosso do Sul': 'Centro-Oeste', 'Distrito Federal': 'Centro-Oeste',
                    'Amazonas': 'Norte', 'Pará': 'Norte', 'Acre': 'Norte', 'Rondônia': 'Norte', 'Roraima': 'Norte', 'Amapá': 'Norte', 'Tocantins': 'Norte'
                }

                df_filtrado['Região'] = df_filtrado['Estado'].map(regioes_map)
                df_filtrado['Região'] = df_filtrado['Região'].fillna('Outros')
                
                # Mostrar informação
                st.success(f"✅ Filtro aplicado: {filtro_genero}")
                st.info(info_msg)
                
            else:
                st.warning(f"⚠️ Nenhum dado encontrado para: {filtro_genero}")
                df_filtrado = df_estado.copy()
                
        except Exception as e:
            st.error(f"❌ Erro ao aplicar filtro de gênero: {str(e)}")
            df_filtrado = df_estado.copy()
    else:
        df_filtrado = df_estado.copy()
    
    # Gráfico principal - Estados
    st.subheader(f"🏆 Todos os Estados")
    
    top_estados = df_filtrado  # Usar todos os estados em vez de head(top_n_estados)
    
    fig_estados = px.bar(
        top_estados,
        x='Docentes',
        y='Estado',
        orientation='h',
        color='Docentes',
        color_continuous_scale='viridis',
        title=f"Todos os Estados por Número de Docentes" + (f" ({filtro_genero})" if filtro_genero != "Todos" else ""),
        labels={'Docentes': 'Número de Docentes', 'Estado': 'Estado'},
        text='Docentes'
    )
    
    fig_estados.update_layout(
        height=max(800, len(top_estados) * 25),  # Ajustar altura para todos os estados
        yaxis={'categoryorder': 'total ascending'},
        showlegend=False
    )
    fig_estados.update_traces(texttemplate='%{text}', textposition='outside')
    
    st.plotly_chart(fig_estados, use_container_width=True)
    
    # Análise regional
    st.subheader("🌎 Análise por Região")
    
    # Mapeamento de estados para regiões (simplificado)
    regioes_map = {
        'São Paulo': 'Sudeste', 'Rio de Janeiro': 'Sudeste', 'Minas Gerais': 'Sudeste', 'Espírito Santo': 'Sudeste',
        'Rio Grande do Sul': 'Sul', 'Paraná': 'Sul', 'Santa Catarina': 'Sul',
        'Bahia': 'Nordeste', 'Pernambuco': 'Nordeste', 'Ceará': 'Nordeste', 'Paraíba': 'Nordeste',
        'Maranhão': 'Nordeste', 'Alagoas': 'Nordeste', 'Sergipe': 'Nordeste', 'Rio Grande do Norte': 'Nordeste', 'Piauí': 'Nordeste',
        'Goiás': 'Centro-Oeste', 'Mato Grosso': 'Centro-Oeste', 'Mato Grosso do Sul': 'Centro-Oeste', 'Distrito Federal': 'Centro-Oeste',
        'Amazonas': 'Norte', 'Pará': 'Norte', 'Acre': 'Norte', 'Rondônia': 'Norte', 'Roraima': 'Norte', 'Amapá': 'Norte', 'Tocantins': 'Norte'
    }
    
    if not df_filtrado.empty:
        df_filtrado['Região'] = df_filtrado['Estado'].map(regioes_map)
        df_filtrado['Região'] = df_filtrado['Região'].fillna('Outros')
        
        regiao_stats = df_filtrado.groupby('Região').agg({
            'Docentes': ['sum', 'count', 'mean']
        }).round(1)
        
        regiao_stats.columns = ['Total Docentes', 'Qtd Estados', 'Média por Estado']
        regiao_stats = regiao_stats.reset_index().sort_values('Total Docentes', ascending=False)
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Gráfico de pizza por região
            fig_regiao_pie = px.pie(
                regiao_stats,
                values='Total Docentes',
                names='Região',
                title="Distribuição de Docentes por Região",
                color_discrete_sequence=px.colors.qualitative.Set3
            )
            st.plotly_chart(fig_regiao_pie, use_container_width=True)
        
        with col2:
            # Tabela de estatísticas regionais
            st.markdown("**📊 Estatísticas por Região**")
            regiao_display = regiao_stats.copy()
            regiao_display['Total Docentes'] = regiao_display['Total Docentes'].apply(lambda x: f"{int(x):,}")
            regiao_display['Média por Estado'] = regiao_display['Média por Estado'].apply(lambda x: f"{x:.0f}")
            
            st.dataframe(
                regiao_display,
                column_config={
                    'Região': '🌎 Região',
                    'Total Docentes': '👥 Total',
                    'Qtd Estados': '🗺️ Estados',
                    'Média por Estado': '📊 Média'
                },
                hide_index=True,
                use_container_width=True
            )
    
    # Tabela detalhada dos estados
    st.subheader("📋 Ranking Detalhado dos Estados")
    
    display_estados = df_filtrado[['Posição', 'Estado', 'Docentes', 'Percentual']].copy()
    if 'Região' in df_filtrado.columns:
        display_estados = df_filtrado[['Posição', 'Estado', 'Região', 'Docentes', 'Percentual']].copy()
    
    display_estados['Docentes'] = display_estados['Docentes'].apply(lambda x: f"{int(x):,}")
    display_estados['Percentual'] = display_estados['Percentual'].apply(lambda x: f"{x:.2f}%")
    
    column_config = {
        'Posição': st.column_config.NumberColumn('🏆 Pos.', width="small"),
        'Estado': st.column_config.TextColumn('🗺️ Estado'),
        'Docentes': st.column_config.TextColumn('👥 Docentes', width="medium"),
        'Percentual': st.column_config.TextColumn('📊 %', width="small")
    }
    
    if 'Região' in display_estados.columns:
        column_config['Região'] = st.column_config.TextColumn('🌎 Região', width="medium")
    
    st.dataframe(
        display_estados,
        column_config=column_config,
        hide_index=True,
        use_container_width=True
    )

# === PÁGINA: DOCENTES POR FORMAÇÃO ===
elif page == "🎓 Docentes por Formação":
    st.header("🎓 Distribuição de Docentes por Grau de Formação")
    
    # Carregar dados
    with st.spinner("📊 Carregando dados por formação..."):
        df_degree_raw, query_degree = get_docentes_por_degree()
    
    if df_degree_raw.empty:
        st.error("❌ Não foi possível carregar os dados por formação.")
        st.stop()
    
    # Processar dados
    df_degree = process_degree_data(df_degree_raw)
    
    # Métricas principais
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("🎓 Tipos de Formação", len(df_degree))
    
    with col2:
        st.metric("👥 Total de Docentes", f"{int(df_degree['Docentes'].sum()):,}")
    
    with col3:
        formacao_lider = df_degree.iloc[0]
        st.metric("🥇 Formação Líder", formacao_lider['GrauFormacao_Formatado'])
    
    with col4:
        st.metric("📊 % da Formação Líder", f"{formacao_lider['Percentual']:.1f}%")
    
    # Gráfico principal - Graus de formação
    st.subheader("📊 Distribuição por Grau de Formação")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Gráfico de barras
        fig_degree_bar = px.bar(
            df_degree,
            x='GrauFormacao_Formatado',
            y='Docentes',
            color='Docentes',
            color_continuous_scale='plasma',
            title="Docentes por Grau de Formação",
            labels={'GrauFormacao_Formatado': 'Grau de Formação', 'Docentes': 'Número de Docentes'},
            text='Docentes'
        )
        fig_degree_bar.update_layout(xaxis_tickangle=-45, showlegend=False)
        fig_degree_bar.update_traces(texttemplate='%{text}', textposition='outside')
        st.plotly_chart(fig_degree_bar, use_container_width=True)
    
    with col2:
        # Gráfico de pizza
        fig_degree_pie = px.pie(
            df_degree,
            values='Docentes',
            names='GrauFormacao_Formatado',
            title="Proporção por Grau de Formação",
            color_discrete_sequence=px.colors.qualitative.Pastel
        )
        st.plotly_chart(fig_degree_pie, use_container_width=True)
    
    # Análise detalhada - REMOVIDO "Concentração Acadêmica"
    st.subheader("📈 Análise Detalhada")
    
    st.markdown("**📊 Estatísticas de Distribuição**")
    
    total_docentes = df_degree['Docentes'].sum()
    stats_formacao = []
    
    for i, row in df_degree.iterrows():
        stats_formacao.append({
            'Formação': row['GrauFormacao_Formatado'],
            'Docentes': f"{int(row['Docentes']):,}",
            'Percentual': f"{row['Percentual']:.2f}%",
            'Posição': i + 1
        })
    
    stats_df = pd.DataFrame(stats_formacao)
    
    st.dataframe(
        stats_df,
        column_config={
            'Posição': st.column_config.NumberColumn('🏆 Pos.', width="small"),
            'Formação': st.column_config.TextColumn('🎓 Formação'),
            'Docentes': st.column_config.TextColumn('👥 Docentes', width="medium"),
            'Percentual': st.column_config.TextColumn('📊 %', width="small")
        },
        hide_index=True,
        use_container_width=True
    )
    
    # REMOVIDO "Insights sobre Formação Acadêmica"

# === PÁGINA: ANÁLISE COMBINADA ===
elif page == "📊 Análise Combinada":
    st.header("📊 Análise Combinada: Estado × Grau de Formação")
    
    # Carregar dados
    with st.spinner("📊 Carregando dados combinados..."):
        df_combined_raw, query_combined = get_docentes_estado_degree()
    
    if df_combined_raw.empty:
        st.error("❌ Não foi possível carregar os dados combinados.")
        st.stop()
    
    # Processar dados
    df_combined = process_combined_data(df_combined_raw)
    
    # Métricas principais
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("🗺️ Estados", len(df_combined['Estado'].unique()))
    
    with col2:
        st.metric("🎓 Formações", len(df_combined['GrauFormacao_Formatado'].unique()))
    
    with col3:
        st.metric("🔗 Combinações", len(df_combined))
    
    with col4:
        st.metric("👥 Total Docentes", f"{int(df_combined['Docentes'].sum()):,}")
    
    # Filtros
    st.subheader("🔧 Filtros Interativos")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        estados_disponiveis = ['Todos'] + sorted(df_combined['Estado'].unique().tolist())
        filtro_estado = st.selectbox("Filtrar por Estado:", estados_disponiveis)
    
    with col2:
        formacoes_disponiveis = ['Todos'] + sorted(df_combined['GrauFormacao_Formatado'].unique().tolist())
        filtro_formacao = st.selectbox("Filtrar por Formação:", formacoes_disponiveis)
    
    with col3:
        min_docentes = st.number_input("Mínimo de docentes:", min_value=0, value=0)
    
    # Aplicar filtros
    df_filtrado = df_combined.copy()
    
    if filtro_estado != 'Todos':
        df_filtrado = df_filtrado[df_filtrado['Estado'] == filtro_estado]
    
    if filtro_formacao != 'Todos':
        df_filtrado = df_filtrado[df_filtrado['GrauFormacao_Formatado'] == filtro_formacao]
    
    if min_docentes > 0:
        df_filtrado = df_filtrado[df_filtrado['Docentes'] >= min_docentes]
    
    st.info(f"📊 Mostrando {len(df_filtrado)} combinações de {len(df_combined)} totais")
    
    if not df_filtrado.empty:
        # REMOVIDO "Heatmap Estado × Formação"
        
        # Análises específicas baseadas nos filtros
        if filtro_estado != 'Todos':
            st.subheader(f"📍 Análise Específica: {filtro_estado}")
            
            estado_data = df_combined[df_combined['Estado'] == filtro_estado].sort_values('Docentes', ascending=False)
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Gráfico de barras para o estado específico
                fig_estado_spec = px.bar(
                    estado_data,
                    x='GrauFormacao_Formatado',
                    y='Docentes',
                    color='Docentes',
                    title=f"Docentes por Formação em {filtro_estado}",
                    color_continuous_scale='plasma'
                )
                fig_estado_spec.update_layout(xaxis_tickangle=-45)
                st.plotly_chart(fig_estado_spec, use_container_width=True)
            
            with col2:
                # Tabela detalhada do estado
                estado_display = estado_data[['GrauFormacao_Formatado', 'Docentes']].copy()
                estado_display['Percentual'] = (estado_display['Docentes'] / estado_display['Docentes'].sum() * 100).round(2)
                estado_display['Docentes'] = estado_display['Docentes'].apply(lambda x: f"{int(x):,}")
                estado_display['Percentual'] = estado_display['Percentual'].apply(lambda x: f"{x:.2f}%")
                
                st.dataframe(
                    estado_display,
                    column_config={
                        'GrauFormacao_Formatado': '🎓 Formação',
                        'Docentes': '👥 Docentes',
                        'Percentual': '📊 %'
                    },
                    hide_index=True,
                    use_container_width=True
                )
        
        elif filtro_formacao != 'Todos':
            st.subheader(f"🎓 Análise Específica: {filtro_formacao}")
            
            formacao_data = df_combined[df_combined['GrauFormacao_Formatado'] == filtro_formacao].sort_values('Docentes', ascending=False)
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Gráfico para formação específica - Top 15
                top_15_formacao = formacao_data.head(15)
                fig_formacao_spec = px.bar(
                    top_15_formacao,
                    x='Docentes',
                    y='Estado',
                    orientation='h',
                    color='Docentes',
                    title=f"Top 15 Estados com {filtro_formacao}",
                    color_continuous_scale='viridis'
                )
                fig_formacao_spec.update_layout(yaxis={'categoryorder': 'total ascending'})
                st.plotly_chart(fig_formacao_spec, use_container_width=True)
            
            with col2:
                # Tabela detalhada da formação
                formacao_display = formacao_data.head(15)[['Estado', 'Docentes']].copy()
                formacao_display['Percentual'] = (formacao_display['Docentes'] / formacao_display['Docentes'].sum() * 100).round(2)
                formacao_display['Posição'] = range(1, len(formacao_display) + 1)
                formacao_display['Docentes'] = formacao_display['Docentes'].apply(lambda x: f"{int(x):,}")
                formacao_display['Percentual'] = formacao_display['Percentual'].apply(lambda x: f"{x:.2f}%")
                
                st.dataframe(
                    formacao_display[['Posição', 'Estado', 'Docentes', 'Percentual']],
                    column_config={
                        'Posição': '🏆 Pos.',
                        'Estado': '🗺️ Estado',
                        'Docentes': '👥 Docentes',
                        'Percentual': '📊 %'
                    },
                    hide_index=True,
                    use_container_width=True
                )
        
        # Tabela geral filtrada
        st.subheader("📋 Dados Detalhados (Filtrados)")
        
        # Preparar dados para exibição
        df_display = df_filtrado.copy()
        df_display = df_display.sort_values('Docentes', ascending=False).head(50)  # Limitar a 50 registros
        df_display['Docentes'] = df_display['Docentes'].apply(lambda x: f"{int(x):,}")
        
        st.dataframe(
            df_display[['Estado', 'GrauFormacao_Formatado', 'Docentes']],
            column_config={
                'Estado': '🗺️ Estado',
                'GrauFormacao_Formatado': '🎓 Formação',
                'Docentes': '👥 Docentes'
            },
            hide_index=True,
            use_container_width=True,
            height=400
        )
        
        # REMOVIDO "Estatísticas dos Dados Filtrados"
    
    else:
        st.warning("⚠️ Nenhum dado encontrado com os filtros aplicados.")

# === PÁGINA: ANÁLISE POR GÊNERO ===
elif page == "⚖️ Análise por Gênero":
    st.header("⚖️ Análise de Docentes por Gênero e Estado")
    
    # Carregar dados
    with st.spinner("📊 Carregando dados por gênero..."):
        df_genero_raw, query_genero = get_docentes_por_sexo()
        df_estado_raw, _ = get_docentes_por_estado()
    
    if df_genero_raw.empty:
        st.error("❌ Não foi possível carregar os dados por gênero.")
        st.stop()
    
    # Processar dados
    df_genero = process_gender_data(df_genero_raw)
    df_estado = process_estado_data(df_estado_raw)
    
    # Análise geral por gênero
    genero_total = df_genero.groupby('Sexo_Formatado')['Docentes'].sum().reset_index()
    genero_total['Percentual'] = (genero_total['Docentes'] / genero_total['Docentes'].sum() * 100).round(2)
    
    # Métricas principais
    st.subheader("📊 Visão Geral por Gênero")
    
    col1, col2, col3, col4 = st.columns(4)
    
    total_docentes_genero = genero_total['Docentes'].sum()
    masculino_total = genero_total[genero_total['Sexo_Formatado'] == 'Masculino']['Docentes'].sum()
    feminino_total = genero_total[genero_total['Sexo_Formatado'] == 'Feminino']['Docentes'].sum()
    sem_sexo_total = genero_total[genero_total['Sexo_Formatado'] == 'Sem sexo registrado']['Docentes'].sum()
    
    with col1:
        st.metric("👥 Total Geral", f"{int(total_docentes_genero):,}")
    
    with col2:
        st.metric("👨 Masculino", f"{int(masculino_total):,}")
    
    with col3:
        st.metric("👩 Feminino", f"{int(feminino_total):,}")
    
    with col4:
        st.metric("❓ Sem registro", f"{int(sem_sexo_total):,}")
    
    # Métricas adicionais
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if total_docentes_genero > 0:
            pct_sem_registro = (sem_sexo_total / total_docentes_genero) * 100
            st.metric("📊 % Sem registro", f"{pct_sem_registro:.1f}%")
    
    with col2:
        if masculino_total > 0 and feminino_total > 0:
            ratio = feminino_total / masculino_total
            st.metric("⚖️ Razão F/M", f"{ratio:.2f}")
    
    with col3:
        total_com_genero = masculino_total + feminino_total
        if total_com_genero > 0:
            st.metric("✅ Com gênero definido", f"{int(total_com_genero):,}")
    
    # Gráficos de distribuição geral
    col1, col2 = st.columns(2)
    
    with col1:
        # Gráfico de pizza geral
        cores_customizadas = {
            'Masculino': '#1f77b4', 
            'Feminino': '#ff7f0e', 
            'Sem sexo registrado': '#d62728'
        }
        
        fig_genero_pie = px.pie(
            genero_total,
            values='Docentes',
            names='Sexo_Formatado',
            title="Distribuição Geral por Gênero",
            color='Sexo_Formatado',
            color_discrete_map=cores_customizadas,
            hole=0.4
        )
        st.plotly_chart(fig_genero_pie, use_container_width=True)
    
    with col2:
        # Gráfico de barras geral
        fig_genero_bar = px.bar(
            genero_total,
            x='Sexo_Formatado',
            y='Docentes',
            color='Sexo_Formatado',
            title="Comparação por Gênero",
            color_discrete_map=cores_customizadas,
            text='Docentes'
        )
        fig_genero_bar.update_traces(texttemplate='%{text}', textposition='outside')
        fig_genero_bar.update_layout(showlegend=False)
        st.plotly_chart(fig_genero_bar, use_container_width=True)
    
    # Análise por estado e gênero - REMOVIDO slider
    st.subheader("🗺️ Distribuição por Estado e Gênero")
    
    # Filtros - REMOVIDO slider, manter apenas 27 estados
    analise_tipo = st.selectbox(
        "Tipo de análise:",
        ["Absolutos", "Percentual por Estado", "Razão F/M", "Incluir Sem Registro"]
    )
    
    # Preparar dados para análise por estado
    pivot_genero = df_genero.pivot_table(
        index='Estado',
        columns='Sexo_Formatado',
        values='Docentes',
        fill_value=0
    ).reset_index()
    
    # Adicionar colunas calculadas
    pivot_genero['Masculino'] = pivot_genero.get('Masculino', 0)
    pivot_genero['Feminino'] = pivot_genero.get('Feminino', 0)
    pivot_genero['Sem_sexo_registrado'] = pivot_genero.get('Sem sexo registrado', 0)
    
    pivot_genero['Total'] = pivot_genero['Masculino'] + pivot_genero['Feminino'] + pivot_genero['Sem_sexo_registrado']
    pivot_genero['Total_com_genero'] = pivot_genero['Masculino'] + pivot_genero['Feminino']
    
    # Calcular percentuais
    pivot_genero['Pct_Feminino'] = (pivot_genero['Feminino'] / pivot_genero['Total'] * 100).round(2)
    pivot_genero['Pct_Masculino'] = (pivot_genero['Masculino'] / pivot_genero['Total'] * 100).round(2)
    pivot_genero['Pct_Sem_registro'] = (pivot_genero['Sem_sexo_registrado'] / pivot_genero['Total'] * 100).round(2)
    
    # Calcular razão F/M apenas para quem tem dados de gênero
    pivot_genero['Razao_F_M'] = (pivot_genero['Feminino'] / pivot_genero['Masculino']).round(3)
    pivot_genero['Razao_F_M'] = pivot_genero['Razao_F_M'].replace([np.inf, -np.inf], 0).fillna(0)
    
    # Selecionar todos os estados em vez de top N
    top_estados_genero = pivot_genero.sort_values('Total', ascending=False)
    
    if analise_tipo == "Absolutos":
        # Gráfico de barras agrupadas - valores absolutos (apenas M e F)
        df_plot = df_genero[df_genero['Sexo_Formatado'].isin(['Masculino', 'Feminino'])]
        
        fig_genero_estados = px.bar(
            df_plot,
            x='Estado',
            y='Docentes',
            color='Sexo_Formatado',
            barmode='group',
            title=f"Todos os Estados - Docentes por Gênero (Valores Absolutos)",
            color_discrete_map={'Masculino': '#1f77b4', 'Feminino': '#ff7f0e'}
        )
        fig_genero_estados.update_layout(xaxis_tickangle=-45, height=800)
        st.plotly_chart(fig_genero_estados, use_container_width=True)
        
    elif analise_tipo == "Percentual por Estado":
        # Gráfico de barras empilhadas - percentuais (apenas M e F)
        df_pct = top_estados_genero.melt(
            id_vars=['Estado'],
            value_vars=['Pct_Masculino', 'Pct_Feminino'],
            var_name='Genero',
            value_name='Percentual'
        )
        df_pct['Genero'] = df_pct['Genero'].map({
            'Pct_Masculino': 'Masculino',
            'Pct_Feminino': 'Feminino'
        })
        
        fig_genero_pct = px.bar(
            df_pct,
            x='Estado',
            y='Percentual',
            color='Genero',
            title=f"Todos os Estados - Distribuição Percentual por Gênero",
            color_discrete_map={'Masculino': '#1f77b4', 'Feminino': '#ff7f0e'},
            labels={'Percentual': 'Percentual (%)'}
        )
        fig_genero_pct.update_layout(xaxis_tickangle=-45, height=800)
        st.plotly_chart(fig_genero_pct, use_container_width=True)
        
    elif analise_tipo == "Razão F/M":
        # Gráfico de barras - razão feminino/masculino
        fig_razao = px.bar(
            top_estados_genero,
            x='Estado',
            y='Razao_F_M',
            color='Razao_F_M',
            title=f"Todos os Estados - Razão Feminino/Masculino",
            color_continuous_scale='RdYlBu',
            labels={'Razao_F_M': 'Razão F/M'}
        )
        fig_razao.update_layout(xaxis_tickangle=-45, height=800)
        fig_razao.add_hline(y=1, line_dash="dash", line_color="red", 
                           annotation_text="Paridade (1:1)")
        st.plotly_chart(fig_razao, use_container_width=True)
        
    else:  # "Incluir Sem Registro"
        # Gráfico incluindo todas as categorias (M, F, Sem registro)
        fig_completo = px.bar(
            df_genero,
            x='Estado',
            y='Docentes',
            color='Sexo_Formatado',
            barmode='group',
            title="Todos os Estados - Docentes por Gênero (Incluindo Sem Registro)",
            color_discrete_map={
                'Masculino': '#1f77b4', 
                'Feminino': '#ff7f0e', 
                'Sem sexo registrado': '#d62728'
            }
        )
        fig_completo.update_layout(xaxis_tickangle=-45, height=800)
        st.plotly_chart(fig_completo, use_container_width=True)
    
    # Tabela detalhada por estado e gênero - REMOVIDO Razão F/M
    st.subheader("📋 Tabela Detalhada por Estado")
    
    # Preparar dados para exibição
    display_genero = top_estados_genero.copy()
    if 'Masculino' in display_genero.columns and 'Feminino' in display_genero.columns:
        display_genero['Masculino'] = display_genero['Masculino'].apply(lambda x: f"{int(x):,}")
        display_genero['Feminino'] = display_genero['Feminino'].apply(lambda x: f"{int(x):,}")
        display_genero['Sem_sexo_registrado'] = display_genero['Sem_sexo_registrado'].apply(lambda x: f"{int(x):,}")
        display_genero['Total'] = display_genero['Total'].apply(lambda x: f"{int(x):,}")
        display_genero['Pct_Feminino'] = display_genero['Pct_Feminino'].apply(lambda x: f"{x:.1f}%")
        display_genero['Pct_Masculino'] = display_genero['Pct_Masculino'].apply(lambda x: f"{x:.1f}%")
        display_genero['Pct_Sem_registro'] = display_genero['Pct_Sem_registro'].apply(lambda x: f"{x:.1f}%")
        
        st.dataframe(
            display_genero[['Estado', 'Masculino', 'Feminino', 'Sem_sexo_registrado', 'Total', 'Pct_Masculino', 'Pct_Feminino', 'Pct_Sem_registro']],
            column_config={
                'Estado': '🗺️ Estado',
                'Masculino': '👨 Masculino',
                'Feminino': '👩 Feminino',
                'Sem_sexo_registrado': '❓ Sem Registro',
                'Total': '👥 Total',
                'Pct_Masculino': '📊 % M',
                'Pct_Feminino': '📊 % F',
                'Pct_Sem_registro': '📊 % S/R'
            },
            hide_index=True,
            use_container_width=True
        )
    
    # REMOVIDO "Insights sobre Paridade de Gênero"

# Download de dados
st.sidebar.markdown("---")
st.sidebar.subheader("📥 Download de Dados")

# Preparar dados para download baseado na página atual
if page == "🗺️ Docentes por Estado":
    if 'df_estado' in locals() and not df_estado.empty:
        csv_data = df_estado.to_csv(index=False).encode('utf-8')
        st.sidebar.download_button(
            label="📊 Baixar Dados dos Estados",
            data=csv_data,
            file_name=f'docentes_por_estado_{pd.Timestamp.now().strftime("%Y%m%d_%H%M")}.csv',
            mime='text/csv'
        )

elif page == "🎓 Docentes por Formação":
    if 'df_degree' in locals() and not df_degree.empty:
        csv_data = df_degree.to_csv(index=False).encode('utf-8')
        st.sidebar.download_button(
            label="🎓 Baixar Dados de Formação",
            data=csv_data,
            file_name=f'docentes_por_formacao_{pd.Timestamp.now().strftime("%Y%m%d_%H%M")}.csv',
            mime='text/csv'
        )

elif page == "📊 Análise Combinada":
    if 'df_combined' in locals() and not df_combined.empty:
        csv_data = df_combined.to_csv(index=False).encode('utf-8')
        st.sidebar.download_button(
            label="📊 Baixar Dados Combinados",
            data=csv_data,
            file_name=f'docentes_estado_formacao_{pd.Timestamp.now().strftime("%Y%m%d_%H%M")}.csv',
            mime='text/csv'
        )

elif page == "⚖️ Análise por Gênero":
    if 'df_genero' in locals() and not df_genero.empty:
        csv_data = df_genero.to_csv(index=False).encode('utf-8')
        st.sidebar.download_button(
            label="⚖️ Baixar Dados por Gênero",
            data=csv_data,
            file_name=f'docentes_por_genero_{pd.Timestamp.now().strftime("%Y%m%d_%H%M")}.csv',
            mime='text/csv'
        )

# Informações técnicas no sidebar
st.sidebar.markdown("---")
st.sidebar.markdown("### ℹ️ Informações Técnicas")
st.sidebar.markdown("""
**🔧 Tecnologias:**
- SPARQL 1.1 (Consultas Federadas)
- DbAcademic + DBpedia
- Streamlit + Plotly
- Cache de 1 hora

**📊 Dados:**
- Atualizados em tempo real
- Integração semântica
- Análise multidimensional
""")

# Rodapé
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #666; padding: 30px; background: linear-gradient(90deg, #667eea 0%, #764ba2 100%); border-radius: 10px; margin: 20px 0;'>
    <h3 style='color: white; margin: 0;'>📚 Dashboard Avançado de Cursos Acadêmicos</h3>
    <p style='color: #f0f0f0; margin: 10px 0;'>
        Dados em tempo real do <strong>DbAcademic</strong> integrados ao <strong>DBpedia</strong>
    </p>
</div>
""", unsafe_allow_html=True)