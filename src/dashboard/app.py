"""
McKinsey Wave Dashboard - Executive Transformation Tracker
Simule l'interface que les clients McKinsey utiliseraient
"""

import streamlit as st
import pandas as pd
import plotly.express as px  # type: ignore
import plotly.graph_objects as go  # type: ignore
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional
import time

# Configuration de la page
st.set_page_config(
    page_title="McKinsey Wave - Transformation Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS personnalisé pour un look McKinsey professionnel
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f4e79;
        text-align: center;
        padding: 1rem 0;
        border-bottom: 3px solid #1f4e79;
        margin-bottom: 2rem;
    }
    
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem;
        border-radius: 10px;
        color: white;
        text-align: center;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    
    .metric-value {
        font-size: 2rem;
        font-weight: bold;
    }
    
    .metric-label {
        font-size: 0.9rem;
        opacity: 0.9;
    }
    
    .alert-critical {
        background-color: #ff4444;
        padding: 1rem;
        border-radius: 5px;
        color: white;
        margin: 1rem 0;
    }
    
    .alert-warning {
        background-color: #ffaa00;
        padding: 1rem;
        border-radius: 5px;
        color: white;
        margin: 1rem 0;
    }
    
    .alert-success {
        background-color: #00aa44;
        padding: 1rem;
        border-radius: 5px;
        color: white;
        margin: 1rem 0;
    }
    
    .sidebar .sidebar-content {
        background-color: #f0f2f6;
    }
</style>
""", unsafe_allow_html=True)


class DashboardDataLoader:
    """Chargeur de données pour le dashboard"""

    def __init__(self):
        self.analytics_dir = Path("data/analytics")
        self.cache_timeout = 300  # 5 minutes

    @st.cache_data(ttl=300)
    def load_portfolio_metrics(_self) -> Dict[str, Any]:
        """Charge les métriques portfolio avec cache"""
        try:
            portfolio_file = _self.analytics_dir / "portfolio_metrics_latest.json"
            if portfolio_file.exists():
                with open(portfolio_file, 'r') as f:
                    return json.load(f)
            return {}
        except Exception as e:
            st.error(f"Erreur lors du chargement des métriques: {e}")
            return {}

    @st.cache_data(ttl=300)
    def load_initiative_health(_self) -> pd.DataFrame:
        """Charge les données de health des initiatives"""
        try:
            health_file = _self.analytics_dir / "initiative_health_latest.parquet"
            if health_file.exists():
                return pd.read_parquet(health_file)
            return pd.DataFrame()
        except Exception as e:
            st.error(f"Erreur lors du chargement des initiatives: {e}")
            return pd.DataFrame()

    @st.cache_data(ttl=300)
    def load_executive_summary(_self) -> pd.DataFrame:
        """Charge le résumé exécutif"""
        try:
            summary_file = _self.analytics_dir / "executive_summary_latest.parquet"
            if summary_file.exists():
                return pd.read_parquet(summary_file)
            return pd.DataFrame()
        except Exception as e:
            st.error(f"Erreur lors du chargement du résumé: {e}")
            return pd.DataFrame()


def create_metric_card(title: str, value: str, delta: Optional[str] = None, delta_color: str = "normal") -> str:
    """Crée une carte de métrique stylée"""
    delta_html = ""
    if delta:
        color = "#00aa44" if delta_color == "normal" else "#ff4444" if delta_color == "inverse" else "#ffaa00"
        delta_html = f'<div style="color: {color}; font-size: 0.8rem; margin-top: 0.5rem;">▲ {delta}</div>'

    return f"""
    <div class="metric-card">
        <div class="metric-value">{value}</div>
        <div class="metric-label">{title}</div>
        {delta_html}
    </div>
    """


def create_health_gauge(score: float, title: str):
    """Crée une jauge de health score"""
    # Déterminer la couleur basée sur le score
    if score >= 80:
        color = "#00aa44"  # Vert
    elif score >= 65:
        color = "#ffaa00"  # Orange
    elif score >= 50:
        color = "#ff6600"  # Orange foncé
    else:
        color = "#ff4444"  # Rouge

    fig = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=score,
        domain={'x': [0, 1], 'y': [0, 1]},
        title={'text': title, 'font': {'size': 16}},
        delta={'reference': 75, 'increasing': {'color': "#00aa44"},
               'decreasing': {'color': "#ff4444"}},
        gauge={
            'axis': {'range': [None, 100], 'tickwidth': 1},
            'bar': {'color': color},
            'steps': [
                {'range': [0, 50], 'color': "#ffeeee"},
                {'range': [50, 75], 'color': "#fff4e6"},
                {'range': [75, 100], 'color': "#eeffee"}
            ],
            'threshold': {
                'line': {'color': "red", 'width': 4},
                'thickness': 0.75,
                'value': 90
            }
        }
    ))

    fig.update_layout(height=300, margin=dict(l=20, r=20, t=40, b=20))
    return fig


def render_executive_overview(loader: DashboardDataLoader):
    """Rend la vue d'ensemble exécutive"""
    st.markdown('<div class="main-header">📊 McKinsey Wave - Executive Dashboard</div>',
                unsafe_allow_html=True)

    # Charger les données
    portfolio_metrics = loader.load_portfolio_metrics()
    health_df = loader.load_initiative_health()

    if not portfolio_metrics:
        st.error(
            "🚨 Aucune donnée disponible. Lancez d'abord les transformations business.")
        st.info("Exécutez: `python src/transformation/calculators.py`")
        return

    # Métadonnées de fraîcheur
    col_info1, col_info2 = st.columns([3, 1])
    with col_info1:
        st.info("📊 **Live Dashboard** - Données mises à jour automatiquement")
    with col_info2:
        if st.button("🔄 Actualiser", key="refresh_main"):
            st.cache_data.clear()
            st.rerun()

    # KPIs principaux
    st.subheader("🎯 Indicateurs Clés de Performance")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        roi_value = f"{portfolio_metrics.get('portfolio_roi', 0):.1f}%"
        st.markdown(create_metric_card("Portfolio ROI",
                    roi_value, "+2.3%"), unsafe_allow_html=True)

    with col2:
        budget_util = f"{portfolio_metrics.get('budget_utilization_rate', 0):.1f}%"
        st.markdown(create_metric_card("Budget Utilization",
                    budget_util, "-1.2%", "inverse"), unsafe_allow_html=True)

    with col3:
        at_risk = portfolio_metrics.get(
            'at_risk_initiatives', {}).get('count', 0)
        total = portfolio_metrics.get('total_initiatives', 1)
        risk_pct = f"{(at_risk/total*100):.0f}%"
        st.markdown(create_metric_card("At Risk", risk_pct,
                    "+0.5%", "inverse"), unsafe_allow_html=True)

    with col4:
        completed = portfolio_metrics.get('completed_initiatives', 0)
        completion_rate = f"{(completed/total*100):.0f}%"
        st.markdown(create_metric_card("Completion Rate",
                    completion_rate, "+5.2%"), unsafe_allow_html=True)

    st.markdown("---")

    # Alertes critiques
    if at_risk > 0:
        st.markdown(f"""
        <div class="alert-warning">
            ⚠️ <strong>Action Requise:</strong> {at_risk} initiative(s) nécessitent une attention immédiate.
            Budget à risque: €{portfolio_metrics.get('at_risk_initiatives', {}).get('total_budget', 0):,.0f}
        </div>
        """, unsafe_allow_html=True)

    # Graphiques principaux
    col1, col2 = st.columns(2)

    with col1:
        if not health_df.empty:
            # Health distribution
            health_counts = health_df['health_status'].value_counts()

            colors = {
                'Excellent': '#00aa44',
                'Good': '#66cc66',
                'Warning': '#ffaa00',
                'Critical': '#ff4444'
            }

            fig = px.pie(
                values=health_counts.values,
                names=health_counts.index,
                title="Distribution des Health Scores",
                color_discrete_map=colors
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        if not health_df.empty:
            # Performance par type
            type_performance = health_df.groupby('type').agg({
                'health_score': 'mean',
                'roi_percentage': 'mean'
            }).round(1)

            fig = go.Figure()

            fig.add_trace(go.Bar(
                x=type_performance.index,
                y=type_performance['health_score'],
                name='Health Score Moyen',
                yaxis='y',
                marker_color='#1f4e79'
            ))

            fig.add_trace(go.Scatter(
                x=type_performance.index,
                y=type_performance['roi_percentage'],
                mode='lines+markers',
                name='ROI Moyen (%)',
                yaxis='y2',
                line=dict(color='#ff6600', width=3)
            ))

            fig.update_layout(
                title='Performance par Type d\'Initiative',
                xaxis=dict(title='Type d\'Initiative'),
                yaxis=dict(title='Health Score', side='left'),
                yaxis2=dict(title='ROI (%)', side='right', overlaying='y'),
                height=400
            )

            st.plotly_chart(fig, use_container_width=True)


def render_initiative_details(loader: DashboardDataLoader):
    """Rend les détails des initiatives"""
    st.subheader("🔍 Analyse Détaillée des Initiatives")

    health_df = loader.load_initiative_health()

    if health_df.empty:
        st.warning("Aucune donnée d'initiative disponible.")
        return

    # Filtres
    col1, col2, col3 = st.columns(3)

    with col1:
        status_filter = st.multiselect(
            "Filtrer par Status",
            options=health_df['status'].unique(),
            default=health_df['status'].unique()
        )

    with col2:
        type_filter = st.multiselect(
            "Filtrer par Type",
            options=health_df['type'].unique(),
            default=health_df['type'].unique()
        )

    with col3:
        health_threshold = st.slider(
            "Health Score minimum",
            min_value=0,
            max_value=100,
            value=0
        )

    # Appliquer les filtres
    filtered_df = health_df[
        (health_df['status'].isin(status_filter)) &
        (health_df['type'].isin(type_filter)) &
        (health_df['health_score'] >= health_threshold)
    ]

    # Tableau interactif des initiatives
    st.subheader("📊 Vue d'ensemble des Initiatives")

    # Colonnes à afficher
    display_columns = [
        'name', 'type', 'status', 'health_score', 'health_status',
        'budget_allocated', 'budget_spent', 'roi_percentage'
    ]

    if not filtered_df.empty:
        display_df = filtered_df[display_columns].copy()

        # Formatage des colonnes
        display_df['budget_allocated'] = display_df['budget_allocated'].apply(
            lambda x: f"€{x:,.0f}")
        display_df['budget_spent'] = display_df['budget_spent'].apply(
            lambda x: f"€{x:,.0f}")
        display_df['roi_percentage'] = display_df['roi_percentage'].apply(
            lambda x: f"{x:.1f}%")
        display_df['health_score'] = display_df['health_score'].apply(
            lambda x: f"{x:.1f}")

        # Renommer les colonnes pour l'affichage
        display_df.columns = [
            'Initiative', 'Type', 'Status', 'Health Score', 'Health Status',
            'Budget Alloué', 'Budget Dépensé', 'ROI'
        ]

        st.dataframe(
            display_df,
            use_container_width=True,
            height=400
        )

        # Détail d'une initiative sélectionnée
        st.subheader("🎯 Focus Initiative")

        selected_initiative = st.selectbox(
            "Sélectionnez une initiative pour plus de détails:",
            options=filtered_df['name'].tolist()
        )

        if selected_initiative:
            initiative_data = filtered_df[filtered_df['name']
                                          == selected_initiative].iloc[0]

            col1, col2 = st.columns(2)

            with col1:
                # Gauge du health score
                health_score = initiative_data['health_score']
                gauge_fig = create_health_gauge(
                    health_score, f"Health Score - {selected_initiative}")
                st.plotly_chart(gauge_fig, use_container_width=True)

                # Informations clés
                st.info(f"""
                **📋 Informations Générales**
                - **Type:** {initiative_data['type']}
                - **Status:** {initiative_data['status']}
                - **Propriétaire:** {initiative_data['owner']}
                - **Début:** {initiative_data['start_date'].strftime('%d/%m/%Y') if pd.notnull(initiative_data['start_date']) else 'N/A'}
                - **Fin prévue:** {initiative_data['target_end_date'].strftime('%d/%m/%Y') if pd.notnull(initiative_data['target_end_date']) else 'N/A'}
                """)

            with col2:
                # Métriques financières
                st.subheader("💰 Performance Financière")

                budget_allocated = initiative_data['budget_allocated']
                budget_spent = initiative_data['budget_spent']
                roi = initiative_data.get('roi_percentage', 0)

                budget_progress = (
                    budget_spent / budget_allocated * 100) if budget_allocated > 0 else 0

                # Progress bar du budget
                st.metric(
                    "Utilisation Budget",
                    f"{budget_progress:.1f}%",
                    f"{budget_spent - budget_allocated:,.0f}€"
                )

                st.metric("ROI", f"{roi:.1f}%")

                # Facteurs de risque
                risk_factors = initiative_data.get('risk_factors', [])
                if risk_factors and len(risk_factors) > 0:
                    st.warning(
                        f"⚠️ **Facteurs de risque:** {', '.join(risk_factors)}")
                else:
                    st.success("✅ Aucun facteur de risque identifié")

    else:
        st.info("Aucune initiative ne correspond aux filtres sélectionnés.")


def main():
    """Application principale du dashboard"""

    # Initialisation
    loader = DashboardDataLoader()

    # Sidebar pour navigation
    st.sidebar.title("🧭 Navigation")
    page = st.sidebar.selectbox(
        "Choisir une vue",
        ["📊 Executive Overview", "🔍 Initiative Details", "📈 Portfolio Analytics"]
    )

    # Informations système
    st.sidebar.markdown("---")
    st.sidebar.markdown("### ℹ️ Informations Système")
    st.sidebar.info(f"**Dernière MAJ:** {datetime.now().strftime('%H:%M:%S')}")

    # Auto-refresh
    auto_refresh = st.sidebar.checkbox("🔄 Auto-refresh (30s)")
    if auto_refresh:
        time.sleep(30)
        st.rerun()

    # Bouton de rafraîchissement global
    if st.sidebar.button("🔄 Actualiser toutes les données"):
        st.cache_data.clear()
        st.success("✅ Cache vidé, données actualisées !")
        st.rerun()

    # Navigation
    if page == "📊 Executive Overview":
        render_executive_overview(loader)
    elif page == "🔍 Initiative Details":
        render_initiative_details(loader)
    elif page == "📈 Portfolio Analytics":
        st.subheader("📈 Portfolio Analytics")
        st.info("🚧 Section en développement - Analytics avancées à venir")

        # Placeholder pour analytics avancées
        portfolio_metrics = loader.load_portfolio_metrics()
        if portfolio_metrics:
            st.json(portfolio_metrics)


if __name__ == "__main__":
    main()
