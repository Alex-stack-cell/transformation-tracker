import pandas as pd
import json
from pathlib import Path


def explore_analytics_data():
    """Explore les données analytics générées"""
    analytics_dir = Path("data/analytics")

    print("📊 EXPLORATION DES DONNÉES ANALYTICS\n")
    print("="*50)

    # 1. Health Scores des initiatives
    health_file = analytics_dir / "initiative_health_latest.parquet"
    if health_file.exists():
        health_df = pd.read_parquet(health_file)

        print("🏥 HEALTH SCORES DES INITIATIVES:")
        print(f"   Initiatives analysées: {len(health_df)}")

        # Top/Bottom initiatives
        print("\n🏆 TOP 3 initiatives (meilleur health score):")
        top_initiatives = health_df.nlargest(3, 'health_score')[
            ['name', 'health_score', 'health_status']]
        for _, row in top_initiatives.iterrows():
            print(
                f"   • {row['name'][:40]:<40} Score: {row['health_score']:.1f} ({row['health_status']})")

        print("\n⚠️  BOTTOM 3 initiatives (à surveiller):")
        bottom_initiatives = health_df.nsmallest(3, 'health_score')[
            ['name', 'health_score', 'health_status']]
        for _, row in bottom_initiatives.iterrows():
            print(
                f"   • {row['name'][:40]:<40} Score: {row['health_score']:.1f} ({row['health_status']})")

        # Répartition par statut
        print(f"\n📈 Répartition par Health Status:")
        status_counts = health_df['health_status'].value_counts()
        for status, count in status_counts.items():
            print(f"   {status}: {count} initiatives")

    # 2. Résumé exécutif
    summary_file = analytics_dir / "executive_summary_latest.parquet"
    if summary_file.exists():
        summary_df = pd.read_parquet(summary_file)

        print(f"\n🎯 RÉSUMÉ EXÉCUTIF (C-SUITE):")
        for _, row in summary_df.iterrows():
            status_emoji = "✅" if row['status'] == 'Good' else "⚠️" if row['status'] == 'Warning' else "🚨"
            print(
                f"   {status_emoji} {row['metric_name']}: {row['current_value']}")
            if row['action_required'] != 'None':
                print(f"      → Action: {row['action_required']}")

    # 3. Métriques portfolio
    portfolio_file = analytics_dir / "portfolio_metrics_latest.json"
    if portfolio_file.exists():
        with open(portfolio_file, 'r') as f:
            portfolio = json.load(f)

        print(f"\n💼 MÉTRIQUES PORTFOLIO DÉTAILLÉES:")
        print(f"   Total Budget: €{portfolio['total_budget_allocated']:,.0f}")
        print(f"   Budget Dépensé: €{portfolio['total_budget_spent']:,.0f}")
        print(
            f"   Impact Financier Total: €{portfolio['total_financial_impact']:,.0f}")
        print(f"   ROI Portfolio: {portfolio['portfolio_roi']:.1f}%")

        print(f"\n📊 Performance par type d'initiative:")
        if 'performance_by_type' in portfolio:
            for init_type, metrics in portfolio['performance_by_type'].items():
                if isinstance(metrics, dict) and 'health_score' in metrics:
                    print(
                        f"   {init_type}: Health Score {metrics['health_score']:.1f}")

        # Initiatives à risque
        at_risk = portfolio.get('at_risk_initiatives', {})
        if at_risk.get('count', 0) > 0:
            print(f"\n🚨 INITIATIVES À RISQUE:")
            for name in at_risk.get('names', [])[:3]:  # Top 3
                print(f"   • {name}")

    print("\n" + "="*50)
    print("✅ Exploration terminée !")


if __name__ == "__main__":
    explore_analytics_data()
