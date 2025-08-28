"""
Calculateurs de métriques business - Simule les transformations Snowflake/Databricks
Focus sur les KPIs critiques pour McKinsey Wave
"""

import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List
from loguru import logger
import warnings
warnings.filterwarnings('ignore')


class BusinessMetricsCalculator:
    """Calculateur de métriques business pour tableaux de bord exécutifs"""

    def __init__(self):
        self.raw_dir = Path("data/raw")
        self.processed_dir = Path("data/processed")
        self.analytics_dir = Path("data/analytics")

        # Créer les dossiers
        for dir_path in [self.processed_dir, self.analytics_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)

        logger.add("logs/business_transformations.log", rotation="1 day")

    def load_latest_data(self) -> Dict[str, pd.DataFrame]:
        """Charge les dernières données collectées"""
        logger.info("Loading latest datasets...")

        datasets = {}

        # Charger toutes les données latest
        for file_name in ['initiatives_latest.parquet', 'financial_metrics_latest.parquet',
                          'operational_metrics_latest.parquet']:
            file_path = self.raw_dir / file_name

            if file_path.exists():
                dataset_name = file_name.replace('_latest.parquet', '')
                datasets[dataset_name] = pd.read_parquet(file_path)
                logger.info(
                    f"Loaded {dataset_name}: {len(datasets[dataset_name])} records")
            else:
                logger.warning(f"File not found: {file_path}")

        return datasets

    def calculate_initiative_health_scores(self, initiatives_df: pd.DataFrame,
                                           financial_df: pd.DataFrame,
                                           operational_df: pd.DataFrame) -> pd.DataFrame:
        """Calcule le health score de chaque initiative (métrique clé McKinsey)"""
        logger.info("Calculating initiative health scores...")

        # Commencer avec les initiatives de base
        health_df = initiatives_df.copy()

        # 1. Score Budget (0-25 points)
        health_df['budget_utilization'] = health_df['budget_spent'] / \
            health_df['budget_allocated']
        health_df['budget_score'] = health_df['budget_utilization'].apply(
            lambda x: 25 if 0.8 <= x <= 1.0 else
            20 if 0.6 <= x < 0.8 or 1.0 < x <= 1.1 else
            10 if 0.4 <= x < 0.6 or 1.1 < x <= 1.2 else 0
        )

        # 2. Score Temporel (0-25 points)
        today = datetime.now()
        health_df['days_since_start'] = (
            today - health_df['start_date']).dt.days
        health_df['total_duration'] = (
            health_df['target_end_date'] - health_df['start_date']).dt.days
        health_df['time_progress'] = health_df['days_since_start'] / \
            health_df['total_duration']

        health_df['time_score'] = health_df.apply(
            lambda row: 25 if row['status'] == 'Completed' else
            20 if row['time_progress'] <= 0.9 else
            15 if row['time_progress'] <= 1.0 else
            5 if row['time_progress'] <= 1.2 else 0,
            axis=1
        )

        # 3. Score Performance Financière (0-30 points)
        financial_agg = financial_df.groupby('initiative_id').agg({
            'revenue_impact': 'sum',
            'cost_reduction': 'sum',
            'roi_percentage': 'mean'
        }).reset_index()

        health_df = health_df.merge(
            financial_agg, on='initiative_id', how='left')
        health_df['financial_score'] = health_df['roi_percentage'].fillna(0).apply(
            lambda x: 30 if x >= 20 else
            25 if x >= 15 else
            20 if x >= 10 else
            15 if x >= 5 else
            10 if x >= 0 else 0
        )

        # 4. Score Opérationnel (0-20 points)
        operational_agg = operational_df.groupby('initiative_id').agg({
            'efficiency_gain_percentage': 'mean',
            'quality_score': 'mean',
            'employee_satisfaction': 'mean'
        }).reset_index()

        health_df = health_df.merge(
            operational_agg, on='initiative_id', how='left')
        health_df['operational_score'] = (
            health_df['efficiency_gain_percentage'].fillna(0) * 0.4 +
            health_df['quality_score'].fillna(80) * 0.2 +
            health_df['employee_satisfaction'].fillna(7) * 2
        ).apply(lambda x: min(20, max(0, x)))

        # 5. Health Score Total (0-100)
        health_df['health_score'] = (
            health_df['budget_score'] +
            health_df['time_score'] +
            health_df['financial_score'] +
            health_df['operational_score']
        )

        # Classification des initiatives
        health_df['health_status'] = health_df['health_score'].apply(
            lambda x: 'Excellent' if x >= 80 else
            'Good' if x >= 65 else
            'Warning' if x >= 50 else
            'Critical'
        )

        # Calcul des prédictions simples
        health_df['predicted_completion_date'] = health_df.apply(
            self._predict_completion_date, axis=1
        )

        health_df['risk_factors'] = health_df.apply(
            self._identify_risk_factors, axis=1
        )

        return health_df

    def calculate_portfolio_metrics(self, health_df: pd.DataFrame,
                                    financial_df: pd.DataFrame) -> Dict[str, any]:
        """Calcule les métriques de portfolio pour le dashboard exécutif"""
        logger.info("Calculating portfolio-level metrics...")

        metrics = {}

        # Métriques globales
        metrics['total_initiatives'] = len(health_df)
        metrics['active_initiatives'] = len(
            health_df[health_df['status'].isin(['In Progress', 'At Risk'])])
        metrics['completed_initiatives'] = len(
            health_df[health_df['status'] == 'Completed'])

        # Budget global
        metrics['total_budget_allocated'] = health_df['budget_allocated'].sum()
        metrics['total_budget_spent'] = health_df['budget_spent'].sum()
        metrics['budget_utilization_rate'] = metrics['total_budget_spent'] / \
            metrics['total_budget_allocated'] * 100

        # Performance financière
        total_revenue_impact = financial_df['revenue_impact'].sum()
        total_cost_reduction = financial_df['cost_reduction'].sum()
        metrics['total_financial_impact'] = total_revenue_impact + \
            total_cost_reduction
        metrics['portfolio_roi'] = (
            metrics['total_financial_impact'] / metrics['total_budget_spent'] - 1) * 100

        # Distribution par health score
        health_distribution = health_df['health_status'].value_counts()
        metrics['health_distribution'] = {
            'excellent': health_distribution.get('Excellent', 0),
            'good': health_distribution.get('Good', 0),
            'warning': health_distribution.get('Warning', 0),
            'critical': health_distribution.get('Critical', 0)
        }

        # Initiatives à risque (pour alertes)
        at_risk = health_df[health_df['health_score'] < 50]
        metrics['at_risk_initiatives'] = {
            'count': len(at_risk),
            'total_budget': at_risk['budget_allocated'].sum() if len(at_risk) > 0 else 0,
            'names': at_risk['name'].tolist() if len(at_risk) > 0 else []
        }

        # Performance par type d'initiative
        type_performance = health_df.groupby('type').agg({
            'health_score': 'mean',
            'budget_allocated': 'sum',
            'roi_percentage': 'mean'
        }).round(2).to_dict()

        metrics['performance_by_type'] = type_performance

        # Prédictions
        upcoming_completions = health_df[
            health_df['predicted_completion_date'] <= datetime.now() +
            timedelta(days=30)
        ]
        metrics['upcoming_completions'] = {
            'count': len(upcoming_completions),
            'names': upcoming_completions['name'].tolist()
        }

        return metrics

    def create_executive_summary(self, portfolio_metrics: Dict) -> pd.DataFrame:
        """Crée un résumé exécutif pour le C-suite"""
        logger.info("Creating executive summary...")

        summary_data = []

        # KPI Principal 1: ROI Global
        summary_data.append({
            'metric_name': 'Portfolio ROI',
            'current_value': f"{portfolio_metrics['portfolio_roi']:.1f}%",
            'status': 'Good' if portfolio_metrics['portfolio_roi'] > 15 else 'Warning',
            'description': 'Return on Investment across all transformation initiatives',
            'action_required': 'None' if portfolio_metrics['portfolio_roi'] > 15 else 'Review underperforming initiatives'
        })

        # KPI Principal 2: Budget Performance
        budget_util = portfolio_metrics['budget_utilization_rate']
        summary_data.append({
            'metric_name': 'Budget Utilization',
            'current_value': f"{budget_util:.1f}%",
            'status': 'Good' if 80 <= budget_util <= 100 else 'Warning',
            'description': 'Overall budget utilization across portfolio',
            'action_required': 'None' if 80 <= budget_util <= 100 else 'Budget reallocation needed'
        })

        # KPI Principal 3: Initiatives At Risk
        at_risk_count = portfolio_metrics['at_risk_initiatives']['count']
        summary_data.append({
            'metric_name': 'At-Risk Initiatives',
            'current_value': f"{at_risk_count}/{portfolio_metrics['total_initiatives']}",
            'status': 'Critical' if at_risk_count > portfolio_metrics['total_initiatives'] * 0.3 else 'Good',
            'description': 'Number of initiatives requiring immediate attention',
            'action_required': 'Immediate intervention required' if at_risk_count > 0 else 'None'
        })

        # KPI Principal 4: Completion Rate
        completion_rate = (
            portfolio_metrics['completed_initiatives'] / portfolio_metrics['total_initiatives']) * 100
        summary_data.append({
            'metric_name': 'Completion Rate',
            'current_value': f"{completion_rate:.1f}%",
            'status': 'Good' if completion_rate > 60 else 'Warning',
            'description': 'Percentage of initiatives successfully completed',
            'action_required': 'Accelerate delivery' if completion_rate < 60 else 'None'
        })

        return pd.DataFrame(summary_data)

    def _predict_completion_date(self, row) -> datetime:
        """Prédiction simple de la date de completion"""
        if row['status'] == 'Completed':
            return row['target_end_date']

        # Prédiction basée sur le progress actuel
        if row['time_progress'] > 0:
            estimated_total_days = row['days_since_start'] / \
                max(row['time_progress'], 0.1)
            return row['start_date'] + timedelta(days=estimated_total_days)
        else:
            return row['target_end_date']

    def _identify_risk_factors(self, row) -> List[str]:
        """Identifie les facteurs de risque pour une initiative"""
        risks = []

        if row['budget_utilization'] > 1.1:
            risks.append('Budget Overrun')

        if row['time_progress'] > 1.0 and row['status'] != 'Completed':
            risks.append('Schedule Delay')

        if row.get('roi_percentage', 0) < 5:
            risks.append('Low ROI')

        if row.get('employee_satisfaction', 8) < 6:
            risks.append('Team Satisfaction')

        return risks

    def run_full_transformation(self) -> Dict[str, any]:
        """Lance toutes les transformations business"""
        logger.info("Starting full business transformation pipeline...")

        start_time = datetime.now()

        # 1. Charger les données
        datasets = self.load_latest_data()

        if len(datasets) < 3:
            logger.error("Missing required datasets")
            return {}

        # 2. Calculer les health scores
        health_df = self.calculate_initiative_health_scores(
            datasets['initiatives'],
            datasets['financial_metrics'],
            datasets['operational_metrics']
        )

        # 3. Calculer les métriques portfolio
        portfolio_metrics = self.calculate_portfolio_metrics(
            health_df, datasets['financial_metrics']
        )

        # 4. Créer le résumé exécutif
        executive_summary = self.create_executive_summary(portfolio_metrics)

        # 5. Sauvegarder tous les résultats
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Analytics tables (prêtes pour dashboard)
        health_df.to_parquet(self.analytics_dir /
                             f"initiative_health_{timestamp}.parquet")
        health_df.to_parquet(self.analytics_dir /
                             "initiative_health_latest.parquet")

        executive_summary.to_parquet(
            self.analytics_dir / f"executive_summary_{timestamp}.parquet")
        executive_summary.to_parquet(
            self.analytics_dir / "executive_summary_latest.parquet")

        # Métriques portfolio en JSON pour API
        import json
        with open(self.analytics_dir / "portfolio_metrics_latest.json", 'w') as f:
            json.dump(portfolio_metrics, f, indent=2, default=str)

        execution_time = (datetime.now() - start_time).total_seconds()

        results = {
            'execution_time_seconds': execution_time,
            'initiatives_processed': len(health_df),
            'health_scores_calculated': True,
            'portfolio_metrics': portfolio_metrics,
            'executive_summary': executive_summary,
            'files_generated': [
                'initiative_health_latest.parquet',
                'executive_summary_latest.parquet',
                'portfolio_metrics_latest.json'
            ]
        }

        logger.info(
            f"Business transformation completed in {execution_time:.2f}s")
        return results


def main():
    """Script principal pour lancer les transformations"""
    calculator = BusinessMetricsCalculator()

    print("🔄 TRANSFORMATIONS BUSINESS McKinsey Wave\n")

    try:
        results = calculator.run_full_transformation()

        if results:
            print("✅ Transformations terminées avec succès !")
            print(
                f"⏱️  Temps d'exécution: {results['execution_time_seconds']:.2f}s")
            print(
                f"📊 Initiatives traitées: {results['initiatives_processed']}")

            # Afficher quelques métriques clés
            portfolio = results['portfolio_metrics']
            print(f"\n📈 MÉTRIQUES PORTFOLIO:")
            print(f"  💰 ROI Global: {portfolio['portfolio_roi']:.1f}%")
            print(
                f"  💳 Budget utilisé: {portfolio['budget_utilization_rate']:.1f}%")
            print(
                f"  ⚠️  Initiatives à risque: {portfolio['at_risk_initiatives']['count']}")
            print(
                f"  ✅ Taux de completion: {(portfolio['completed_initiatives']/portfolio['total_initiatives']*100):.1f}%")

            print(f"\n📁 Fichiers générés dans: data/analytics/")

        else:
            print("❌ Échec des transformations - vérifiez les logs")

    except Exception as e:
        print(f"❌ Erreur: {e}")
        logger.error(f"Transformation failed: {e}")


if __name__ == "__main__":
    main()
