# src/ingestion/validators.py
"""
Validateurs de qualité des données - Simule les checks AWS Glue Data Quality
Implémente les data contracts et validation rules
"""

import pandas as pd
from typing import Dict, Any
from pathlib import Path
from datetime import datetime
from loguru import logger
import json


class DataQualityValidator:
    """Validateur de qualité des données avec rules business"""

    def __init__(self):
        self.results_dir = Path("data/quality_reports")
        self.results_dir.mkdir(parents=True, exist_ok=True)

        logger.add("logs/data_quality.log", rotation="1 day")

        # Data contracts - définit les règles attendues
        self.data_contracts: Dict[str, Dict[str, Any]] = {
            'initiatives': {
                'required_columns': ['initiative_id', 'name', 'type', 'budget_allocated', 'budget_spent', 'status'],
                'unique_columns': ['initiative_id'],
                'non_null_columns': ['initiative_id', 'name', 'type', 'status'],
                'value_ranges': {
                    'budget_allocated': (10000, 5000000),  # Entre 10K et 5M
                    'budget_spent': (0, 10000000),
                },
                'allowed_values': {
                    'status': ['Planning', 'In Progress', 'At Risk', 'Completed', 'On Hold'],
                    'type': ['Digital', 'Operational', 'HR', 'Financial']
                },
                'business_rules': [
                    'budget_spent <= budget_allocated * 1.2',  # Max 20% dépassement
                    'start_date <= target_end_date'
                ]
            },
            'financial_metrics': {
                'required_columns': ['initiative_id', 'date', 'revenue_impact', 'cost_reduction', 'roi_percentage'],
                'non_null_columns': ['initiative_id', 'date'],
                'value_ranges': {
                    'roi_percentage': (-50, 100),  # ROI entre -50% et 100%
                    'revenue_impact': (0, 1000000),
                    'cost_reduction': (0, 500000)
                },
                'business_rules': [
                    'date >= "2023-01-01"',  # Pas de données trop anciennes
                    'budget_burn_rate >= 0'
                ]
            },
            'operational_metrics': {
                'required_columns': ['initiative_id', 'date', 'efficiency_gain_percentage', 'quality_score'],
                'non_null_columns': ['initiative_id', 'date'],
                'value_ranges': {
                    'efficiency_gain_percentage': (0, 50),
                    'quality_score': (0, 100),
                    'employee_satisfaction': (0, 10),
                    'customer_satisfaction': (0, 10)
                }
            }
        }

    def validate_schema(self, df: pd.DataFrame, dataset_name: str) -> Dict[str, Any]:
        """Valide la structure et le schéma des données"""
        logger.info(f"Validating schema for {dataset_name}")

        contract = self.data_contracts.get(dataset_name, {})
        results: Dict[str, Any] = {
            'dataset': dataset_name,
            'timestamp': datetime.now(),
            'schema_checks': {},  # type: Dict[str, Any]
            'passed': True,       # type: bool
            'errors': []          # type: list[str]
        }

        # Vérifier colonnes requises
        required_cols = contract.get('required_columns', [])
        missing_cols = [col for col in required_cols if col not in df.columns]

        if missing_cols:
            results['schema_checks']['missing_columns'] = missing_cols
            results['errors'].append(
                f"Missing required columns: {missing_cols}")
            results['passed'] = False
        else:
            results['schema_checks']['missing_columns'] = []

        # Vérifier colonnes uniques
        unique_cols = contract.get('unique_columns', [])
        duplicate_issues = {}

        for col in unique_cols:
            if col in df.columns:
                # type: ignore
                # type: ignore
                # type: ignore
                duplicates = df[df[col].duplicated()][col].tolist()
                if duplicates:
                    duplicate_issues[col] = duplicates[:5]  # Max 5 exemples
                    results['passed'] = False

        results['schema_checks']['duplicates'] = duplicate_issues

        # Vérifier valeurs non-nulles
        non_null_cols = contract.get('non_null_columns', [])
        null_issues = {}

        for col in non_null_cols:
            if col in df.columns:
                null_count = df[col].isnull().sum()  # type: ignore
                if null_count > 0:
                    null_issues[col] = {
                        'null_count': int(null_count),  # type: ignore
                        # type: ignore
                        # type: ignore
                        # type: ignore
                        # type: ignore
                        'null_percentage': round(null_count / len(df) * 100, 2)
                    }
                    results['passed'] = False

        results['schema_checks']['null_values'] = null_issues

        return results

    def validate_data_ranges(self, df: pd.DataFrame, dataset_name: str) -> Dict[str, Any]:
        """Valide les plages de valeurs et règles business"""
        logger.info(f"Validating data ranges for {dataset_name}")

        contract = self.data_contracts.get(dataset_name, {})
        results = {
            'dataset': dataset_name,
            'range_checks': {},
            'value_checks': {},
            'business_rules': {},
            'passed': True,
            'warnings': []
        }

        # Vérifier plages de valeurs
        value_ranges = contract.get('value_ranges', {})
        range_violations = {}

        for col, (min_val, max_val) in value_ranges.items():
            if col in df.columns:
                violations = df[(df[col] < min_val) | (
                    df[col] > max_val)]  # type: ignore
                if len(violations) > 0:  # type: ignore
                    range_violations[col] = {
                        'violation_count': len(violations),  # type: ignore
                        'expected_range': f"{min_val} - {max_val}",
                        'actual_min': float(df[col].min()),  # type: ignore
                        'actual_max': float(df[col].max())  # type: ignore
                    }
                    results['passed'] = False

        results['range_checks'] = range_violations

        # Vérifier valeurs autorisées
        allowed_values = contract.get('allowed_values', {})
        value_violations = {}

        for col, allowed_list in allowed_values.items():
            if col in df.columns:
                invalid_values = df[~df[col].isin(
                    allowed_list)][col].unique()  # type: ignore
                if len(invalid_values) > 0:  # type: ignore
                    value_violations[col] = {
                        'invalid_values': invalid_values.tolist(),  # type: ignore
                        'allowed_values': allowed_list
                    }
                    results['passed'] = False

        results['value_checks'] = value_violations

        # Règles business personnalisées
        business_rules = contract.get('business_rules', [])
        rule_violations = {}

        for rule in business_rules:
            try:
                # Évaluation sécurisée des règles business
                if 'budget_spent <= budget_allocated' in rule:
                    if 'budget_spent' in df.columns and 'budget_allocated' in df.columns:
                        violations = df[df['budget_spent']
                                        > df['budget_allocated'] * 1.2]
                        if len(violations) > 0:
                            rule_violations['budget_overrun'] = {
                                'rule': rule,
                                'violations': len(violations),
                                # type: ignore
                                'examples': violations[['initiative_id', 'budget_allocated', 'budget_spent']].head(3).to_dict('records')
                            }

                elif 'start_date <= target_end_date' in rule:
                    if 'start_date' in df.columns and 'target_end_date' in df.columns:
                        violations = df[df['start_date']
                                        > df['target_end_date']]
                        if len(violations) > 0:
                            rule_violations['invalid_dates'] = {
                                'rule': rule,
                                'violations': len(violations)
                            }

            except Exception as e:
                logger.warning(
                    f"Could not evaluate business rule '{rule}': {e}")

        results['business_rules'] = rule_violations
        if rule_violations:
            results['passed'] = False

        return results

    def generate_quality_summary(self, df: pd.DataFrame, dataset_name: str) -> Dict[str, Any]:
        """Génère un résumé de qualité des données"""

        summary = {
            'dataset': dataset_name,
            'timestamp': datetime.now(),
            'record_count': len(df),
            'column_count': len(df.columns),
            'data_freshness': self._check_data_freshness(df),
            'completeness': self._check_completeness(df),
            'consistency': self._check_consistency(df, dataset_name)
        }

        return summary

    def _check_data_freshness(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Vérifie la fraîcheur des données"""
        freshness = {'status': 'unknown'}

        if 'collection_timestamp' in df.columns:
            latest_collection = pd.to_datetime(
                df['collection_timestamp']).max()
            age_hours = (datetime.now() -
                         latest_collection).total_seconds() / 3600

            freshness = {
                'latest_collection': latest_collection.isoformat(),
                'age_hours': round(age_hours, 2),
                'status': 'fresh' if age_hours < 24 else 'stale'
            }

        return freshness

    def _check_completeness(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Vérifie la complétude des données"""
        null_percentages = (df.isnull().sum() / len(df) * 100).round(2)

        return {
            'overall_completeness': round(100 - null_percentages.mean(), 2),
            'column_completeness': null_percentages.to_dict()
        }

    def _check_consistency(self, df: pd.DataFrame, dataset_name: str) -> Dict[str, Any]:
        """Vérifie la cohérence des données"""
        consistency_score = 100  # Score de base
        issues = []

        # Vérifications spécifiques par type de dataset
        if dataset_name == 'initiatives':
            # Vérifier cohérence des budgets
            if 'budget_spent' in df.columns and 'budget_allocated' in df.columns:
                over_budget = (df['budget_spent'] >
                               df['budget_allocated']).sum()
                if over_budget > 0:
                    issues.append(f"{over_budget} initiatives over budget")
                    consistency_score -= 10

        return {
            'score': consistency_score,
            'issues': issues
        }

    def run_full_validation(self, file_path: Path) -> Dict[str, Any]:
        """Lance une validation complète d'un fichier"""
        logger.info(f"Running full validation for {file_path}")

        # Déterminer le type de dataset à partir du nom du fichier
        dataset_name = None
        if 'initiatives' in file_path.name:
            dataset_name = 'initiatives'
        elif 'financial_metrics' in file_path.name:
            dataset_name = 'financial_metrics'
        elif 'operational_metrics' in file_path.name:
            dataset_name = 'operational_metrics'

        if not dataset_name:
            logger.warning(f"Cannot determine dataset type for {file_path}")
            return {}

        # Charger les données
        try:
            df = pd.read_parquet(file_path)
        except Exception as e:
            logger.error(f"Failed to load {file_path}: {e}")
            return {'error': str(e)}

        # Exécuter toutes les validations
        validation_results = {
            'file_path': str(file_path),
            'dataset_name': dataset_name,
            'validation_timestamp': datetime.now(),
            'schema_validation': self.validate_schema(df, dataset_name),
            'data_validation': self.validate_data_ranges(df, dataset_name),
            'quality_summary': self.generate_quality_summary(df, dataset_name)
        }

        # Déterminer le statut global
        overall_passed = (
            validation_results['schema_validation']['passed'] and
            validation_results['data_validation']['passed']
        )

        validation_results['overall_status'] = 'PASSED' if overall_passed else 'FAILED'

        # Sauvegarder le rapport
        report_file = self.results_dir / \
            f"quality_report_{dataset_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w') as f:
            json.dump(validation_results, f, indent=2, default=str)

        logger.info(f"Validation report saved to {report_file}")

        return validation_results


def main():
    """Script principal pour valider toutes les données"""
    validator = DataQualityValidator()
    raw_dir = Path("data/raw")

    print("🔍 VALIDATION DE LA QUALITÉ DES DONNÉES\n")

    # Valider tous les fichiers latest
    latest_files = list(raw_dir.glob("*_latest.parquet"))

    if not latest_files:
        print("❌ Aucun fichier de données trouvé")
        return

    all_results = []

    for file_path in latest_files:
        print(f"🔎 Validation de {file_path.name}...")
        result = validator.run_full_validation(file_path)
        all_results.append(result)

        if result.get('overall_status') == 'PASSED':
            print(f"  ✅ PASSED - Données de qualité")
        else:
            print(f"  ❌ FAILED - Problèmes détectés")

        # Afficher résumé
        if 'quality_summary' in result:
            summary = result['quality_summary']
            print(
                f"  📊 {summary['record_count']} records, complétude: {summary['completeness']['overall_completeness']}%")

    print(
        f"\n📋 Validation terminée - {len([r for r in all_results if r.get('overall_status') == 'PASSED'])}/{len(all_results)} datasets valides")
    print(f"📁 Rapports sauvés dans: data/quality_reports/")


if __name__ == "__main__":
    main()
