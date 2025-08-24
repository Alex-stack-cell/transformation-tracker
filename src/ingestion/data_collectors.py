# src/ingestion/data_collectors.py
"""
Collecteurs de données depuis les APIs mock
Simule l'ingestion de données comme AWS Lambda/Glue
"""

import requests
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional
from loguru import logger
import time


class DataCollector:
    """Classe de base pour la collecte de données"""

    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
        self.data_dir = Path("data")
        self.raw_dir = self.data_dir / "raw"
        self.processed_dir = self.data_dir / "processed"

        # Créer les dossiers si nécessaire
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.processed_dir.mkdir(parents=True, exist_ok=True)

        logger.add("logs/data_collection.log", rotation="1 day")

    def _make_request(self, endpoint: str, params: Optional[Dict[str, str]] = None) -> Dict[str, object]:
        """Effectue une requête HTTP avec gestion d'erreurs"""
        try:
            url = f"{self.base_url}{endpoint}"
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching {endpoint}: {e}")
            raise

    def collect_initiatives(self) -> pd.DataFrame:
        """Collecte les données d'initiatives"""
        logger.info("Collecting initiatives data...")

        start_time = time.time()
        data = self._make_request("/initiatives")

        # Assurer le typage pour json_normalize
        data_list: list[dict[str, object]] = data if isinstance(data, list) else [
            data]

        # Convertir en DataFrame
        df: pd.DataFrame = pd.json_normalize(data_list)  # type: ignore

        # Nettoyage de base des types
        df['start_date'] = pd.to_datetime(df['start_date'])
        df['target_end_date'] = pd.to_datetime(df['target_end_date'])
        df['budget_allocated'] = df['budget_allocated'].astype(float)
        df['budget_spent'] = df['budget_spent'].astype(float)

        # Métadonnées de collecte
        df['collection_timestamp'] = datetime.now()
        df['data_source'] = 'mock_api'

        # Sauvegarder raw data
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        raw_file = self.raw_dir / f"initiatives_{timestamp}.parquet"
        df.to_parquet(raw_file)

        # Sauvegarder latest
        latest_file = self.raw_dir / "initiatives_latest.parquet"
        df.to_parquet(latest_file)

        execution_time = time.time() - start_time
        logger.info(
            f"Collected {len(df)} initiatives in {execution_time:.2f}s")

        return df

    def collect_financial_metrics(self, days_back: int = 30) -> pd.DataFrame:
        """Collecte les métriques financières"""
        logger.info(f"Collecting financial metrics for {days_back} days...")

        start_time = time.time()
        data = self._make_request(
            "/financial-metrics", {"days_back": str(days_back)})

        df = pd.json_normalize(data)  # type: ignore

        # Nettoyage des types
        df['date'] = pd.to_datetime(df['date'])
        for col in ['revenue_impact', 'cost_reduction', 'roi_percentage',
                    'budget_burn_rate', 'forecast_completion_cost']:
            df[col] = df[col].astype(float)

        df['collection_timestamp'] = datetime.now()
        df['data_source'] = 'mock_api'

        # Sauvegarder
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        raw_file = self.raw_dir / f"financial_metrics_{timestamp}.parquet"
        df.to_parquet(raw_file)

        latest_file = self.raw_dir / "financial_metrics_latest.parquet"
        df.to_parquet(latest_file)

        execution_time = time.time() - start_time
        logger.info(
            f"Collected {len(df)} financial metrics in {execution_time:.2f}s")

        return df

    def collect_operational_metrics(self, days_back: int = 30) -> pd.DataFrame:
        """Collecte les métriques opérationnelles"""
        logger.info(f"Collecting operational metrics for {days_back} days...")

        start_time = time.time()
        data = self._make_request(
            "/operational-metrics", {"days_back": str(days_back)})

        df = pd.json_normalize(data)  # type: ignore

        # Nettoyage des types
        df['date'] = pd.to_datetime(df['date'])
        for col in ['efficiency_gain_percentage', 'process_cycle_time',
                    'quality_score', 'employee_satisfaction', 'customer_satisfaction']:
            df[col] = df[col].astype(float)

        df['collection_timestamp'] = datetime.now()
        df['data_source'] = 'mock_api'

        # Sauvegarder
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        raw_file = self.raw_dir / f"operational_metrics_{timestamp}.parquet"
        df.to_parquet(raw_file)

        latest_file = self.raw_dir / "operational_metrics_latest.parquet"
        df.to_parquet(latest_file)

        execution_time = time.time() - start_time
        logger.info(
            f"Collected {len(df)} operational metrics in {execution_time:.2f}s")

        return df

    def run_full_collection(self) -> Dict[str, pd.DataFrame]:
        """Lance une collecte complète de toutes les sources"""
        logger.info("Starting full data collection...")

        results: Dict[str, pd.DataFrame] = {}

        try:
            # Vérifier que l'API est disponible
            health = self._make_request("/health")
            logger.info(f"API Health: {health}")

            # Collecte des données
            results['initiatives'] = self.collect_initiatives()
            results['financial_metrics'] = self.collect_financial_metrics()
            results['operational_metrics'] = self.collect_operational_metrics()

            # Statistiques finales
            total_records = sum(len(df) for df in results.values())
            logger.info(
                f"Full collection completed: {total_records} total records")

            return results

        except Exception as e:
            logger.error(f"Collection failed: {e}")
            raise


def main():
    """Script principal pour lancer la collecte"""
    collector = DataCollector()

    try:
        # Lancer collecte complète
        data = collector.run_full_collection()

        # Afficher résumé
        print("\n📊 Collection Summary:")
        for source, df in data.items():
            print(f"  {source}: {len(df)} records")

        print("\n✅ Data collection completed successfully!")
        print(f"📁 Data saved in: {collector.raw_dir}")

    except Exception as e:
        print(f"❌ Collection failed: {e}")
        exit(1)


if __name__ == "__main__":
    main()
