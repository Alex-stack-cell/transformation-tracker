"""
Simulation d'APIs pour générer des données de transformation business
Simule le type de données que McKinsey Wave collecterait
"""

from fastapi import FastAPI
from models import InitiativeData, FinancialMetric, OperationalMetric
from typing import List
from datetime import datetime, timedelta
from faker import Faker
import random
from data_generators import generate_initiatives

from typing import Dict
from config import Config

config = Config()

app = FastAPI(title=config.APP_NAME, version=config.APP_VERSION,
              debug=config.DEBUG)
fake = Faker()

# Cache des données pour cohérence
INITIATIVES_CACHE = generate_initiatives()


@app.get("/")
def root():
    return {"message": config.APP_NAME, "version": config.APP_VERSION}


@app.get("/initiatives", response_model=List[InitiativeData])
def get_initiatives():
    """Retourne toutes les initiatives de transformation"""
    return INITIATIVES_CACHE


@app.get("/initiatives/{initiative_id}", response_model=InitiativeData)
def get_initiative(initiative_id: str):
    """Retourne une initiative spécifique"""
    for initiative in INITIATIVES_CACHE:
        if initiative.initiative_id == initiative_id:
            return initiative
    return {"error": "Initiative not found"}


@app.get("/financial-metrics", response_model=List[FinancialMetric])
def get_financial_metrics(days_back: int = 30) -> List[FinancialMetric]:
    """Génère des métriques financières pour les initiatives"""
    metrics: List[FinancialMetric] = []

    for initiative in INITIATIVES_CACHE:
        # Générer des données pour les X derniers jours
        for day_offset in range(days_back):
            date = datetime.now() - timedelta(days=day_offset)

            # Simulation de métriques réalistes
            base_revenue = random.uniform(10000, 100000)
            base_cost_reduction = random.uniform(5000, 50000)

            metric = FinancialMetric(
                initiative_id=initiative.initiative_id,
                date=date,
                revenue_impact=base_revenue * random.uniform(0.8, 1.2),
                cost_reduction=base_cost_reduction * random.uniform(0.7, 1.3),
                # ROI peut être négatif au début
                roi_percentage=random.uniform(-10, 35),
                budget_burn_rate=random.uniform(
                    0.5, 3.0),  # % du budget par semaine
                forecast_completion_cost=initiative.budget_allocated *
                random.uniform(0.9, 1.15)
            )
            metrics.append(metric)

    return metrics


@app.get("/operational-metrics", response_model=List[OperationalMetric])
def get_operational_metrics(days_back: int = 30):
    """Génère des métriques opérationnelles"""
    metrics: List[OperationalMetric] = []

    for initiative in INITIATIVES_CACHE:
        for day_offset in range(days_back):
            date = datetime.now() - timedelta(days=day_offset)

            metric = OperationalMetric(
                initiative_id=initiative.initiative_id,
                date=date,
                efficiency_gain_percentage=random.uniform(0, 25),
                process_cycle_time=random.uniform(1, 48),
                quality_score=random.uniform(70, 98),
                employee_satisfaction=random.uniform(6, 9),
                customer_satisfaction=random.uniform(7, 9.5)
            )
            metrics.append(metric)

    return metrics


@app.get("/health")
def health_check() -> Dict[str, object]:
    """Health check pour monitoring"""
    return {
        "status": "healthy",
        "timestamp": datetime.now(),
        "initiatives_count": len(INITIATIVES_CACHE)
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
